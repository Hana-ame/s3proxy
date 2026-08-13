package api

// End-to-end tests through the full S3 frontend: authentication, object
// round-trips, ranges, conditionals, listing, copying, multipart and the
// tier interplay (a migrated cold object must still be served).

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"s3proxy/internal/store"
	"s3proxy/internal/tier"
)

// newTestServer wires an api server over an in-memory hot+cold tier.
func newTestServer(t *testing.T, hotName, coldName string, cfg tier.Config) (*httptest.Server, *Server, *tier.TieredStore) {
	t.Helper()
	hot := store.NewMem(hotName)
	cold := store.NewMem(coldName)
	tr, err := tier.New([]store.Store{hot, cold}, cfg, t.TempDir()+"/objects.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	srv, err := New(tr, testCreds(), "us-east-1", "", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts, srv, tr
}

// signedRequest builds a SigV4-signed request like the AWS SDKs would.
func signedRequest(t *testing.T, method, rawURL string, body []byte, hdr map[string]string) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	r, err := http.NewRequest(method, rawURL, reader)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	payload := unsignedPayload
	if len(body) > 0 {
		payload = sha256Hex(body)
	}
	r.Header.Set("X-Amz-Content-Sha256", payload)
	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK}, r, payload, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatal(err)
	}
	return r
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func doSigned(t *testing.T, method, rawURL string, body []byte, hdr map[string]string) *http.Response {
	t.Helper()
	req := signedRequest(t, method, rawURL, body, hdr)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readAll(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestApiObjectRoundTrip(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})

	resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d %s", resp.StatusCode, readAll(t, resp))
	}

	body := []byte("hello s3 world")
	put := doSigned(t, "PUT", ts.URL+"/bkt/a/b.txt", body, map[string]string{
		"Content-Type":     "text/plain",
		"x-amz-meta-owner": "me",
	})
	if put.StatusCode != http.StatusOK {
		t.Fatalf("put: %d %s", put.StatusCode, readAll(t, put))
	}
	etag := put.Header.Get("ETag")
	put.Body.Close()
	if etag == "" {
		t.Fatal("no ETag on put")
	}

	get := doSigned(t, "GET", ts.URL+"/bkt/a/b.txt", nil, nil)
	if get.StatusCode != http.StatusOK {
		t.Fatalf("get: %d %s", get.StatusCode, readAll(t, get))
	}
	if got := readAll(t, get); got != string(body) {
		t.Fatalf("get body %q", got)
	}
	if get.Header.Get("x-amz-meta-owner") != "me" {
		t.Fatal("user metadata lost")
	}

	head := doSigned(t, "HEAD", ts.URL+"/bkt/a/b.txt", nil, nil)
	if head.StatusCode != http.StatusOK {
		t.Fatalf("head: %d", head.StatusCode)
	}
	if head.Header.Get("Content-Length") != fmt.Sprint(len(body)) {
		t.Fatalf("head content-length %q", head.Header.Get("Content-Length"))
	}
	head.Body.Close()

	// Range request.
	req := signedRequest(t, "GET", ts.URL+"/bkt/a/b.txt", nil, map[string]string{"Range": "bytes=6-10"})
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusPartialContent {
		t.Fatalf("range status %d", res.StatusCode)
	}
	if got := readAll(t, res); got != "s3 wo" {
		t.Fatalf("range body %q", got)
	}

	// Conditional: If-None-Match with matching etag -> 304.
	req = signedRequest(t, "GET", ts.URL+"/bkt/a/b.txt", nil, map[string]string{"If-None-Match": etag})
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusNotModified {
		t.Fatalf("if-none-match: %d", res.StatusCode)
	}

	// 404 semantics.
	missing := doSigned(t, "GET", ts.URL+"/bkt/nope", nil, nil)
	if missing.StatusCode != http.StatusNotFound {
		t.Fatalf("missing: %d", missing.StatusCode)
	}
	if !strings.Contains(readAll(t, missing), "NoSuchKey") {
		t.Fatal("missing object must return NoSuchKey")
	}

	// Delete + NoSuchKey after.
	del := doSigned(t, "DELETE", ts.URL+"/bkt/a/b.txt", nil, nil)
	if del.StatusCode != http.StatusNoContent {
		t.Fatalf("delete: %d", del.StatusCode)
	}
	del.Body.Close()
}

func TestApiAuthentication(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	// Unsigned request must be rejected before reaching storage.
	resp, err := http.Get(ts.URL + "/bkt/key")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("unsigned: %d", resp.StatusCode)
	}
	if !strings.Contains(readAll(t, resp), "SignatureDoesNotMatch") {
		t.Fatal("unsigned must yield SignatureDoesNotMatch")
	}
}

func TestApiPresignedURL(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	doSigned(t, "PUT", ts.URL+"/bkt/f.txt", []byte("file-data"), nil).Body.Close()

	// Presign as the SDK does; the URL alone must fetch the object
	// (browser download scenario).
	req, _ := http.NewRequest("GET", ts.URL+"/bkt/f.txt", nil)
	req.Header.Set("Host", req.URL.Host)
	signer := v4.NewSigner()
	expiry := time.Now().Add(5 * time.Minute)
	expires := int64(expiry.Sub(time.Now()) / time.Second)
	q := req.URL.Query()
	q.Set("X-Amz-Expires", fmt.Sprint(expires))
	req.URL.RawQuery = q.Encode()
	ps, _, err := signer.PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK}, req, unsignedPayload, "s3", "us-east-1", expiry)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Get(ps)
	if err != nil {
		t.Fatal(err)
	}
	if got := readAll(t, resp); got != "file-data" {
		t.Fatalf("presigned get: %q", got)
	}
}

func TestApiListPagination(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	for _, k := range []string{"a.txt", "b.txt", "dir/c.txt", "dir/sub/d.txt"} {
		doSigned(t, "PUT", ts.URL+"/bkt/"+k, []byte(k), nil).Body.Close()
	}

	// V2 with delimiter: one page of 2 (a.txt, b.txt as contents) etc.
	resp := doSigned(t, "GET", ts.URL+"/bkt?list-type=2&delimiter=/", nil, nil)
	body := readAll(t, resp)
	if !strings.Contains(body, "<Key>a.txt</Key>") || !strings.Contains(body, "<Prefix>dir/</Prefix>") {
		t.Fatalf("v2 delimiter list: %s", body)
	}
	if strings.Contains(body, "dir/c.txt") {
		t.Fatal("delimiter must hide grouped keys")
	}

	// Pagination: max-keys=1 returns a continuation token.
	resp = doSigned(t, "GET", ts.URL+"/bkt?list-type=2&max-keys=1", nil, nil)
	body = readAll(t, resp)
	if !strings.Contains(body, "<IsTruncated>true</IsTruncated>") || !strings.Contains(body, "NextContinuationToken") {
		t.Fatalf("paginated list: %s", body)
	}
	var doc struct {
		NextToken string `xml:"NextContinuationToken"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil {
		t.Fatal(err)
	}
	resp = doSigned(t, "GET", ts.URL+"/bkt?list-type=2&max-keys=1&continuation-token="+doc.NextToken, nil, nil)
	body = readAll(t, resp)
	if !strings.Contains(body, "b.txt") {
		t.Fatalf("continuation page: %s", body)
	}

	// V1 with marker.
	resp = doSigned(t, "GET", ts.URL+"/bkt?max-keys=2&marker=b.txt", nil, nil)
	body = readAll(t, resp)
	if !strings.Contains(body, "dir/c.txt") || strings.Contains(body, "a.txt") {
		t.Fatalf("v1 marker list: %s", body)
	}
}

func TestApiCopyObject(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	doSigned(t, "PUT", ts.URL+"/bkt/src.txt", []byte("copy-me"), map[string]string{"Content-Type": "text/plain"}).Body.Close()

	resp := doSigned(t, "PUT", ts.URL+"/bkt/dst.txt", nil, map[string]string{"x-amz-copy-source": "/bkt/src.txt"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("copy: %d %s", resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()
	get := doSigned(t, "GET", ts.URL+"/bkt/dst.txt", nil, nil)
	if got := readAll(t, get); got != "copy-me" {
		t.Fatalf("copied bytes: %q", got)
	}
}

func TestApiMultipart(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()

	// Create upload -> UploadId.
	resp := doSigned(t, "POST", ts.URL+"/bkt/big.bin?uploads", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initiate: %d %s", resp.StatusCode, readAll(t, resp))
	}
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	body := readAll(t, resp)
	if err := xml.Unmarshal([]byte(body), &init); err != nil {
		t.Fatal(err)
	}
	if init.UploadID == "" {
		t.Fatal("no upload id")
	}

	// Two parts (the sizes here are intentionally different).
	part1 := bytes.Repeat([]byte("a"), 1024)
	part2 := bytes.Repeat([]byte("b"), 512)
	p1 := doSigned(t, "PUT", ts.URL+"/bkt/big.bin?uploadId="+init.UploadID+"&partNumber=1", part1, nil)
	if p1.StatusCode != http.StatusOK {
		t.Fatalf("part1: %d %s", p1.StatusCode, readAll(t, p1))
	}
	etag1 := p1.Header.Get("ETag")
	p1.Body.Close()
	p2 := doSigned(t, "PUT", ts.URL+"/bkt/big.bin?uploadId="+init.UploadID+"&partNumber=2", part2, nil)
	if p2.StatusCode != http.StatusOK {
		t.Fatalf("part2: %d %s", p2.StatusCode, readAll(t, p2))
	}
	etag2 := p2.Header.Get("ETag")
	p2.Body.Close()

	// ListParts shows the staged parts.
	resp = doSigned(t, "GET", ts.URL+"/bkt/big.bin?uploadId="+init.UploadID, nil, nil)
	body = readAll(t, resp)
	if !strings.Contains(body, "<PartNumber>1</PartNumber>") || !strings.Contains(body, "<PartNumber>2</PartNumber>") {
		t.Fatalf("list parts: %s", body)
	}

	// Complete; the composite etag must carry the "-2" suffix AWS clients
	// validate for multipart uploads.
	complete := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part><Part><PartNumber>2</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>`, etag1, etag2)
	resp = doSigned(t, "POST", ts.URL+"/bkt/big.bin?uploadId="+init.UploadID, []byte(complete), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete: %d %s", resp.StatusCode, readAll(t, resp))
	}
	var comp struct {
		ETag string `xml:"ETag"`
	}
	body = readAll(t, resp)
	if err := xml.Unmarshal([]byte(body), &comp); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(comp.ETag, `-2"`) {
		t.Fatalf("composite etag %q must end with -2", comp.ETag)
	}

	// The assembled object is both parts in order.
	get := doSigned(t, "GET", ts.URL+"/bkt/big.bin", nil, nil)
	got := readAll(t, get)
	want := string(part1) + string(part2)
	if got != want {
		t.Fatalf("assembled: %d bytes, want %d", len(got), len(want))
	}
}

func TestApiMultipartAbort(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	resp := doSigned(t, "POST", ts.URL+"/bkt/x?uploads", nil, nil)
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(readAll(t, resp)), &init); err != nil {
		t.Fatal(err)
	}
	resp = doSigned(t, "DELETE", ts.URL+"/bkt/x?uploadId="+init.UploadID, nil, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("abort: %d", resp.StatusCode)
	}
	resp.Body.Close()
	// Re-abort of a finished upload -> NoSuchUpload.
	resp = doSigned(t, "DELETE", ts.URL+"/bkt/x?uploadId="+init.UploadID, nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("double abort: %d", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestApiBatchDelete(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	doSigned(t, "PUT", ts.URL+"/bkt/k1", []byte("1"), nil).Body.Close()
	doSigned(t, "PUT", ts.URL+"/bkt/k2", []byte("2"), nil).Body.Close()

	deleteXML := `<Delete><Object><Key>k1</Key></Object><Object><Key>k2</Key></Object></Delete>`
	resp := doSigned(t, "POST", ts.URL+"/bkt?delete", []byte(deleteXML), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("batch delete: %d %s", resp.StatusCode, readAll(t, resp))
	}
	body := readAll(t, resp)
	if !strings.Contains(body, "<Key>k1</Key>") || !strings.Contains(body, "<Key>k2</Key>") {
		t.Fatalf("batch delete result: %s", body)
	}
	for _, k := range []string{"k1", "k2"} {
		r := doSigned(t, "HEAD", ts.URL+"/bkt/"+k, nil, nil)
		if r.StatusCode != http.StatusNotFound {
			t.Fatalf("%s still present", k)
		}
		r.Body.Close()
	}
}

func TestApiServesMigratedColdObject(t *testing.T) {
	// The headline scenario: upload lands in hot, ages, gets drained to
	// cold by the policy loop, and the API still serves it transparently —
	// including its original ETag from the index.
	clk := &clock{t: time.Now()}
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	tr, err := tier.New([]store.Store{hot, cold}, tier.Config{Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour}, t.TempDir()+"/objects.json")
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	tr.SetNow(clk.now)
	srv, err := New(tr, testCreds(), "us-east-1", "", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	defer ts.Close()

	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	put := doSigned(t, "PUT", ts.URL+"/bkt/old.txt", []byte("cold-data"), nil)
	etag := put.Header.Get("ETag")
	put.Body.Close()

	clk.advance(2 * time.Hour)
	tr.RunOnce()

	got := doSigned(t, "GET", ts.URL+"/bkt/old.txt", nil, nil)
	if got.StatusCode != http.StatusOK {
		t.Fatalf("cold get: %d %s", got.StatusCode, readAll(t, got))
	}
	if body := readAll(t, got); body != "cold-data" {
		t.Fatalf("cold body %q", body)
	}
	if got.Header.Get("ETag") != etag {
		t.Fatalf("etag changed across migration: %q vs %q", got.Header.Get("ETag"), etag)
	}
	// List still shows the migrated key.
	resp := doSigned(t, "GET", ts.URL+"/bkt?list-type=2", nil, nil)
	if body := readAll(t, resp); !strings.Contains(body, "old.txt") {
		t.Fatalf("migrated key missing from list: %s", body)
	}
}

// clock is a test time source shared with tier tests.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
