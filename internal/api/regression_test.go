package api

// Regression tests for bugs found during the 2026-08 review. Each test
// documents how the bug was discovered and what fix it protects.

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"s3proxy/internal/store"
	"s3proxy/internal/tier"
)

// TestReviewBadDigestKeepsOldObject — a failed overwrite (wrong Content-MD5)
// must leave the previous object intact, like AWS does.
//
// Discovery background: code review of handlePutObject. The old code wrote
// the new object through the tier FIRST (releasing the old content's
// refcount, sweeping its bytes), then detected the digest mismatch and
// deleted the new mapping — the old object was gone entirely. Fix: the
// tier now verifies Content-MD5 while streaming, BEFORE any refcount
// release, and fails the write without touching the previous object.
func TestReviewBadDigestKeepsOldObject(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	if resp := doSigned(t, "PUT", ts.URL+"/bkt/k", []byte("original-value"), nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("initial put: %d", resp.StatusCode)
	}

	// Overwrite with a body whose Content-MD5 does not match it.
	body := []byte("should-not-commit")
	_ = md5.Sum(body) // real digest computed below for the mismatch test
	wrongMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))
	resp := doSigned(t, "PUT", ts.URL+"/bkt/k", body, map[string]string{"Content-MD5": wrongMD5})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 BadDigest, got %d: %s", resp.StatusCode, readAll(t, resp))
	}
	readAll(t, resp)

	get := doSigned(t, "GET", ts.URL+"/bkt/k", nil, nil)
	if get.StatusCode != http.StatusOK {
		t.Fatalf("old object lost after bad-digest overwrite: %d %s", get.StatusCode, readAll(t, get))
	}
	if got := readAll(t, get); got != "original-value" {
		t.Fatalf("old object corrupted: %q", got)
	}
}

// TestReviewBadDigestNoTierWrite — the digest failure must not leave any
// trace in the tier (no object, no orphaned resource).
//
// Discovery background: companion to TestReviewBadDigestKeepsOldObject,
// written while verifying that fix. The old code's post-hoc cleanup
// (DeleteObject after the fact) could not guarantee a clean tier: a digest
// failure raced an intervening read or overwrite, and the failure path
// itself left the refcount released. The pre-commit check makes this
// invariant structural: nothing is written, so nothing can be left behind.
func TestReviewBadDigestNoTierWrite(t *testing.T) {
	ts, _, tr := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	body := []byte("never-committed")
	wrongMD5 := base64.StdEncoding.EncodeToString(make([]byte, 16))
	resp := doSigned(t, "PUT", ts.URL+"/bkt/k", body, map[string]string{"Content-MD5": wrongMD5})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
	readAll(t, resp)
	if _, err := tr.HeadObject(t.Context(), "bkt", "k"); err != store.ErrNotFound {
		t.Fatalf("failed put left an object behind: %v", err)
	}
}

// TestReviewStaleUploadCleanup — staged multipart uploads older than the
// staleness bound are removed (manifest + parts), newer ones survive.
//
// Discovery background: code review of multipart.go — the staging area had
// no lifecycle, so an interrupted upload left its parts on disk forever
// (unbounded disk growth / DoS). Fix: staleUploadTTL + uploadStore.
// cleanupStale(), invoked on upload initiation and part upload.
func TestReviewStaleUploadCleanup(t *testing.T) {
	ts, srv, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})

	// Old upload whose parts must be purged.
	old := &uploadMeta{
		UploadID:  "old-stale",
		Bucket:    "bkt",
		Key:       "k",
		Initiated: time.Now().Add(-25 * time.Hour),
		Parts:     []partMeta{{PartNumber: 1, Size: 4, MD5: []byte("abcd")}},
	}
	if err := os.MkdirAll(srv.uploads.partDir(old.UploadID), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srv.uploads.manifestPath(old.UploadID), mustJSON(t, old), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srv.uploads.partDir(old.UploadID), "1.bin"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Fresh upload that must survive.
	fresh := &uploadMeta{
		UploadID:  "fresh",
		Bucket:    "bkt",
		Key:       "k",
		Initiated: time.Now().Add(-time.Hour),
	}
	if err := os.MkdirAll(srv.uploads.partDir(fresh.UploadID), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(srv.uploads.manifestPath(fresh.UploadID), mustJSON(t, fresh), 0o644); err != nil {
		t.Fatal(err)
	}

	// Trigger cleanup through a public entry point.
	resp := doSigned(t, "POST", ts.URL+"/bkt/k?uploads", nil, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("initiate multipart: %d", resp.StatusCode)
	}
	readAll(t, resp)

	if _, err := os.Stat(srv.uploads.manifestPath(old.UploadID)); !os.IsNotExist(err) {
		t.Fatalf("stale manifest survived: %v", err)
	}
	if _, err := os.Stat(srv.uploads.partDir(old.UploadID)); !os.IsNotExist(err) {
		t.Fatalf("stale part dir survived: %v", err)
	}
	if _, err := os.Stat(srv.uploads.manifestPath(fresh.UploadID)); err != nil {
		t.Fatalf("fresh upload was purged: %v", err)
	}
}

// TestReviewCorrectContentMD5Accepted — a PUT carrying a CORRECT
// Content-MD5 must succeed and store the object.
//
// Discovery background: code review + a throwaway verification test. The
// tier compared the client's MD5 hex (32 chars) against its own SHA-256
// content id (64 chars) — two hex strings of different lengths that can
// never be equal, so every upload with Content-MD5 was rejected with 400
// BadDigest, even when the digest was right. The existing BadDigest tests
// only exercised the mismatch branch, so the bug shipped green. Fix: the
// tier now computes the MD5 of the streamed bytes in parallel and compares
// against that.
func TestReviewCorrectContentMD5Accepted(t *testing.T) {
	ts, _, tr := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	body := []byte("hello world")
	sum := md5.Sum(body)
	good := base64.StdEncoding.EncodeToString(sum[:])
	resp := doSigned(t, "PUT", ts.URL+"/bkt/k", body, map[string]string{"Content-MD5": good})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CORRECT Content-MD5 rejected: %d %s", resp.StatusCode, readAll(t, resp))
	}
	readAll(t, resp)
	e, _, err := tr.GetObject(t.Context(), "bkt", "k", store.Range{Start: 0, End: -1})
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(e.Body)
	e.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(body) {
		t.Fatalf("stored bytes mismatch: %q", b)
	}
}

// TestReviewCorrectPartMD5Accepted — a part upload carrying a CORRECT
// Content-MD5 must succeed.
//
// Discovery background: same review as TestReviewCorrectContentMD5Accepted,
// different encoding bug: handleUploadPart compared the hex part digest
// against the RAW base64 header (hex is 32 chars, base64 24) — never equal,
// so any SDK that sends Content-MD5 on parts (boto3/aws-cli large uploads)
// always got BadDigest. Fix: base64-decode the header before comparing,
// mirroring the plain-PUT path.
func TestReviewCorrectPartMD5Accepted(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	init := doSigned(t, "POST", ts.URL+"/bkt/k?uploads", nil, nil)
	var i struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(readAll(t, init)), &i); err != nil {
		t.Fatal(err)
	}
	if i.UploadID == "" {
		t.Fatal("no upload id")
	}
	part := []byte("part-one")
	sum := md5.Sum(part)
	good := base64.StdEncoding.EncodeToString(sum[:])
	resp := doSigned(t, "PUT", ts.URL+"/bkt/k?partNumber=1&uploadId="+i.UploadID, part, map[string]string{"Content-MD5": good})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CORRECT part Content-MD5 rejected: %d %s", resp.StatusCode, readAll(t, resp))
	}
	readAll(t, resp)
}

// TestReviewListUploadsScopedToBucket — GET /bucket?uploads must only list
// that bucket's in-flight uploads.
//
// Discovery background: code review of handleListMultipartUploads — the
// response iterated the whole staging dir with no bucket filter, so every
// tenant's in-flight upload appeared in every bucket's listing. Fix: skip
// manifests whose bucket differs.
func TestReviewListUploadsScopedToBucket(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	for _, b := range []string{"bkt-a", "bkt-b"} {
		if resp := doSigned(t, "PUT", ts.URL+"/"+b, nil, nil); resp.StatusCode != http.StatusOK {
			t.Fatalf("create %s: %d", b, resp.StatusCode)
		}
	}
	var ids []string
	for _, b := range []string{"bkt-a", "bkt-b"} {
		resp := doSigned(t, "POST", ts.URL+"/"+b+"/k?uploads", nil, nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("initiate %s: %d", b, resp.StatusCode)
		}
		var out struct {
			UploadID string `xml:"UploadId"`
		}
		if err := xml.Unmarshal([]byte(readAll(t, resp)), &out); err != nil || out.UploadID == "" {
			t.Fatalf("initiate %s: no upload id: %v", b, err)
		}
		ids = append(ids, out.UploadID)
	}

	list := doSigned(t, "GET", ts.URL+"/bkt-a?uploads", nil, nil)
	if list.StatusCode != http.StatusOK {
		t.Fatalf("list bkt-a: %d", list.StatusCode)
	}
	body := readAll(t, list)
	if !strings.Contains(body, ids[0]) {
		t.Fatalf("bkt-a listing misses its own upload")
	}
	if strings.Contains(body, ids[1]) {
		t.Fatalf("bkt-a listing leaked bkt-b's upload into its uploads: %s", body)
	}
}

// TestReviewEmptySegmentKeys — keys with empty segments ("dir//file") and
// trailing slashes ("dir/") are legal S3 keys and must round-trip verbatim.
//
// Discovery background: code review of parseTarget — joinSegments skipped
// empty segments, rewriting "dir//file" into "dir/file" and stripping
// trailing slashes, so those objects were unreachable under their real
// keys. Fix: empty segments are kept and joined back.
func TestReviewEmptySegmentKeys(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	for _, key := range []string{"dir//file", "dir/"} {
		resp := doSigned(t, "PUT", ts.URL+"/bkt/"+key, []byte(key), nil)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("PUT %q: %d %s", key, resp.StatusCode, readAll(t, resp))
		}
		readAll(t, resp)
		get := doSigned(t, "GET", ts.URL+"/bkt/"+key, nil, nil)
		if get.StatusCode != http.StatusOK {
			t.Fatalf("GET %q: %d %s", key, get.StatusCode, readAll(t, get))
		}
		if got := readAll(t, get); got != key {
			t.Fatalf("GET %q returned %q", key, got)
		}
	}
}

// TestReviewReversedRangeReturns416 — "bytes=5-2" (end before start) must
// be answered 416 InvalidRange like AWS, with a bytes */N Content-Range —
// not 200 with the entire object.
//
// Discovery background: 2026-08 review of parseRange — the end<start
// branch returned ok=false, and handleGetObject only sends 416 when the
// header actually parsed as a range (rngErr && isRange), so this case fell
// through to a full 200. Inconsistent with the start>=size case, which
// already answered 416. Fix: the branch reports the header as a range.
func TestReviewReversedRangeReturns416(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	if resp := doSigned(t, "PUT", ts.URL+"/bkt/k", []byte("0123456789"), nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("put: %d", resp.StatusCode)
	}
	resp := doSigned(t, "GET", ts.URL+"/bkt/k", nil, map[string]string{"Range": "bytes=5-2"})
	readAll(t, resp)
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("reversed range status = %d, want 416", resp.StatusCode)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "bytes */10" {
		t.Fatalf("Content-Range = %q, want bytes */10", cr)
	}
}

// TestReviewUploadIDTraversalRejected — any uploadId that is not the
// 32-hex shape newUploadID generates (path traversal like "../../escape")
// must be refused with NoSuchUpload before a filesystem path is built from
// it, on every uploadId route.
//
// Discovery background: 2026-08 review of multipart.go — uploadId was
// joined raw into filepath.Join (part dir / manifest path), so a crafted
// id could read manifests and write part files outside the staging area
// (fully exploitable when a JSON manifest readable at the escaped location
// matches the request's bucket/key). Fix: validUploadID gate at the top of
// serveObject.
func TestReviewUploadIDTraversalRejected(t *testing.T) {
	ts, srv, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	esc := "../../escape"
	cases := [][2]string{
		{"GET", "/bkt/k?uploadId=" + esc},
		{"PUT", "/bkt/k?partNumber=1&uploadId=" + esc},
		{"DELETE", "/bkt/k?uploadId=" + esc},
		{"POST", "/bkt/k?uploadId=" + esc},
	}
	for _, c := range cases {
		resp := doSigned(t, c[0], ts.URL+c[1], nil, nil)
		readAll(t, resp)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s %s: status %d, want 404", c[0], c[1], resp.StatusCode)
		}
	}
	// Nothing may have been created outside the staging dirs.
	for _, p := range []string{
		filepath.Join(srv.uploads.root, "escape.json"),
		filepath.Join(srv.uploads.root, "parts", "escape"),
	} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("traversal left state at %s: %v", p, err)
		}
	}
}

// TestReviewBadPartMD5KeepsOldPart — re-uploading a part number with a
// wrong Content-MD5 must leave the previously committed part intact:
// Complete must later succeed with the ORIGINAL bytes.
//
// Discovery background: 2026-08 review of handleUploadPart — the digest
// check ran AFTER os.Rename(tmp, partPath), and failure removed partPath:
// when the part number already existed, that destroyed the OLD part's .bin
// while the manifest still referenced it, so Complete failed with "staged
// part lost". Fix: digest verified before the rename; failures remove only
// the temp file.
func TestReviewBadPartMD5KeepsOldPart(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	if resp := doSigned(t, "PUT", ts.URL+"/bkt", nil, nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("create bucket: %d", resp.StatusCode)
	}
	init := doSigned(t, "POST", ts.URL+"/bkt/k?uploads", nil, nil)
	var i struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(readAll(t, init)), &i); err != nil {
		t.Fatal(err)
	}

	part := []byte("good-part-bytes")
	sum := md5.Sum(part)
	good := base64.StdEncoding.EncodeToString(sum[:])
	resp := doSigned(t, "PUT", ts.URL+"/bkt/k?partNumber=1&uploadId="+i.UploadID, part, map[string]string{"Content-MD5": good})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("first part: %d %s", resp.StatusCode, readAll(t, resp))
	}
	readAll(t, resp)

	// Re-upload part 1 with a WRONG digest.
	bad := base64.StdEncoding.EncodeToString(make([]byte, 16))
	resp = doSigned(t, "PUT", ts.URL+"/bkt/k?partNumber=1&uploadId="+i.UploadID, []byte("replacement"), map[string]string{"Content-MD5": bad})
	readAll(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad part digest: status %d, want 400", resp.StatusCode)
	}

	// Complete with the ORIGINAL part's etag must succeed and produce the
	// original bytes (the rejected part must not have replaced it).
	completeXML := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`,
		hex.EncodeToString(sum[:]))
	resp = doSigned(t, "POST", ts.URL+"/bkt/k?uploadId="+i.UploadID, []byte(completeXML), nil)
	body := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("complete after rejected part: %d %s", resp.StatusCode, body)
	}
	get := doSigned(t, "GET", ts.URL+"/bkt/k", nil, nil)
	if got := readAll(t, get); got != string(part) {
		t.Fatalf("object = %q, want original part bytes", got)
	}
}

// TestReviewCorruptManifestCleaned — an unreadable upload manifest must be
// removed together with its part dir by the stale sweep, not linger as a
// permanent tombstone that every sweep re-discovers and re-removes.
//
// Discovery background: 2026-08 review of cleanupStale — the
// unreadable-manifest branch removed only the part dir; the .json stayed,
// so the id was re-scanned and the (already gone) dir re-removed forever.
// Fix: the manifest file is removed too.
func TestReviewCorruptManifestCleaned(t *testing.T) {
	u, err := newUploadStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("a", 32)
	if err := os.WriteFile(u.manifestPath(id), []byte("not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(u.partDir(id), 0o755); err != nil {
		t.Fatal(err)
	}
	u.cleanupStale(time.Now())
	if _, err := os.Stat(u.manifestPath(id)); !os.IsNotExist(err) {
		t.Fatalf("corrupt manifest survived: %v", err)
	}
	if _, err := os.Stat(u.partDir(id)); !os.IsNotExist(err) {
		t.Fatalf("corrupt part dir survived: %v", err)
	}
}

// TestReviewUploadLocksReclaimed — the per-upload lock table must not
// retain one mutex per upload id ever touched.
//
// Discovery background: 2026-08 review of uploadStore — locks was a
// map[string]*sync.Mutex whose entries were never removed: upload ids are
// random 32-hex, so over months of operation the map grew monotonically by
// one mutex per historically initiated upload (the same unbounded-memory
// pattern the tier lockTable was written to fix). Fix: refcounted
// uploadLockTable with refs-before-block ordering.
func TestReviewUploadLocksReclaimed(t *testing.T) {
	u, err := newUploadStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		unlock := u.lock(fmt.Sprintf("%032d", i))
		unlock()
	}
	u.mu.Lock()
	n := len(u.locks.locks)
	u.mu.Unlock()
	if n >= 10 {
		t.Fatalf("lock table retained %d idle entries", n)
	}
	// Re-locking a name must not resurrect stale entries either.
	unlock := u.lock("00000000000000000000000000000000")
	unlock()
	u.mu.Lock()
	n = len(u.locks.locks)
	u.mu.Unlock()
	if n != 0 {
		t.Fatalf("re-lock left %d entries, want 0 (reclaimed again)", n)
	}
}

// mustJSON serializes m for test fixtures.
func mustJSON(t *testing.T, m *uploadMeta) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestReviewVhostResolvesWithPort — virtual-host requests whose Host header
// carries a port must still resolve the bucket.
//
// Discovery background: 2026-08 review of vhostBucket — the suffix match
// ran against the raw r.Host, and any non-default port
// ("bkt.s3.example.com:9000") broke it: the request silently fell back to
// path-style parsing, the bucket was taken from the URL path, and the
// signature (canonical host header signed over the vhost name WITH port)
// could never match — every real virtual-host request through a port got
// 403 SignatureDoesNotMatch. Fix: strip the port before matching, on both
// sides of the comparison.
func TestReviewVhostResolvesWithPort(t *testing.T) {
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	tr, err := tier.New([]store.Store{hot, cold}, tier.Config{Hot: "hot", Cold: []string{"cold"}}, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	srv, err := New(tr, testCreds(), "us-east-1", "s3.example.com", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Sign against the virtual host INCLUDING the port. The SDK signer
	// uses req.Host for the canonical host header and the delivered
	// request carries the same Host — exactly what a client pointed at
	// http://bkt.s3.example.com:9000 sends (the URL host only routes the
	// connection to the test server).
	do := func(method, path string, body []byte) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(string(body)))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "bkt.s3.example.com:9000"
		payload := unsignedPayload
		if len(body) > 0 {
			payload = sha256Hex(body)
		}
		req.Header.Set("X-Amz-Content-Sha256", payload)
		signer := v4.NewSigner()
		if err := signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK}, req, payload, "s3", "us-east-1", time.Now()); err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	if resp := do("PUT", "/", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("vhost create bucket: %d %s", resp.StatusCode, readAll(t, resp))
	} else {
		readAll(t, resp)
	}
	if resp := do("PUT", "/key", []byte("vhost-data")); resp.StatusCode != http.StatusOK {
		t.Fatalf("vhost put: %d %s", resp.StatusCode, readAll(t, resp))
	} else {
		readAll(t, resp)
	}
	get := do("GET", "/key", nil)
	if get.StatusCode != http.StatusOK {
		t.Fatalf("vhost get: %d %s", get.StatusCode, readAll(t, get))
	}
	if got := readAll(t, get); got != "vhost-data" {
		t.Fatalf("vhost round-trip got %q", got)
	}
}

// TestReviewMaxKeysZeroEmptyPage — max-keys=0 is a valid AWS page size.
//
// Discovery background: 2026-08 review — tier.ListObjects collapsed
// MaxKeys<=0 to 1000, so a `?max-keys=0` probe (tooling tests emptiness
// this way) returned the whole bucket. Fix: 0 emits an empty page that
// stays IsTruncated when keys match, and the continuation token advances
// past the scan position so the follow-up request does not loop on empty
// pages.
func TestReviewMaxKeysZeroEmptyPage(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	for _, k := range []string{"a.txt", "b.txt"} {
		doSigned(t, "PUT", ts.URL+"/bkt/"+k, []byte(k), nil).Body.Close()
	}
	resp := doSigned(t, "GET", ts.URL+"/bkt?list-type=2&max-keys=0", nil, nil)
	body := readAll(t, resp)
	if strings.Contains(body, "<Key>") {
		t.Fatalf("max-keys=0 must not emit keys: %s", body)
	}
	if !strings.Contains(body, "<KeyCount>0</KeyCount>") || !strings.Contains(body, "<IsTruncated>true</IsTruncated>") {
		t.Fatalf("max-keys=0 must report a truncated empty page: %s", body)
	}
	var doc struct {
		NextToken string `xml:"NextContinuationToken"`
	}
	if err := xml.Unmarshal([]byte(body), &doc); err != nil || doc.NextToken == "" {
		t.Fatalf("max-keys=0 must return an advancing token: %q err=%v", doc.NextToken, err)
	}
	// The token resumes after the scanned position; the remaining keys
	// come back on the next page.
	resp = doSigned(t, "GET", ts.URL+"/bkt?list-type=2&max-keys=10&continuation-token="+doc.NextToken, nil, nil)
	if body = readAll(t, resp); !strings.Contains(body, "<Key>b.txt</Key>") {
		t.Fatalf("continuation after max-keys=0 page: %s", body)
	}
}

// TestReviewMultipartUploadScope — ListParts/Complete against a URL
// bucket/key different from the upload's own must be NoSuchUpload, never
// an echo of another upload's parts or a write under the URL key.
//
// Discovery background: 2026-08 review — handleListParts served the
// manifest's parts under any URL key, and handleCompleteMultipart ignored
// the URL bucket/key entirely: a POST against a different key completed
// the upload and wrote the assembled object there. AWS scopes an upload
// to the bucket/key it was initiated for.
func TestReviewMultipartUploadScope(t *testing.T) {
	ts, _, _ := newTestServer(t, "hot", "cold", tier.Config{Hot: "hot", Cold: []string{"cold"}})
	doSigned(t, "PUT", ts.URL+"/bkt", nil, nil).Body.Close()
	resp := doSigned(t, "POST", ts.URL+"/bkt/real.txt?uploads", nil, nil)
	var init struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal([]byte(readAll(t, resp)), &init); err != nil {
		t.Fatal(err)
	}
	// ListParts through another key.
	resp = doSigned(t, "GET", ts.URL+"/bkt/other.txt?uploadId="+init.UploadID, nil, nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("list-parts under foreign key: %d %s", resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()
	// Complete through another key.
	resp = doSigned(t, "POST", ts.URL+"/bkt/other.txt?uploadId="+init.UploadID, []byte("<CompleteMultipartUpload></CompleteMultipartUpload>"), nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("complete under foreign key: %d %s", resp.StatusCode, readAll(t, resp))
	}
	resp.Body.Close()
	// Nothing was written under either key.
	for _, k := range []string{"real.txt", "other.txt"} {
		resp = doSigned(t, "GET", ts.URL+"/bkt/"+k, nil, nil)
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s exists after scoped rejection: %d", k, resp.StatusCode)
		}
		resp.Body.Close()
	}
}
