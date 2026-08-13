package api

// SigV4 verification tests, ported from the original cmd/s3-proxy suite
// when the frontend moved into the api package.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

const (
	testAK = "test-access-key"
	testSK = "test-secret-key"
)

func testCreds() map[string]string {
	return map[string]string{testAK: testSK}
}

func signRequest(t *testing.T, r *http.Request, payloadHash string) *http.Request {
	t.Helper()
	signer := v4.NewSigner()
	if err := signer.SignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK}, r, payloadHash, "s3", "us-east-1", time.Now()); err != nil {
		t.Fatalf("sign: %v", err)
	}
	return r
}

func newTestRequest(method, rawURL string, body string) *http.Request {
	r := httptest.NewRequest(method, rawURL, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "text/plain")
	}
	return r
}

func TestVerifyHeaderSig(t *testing.T) {
	r := newTestRequest("GET", "http://s3.example.com/bucket/key.txt", "")
	r = signRequest(t, r, unsignedPayload)

	ak, ok := verifySigV4(r, testCreds(), time.Now())
	if !ok || ak != testAK {
		t.Fatalf("expected valid signature for %s, got ok=%v ak=%q", testAK, ok, ak)
	}

	// Tampered path must fail.
	bad := newTestRequest("GET", "http://s3.example.com/bucket/other.txt", "")
	bad = signRequest(t, bad, unsignedPayload)
	bad.URL.Path = "/bucket/key.txt"
	if _, ok := verifySigV4(bad, testCreds(), time.Now()); ok {
		t.Fatal("tampered request passed verification")
	}

	// Unknown access key must fail (credential lifetime / rotation check).
	r2 := newTestRequest("GET", "http://s3.example.com/bucket/key.txt", "")
	r2 = signRequest(t, r2, unsignedPayload)
	if _, ok := verifySigV4(r2, map[string]string{"nope": "sk"}, time.Now()); ok {
		t.Fatal("unknown credential passed verification")
	}

	// Expired (beyond skew window) must fail.
	old := newTestRequest("GET", "http://s3.example.com/bucket/key.txt", "")
	old = signRequest(t, old, unsignedPayload)
	old.Header.Set("X-Amz-Date", time.Now().Add(-2*time.Hour).UTC().Format("20060102T150405Z"))
	if _, ok := verifySigV4(old, testCreds(), time.Now()); ok {
		t.Fatal("expired request passed verification")
	}
}

func TestVerifyPresignedSig(t *testing.T) {
	signer := v4.NewSigner()
	presign := func(expiry time.Time) string {
		t.Helper()
		req := newTestRequest("GET", "http://s3.example.com/bucket/key.txt", "")
		expires := int64(expiry.Sub(time.Now()) / time.Second)
		q := req.URL.Query()
		q.Set("X-Amz-Expires", fmt.Sprint(expires))
		req.URL.RawQuery = q.Encode()
		ps, _, err := signer.PresignHTTP(context.Background(), aws.Credentials{AccessKeyID: testAK, SecretAccessKey: testSK}, req, unsignedPayload, "s3", "us-east-1", expiry)
		if err != nil {
			t.Fatalf("presign: %v", err)
		}
		return ps
	}

	presigned, err := http.NewRequest("GET", presign(time.Now().Add(5*time.Minute)), nil)
	if err != nil {
		t.Fatal(err)
	}
	presigned.Host = "s3.example.com"
	if _, ok := verifySigV4(presigned, testCreds(), time.Now()); !ok {
		t.Fatal("valid presigned URL failed verification")
	}

	expired, _ := http.NewRequest("GET", presign(time.Now().Add(-1*time.Hour)), nil)
	expired.Host = "s3.example.com"
	if _, ok := verifySigV4(expired, testCreds(), time.Now()); ok {
		t.Fatal("expired presigned URL passed verification")
	}
}

func TestVerifyPayloadHash(t *testing.T) {
	body := "hello world"
	hash := sha256.Sum256([]byte(body))
	r := newTestRequest("PUT", "http://s3.example.com/bucket/key.txt", body)
	r.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(hash[:]))
	r = signRequest(t, r, hex.EncodeToString(hash[:]))

	if _, ok := verifySigV4(r, testCreds(), time.Now()); !ok {
		t.Fatal("valid signed PUT failed verification")
	}
}

func TestCanonicalQueryEncoding(t *testing.T) {
	u, _ := url.Parse("http://s3.example.com/?a=1&b=hello%20world&a=2")
	q := canonicalQuery(u)
	if !strings.Contains(q, "a=1") || !strings.Contains(q, "a=2") || !strings.Contains(q, "b=hello%20world") {
		t.Fatalf("canonical query mismatch: %q", q)
	}
	if strings.Index(q, "a=1") > strings.Index(q, "a=2") {
		t.Fatal("canonical query not sorted")
	}
}
