package s3store

// Tests the remote S3 plugin against a small in-memory S3-compatible HTTP
// server defined below. This doubles as the contract that real backends
// (MinIO/R2) must satisfy for the proxy to work.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"s3proxy/internal/store"
)

const (
	testAK = "ak-test"
	testSK = "sk-test"
)

// memS3 is a minimal S3 server speaking exactly the subset the plugin uses
// (path-style Put/Get/Head/Delete/ListObjectsV2/CreateBucket/HeadBucket).
type memS3 struct {
	mu   sync.Mutex
	data map[string][]byte
	ubs  map[string]bool
	reqs []string // request log for assertions
}

func (m *memS3) handler(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	m.reqs = append(m.reqs, r.Method+" "+r.URL.String())
	m.mu.Unlock()

	clean := strings.TrimPrefix(r.URL.Path, "/")
	b, rest, _ := strings.Cut(clean, "/")
	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		if rest == "" {
			m.ubs[b] = true
			w.WriteHeader(http.StatusOK)
			return
		}
		m.data[b+"/"+rest] = body
		w.Header().Set("ETag", `"`+fmt.Sprintf("%x", body)+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if rest == "" {
			if !m.ubs[b] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			prefix := r.URL.Query().Get("prefix")
			start := r.URL.Query().Get("continuation-token")
			var keys []string
			// Prefix matches run against the object key without the bucket
			// component, mirroring real S3.
			for k := range m.data {
				objKey := strings.TrimPrefix(k, b+"/")
				if strings.HasPrefix(objKey, prefix) && (start == "" || objKey > start) {
					keys = append(keys, objKey)
				}
			}
			sort.Strings(keys)
			var sb strings.Builder
			sb.WriteString(`<?xml version="1.0"?><ListBucketResult>`)
			for _, objKey := range keys {
				// Recompute the full key for the size lookup; the
				// response echoes the bare object key like real S3.
				sb.WriteString(fmt.Sprintf("<Contents><Key>%s</Key><Size>%d</Size><LastModified>2026-01-01T00:00:00Z</LastModified></Contents>", objKey, len(m.data[b+"/"+objKey])))
			}
			sb.WriteString("<IsTruncated>false</IsTruncated></ListBucketResult>")
			io.WriteString(w, sb.String())
			return
		}
		body, ok := m.data[b+"/"+rest]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Minimal bytes=start-end / bytes=start- range handling: real S3
		// answers 206 + Content-Range, which the plugin relies on for the
		// served span.
		if rng := r.Header.Get("Range"); rng != "" {
			var start, end int
			fmt.Sscanf(strings.TrimPrefix(rng, "bytes="), "%d-%d", &start, &end)
			if end == 0 {
				end = len(body) - 1
			}
			if start < 0 || start >= len(body) {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(body)))
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= len(body) {
				end = len(body) - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
			w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(body[start : end+1])
			return
		}
		w.Write(body)
	case http.MethodHead:
		if rest == "" {
			if m.ubs[b] {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
		if _, ok := m.data[b+"/"+rest]; !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Real S3 always returns Content-Length on HEAD; the plugin takes
		// the object size from it.
		w.Header().Set("Content-Length", fmt.Sprint(len(m.data[b+"/"+rest])))
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		delete(m.data, b+"/"+rest)
		w.WriteHeader(http.StatusNoContent)
	}
}

func newTestStore(t *testing.T, cfg Config) (*Store, *memS3, *httptest.Server) {
	t.Helper()
	backend := &memS3{data: map[string][]byte{}, ubs: map[string]bool{}}
	ts := httptest.NewServer(http.HandlerFunc(backend.handler))
	t.Cleanup(ts.Close)
	cfg.Endpoint = ts.URL
	cfg.AK = testAK
	cfg.SK = testSK
	st, err := New("s3-test", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st, backend, ts
}

func TestS3PutGetHeadDelete(t *testing.T) {
	// Discovery background: first integration of the plugin with a
	// remote; the ResponseContentRange parse (bytes a-b/c) turned out to
	// be the subtle part of ranged GETs.
	st, _, _ := newTestStore(t, Config{Region: "us-east-1", Bucket: "archive"})

	ctx := context.Background()
	if err := st.EnsureBucket(ctx, "mybkt"); err != nil {
		t.Fatal(err)
	}
	info, err := st.Put(ctx, "mybkt/a/b.txt", strings.NewReader("hello world"), 11, "text/plain", store.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag == "" {
		t.Fatal("missing ETag from remote")
	}

	got, err := st.Get(ctx, "mybkt/a/b.txt", store.Range{Start: 6, End: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	body, _ := io.ReadAll(got.Body)
	if string(body) != "world" {
		t.Fatalf("ranged get: %q", body)
	}
	if got.Span.Start != 6 || got.Span.End != 10 {
		t.Fatalf("span: %+v", got.Span)
	}

	head, err := st.Head(ctx, "mybkt/a/b.txt")
	if err != nil {
		t.Fatal(err)
	}
	if head.Size != 11 {
		t.Fatalf("head size %d", head.Size)
	}

	if err := st.Delete(ctx, "mybkt/a/b.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Head(ctx, "mybkt/a/b.txt"); err != store.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestS3PrefixModeLists(t *testing.T) {
	// Prefix mode: every frontend bucket lives under one remote bucket.
	st, backend, _ := newTestStore(t, Config{Region: "us-east-1", Bucket: "archive"})
	ctx := context.Background()
	st.EnsureBucket(ctx, "bktA")
	st.EnsureBucket(ctx, "bktB")
	if _, err := st.Put(ctx, "bktA/one.txt", strings.NewReader("1"), 1, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "bktA/two.txt", strings.NewReader("22"), 2, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "bktB/three.txt", strings.NewReader("333"), 3, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	pg, err := st.List(ctx, "bktA/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pg.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d (%v)", len(pg.Entries), pg.Entries)
	}
	// The remote must have stored everything under the single archive
	// bucket (flat keys), proving prefix-mode mapping.
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for k := range backend.data {
		if !strings.HasPrefix(k, "archive/") {
			t.Fatalf("key %q escaped prefix-mode bucket", k)
		}
	}
}

func TestS3PerBucketMode(t *testing.T) {
	// Per-bucket mode: the remote bucket name equals the frontend bucket.
	// The tier always EnsuresBucket before use (matching the api
	// CreateBucket flow), so this test mirrors that.
	st, _, _ := newTestStore(t, Config{Region: "us-east-1"})
	ctx := context.Background()
	if err := st.EnsureBucket(ctx, "mybkt"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctx, "mybkt/one.txt", strings.NewReader("x"), 1, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Head(ctx, "mybkt/one.txt"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := st.BucketExists(ctx, "mybkt"); !ok {
		t.Fatal("bucket should exist")
	}
}

func TestS3NotFoundMaps(t *testing.T) {
	// Discovery background: without mapping remote 404 codes/NoSuchKey to
	// store.ErrNotFound, the tier's read-through probing treated misses as
	// backend failures and errored instead of healing.
	st, _, _ := newTestStore(t, Config{Region: "us-east-1", Bucket: "archive"})
	ctx := context.Background()
	if _, err := st.Head(ctx, "nope/nothing"); err != store.ErrNotFound {
		t.Fatalf("head: got %v", err)
	}
	if _, err := st.Get(ctx, "nope/nothing", store.Range{}); err != store.ErrNotFound {
		t.Fatalf("get: got %v", err)
	}
	// Delete of a missing key must not error either (tier sweeps every
	// pool unconditionally).
	if err := st.Delete(ctx, "nope/nothing"); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

var _ = time.Now
