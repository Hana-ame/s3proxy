package local

// Plugin-level tests; the HTTP adapter tests live in cmd/s3-store.

import (
	"context"
	"strings"
	"testing"

	"s3proxy/internal/store"
)

func TestTraversalRejected(t *testing.T) {
	// Discovery background: found during a code review of the original
	// s3-store, where a PUT "/bucket/../escape" could write outside the
	// data root. The HTTP adapter cannot rely on the net/http path cleaner
	// (it normalizes literal ".." away before the handler sees it), so the
	// plugin itself must reject such keys — this matters in-process, where
	// the tier hands over arbitrary keys.
	s, err := New("local", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, bad := range []string{"b/../escape", "b/./x", "b//x", "/abs", `b\win`, "b/\x00nul"} {
		if _, err := s.Put(ctx, bad, strings.NewReader("x"), 1, "", store.PutOptions{}); err == nil {
			t.Fatalf("key %q accepted", bad)
		}
		if _, err := s.Get(ctx, bad, store.Range{}); err == nil {
			t.Fatalf("key %q readable", bad)
		}
	}
	if err := s.EnsureBucket(ctx, "ok-bucket"); err != nil {
		t.Fatalf("valid bucket rejected: %v", err)
	}
}

// TestEmptyObjectGet — a full GET of a 0-byte object must succeed with an
// empty body.
//
// Discovery background: found during the 2026-08 review. local.Get
// resolved the open-ended range {0,-1} down to end=size-1=-1 and then
// rejected start(0) > end(-1) as an invalid range, so every GET of an
// empty object returned an error that the tier surfaced as 500 — while
// MemStore served it fine. Fix: size==0 returns an empty body before the
// range math.
func TestEmptyObjectGet(t *testing.T) {
	s, err := New("local", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := s.Put(ctx, "b/k", strings.NewReader(""), 0, "application/octet-stream", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, "b/k", store.Range{Start: 0, End: -1})
	if err != nil {
		t.Fatalf("empty-object GET failed: %v", err)
	}
	defer got.Body.Close()
	buf := make([]byte, 4)
	n, err := got.Body.Read(buf)
	if n != 0 || err == nil {
		t.Fatalf("empty body served %d bytes (%v)", n, err)
	}
	if got.Info.Size != 0 {
		t.Fatalf("info size = %d", got.Info.Size)
	}
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	s, err := New("local", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	info, err := s.Put(ctx, "b/k.txt", strings.NewReader("hello world"), 11, "text/plain", store.PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.ETag == "" || info.Size != 11 {
		t.Fatalf("put info: %+v", info)
	}
	got, err := s.Get(ctx, "b/k.txt", store.Range{Start: 6, End: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.Span.Start != 6 || got.Span.End != 10 {
		t.Fatalf("span: %+v", got.Span)
	}
	buf := make([]byte, 16)
	n, _ := got.Body.Read(buf)
	if string(buf[:n]) != "world" {
		t.Fatalf("range read got %q", buf[:n])
	}
	if err := s.Delete(ctx, "b/k.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Head(ctx, "b/k.txt"); err != store.ErrNotFound {
		t.Fatalf("head after delete: %v", err)
	}
}
