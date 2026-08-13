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
