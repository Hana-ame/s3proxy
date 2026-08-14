package store

import (
	"context"
	"io"
	"strings"
	"testing"
)

// TestReviewChunkedPutUnknownSize — a Put with size<0 (chunked upload)
// must store the full body.
//
// Discovery background: 2026-08 review of MemStore.Put —
// LimitReader(r, size+1) with size=-1 produces an empty body, so every
// chunked PUT silently stored 0 bytes while the tier trusted the
// advertised size and the pool index reported it (readers got nothing).
// Fix: read to EOF for size<0, mirroring the local pool.
func TestReviewChunkedPutUnknownSize(t *testing.T) {
	m := NewMem("m")
	info, err := m.Put(context.Background(), "bkt/k", strings.NewReader("chunked-body"), -1, "application/octet-stream", PutOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != int64(len("chunked-body")) {
		t.Fatalf("size %d, want %d", info.Size, len("chunked-body"))
	}
	got, err := m.Get(context.Background(), "bkt/k", Range{Start: 0, End: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	b, err := io.ReadAll(got.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "chunked-body" {
		t.Fatalf("body %q", b)
	}
}
