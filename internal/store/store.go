// Package store defines the storage-plugin interface implemented by every
// backend (local filesystem, remote S3-compatible service, in-memory test
// double). Plugins hold object bytes only; placement/tier decisions and
// metadata truth live in the tier package one level up.
package store

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// md5sum is the local-store ETag convention: bare hex of the object bytes,
// quoted by the caller when it surfaces the ETag header.
func md5sum(b []byte) []byte {
	h := md5.Sum(b)
	return h[:]
}

// ErrNotFound is returned by plugin methods when the requested bucket or
// object does not exist in that backend. The tier layer translates it into
// S3 404 semantics and uses it to trigger read-through probing of other pools.
var ErrNotFound = errors.New("store: not found")

// ErrBucketExists is returned when creating a bucket that already exists.
var ErrBucketExists = errors.New("store: bucket already exists")

// ErrNotEmpty is returned when deleting a bucket that still holds objects
// (S3 semantics: DeleteBucket fails with BucketNotEmpty unless empty).
var ErrNotEmpty = errors.New("store: bucket not empty")

// BucketInfo describes one bucket for ListBuckets.
type BucketInfo struct {
	Name    string
	Created time.Time
}

// ObjectInfo is the metadata a plugin knows about one stored object.
type ObjectInfo struct {
	Key          string    // object key, "bucket/key" within the plugin
	Size         int64     // total object size in bytes
	ETag         string    // backend-computed entity tag (quoted)
	ContentType  string    // stored Content-Type, "" means application/octet-stream
	LastModified time.Time // server-side modification time
	StorageClass string    // e.g. "STANDARD"; "" means STANDARD
}

// ListEntry is one object row returned by List.
type ListEntry = ObjectInfo

// ListOutput is a page of a flat list-keys response from a plugin.
// Plugins never group by delimiter — delimiter grouping is done by the tier
// layer, which owns the authoritative key index.
type ListOutput struct {
	Entries     []ListEntry
	IsTruncated bool
	NextToken   string // opaque; pass back as startAfter to continue paging
}

// Range identifies a byte span to fetch. Start <= End (inclusive). For an
// open-ended range (bytes=N-), End = -1; the plugin decides the real end from
// object size.
type Range struct {
	Start int64
	End   int64 // -1 = to EOF
}

// GetResult carries a streamed object body plus the metadata of the full
// object and the byte span actually served.
type GetResult struct {
	Body io.ReadCloser
	Info ObjectInfo
	Span Range // resolved span; {0, size-1} for a full read
}

// Store is the storage-plugin contract. All methods are safe for concurrent
// use, and every object key is expected to be already validated
// (no empty segments, "..", etc.) by the caller.
type Store interface {
	// Name returns the configured pool name; used by the tier index to
	// remember which pool physically holds each object.
	Name() string

	// Put streams exactly size bytes into key, replacing any previous
	// object. The returned ObjectInfo must be filled with ETag, Size,
	// ContentType, LastModified (and StorageClass if non-standard).
	Put(ctx context.Context, key string, r io.Reader, size int64, contentType string, opts PutOptions) (ObjectInfo, error)

	// Get streams the requested byte span of key. Returns ErrNotFound when
	// the object (or bucket) is absent. The backend resolves open-ended
	// ranges itself.
	Get(ctx context.Context, key string, rng Range) (GetResult, error)

	// Head returns metadata of key without a body. ErrNotFound when absent.
	Head(ctx context.Context, key string) (ObjectInfo, error)

	// Delete removes key. Deleting a missing object is not an error, so the
	// tier layer can sweep every pool unconditionally.
	Delete(ctx context.Context, key string) error

	// List pages keys starting with keyPrefix (e.g. "bucket" or
	// "bucket/sub/"), sorted by key. startAfter resumes a previous page
	// (pass NextToken). maxKeys caps entries per page.
	List(ctx context.Context, keyPrefix string, startAfter string, maxKeys int) (ListOutput, error)

	// EnsureBucket creates the bucket if needed (idempotent).
	EnsureBucket(ctx context.Context, bucket string) error

	// Buckets lists all buckets physically present in this backend.
	Buckets(ctx context.Context) ([]string, error)

	// BucketExists reports whether the bucket exists.
	BucketExists(ctx context.Context, bucket string) (bool, error)

	// Close releases plugin resources.
	Close() error
}

// Renamer is an optional Store extension: renames an object within the
// backend. The tier layer uses it to finalize content-addressed writes
// (temporary key -> sha256 name) without copying bytes. Plugins without
// Renamer fall back to copy+delete.
type Renamer interface {
	Rename(ctx context.Context, fromKey, toKey string) error
}

// PutOptions carries optional per-object hints.
type PutOptions struct {
	// StorageClass requested by the client ("" = backend default STANDARD).
	StorageClass string
	// ETag the caller computed (multipart composite) that the frontend will
	// report; plugins keep their own ETag but may use this for Content-MD5
	// integrity if it matches the byte content (it doesn't for composites).
	ETag string
	// Metadata copied verbatim to the backend response headers if supported.
	RequestedMetadata map[string]string
}

// MemStore is an in-memory plugin used by unit tests and as a fastest
// possible hot pool. Not for production persistence.
type MemStore struct {
	name string
	mu   sync.Mutex
	data map[string][]byte // "bucket/key" -> bytes
	meta map[string]ObjectInfo
	ubs  map[string]struct{} // buckets
}

// NewMem returns a MemStore named name.
func NewMem(name string) *MemStore {
	return &MemStore{
		name: name,
		data: make(map[string][]byte),
		meta: make(map[string]ObjectInfo),
		ubs:  make(map[string]struct{}),
	}
}

func (m *MemStore) Name() string { return m.name }

func (m *MemStore) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string, opts PutOptions) (ObjectInfo, error) {
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return ObjectInfo{}, err
	}
	if int64(len(data)) != size {
		return ObjectInfo{}, io.ErrUnexpectedEOF
	}
	bucket, _, _ := strings.Cut(key, "/")
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = data
	info := ObjectInfo{
		Key:          key,
		Size:         size,
		ETag:         `"` + hex.EncodeToString(md5sum(data)) + `"`,
		ContentType:  contentType,
		LastModified: time.Now(),
		StorageClass: opts.StorageClass,
	}
	// A caller-supplied ETag (multipart composite, rehydration) is
	// authoritative; the store's own byte-md5 would not match what clients
	// expect for those objects.
	if opts.ETag != "" {
		info.ETag = opts.ETag
	}
	if info.StorageClass == "" {
		info.StorageClass = "STANDARD"
	}
	m.meta[key] = info
	m.ubs[bucket] = struct{}{}
	return info, nil
}

func (m *MemStore) Get(ctx context.Context, key string, rng Range) (GetResult, error) {
	m.mu.Lock()
	data, ok := m.data[key]
	info := m.meta[key]
	m.mu.Unlock()
	if !ok {
		return GetResult{}, ErrNotFound
	}
	if rng.Start < 0 || rng.Start > int64(len(data)) {
		return GetResult{}, ErrNotFound
	}
	end := rng.End
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	if rng.End >= 0 && rng.End < rng.Start {
		return GetResult{}, ErrNotFound
	}
	body := io.NopCloser(bytes.NewReader(data[rng.Start : end+1]))
	return GetResult{Body: body, Info: info, Span: Range{Start: rng.Start, End: end}}, nil
}

func (m *MemStore) Head(ctx context.Context, key string) (ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	info, ok := m.meta[key]
	if !ok {
		return ObjectInfo{}, ErrNotFound
	}
	return info, nil
}

func (m *MemStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	delete(m.meta, key)
	return nil
}

// Rename moves an object's bytes to another key within the store (content
// hashing finalize step). Overwriting an existing target is allowed; the
// bytes are identical in that case by construction.
func (m *MemStore) Rename(ctx context.Context, fromKey, toKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.data[fromKey]
	if !ok {
		return ErrNotFound
	}
	info := m.meta[fromKey]
	delete(m.data, fromKey)
	delete(m.meta, fromKey)
	info.Key = toKey
	m.data[toKey] = data
	m.meta[toKey] = info
	return nil
}

func (m *MemStore) List(ctx context.Context, keyPrefix string, startAfter string, maxKeys int) (ListOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.meta {
		if strings.HasPrefix(k, keyPrefix) && (startAfter == "" || k > startAfter) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	var out ListOutput
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	for i, k := range keys {
		if len(out.Entries) >= maxKeys {
			out.IsTruncated = true
			break
		}
		info := m.meta[k]
		info.Key = k
		out.Entries = append(out.Entries, info)
		if i == len(keys)-1 {
			out.NextToken = k
		}
	}
	if out.IsTruncated {
		out.NextToken = out.Entries[len(out.Entries)-1].Key
	}
	return out, nil
}

func (m *MemStore) EnsureBucket(ctx context.Context, bucket string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ubs[bucket] = struct{}{}
	return nil
}

func (m *MemStore) Buckets(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for b := range m.ubs {
		out = append(out, b)
	}
	sort.Strings(out)
	return out, nil
}

func (m *MemStore) BucketExists(ctx context.Context, bucket string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.ubs[bucket]
	return ok, nil
}

func (m *MemStore) Close() error { return nil }
