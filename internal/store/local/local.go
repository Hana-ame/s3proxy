// Package local implements the Store interface on a plain filesystem
// directory. It is the "hot buffer" pool in the default deployment: cheap,
// instant access, and the tiering engine later streams idle objects out of
// it into remote S3 pools.
//
// Layout inside the data dir:
//
//	<root>/<bucket>/<key...>          object bytes
//	<root>/<bucket>/<key>.meta.json   sidecar: ContentType/ETag/Modified
//	<root>/<bucket>/<key>.tmp         in-flight writes (cleaned by WriteTemp)
//
// The sidecar exists so the plugin remains self-describing for index rebuilds
// (see tier); it does not store tier information.
package local

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"s3proxy/internal/store"
)

const metaSuffix = ".meta.json"
const tmpSuffix = ".tmp"

// Store stores objects under root.
type Store struct {
	name string
	root string
}

// New returns a Store named name persisting under root.
func New(name, root string) (*Store, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("local: create %s: %w", root, err)
	}
	return &Store{name: name, root: root}, nil
}

func (s *Store) Name() string { return s.name }

func (s *Store) Close() error { return nil }

func (s *Store) objectPath(key string) string {
	return filepath.Join(s.root, filepath.FromSlash(key))
}

// validKey rejects segments that could escape the data root (path traversal)
// or confuse the sidecar layout. Discovery background: the old s3-store
// upstream had no such guard and let "PUT /bucket/../etc/x" write outside
// the data dir; the guard was added after a code review of that file.
func validKey(key string) bool {
	if key == "" {
		return false
	}
	for _, seg := range strings.Split(key, "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsAny(seg, "\\\x00") {
			return false
		}
	}
	return true
}

type sidecar struct {
	ContentType string    `json:"content_type"`
	ETag        string    `json:"etag"`
	Modified    time.Time `json:"modified"`
}

func (s *Store) loadSidecar(objPath string) (sidecar, error) {
	var m sidecar
	data, err := os.ReadFile(objPath + metaSuffix)
	if err != nil {
		// A missing sidecar is tolerated on rebuild: the file itself is
		// the source of truth for size, and zero metadata is fine for a
		// reconstructed index. But Head/Get on a live object SHOULD have
		// one, so this also covers objects written by other tools.
		if os.IsNotExist(err) {
			return m, nil
		}
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, err
	}
	return m, nil
}

func (s *Store) saveSidecar(objPath string, m sidecar) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// Write to a temp file then rename so readers never observe a torn
	// sidecar (same pattern as the object bytes themselves).
	tmp := objPath + metaSuffix + tmpSuffix
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, objPath+metaSuffix)
}

// fileMD5 returns the hex MD5 of a file's bytes; the local plugin's ETag
// convention (S3 uses object md5 for non-multipart writes).
func fileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func stripQuotes(etag string) string { return strings.Trim(etag, `"`) }

func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string, opts store.PutOptions) (store.ObjectInfo, error) {
	if !validKey(key) {
		return store.ObjectInfo{}, fmt.Errorf("local: invalid key %q", key)
	}
	objPath := s.objectPath(key)
	if err := os.MkdirAll(filepath.Dir(objPath), 0o755); err != nil {
		return store.ObjectInfo{}, err
	}
	// Write to <key>.tmp then rename: an interrupted PUT never leaves a
	// half-written object visible under its final name, and concurrent
	// readers keep seeing either the old or the new object, never a mix.
	tmp := objPath + tmpSuffix
	f, err := os.Create(tmp)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	if size > 0 {
		if _, err := io.CopyN(f, r, size); err != nil {
			f.Close()
			os.Remove(tmp)
			return store.ObjectInfo{}, err
		}
	} else {
		// size<0 means "read to EOF" (chunked Transfer-Encoding upload);
		// the real size is taken from the file after the copy.
		if _, err := io.Copy(f, r); err != nil {
			f.Close()
			os.Remove(tmp)
			return store.ObjectInfo{}, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return store.ObjectInfo{}, err
		}
		size = st.Size()
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return store.ObjectInfo{}, err
	}
	if err := os.Rename(tmp, objPath); err != nil {
		os.Remove(tmp)
		return store.ObjectInfo{}, err
	}

	var etag string
	if opts.ETag != "" {
		// Caller (multipart complete) supplies the composite ETag S3
		// clients validate; the on-disk bytes are the concatenation of
		// parts so their local MD5 would not equal it anyway.
		etag = opts.ETag
	} else {
		sum, err := fileMD5(objPath)
		if err != nil {
			return store.ObjectInfo{}, err
		}
		etag = `"` + sum + `"`
	}
	now := time.Now()
	sc := sidecar{ContentType: contentType, ETag: etag, Modified: now}
	if err := s.saveSidecar(objPath, sc); err != nil {
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{
		Key:          key,
		Size:         size,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: now,
		StorageClass: "STANDARD",
	}, nil
}

func (s *Store) Get(ctx context.Context, key string, rng store.Range) (store.GetResult, error) {
	if !validKey(key) {
		return store.GetResult{}, store.ErrNotFound
	}
	objPath := s.objectPath(key)
	sc, err := s.loadSidecar(objPath)
	if err != nil {
		return store.GetResult{}, err
	}
	f, err := os.Open(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return store.GetResult{}, store.ErrNotFound
		}
		return store.GetResult{}, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return store.GetResult{}, err
	}
	start, end := rng.Start, rng.End
	if end < 0 || end >= st.Size() {
		end = st.Size() - 1
	}
	if start < 0 || start > end {
		f.Close()
		return store.GetResult{}, fmt.Errorf("local: range %d-%d invalid for size %d", rng.Start, rng.End, st.Size())
	}
	body := &rangeReader{f: f, section: io.NewSectionReader(f, start, end-start+1)}
	return store.GetResult{
		Body: body,
		Info: store.ObjectInfo{
			Key:          key,
			Size:         st.Size(),
			ETag:         sc.ETag,
			ContentType:  sc.ContentType,
			LastModified: sc.Modified,
			StorageClass: "STANDARD",
		},
		Span: store.Range{Start: start, End: end},
	}, nil
}

// rangeReader streams a byte span and closes the underlying file. The file
// descriptor is the read cursor: SectionReader offsets are relative to its
// start, and io.NewSectionReader positions it once at construction.
type rangeReader struct {
	f       *os.File
	section *io.SectionReader
}

func (rr *rangeReader) Read(p []byte) (int, error) { return rr.section.Read(p) }
func (rr *rangeReader) Close() error               { return rr.f.Close() }

func (s *Store) Head(ctx context.Context, key string) (store.ObjectInfo, error) {
	if !validKey(key) {
		return store.ObjectInfo{}, store.ErrNotFound
	}
	objPath := s.objectPath(key)
	sc, err := s.loadSidecar(objPath)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	st, err := os.Stat(objPath)
	if err != nil {
		if os.IsNotExist(err) {
			return store.ObjectInfo{}, store.ErrNotFound
		}
		return store.ObjectInfo{}, err
	}
	return store.ObjectInfo{
		Key:          key,
		Size:         st.Size(),
		ETag:         sc.ETag,
		ContentType:  sc.ContentType,
		LastModified: sc.Modified,
		StorageClass: "STANDARD",
	}, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	if !validKey(key) {
		return nil
	}
	objPath := s.objectPath(key)
	if err := os.Remove(objPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Best-effort sidecar cleanup; missing sidecar is fine.
	os.Remove(objPath + metaSuffix)
	os.Remove(objPath + tmpSuffix)
	return nil
}

func (s *Store) List(ctx context.Context, keyPrefix string, startAfter string, maxKeys int) (store.ListOutput, error) {
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	var out store.ListOutput
	keys := make([]string, 0, 64)
	// Walk the whole data root and filter: a prefix like "bkt2/x" is not
	// necessarily a real directory, so it cannot be the walk root. Full
	// scans are fine at this pool's scale; the tier index serves the
	// frontend paging anyway.
	err := filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return filepath.SkipDir
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(s.root, path)
		if err != nil {
			return nil
		}
		key := filepath.ToSlash(rel)
		if strings.HasSuffix(key, metaSuffix) || strings.HasSuffix(key, tmpSuffix) {
			return nil
		}
		if !strings.HasPrefix(key, keyPrefix) {
			return nil
		}
		if startAfter != "" && key <= startAfter {
			return nil
		}
		keys = append(keys, key)
		return nil
	})
	if err != nil {
		return out, err
	}
	sort.Strings(keys)
	for _, k := range keys {
		if len(out.Entries) >= maxKeys {
			out.IsTruncated = true
			break
		}
		info, err := s.Head(ctx, k)
		if err != nil {
			info = store.ObjectInfo{Key: k}
		}
		out.Entries = append(out.Entries, info)
	}
	if out.IsTruncated {
		out.NextToken = out.Entries[len(out.Entries)-1].Key
	}
	return out, nil
}

func (s *Store) EnsureBucket(ctx context.Context, bucket string) error {
	if !validKey(bucket) {
		return fmt.Errorf("local: invalid bucket name %q", bucket)
	}
	return os.MkdirAll(filepath.Join(s.root, bucket), 0o755)
}

func (s *Store) BucketExists(ctx context.Context, bucket string) (bool, error) {
	st, err := os.Stat(filepath.Join(s.root, bucket))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return st.IsDir(), nil
}

func (s *Store) Buckets(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && validKey(e.Name()) {
			out = append(out, e.Name())
		}
	}
	sort.Strings(out)
	return out, nil
}

// RemoveStaleTemps deletes leftover ".tmp" write files older than age.
// Discovery background: an interrupted PUT (process kill mid-write, disk
// full) leaves <key>.tmp behind forever; found while auditing disk usage of
// an old deployment. The tier loop calls this periodically.
func (s *Store) RemoveStaleTemps(age time.Duration) error {
	cutoff := time.Now().Add(-age)
	return filepath.WalkDir(s.root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, tmpSuffix) {
			return nil
		}
		info, err := d.Info()
		if err == nil && info.ModTime().Before(cutoff) {
			os.Remove(path)
		}
		return nil
	})
}

var _ store.Store = (*Store)(nil)
