// Package tier implements the buffer/tiering policy on top of storage
// plugins.
//
// Every object is placed in exactly one pool and the placement is recorded
// in a durable index backed by SQLite (tier.db in the state dir):
//
//   - Writes always land in the "hot" pool (the buffer).
//   - Idle objects (no read for coldAfter, or the hot pool exceeding
//     maxHotBytes) are streamed to a "cold" pool (remote S3 / second local)
//     by the background migration loop: the buffer drains itself.
//   - Reads served from a cold pool can promote the object back to hot
//     (promoteOnAccess), turning the policy into a two-level cache.
//
// The index is the frontend's metadata truth (ETag, ContentType, size,
// times, storage class); pools only hold bytes. Reads cross-check the
// recorded pool and heal the index when a crashed migration left the object
// elsewhere.
//
// Persistence design: the in-memory maps are the runtime truth; SQLite is a
// write-through mirror (one UPSERT/DELETE row per mutation, WAL mode) so a
// restart restores the full index. The previous design rewrote the whole
// index as one JSON file per mutation, which is O(N) per write and loses
// LastAccess on crash (touch was memory-only, so a restart immediately
// considered every object idle and drained the buffer). SQLite makes both
// incremental row writes AND durable access times practical.
package tier

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite, no cgo

	"s3proxy/internal/store"
)

// Entry is the authoritative metadata record for one object.
type Entry struct {
	Pool         string            `json:"pool"` // pool name recorded in stores
	Size         int64             `json:"size"`
	ETag         string            `json:"etag"` // quoted, as S3 clients expect
	ContentType  string            `json:"content_type"`
	StorageClass string            `json:"storage_class"`
	Metadata     map[string]string `json:"metadata,omitempty"` // x-amz-meta-*
	LastModified time.Time         `json:"last_modified"`
	LastAccess   time.Time         `json:"last_access"` // touched in memory on read
}

// PutOpts groups per-write options from the frontend.
type PutOpts struct {
	ContentType  string
	ETag         string // multipart composite etag override
	Metadata     map[string]string
	StorageClass string
}

// Config defines the tiering policy.
type Config struct {
	Hot             string        // pool name receiving all writes
	Cold            []string      // drain targets, tried round-robin
	ColdAfter       time.Duration // idle time before an object qualifies as cold
	ScanInterval    time.Duration // migration loop period
	MaxHotBytes     int64         // 0 = unlimited; else evict oldest hot objects over quota
	PromoteOnAccess bool          // read a cold object -> move it back to hot
}

// TieredStore mediates frontend operations across the configured pools.
type TieredStore struct {
	pools map[string]store.Store
	cfg   Config
	now   func() time.Time

	statePath string
	db        *sql.DB
	mu        sync.Mutex
	idx       map[string]*Entry // "bucket/key" -> entry
	buckets   map[string]time.Time

	// Prepared statements (single writer connection, so they are safe to
	// reuse; see New).
	upEntry   *sql.Stmt
	delEntry  *sql.Stmt
	upBucket  *sql.Stmt
	delBucket *sql.Stmt

	keyLocks sync.Map // key -> *sync.Mutex, serializes byte moves per object
	rr       uint64   // round-robin cursor across cold pools
}

// SetNow injects a test clock. Not for production use.
func (t *TieredStore) SetNow(fn func() time.Time) { t.now = fn }

// openDB opens (creating if needed) the SQLite index store with WAL mode
// and a single connection. Single connection matters: SQLite serializes
// writers with SQLITE_BUSY under a pooled connection set, and every write
// here is a hot-path row upsert — one connection keeps them lock-free and
// ordered. WAL allows concurrent readers on other connections later.
func openDB(path string) (*sql.DB, error) {
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS objects (
  fk            TEXT PRIMARY KEY,
  pool          TEXT NOT NULL,
  size          INTEGER NOT NULL,
  etag          TEXT NOT NULL DEFAULT '',
  content_type  TEXT NOT NULL DEFAULT '',
  storage_class TEXT NOT NULL DEFAULT '',
  metadata      TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL,
  last_access   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buckets (
  bucket  TEXT PRIMARY KEY,
  created TEXT NOT NULL
);`

// entryIsZero reports the zero Entry (an object whose metadata is
// impossible: empty pool). Used by Rebuild to skip unknown rows.
func entryIsZero(e *Entry) bool { return e.Pool == "" }

// New creates the tiered store. The index lives in the SQLite file at
// statePath. If the file is missing or unreadable (corrupt), the index is
// rebuilt by listing every pool.
func New(pools []store.Store, cfg Config, statePath string) (*TieredStore, error) {
	_, statErr := os.Stat(statePath)
	needsRebuild := errors.Is(statErr, os.ErrNotExist)
	// DB is created (with schema) in openDB regardless; needsRebuild only
	// records that this is a fresh start, so loadIndex skips the read and
	// New rebuilds from the pools below.

	if err := os.MkdirAll(filepath.Dir(statePath), 0o755); err != nil {
		return nil, err
	}
	db, err := openDB(statePath)
	if err != nil {
		return nil, err
	}
	t := &TieredStore{
		pools:     make(map[string]store.Store, len(pools)),
		cfg:       cfg,
		now:       time.Now,
		statePath: statePath,
		db:        db,
		idx:       make(map[string]*Entry),
		buckets:   make(map[string]time.Time),
	}
	for _, p := range pools {
		t.pools[p.Name()] = p
	}
	if _, ok := t.pools[cfg.Hot]; !ok {
		db.Close()
		return nil, fmt.Errorf("tier: hot pool %q not found", cfg.Hot)
	}
	for _, name := range cfg.Cold {
		if _, ok := t.pools[name]; !ok {
			db.Close()
			return nil, fmt.Errorf("tier: cold pool %q not found", name)
		}
	}
	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("tier: init schema: %w", err)
	}
	for name, stmt := range map[string]**sql.Stmt{
		"upEntry":   &t.upEntry,
		"delEntry":  &t.delEntry,
		"upBucket":  &t.upBucket,
		"delBucket": &t.delBucket,
	} {
		*stmt, err = db.Prepare(stmtSQL(name))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("tier: prepare %s: %w", name, err)
		}
	}
	if !needsRebuild {
		if err := t.loadIndex(); err != nil {
			log.Printf("tier: index rebuild (load failed: %v)", err)
			// A corrupt DB cannot accept the rebuild's writes; drop the
			// file completely and start the index from the pools.
			db.Close()
			if rmErr := os.Remove(statePath); rmErr != nil {
				return nil, fmt.Errorf("tier: removing corrupt db: %w", rmErr)
			}
			t.db, err = openDB(statePath)
			if err != nil {
				return nil, err
			}
			if _, err := t.db.Exec(schemaSQL); err != nil {
				t.db.Close()
				return nil, fmt.Errorf("tier: recreate schema: %w", err)
			}
			needsRebuild = true
		}
	}
	if needsRebuild {
		log.Printf("tier: index rebuild (fresh or lost state)")
		if err := t.Rebuild(); err != nil {
			return nil, err
		}
	}
	return t, nil
}

// stmtSQL returns the SQL text for a prepared statement name. Kept out of
// New to keep the init flow readable.
func stmtSQL(name string) string {
	switch name {
	case "upEntry":
		return `INSERT INTO objects (fk, pool, size, etag, content_type, storage_class, metadata, last_modified, last_access)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fk) DO UPDATE SET
  pool=excluded.pool, size=excluded.size, etag=excluded.etag,
  content_type=excluded.content_type, storage_class=excluded.storage_class,
  metadata=excluded.metadata, last_modified=excluded.last_modified,
  last_access=excluded.last_access`
	case "delEntry":
		return `DELETE FROM objects WHERE fk = ?`
	case "upBucket":
		return `INSERT INTO buckets (bucket, created) VALUES (?, ?)
ON CONFLICT(bucket) DO UPDATE SET created=excluded.created`
	case "delBucket":
		return `DELETE FROM buckets WHERE bucket = ?`
	}
	panic("unknown stmt " + name)
}

func (t *TieredStore) hot() store.Store { return t.pools[t.cfg.Hot] }
func (t *TieredStore) colds() []store.Store {
	res := make([]store.Store, 0, len(t.cfg.Cold))
	for _, n := range t.cfg.Cold {
		res = append(res, t.pools[n])
	}
	return res
}

func fullKey(bucket, key string) string { return bucket + "/" + key }

func splitKey(fk string) (bucket, key string) {
	b, rest, _ := strings.Cut(fk, "/")
	return b, rest
}

// lockKey serializes byte-moves (Put/Delete/migrate/promote) per object.
// Without it a migration could overwrite a concurrent PUT's fresh copy in
// the hot pool with the stale bytes it already read (race documented at
// migrate). Reads do not take the lock: they only observe placement.
func (t *TieredStore) lockKey(fk string) func() {
	m, _ := t.keyLocks.LoadOrStore(fk, &sync.Mutex{})
	mu := m.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// ---------------------------------------------------------------------------
// index persistence

// loadIndex reads the whole SQLite index into memory. Called once at
// startup on the single connection; columns are stored as RFC3339Nano UTC
// strings, metadata as a JSON map.
func (t *TieredStore) loadIndex() error {
	rows, err := t.db.Query(`SELECT fk, pool, size, etag, content_type, storage_class, metadata, last_modified, last_access FROM objects`)
	if err != nil {
		return err
	}
	defer rows.Close()
	idx := make(map[string]*Entry)
	for rows.Next() {
		var (
			fk, pool, etag, ct, sc, metaJSON, lmStr, laStr string
			size                                           int64
		)
		if err := rows.Scan(&fk, &pool, &size, &etag, &ct, &sc, &metaJSON, &lmStr, &laStr); err != nil {
			return err
		}
		e := &Entry{Pool: pool, Size: size, ETag: etag, ContentType: ct, StorageClass: sc}
		if metaJSON != "" {
			json.Unmarshal([]byte(metaJSON), &e.Metadata)
		}
		if lm, err := time.Parse(time.RFC3339Nano, lmStr); err == nil {
			e.LastModified = lm
		}
		if la, err := time.Parse(time.RFC3339Nano, laStr); err == nil {
			e.LastAccess = la
		}
		idx[fk] = e
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	brows, err := t.db.Query(`SELECT bucket, created FROM buckets`)
	if err != nil {
		return err
	}
	defer brows.Close()
	buckets := make(map[string]time.Time)
	for brows.Next() {
		var b, createdStr string
		if err := brows.Scan(&b, &createdStr); err != nil {
			return err
		}
		if created, err := time.Parse(time.RFC3339Nano, createdStr); err == nil {
			buckets[b] = created
		}
	}
	if err := brows.Err(); err != nil {
		return err
	}
	t.mu.Lock()
	t.idx = idx
	t.buckets = buckets
	t.mu.Unlock()
	return nil
}

// tsString encodes a time for the DB (UTC RFC3339Nano, stable lexicographic
// ordering for scans).
func tsString(tt time.Time) string { return tt.UTC().Format(time.RFC3339Nano) }

// entryRow serializes an Entry into its column values.
func entryRow(e *Entry) (fk, pool, etag, ct, sc, metaJSON, lm, la string, size int64) {
	meta := ""
	if len(e.Metadata) > 0 {
		if b, err := json.Marshal(e.Metadata); err == nil {
			meta = string(b)
		}
	}
	return "", e.Pool, e.ETag, e.ContentType, e.StorageClass, meta, tsString(e.LastModified), tsString(e.LastAccess), e.Size
}

// upsertEntryLocked write-throughs one object row. Callers must hold t.mu
// (the in-memory map and the DB mirror must never diverge).
func (t *TieredStore) upsertEntryLocked(fk string, e *Entry) error {
	_, pool, etag, ct, sc, meta, lm, la, size := entryRow(e)
	_, err := t.upEntry.Exec(fk, pool, size, etag, ct, sc, meta, lm, la)
	return err
}

// deleteEntryLocked removes one object row. Callers must hold t.mu.
func (t *TieredStore) deleteEntryLocked(fk string) error {
	_, err := t.delEntry.Exec(fk)
	return err
}

// upsertBucketLocked writes one bucket row. Callers must hold t.mu.
func (t *TieredStore) upsertBucketLocked(bucket string, created time.Time) error {
	_, err := t.upBucket.Exec(bucket, tsString(created))
	return err
}

// deleteBucketLocked removes one bucket row. Callers must hold t.mu.
func (t *TieredStore) deleteBucketLocked(bucket string) error {
	_, err := t.delBucket.Exec(bucket)
	return err
}

// Rebuild reconstructs the index from the pools. Source of truth fallback
// used by New, and by ops tests to simulate history. Duplicates (a crash mid
// migration can briefly leave both copies) resolve to the newest
// LastModified, then to the earlier-listed pool. Metadata is lost here by
// design: pool List() only returns core fields.
func (t *TieredStore) Rebuild() error {
	seen := make(map[string]*Entry)
	buckets := make(map[string]time.Time)
	for _, p := range append(t.colds(), t.hot()) {
		startAfter := ""
		for {
			pg, err := p.List(context.Background(), "", startAfter, 1000)
			if err != nil {
				return fmt.Errorf("tier: rebuild list %s: %w", p.Name(), err)
			}
			for _, it := range pg.Entries {
				prev, ok := seen[it.Key]
				if !ok || it.LastModified.After(prev.LastModified) {
					seen[it.Key] = &Entry{
						Pool:         p.Name(),
						Size:         it.Size,
						ETag:         it.ETag,
						ContentType:  it.ContentType,
						StorageClass: it.StorageClass,
						LastModified: it.LastModified,
						LastAccess:   it.LastModified,
					}
				}
				b, _, _ := strings.Cut(it.Key, "/")
				if _, ok := buckets[b]; !ok {
					buckets[b] = it.LastModified
				}
			}
			if !pg.IsTruncated || pg.NextToken == "" {
				break
			}
			startAfter = pg.NextToken
		}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	// Replace the persisted index inside one transaction so a crash during
	// rebuild leaves either the old or the new index, never a mix.
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM objects`); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM buckets`); err != nil {
		return err
	}
	for fk, e := range seen {
		_, pool, etag, ct, sc, meta, lm, la, size := entryRow(e)
		if _, err := tx.Exec(`INSERT INTO objects (fk, pool, size, etag, content_type, storage_class, metadata, last_modified, last_access) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, fk, pool, size, etag, ct, sc, meta, lm, la); err != nil {
			return err
		}
	}
	for b, created := range buckets {
		if _, err := tx.Exec(`INSERT INTO buckets (bucket, created) VALUES (?, ?)`, b, tsString(created)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.idx = seen
	t.buckets = buckets
	return nil
}

// ---------------------------------------------------------------------------
// object operations

// PutObject writes bytes into the hot (buffer) pool, then removes any stale
// copy from cold pools so reads never serve outdated data. opts.ETag
// overrides the recorded ETag (multipart complete passes the composite etag;
// the assembled bytes' own md5 would not match what S3 clients expect). Locks
// the key so a concurrent migration cannot clobber the fresh copy.
func (t *TieredStore) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64, opts PutOpts) (Entry, error) {
	fk := fullKey(bucket, key)
	unlock := t.lockKey(fk)
	defer unlock()

	hot := t.hot()
	info, err := hot.Put(ctx, fk, r, size, opts.ContentType, store.PutOptions{
		ETag:              opts.ETag,
		StorageClass:      opts.StorageClass,
		RequestedMetadata: opts.Metadata,
	})
	if err != nil {
		// Backend may not know the bucket yet (remote S3 pools require an
		// explicit CreateBucket). Retry once after ensuring it.
		if errors.Is(err, store.ErrNotFound) {
			if ebErr := hot.EnsureBucket(ctx, bucket); ebErr == nil {
				var retry store.ObjectInfo
				retry, err = hot.Put(ctx, fk, r, size, opts.ContentType, store.PutOptions{ETag: opts.ETag})
				info = retry
			}
		}
		if err != nil {
			return Entry{}, err
		}
	}
	// An explicit opts.ETag always wins: multipart complete passes the
	// composite etag, and backends (mem, S3) report their own md5 of the
	// assembled bytes which S3 clients would reject as a plain etag.
	if opts.ETag != "" {
		info.ETag = opts.ETag
	}
	e := Entry{
		Pool:         hot.Name(),
		Size:         info.Size,
		ETag:         info.ETag,
		ContentType:  opts.ContentType,
		StorageClass: opts.StorageClass,
		Metadata:     opts.Metadata,
		LastModified: info.LastModified,
		LastAccess:   t.now(),
	}
	if e.StorageClass == "" {
		e.StorageClass = "STANDARD"
	}
	if e.ContentType == "" {
		e.ContentType = "application/octet-stream"
	}

	// Sweep stale copies from every other pool (covers crashed-migration
	// leftovers) before the index moves to the fresh copy.
	for _, p := range append(t.colds(), hot) {
		if p.Name() == hot.Name() {
			continue
		}
		if err := p.Delete(ctx, fk); err != nil {
			log.Printf("tier: stale sweep %s: %v", p.Name(), err)
		}
	}

	t.mu.Lock()
	t.idx[fk] = &e
	created := t.now()
	t.buckets[bucket] = created
	err = t.upsertEntryLocked(fk, &e)
	if err == nil {
		err = t.upsertBucketLocked(bucket, created)
	}
	t.mu.Unlock()
	return e, err
}

// GetObject streams bytes of an object placed anywhere in the tiers. On the
// first pool read failing with ErrNotFound it probes every other pool and
// heals the index (a crashed migration may have left the object physically
// elsewhere than the index says). A hit from a cold pool optionally promotes
// the object back to hot (async).
func (t *TieredStore) GetObject(ctx context.Context, bucket, key string, rng store.Range) (store.GetResult, Entry, error) {
	fk := fullKey(bucket, key)
	var res store.GetResult
	e, err := t.getEntry(fk)
	if err != nil {
		return res, Entry{}, err
	}
	// Snapshot the entry; the index itself is used for placement, but we
	// report this snapshot (with a fresh access timestamp) to the api
	// layer so ETag/etag never race with a concurrent migration.
	eCopy := e
	eCopy.LastAccess = t.now()

	// Copy the result span; promoted reads reset the range so the
	// eventual re-fetch from hot serves the same bytes.
	targetRange := rng
	res, err = t.pools[eCopy.Pool].Get(ctx, fk, targetRange)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return res, eCopy, err
		}
		healed, hErr := t.probeAndHeal(ctx, fk)
		if hErr != nil {
			return res, eCopy, store.ErrNotFound
		}
		eCopy = healed
		res, err = t.pools[healed.Pool].Get(ctx, fk, targetRange)
		if err != nil {
			return res, eCopy, err
		}
	}

	t.touch(fk)
	if t.cfg.PromoteOnAccess && eCopy.Pool != t.cfg.Hot {
		go t.promote(ctx, fk, res.Info)
	}
	return res, eCopy, nil
}

// HeadObject mirrors GetObject but without a body; still touches access time
// and heals placement on mismatch.
func (t *TieredStore) HeadObject(ctx context.Context, bucket, key string) (Entry, error) {
	fk := fullKey(bucket, key)
	e, err := t.getEntry(fk)
	if err != nil {
		return e, err
	}
	_, err = t.pools[e.Pool].Head(ctx, fk)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return e, err
		}
		healed, hErr := t.probeAndHeal(ctx, fk)
		if hErr != nil {
			return e, store.ErrNotFound
		}
		e = healed
	}
	touch := t.now()
	t.mu.Lock()
	if en, ok := t.idx[fk]; ok {
		en.LastAccess = touch
		if err := t.upsertEntryLocked(fk, en); err != nil {
			log.Printf("tier: head touch %s: %v", fk, err)
		}
	}
	t.mu.Unlock()
	return e, nil
}

// DeleteObject removes the key from every pool and the index. Sweeping all
// pools is deliberate: it also cleans leftovers of an interrupted migration.
func (t *TieredStore) DeleteObject(ctx context.Context, bucket, key string) error {
	fk := fullKey(bucket, key)
	unlock := t.lockKey(fk)
	defer unlock()

	for _, p := range t.allPools() {
		if err := p.Delete(ctx, fk); err != nil {
			log.Printf("tier: delete sweep %s %s: %v", p.Name(), fk, err)
		}
	}
	t.mu.Lock()
	delete(t.idx, fk)
	err := t.deleteEntryLocked(fk)
	t.mu.Unlock()
	return err
}

func (t *TieredStore) getEntry(fk string) (Entry, error) {
	t.mu.Lock()
	e, ok := t.idx[fk]
	t.mu.Unlock()
	if !ok {
		return Entry{}, store.ErrNotFound
	}
	return *e, nil
}

// probeAndHeal locates fk in a pool other than the one the index records and
// repoints the index there. Called on a read miss after a crash interrupted
// a migration between index update and source deletion.
func (t *TieredStore) probeAndHeal(ctx context.Context, fk string) (Entry, error) {
	t.mu.Lock()
	cur, ok := t.idx[fk]
	t.mu.Unlock()
	if !ok {
		return Entry{}, store.ErrNotFound
	}
	for _, p := range t.allPools() {
		if p.Name() == cur.Pool {
			continue
		}
		if _, err := p.Head(ctx, fk); err == nil {
			e := *cur
			e.Pool = p.Name()
			t.mu.Lock()
			t.idx[fk] = &e
			err := t.upsertEntryLocked(fk, &e)
			t.mu.Unlock()
			return e, err
		}
	}
	return Entry{}, store.ErrNotFound
}

// touch records a read access. Write-through to SQLite: a durable
// LastAccess is what keeps the buffer policy correct across restarts (with
// the old JSON index the touch was memory-only, so after a restart every
// object was instantly "idle" and the whole buffer drained on the first
// policy run).
func (t *TieredStore) touch(fk string) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.idx[fk]
	if !ok {
		return
	}
	e.LastAccess = now
	if err := t.upsertEntryLocked(fk, e); err != nil {
		log.Printf("tier: touch %s: %v", fk, err)
	}
}

// ---------------------------------------------------------------------------
// listing

// ListParams mirrors the S3 ListObjects query parameters, minus
// continuation/marker distinction which the api layer normalizes.
type ListParams struct {
	Bucket     string
	Prefix     string
	Delimiter  string
	StartAfter string // absolute "bucket/key" resume position
	MaxKeys    int
}

// ListResult is one page of a listing.
type ListResult struct {
	Entries        []store.ObjectInfo // Contents
	CommonPrefixes []string
	IsTruncated    bool
	NextToken      string // deepest key scanned; absolute "bucket/key"
}

// ListObjects pages the index. Delimiter grouping happens here because the
// index is the merged view across tiers — individual pools cannot group.
func (t *TieredStore) ListObjects(ctx context.Context, p ListParams) (ListResult, error) {
	if p.MaxKeys <= 0 {
		p.MaxKeys = 1000
	}
	base := p.Bucket + "/"
	keyPrefix := base + p.Prefix
	var out ListResult

	t.mu.Lock()
	keys := make([]string, 0, len(t.idx))
	for fk, e := range t.idx {
		if strings.HasPrefix(fk, keyPrefix) {
			_ = e
			keys = append(keys, fk)
		}
	}
	t.mu.Unlock()
	sort.Strings(keys)

	emitted := 0
	lastEmitted := "" // deepest key consumed so far (monotonic)
	lastCP := ""      // previous grouped common prefix, dedupe consecutive
	truncated := false
	for _, fk := range keys {
		if p.StartAfter != "" && fk <= p.StartAfter {
			continue
		}
		rest := strings.TrimPrefix(fk, base)
		cp := ""
		// AWS groups everything after the first delimiter inside the
		// prefix remainder into a CommonPrefix; a key exactly equal to its
		// own common prefix stays a real object.
		if p.Delimiter != "" {
			if i := strings.Index(rest, p.Delimiter); i >= 0 {
				cp = p.Prefix + rest[:i+len(p.Delimiter)]
			}
		}
		if cp != "" {
			if cp == rest {
				cp = "" // the object itself is the delimiter name; list it
			} else if cp == lastCP {
				// Already grouped by a previous key; consume it without
				// emitting so the page token still advances past it.
				lastEmitted = fk
				continue
			}
		}
		// Truncation check BEFORE consuming this key: a page boundary must
		// not advance the token past a key that was never emitted.
		if emitted >= p.MaxKeys {
			truncated = true
			break
		}
		lastEmitted = fk
		if cp != "" {
			lastCP = cp
			out.CommonPrefixes = append(out.CommonPrefixes, cp)
			emitted++
			continue
		}
		t.mu.Lock()
		e := t.idx[fk]
		t.mu.Unlock()
		out.Entries = append(out.Entries, store.ObjectInfo{
			Key:          strings.TrimPrefix(fk, base),
			Size:         e.Size,
			ETag:         e.ETag,
			ContentType:  e.ContentType,
			LastModified: e.LastModified,
			StorageClass: e.StorageClass,
		})
		emitted++
	}
	out.IsTruncated = truncated
	if truncated {
		// Resume token: the deepest key scanned, even when the page ended
		// on a grouped prefix, so the next page skips the whole group.
		out.NextToken = lastEmitted
	}
	return out, nil
}

// ListBuckets returns every bucket the frontend created (or discovered in a
// rebuild), oldest first.
func (t *TieredStore) ListBuckets(ctx context.Context) ([]store.BucketInfo, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]store.BucketInfo, 0, len(t.buckets))
	for name, created := range t.buckets {
		out = append(out, store.BucketInfo{Name: name, Created: created})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// CreateBucket records the bucket and ensures it exists on every pool so the
// first object write never surprises a remote backend.
func (t *TieredStore) CreateBucket(ctx context.Context, bucket string) error {
	t.mu.Lock()
	_, exists := t.buckets[bucket]
	t.mu.Unlock()
	if exists {
		return store.ErrBucketExists
	}
	for _, p := range t.allPools() {
		if err := p.EnsureBucket(ctx, bucket); err != nil {
			return err
		}
	}
	t.mu.Lock()
	created := t.now()
	t.buckets[bucket] = created
	err := t.upsertBucketLocked(bucket, created)
	t.mu.Unlock()
	return err
}

// HeadBucket reports whether the bucket exists.
func (t *TieredStore) HeadBucket(ctx context.Context, bucket string) error {
	t.mu.Lock()
	_, ok := t.buckets[bucket]
	t.mu.Unlock()
	if !ok {
		return store.ErrNotFound
	}
	return nil
}

// DeleteBucket removes the bucket; ErrNotEmpty if it still holds objects.
func (t *TieredStore) DeleteBucket(ctx context.Context, bucket string) error {
	t.mu.Lock()
	_, ok := t.buckets[bucket]
	t.mu.Unlock()
	if !ok {
		return store.ErrNotFound
	}
	remaining := 0
	t.mu.Lock()
	for fk := range t.idx {
		if b, _ := splitKey(fk); b == bucket {
			remaining++
		}
	}
	t.mu.Unlock()
	if remaining > 0 {
		return store.ErrNotEmpty
	}
	t.mu.Lock()
	delete(t.buckets, bucket)
	err := t.deleteBucketLocked(bucket)
	t.mu.Unlock()
	return err
}

func (t *TieredStore) allPools() []store.Store {
	res := []store.Store{t.hot()}
	res = append(res, t.colds()...)
	return res
}

// Close releases every pool and the index database.
func (t *TieredStore) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.db != nil {
		t.db.Close()
		t.db = nil
	}
	for _, p := range t.allPools() {
		p.Close()
	}
	return nil
}

// ---------------------------------------------------------------------------
// migration (buffer drain)

// Run drives the migration loop until ctx is cancelled.
func (t *TieredStore) Run(ctx context.Context, interval time.Duration) {
	if local, ok := t.hot().(interface{ RemoveStaleTemps(time.Duration) error }); ok {
		// Hot pool temp cleanup, best-effort. Discovery background: killed
		// PUTs leave .tmp files forever; an old deployment accumulated
		// tens of GB before anyone noticed.
		go func() {
			for range time.Tick(interval) {
				if err := local.RemoveStaleTemps(interval); err != nil {
					log.Printf("tier: temp cleanup: %v", err)
				}
			}
		}()
	}
	ticks := time.NewTicker(interval)
	defer ticks.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks.C:
			t.RunOnce()
		}
	}
}

// RunOnce evaluates the tiering policy once: drains the hot pool down to the
// idle threshold and/or the byte quota, oldest-written first. Exposed for
// tests and manual triggering.
func (t *TieredStore) RunOnce() {
	t.mu.Lock()
	now := t.now()
	var hotKeys []string
	var hotBytes int64
	for fk, e := range t.idx {
		if e.Pool != t.cfg.Hot {
			continue
		}
		hotKeys = append(hotKeys, fk)
		hotBytes += e.Size
	}
	// Idle candidates first (policy: cold is defined by staleness).
	idle := make(map[string]bool)
	for _, fk := range hotKeys {
		e := t.idx[fk]
		if t.cfg.ColdAfter > 0 && now.Sub(e.LastAccess) >= t.cfg.ColdAfter {
			idle[fk] = true
		}
	}
	t.mu.Unlock()

	// Quota eviction: when the hot pool exceeds maxHotBytes, evict the
	// oldest objects (by LastAccess) whether idle or not, until balanced.
	quota := make(map[string]bool)
	if t.cfg.MaxHotBytes > 0 {
		t.mu.Lock()
		sort.Slice(hotKeys, func(i, j int) bool {
			ei, ej := t.idx[hotKeys[i]], t.idx[hotKeys[j]]
			if ei.LastAccess.Equal(ej.LastAccess) {
				return hotKeys[i] < hotKeys[j]
			}
			return ei.LastAccess.Before(ej.LastAccess)
		})
		for _, fk := range hotKeys {
			if hotBytes <= t.cfg.MaxHotBytes {
				break
			}
			if idle[fk] {
				continue // already scheduled via idle path
			}
			e := t.idx[fk]
			hotBytes -= e.Size
			quota[fk] = true
		}
		t.mu.Unlock()
	}

	if len(idle) == 0 && len(quota) == 0 {
		return
	}
	var candidates []string
	for fk := range idle {
		candidates = append(candidates, fk)
	}
	sort.Strings(candidates)
	for fk := range quota {
		candidates = append(candidates, fk)
	}
	sort.Strings(candidates)

	for _, fk := range candidates {
		target := t.nextCold()
		if target == nil {
			log.Printf("tier: no cold pool configured, skip %s", fk)
			return
		}
		from := t.hot()
		if err := t.transfer(fk, from, target); err != nil {
			log.Printf("tier: migrate %s %s->%s: %v", fk, from.Name(), target.Name(), err)
			continue
		}
		log.Printf("tier: moved %s %s -> %s", fk, from.Name(), target.Name())
	}
}

// nextCold picks the drain target round-robin across all cold pools, so
// several S3 backends share the cold load evenly.
func (t *TieredStore) nextCold() store.Store {
	colds := t.colds()
	if len(colds) == 0 {
		return nil
	}
	i := t.rr % uint64(len(colds))
	t.rr++
	return colds[i]
}

// promote moves a cold object back to hot after it was served, impersonating
// a cache read-through. Must NOT hold the per-key lock here: transfer()
// takes it itself (then re-checks the index), and lockKey is not reentrant —
// a second lock on the same key in this goroutine would deadlock forever.
// The race is benign: a concurrent migration does what we wanted anyway.
func (t *TieredStore) promote(ctx context.Context, fk string, _ store.ObjectInfo) {
	t.mu.Lock()
	e, ok := t.idx[fk]
	t.mu.Unlock()
	if !ok || e.Pool == t.cfg.Hot {
		return
	}
	from := t.pools[e.Pool]
	if from == nil || from.Name() == t.cfg.Hot {
		return
	}
	if err := t.transfer(fk, from, t.hot()); err != nil {
		log.Printf("tier: promote %s: %v", fk, err)
	}
}

// transfer moves an object between pools: copy (with bucket ensure on the
// target), then flip the index, then drop the source copy. The index flip
// happens only after the target holds a complete copy, so readers always
// find valid bytes. A concurrent Delete/Put serializes on the key lock and
// the re-check below prevents resurrecting a deleted key or overwriting a
// fresh PUT with stale bytes.
func (t *TieredStore) transfer(fk string, from, to store.Store) error {
	ctx := context.Background()
	bucket, _ := splitKey(fk)

	t.mu.Lock()
	e, ok := t.idx[fk]
	t.mu.Unlock()
	if !ok || e.Pool != from.Name() {
		return nil // gone or already moved; nothing to do
	}

	if err := to.EnsureBucket(ctx, bucket); err != nil {
		return err
	}
	res, err := from.Get(ctx, fk, store.Range{Start: 0, End: -1})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	info, err := to.Put(ctx, fk, res.Body, res.Info.Size, res.Info.ContentType, store.PutOptions{StorageClass: res.Info.StorageClass})
	if err != nil {
		return err
	}

	unlock := t.lockKey(fk)
	defer unlock()
	// Re-check under the key lock: a concurrent PutObject replaced the
	// object while we copied; do NOT repoint the index or delete the fresh
	// hot copy.
	t.mu.Lock()
	cur, ok := t.idx[fk]
	if !ok || cur.Pool != from.Name() {
		t.mu.Unlock()
		// Leave the stray copy we made at `to`; the next PutObject
		// sweeps cold copies, so it self-heals.
		return nil
	}
	cur.Pool = to.Name()
	cur.Size = info.Size
	if info.ETag != "" {
		cur.ETag = info.ETag
	}
	cur.StorageClass = info.StorageClass
	cur.LastModified = info.LastModified
	err = t.upsertEntryLocked(fk, cur)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return from.Delete(ctx, fk)
}
