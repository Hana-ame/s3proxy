// Package tier implements the buffer/tiering policy on top of storage
// plugins, with content-addressed deduplication.
//
// Two layers, mirroring the S3 object model:
//
//   - objects (the name layer): "bucket/key" -> content id (sha256 of the
//     bytes) plus per-key metadata (ContentType, x-amz-meta-*, times).
//     Multiple keys can reference the same id; a copy is then a pure
//     mapping insert (zero bytes moved).
//   - resources (the content layer): id -> the pool physically holding the
//     bytes, refcount, size and entity tag. Every id exists at most once
//     across all pools: same content uploaded to different keys shares one
//     resource ("content dedup").
//
// Pools store bytes keyed by the content id (not by bucket/key), so the
// name layer is the ONLY place that knows names. Consequences, documented
// up front:
//
//   - Rebuild() can restore resources from a pool listing, but never the
//     name layer — bucket/key names are not recoverable from content
//     addressing. After a lost index, names are gone for good.
//   - S3 cold pools must run in prefix mode (single remote bucket for all
//     ids); per-bucket mode conflicts with global dedup. main validates.
//   - Overwriting a key releases the old content's refcount immediately;
//     there is no versioning (a previous version is deleted as soon as its
//     last reference disappears).
//
// Tiering policy on top: writes always land in the hot pool; idle
// resources (no access for coldAfter, or the hot pool exceeding
// maxHotBytes) are migrated to a cold pool by the background loop;
// reads from a cold pool can promote the resource back to hot.
// The LastAccess of a resource is the max over its referencing keys, so
// deduplicated content benefits from the accesses of all its aliases.
//
// Persistence: SQLite (WAL, single writer connection) mirrors the in-memory
// maps with one row write per mutation; access times are durable so a
// restart never drains the buffer instantly (a failure of the previous
// JSON-index design, where touch was memory-only).
//
// External control (no HTTP admin endpoint): a second process (see cmd/
// s3-admin) opens the same SQLite file and writes INTO control tables. The
// tier consumes them on two occasions only — at startup, and when the
// sentinel file (statePath + ".ctl") is touched AFTER the write. There is
// NO periodic polling of the control tables (an early design polled the
// DB every second; the sentinel design makes the steady-state cost zero).
//
//   - control(k,v): runtime overrides of auto_enabled / cold_after_ms /
//     max_hot_bytes / promote_on_access — the memory maps are authoritative
//     for object data, so external edits MUST go through these tables or
//     they get silently overwritten by the next upsert.
//   - commands(seq,verb,arg): force operations (migrate / promote by key or
//     content id) executed on ingest.
//
// Read-side queries (status, idle detection) can be done directly with any
// SQLite client against the resources/objects tables — they are a live
// mirror of the in-memory state; v_cold_status provides ready-made idle
// seconds per resource.
package tier

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	_ "modernc.org/sqlite" // pure-Go SQLite, no cgo

	"s3proxy/internal/store"
)

// Entry is the aggregate view the frontend consumes: per-key metadata from
// the name layer joined with content facts (size, etag, pool placement)
// from the resource layer.
type Entry struct {
	ID           string // content id (sha256 hex)
	Pool         string // pool name physically holding the bytes
	Size         int64
	ETag         string // quoted, as S3 clients expect
	ContentType  string
	StorageClass string
	Metadata     map[string]string // x-amz-meta-* (per key)
	LastModified time.Time         // when this KEY was last written
	LastAccess   time.Time         // last read of this KEY
}

// PutOpts groups per-write options from the frontend.
type PutOpts struct {
	ContentType  string
	ETag         string // multipart composite etag override
	MD5Hex       string // expected content digest (from Content-MD5); empty = no check
	Metadata     map[string]string
	StorageClass string
}

// ErrBadDigest reports that the client-supplied Content-MD5 did not match
// the ingested bytes; the write was refused and no state was changed.
var ErrBadDigest = errors.New("bad digest")

// Config defines the tiering policy.
type Config struct {
	Hot             string        // pool name receiving all writes
	Cold            []string      // drain targets, tried round-robin
	ColdAfter       time.Duration // idle time before a resource qualifies as cold
	ScanInterval    time.Duration // migration loop period
	MaxHotBytes     int64         // 0 = unlimited; else evict oldest hot resources over quota
	PromoteOnAccess bool          // read a cold resource -> move it back to hot
}

// objRow is the name layer: one row per key.
type objRow struct {
	ID           string
	ContentType  string
	StorageClass string
	Metadata     map[string]string
	LastModified time.Time
	LastAccess   time.Time
}

// resRow is the content layer: one row per unique bytes.
type resRow struct {
	Pool         string
	Refs         int
	Size         int64
	ETag         string
	LastModified time.Time
	LastAccess   time.Time // max of referencing keys' access
}

// overrides carries the runtime policy knobs set through the control table
// by an external admin process; nil fields mean "use the Config value".
// Swapped atomically; the polling loop publishes a new struct on change.
type overrides struct {
	ColdAfter       *time.Duration
	MaxHotBytes     *int64
	PromoteOnAccess *bool
}

// TieredStore mediates frontend operations across the configured pools.
type TieredStore struct {
	pools map[string]store.Store
	cfg   Config
	now   func() time.Time

	statePath    string
	sentinelPath string // statePath + ".ctl": touch = external control input
	db           *sql.DB
	mu           sync.Mutex
	idx          map[string]*objRow // "bucket/key" -> name row
	res          map[string]*resRow // content id -> resource row
	buckets      map[string]time.Time

	upObj     *sql.Stmt
	delObj    *sql.Stmt
	upRes     *sql.Stmt
	delRes    *sql.Stmt
	upBucket  *sql.Stmt
	delBucket *sql.Stmt

	keyLocks lockTable // name locks; see lockTable. Never re-entered
	// (documented promote deadlock history: transfer used to be called
	// while holding a key lock).
	// rr is atomic: it is advanced from TWO goroutines — the migration
	// loop (RunOnce) and the external-command consumer (execCommand) —
	// so a plain uint64 was a data race (benign in effect, still a race
	// under -race; discovered in the 2026-08 review).
	rr atomic.Uint64 // round-robin cursor across cold pools

	// External control state, owned by the polling loop (except autoOn
	// which the Run loop reads and test code flips via control rows):
	autoOn atomic.Bool // false = background migration loop is paused
	ov     atomic.Pointer[overrides]
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
  id            TEXT NOT NULL,
  content_type  TEXT NOT NULL DEFAULT '',
  storage_class TEXT NOT NULL DEFAULT '',
  metadata      TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL,
  last_access   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS resources (
  id            TEXT PRIMARY KEY,
  pool          TEXT NOT NULL,
  refs          INTEGER NOT NULL,
  size          INTEGER NOT NULL,
  etag          TEXT NOT NULL DEFAULT '',
  last_modified TEXT NOT NULL,
  last_access   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS buckets (
  bucket  TEXT PRIMARY KEY,
  created TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS control (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS commands (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  verb TEXT NOT NULL,
  arg  TEXT NOT NULL DEFAULT ''
);
CREATE VIEW IF NOT EXISTS v_cold_status AS
SELECT id, pool, refs, size, etag,
       last_modified, last_access,
       CAST(strftime('%s','now') AS INTEGER)
         - CAST(strftime('%s', last_access) AS INTEGER) AS idle_seconds
FROM resources;`

// New creates the tiered store. The index lives in the SQLite file at
// statePath. If the file is missing or unreadable (corrupt), the content
// layer is rebuilt by listing every pool; the name layer (key -> id,
// buckets) cannot be recovered from content addressing and is lost —
// documented in the package comment.
func New(pools []store.Store, cfg Config, statePath string) (*TieredStore, error) {
	_, statErr := os.Stat(statePath)
	needsRebuild := errors.Is(statErr, os.ErrNotExist)

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
		idx:       make(map[string]*objRow),
		res:       make(map[string]*resRow),
		buckets:   make(map[string]time.Time),
		keyLocks:  lockTable{locks: make(map[string]*lockEntry)},
	}
	t.autoOn.Store(true) // auto migration starts enabled; control can pause
	// Ensure the sentinel exists so the watcher can attach to it; existing
	// file is left untouched (its mtime is the "admin wrote rows" signal).
	sentinel := statePath + ".ctl"
	if f, err := os.OpenFile(sentinel, os.O_CREATE, 0o644); err == nil {
		f.Close()
		t.sentinelPath = sentinel
	} else {
		return nil, fmt.Errorf("tier: create sentinel %s: %w", sentinel, err)
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
	statements := map[string]**sql.Stmt{
		"upObj":     &t.upObj,
		"delObj":    &t.delObj,
		"upRes":     &t.upRes,
		"delRes":    &t.delRes,
		"upBucket":  &t.upBucket,
		"delBucket": &t.delBucket,
	}
	for name, stmt := range statements {
		*stmt, err = db.Prepare(stmtSQL(name))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("tier: prepare %s: %w", name, err)
		}
	}
	if !needsRebuild {
		if err := t.loadIndex(); err != nil {
			log.Printf("tier: index rebuild (load failed: %v)", err)
			db.Close()
			// Remove the corrupt database AND its WAL sidecars: a
			// stale -wal file left behind can mislead a fresh
			// database opened at the same path (checksummed against
			// the deleted file). Discovery background: code review —
			// the old code removed only tier.db.
			for _, p := range []string{statePath, statePath + "-wal", statePath + "-shm"} {
				if rmErr := os.Remove(p); rmErr != nil && !os.IsNotExist(rmErr) {
					return nil, fmt.Errorf("tier: removing corrupt db %s: %w", p, rmErr)
				}
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
	// Refs were loaded from disk; fix any inconsistency left by a crash or
	// a pre-fix version. Not run after Rebuild: there the refcount is
	// unknowable by design (refs=-1).
	if !needsRebuild {
		if err := t.reconcileRefs(); err != nil {
			return nil, fmt.Errorf("tier: reconcile refs: %w", err)
		}
	}
	// Apply persisted control rows immediately: a leftover auto_enabled=0
	// must survive a restart (e.g. maintenance window already paused).
	t.consumeControl(context.Background())
	return t, nil
}

// stmtSQL returns the SQL text for a prepared statement name.
func stmtSQL(name string) string {
	switch name {
	case "upObj":
		return `INSERT INTO objects (fk, id, content_type, storage_class, metadata, last_modified, last_access)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(fk) DO UPDATE SET
  id=excluded.id, content_type=excluded.content_type,
  storage_class=excluded.storage_class, metadata=excluded.metadata,
  last_modified=excluded.last_modified, last_access=excluded.last_access`
	case "delObj":
		return `DELETE FROM objects WHERE fk = ?`
	case "upRes":
		return `INSERT INTO resources (id, pool, refs, size, etag, last_modified, last_access)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
  pool=excluded.pool, size=excluded.size, etag=excluded.etag,
  last_modified=excluded.last_modified, last_access=excluded.last_access`
	case "delRes":
		return `DELETE FROM resources WHERE id = ?`
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

func (t *TieredStore) allPools() []store.Store {
	res := []store.Store{t.hot()}
	res = append(res, t.colds()...)
	return res
}

func fullKey(bucket, key string) string { return bucket + "/" + key }

func splitKey(fk string) (bucket, key string) {
	b, rest, _ := strings.Cut(fk, "/")
	return b, rest
}

// lockTable is a refcounted mutex table. Entries are removed once the last
// holder or waiter unlocks, so a long-running proxy does not accumulate one
// mutex per key ever touched (the sync.Map predecessor did: millions of
// keys left millions of live mutexes).
type lockTable struct {
	mu    sync.Mutex
	locks map[string]*lockEntry
}

type lockEntry struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the lock for name and returns the release func. The entry
// stays in the table while refs > 0: waiters increment refs BEFORE blocking
// on the mutex, so an entry is removed only when nobody holds or waits on
// it — a later lock for the same name can then never be racing a removal
// (which would split one logical lock into two mutexes).
func (lt *lockTable) lock(name string) func() {
	lt.mu.Lock()
	e := lt.locks[name]
	if e == nil {
		e = &lockEntry{}
		lt.locks[name] = e
	}
	e.refs++
	lt.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		lt.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(lt.locks, name)
		}
		lt.mu.Unlock()
	}
}

// lockKey serializes mutations per lock name. Namespaces: "bucket/key" for
// name-layer ops (Put/Delete/Copy of one key) and the content id for
// resource-layer ops (register/transfer/refcount). Callers must never call
// it twice in one goroutine on the same name: the underlying mutex is not
// reentrant, and a promote deadlock built on that mistake is documented at
// promote.
func (t *TieredStore) lockKey(name string) func() {
	return t.keyLocks.lock(name)
}

// ---------------------------------------------------------------------------
// index persistence

// tsString encodes a time for the DB (UTC RFC3339Nano).
func tsString(tt time.Time) string { return tt.UTC().Format(time.RFC3339Nano) }

func parseTS(s string) (time.Time, error) { return time.Parse(time.RFC3339Nano, s) }

func (t *TieredStore) loadIndex() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	rows, err := t.db.Query(`SELECT fk, id, content_type, storage_class, metadata, last_modified, last_access FROM objects`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var fk, id, ct, sc, meta, lm, la string
		if err := rows.Scan(&fk, &id, &ct, &sc, &meta, &lm, &la); err != nil {
			return err
		}
		o := &objRow{ID: id, ContentType: ct, StorageClass: sc}
		if meta != "" {
			json.Unmarshal([]byte(meta), &o.Metadata)
		}
		if v, err := parseTS(lm); err == nil {
			o.LastModified = v
		}
		if v, err := parseTS(la); err == nil {
			o.LastAccess = v
		}
		t.idx[fk] = o
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()

	rrows, err := t.db.Query(`SELECT id, pool, refs, size, etag, last_modified, last_access FROM resources`)
	if err != nil {
		return err
	}
	defer rrows.Close()
	for rrows.Next() {
		var id, pool, etag, lm, la string
		var refs int
		var size int64
		if err := rrows.Scan(&id, &pool, &refs, &size, &etag, &lm, &la); err != nil {
			return err
		}
		r := &resRow{Pool: pool, Refs: refs, Size: size, ETag: etag}
		if v, err := parseTS(lm); err == nil {
			r.LastModified = v
		}
		if v, err := parseTS(la); err == nil {
			r.LastAccess = v
		}
		t.res[id] = r
	}
	if err := rrows.Err(); err != nil {
		return err
	}
	rrows.Close()

	brows, err := t.db.Query(`SELECT bucket, created FROM buckets`)
	if err != nil {
		return err
	}
	defer brows.Close()
	for brows.Next() {
		var b, created string
		if err := brows.Scan(&b, &created); err != nil {
			return err
		}
		if v, err := parseTS(created); err == nil {
			t.buckets[b] = v
		}
	}
	return brows.Err()
}

// upsertObjLocked / delObjLocked / upsertResLocked / delResLocked /
// upBucketLocked / delBucketLocked: write-through mirror rows. Callers hold
// t.mu (and usually a business lock as well; t.mu is always the inner lock
// so lock ordering is uniform).
func (t *TieredStore) upsertObjLocked(fk string, o *objRow) error {
	meta := ""
	if len(o.Metadata) > 0 {
		if b, err := json.Marshal(o.Metadata); err == nil {
			meta = string(b)
		}
	}
	_, err := t.upObj.Exec(fk, o.ID, o.ContentType, o.StorageClass, meta, tsString(o.LastModified), tsString(o.LastAccess))
	return err
}

func (t *TieredStore) delObjLocked(fk string) error {
	_, err := t.delObj.Exec(fk)
	return err
}

func (t *TieredStore) upsertResLocked(id string, r *resRow) error {
	_, err := t.upRes.Exec(id, r.Pool, r.Refs, r.Size, r.ETag, tsString(r.LastModified), tsString(r.LastAccess))
	return err
}

func (t *TieredStore) delResLocked(id string) error {
	_, err := t.delRes.Exec(id)
	return err
}

func (t *TieredStore) upBucketLocked(bucket string, created time.Time) error {
	_, err := t.upBucket.Exec(bucket, tsString(created))
	return err
}

func (t *TieredStore) delBucketLocked(bucket string) error {
	_, err := t.delBucket.Exec(bucket)
	return err
}

// Rebuild reconstructs the CONTENT layer from the pools; the name layer is
// unrecoverable from content addressing (documented in the package
// comment), so it logs a warning and starts empty. Resources come back with
// refs=0: they are unreferenced orphans until a future upload of the same
// content dedups into them. Duplicates across pools resolve to the newest
// LastModified, then to the later-listed pool (hot wins ties by listing
// colds first).
func (t *TieredStore) Rebuild() error {
	seen := make(map[string]*resRow)
	for _, p := range append(t.colds(), t.hot()) {
		startAfter := ""
		for {
			pg, err := p.List(context.Background(), "", startAfter, 1000)
			if err != nil {
				return fmt.Errorf("tier: rebuild list %s: %w", p.Name(), err)
			}
			for _, it := range pg.Entries {
				// Stray temporary keys (interrupted content-hash writes)
				// are never resource names.
				if strings.HasPrefix(it.Key, tmpPrefix) {
					continue
				}
				prev, ok := seen[it.Key]
				if !ok || it.LastModified.After(prev.LastModified) {
					seen[it.Key] = &resRow{
						Pool:         p.Name(),
						Refs:         -1, // unknowable: name layer is lost
						Size:         it.Size,
						ETag:         it.ETag,
						LastModified: it.LastModified,
						LastAccess:   it.LastModified,
					}
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
	tx, err := t.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM resources`); err != nil {
		return err
	}
	for id, r := range seen {
		if _, err := tx.Exec(`INSERT INTO resources (id, pool, refs, size, etag, last_modified, last_access) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, r.Pool, r.Refs, r.Size, r.ETag, tsString(r.LastModified), tsString(r.LastAccess)); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.res = seen
	t.idx = make(map[string]*objRow)
	t.buckets = make(map[string]time.Time)
	log.Printf("tier: rebuild: name layer lost (keys/buckets unrecoverable from content ids)")
	log.Printf("tier: rebuild: %d resources marked refs=-1 (refcount unknowable, bytes kept)", len(seen))
	return nil
}

// ---------------------------------------------------------------------------
// object operations

// tmpPrefix names in-flight content-hash writes. Bytes stream to
// "tmp/<nonce>"; once the sha256 is known the entry is renamed to the id (or
// deleted on a dedup hit).
const tmpPrefix = "tmp/"

func newTmpKey() string {
	var b [16]byte
	rand.Read(b[:])
	return tmpPrefix + hex.EncodeToString(b[:])
}

// contentID returns a sha256 writer; the id is a function of the object
// bytes, always (multipart composite etags are stored on the resource, not
// used as the id).
func contentID() hash.Hash { return sha256.New() }

// PutObject stream-writes into the hot pool under a temporary key while
// hashing the bytes, then finalizes the content-addressed registration:
//
//   - existing id (dedup hit): temp bytes are deleted, refcount++.
//   - new id: temp is renamed to the id, resource row created, refs=1.
//
// Whatever the old mapping of this key was is released afterwards
// (refcount--; the resource is deleted when its refcount reaches zero,
// which also removes the bytes from every pool). Registration and release
// serialize on the content-id lock; the key lock serializes concurrent
// writes to the same key.
func (t *TieredStore) PutObject(ctx context.Context, bucket, key string, r io.Reader, size int64, opts PutOpts) (Entry, error) {
	fk := fullKey(bucket, key)
	// NO key lock during the stream: the id is only known after the bytes
	// have been hashed, and every concurrent PUT of this key streams to
	// its own unique temp key, so nothing on the name layer needs
	// serializing yet. The key lock is taken right before the mapping swap
	// below (last-writer-wins). Discovery background: the lock used to
	// wrap the whole body upload, so a slow client's multi-GB PUT blocked
	// every other write to the same key for the entire transfer.
	hot := t.hot()
	// Ensure the resource bucket on the pool. Content-addressed resources
	// live in a fixed "data" namespace: prefix-mode S3 pools map it to
	// their configured bucket; per-bucket pools are rejected in config.
	if err := hot.EnsureBucket(ctx, "data"); err != nil {
		return Entry{}, err
	}

	tmpKey := newTmpKey()
	h := contentID()
	hashed := io.TeeReader(r, h)
	// When the client supplied a Content-MD5 we ALSO compute the MD5 while
	// streaming (a second tee on the same bytes; the digest is only used
	// for this check). The sha256 id is the content-address key, NOT the
	// digest the client means: comparing MD5Hex against it would fail on
	// every upload, correct or not (hex lengths differ, 32 vs 64).
	md5h := md5.New()
	if opts.MD5Hex != "" {
		hashed = io.TeeReader(hashed, md5h)
	}
	info, err := hot.Put(ctx, tmpKey, hashed, size, opts.ContentType, store.PutOptions{
		ETag:              opts.ETag,
		StorageClass:      opts.StorageClass,
		RequestedMetadata: opts.Metadata,
	})
	if err != nil {
		return Entry{}, err
	}
	id := hex.EncodeToString(h.Sum(nil))
	if opts.ETag != "" {
		info.ETag = opts.ETag
	}
	// Content-MD5 pre-commit check: the digest of the bytes we just
	// ingested must match what the client claimed — otherwise refuse the
	// write BEFORE any refcount/old-content bookkeeping, so the previous
	// object stays intact (AWS semantics).
	// Discovery background: the old code accepted the write and the API
	// layer deleted the *whole object* (including the old bytes) after the
	// fact, turning a failed overwrite into a 404; a later attempt to
	// compare against the sha256 id rejected every upload with a correct
	// digest (32-char hex vs 64-char hex can never be equal).
	if opts.MD5Hex != "" && !strings.EqualFold(opts.MD5Hex, hex.EncodeToString(md5h.Sum(nil))) {
		if err := hot.Delete(ctx, tmpKey); err != nil {
			log.Printf("tier: bad-digest tmp cleanup: %v", err)
		}
		return Entry{}, ErrBadDigest
	}

	unlockKey := t.lockKey(fk)
	defer unlockKey()
	// Order matters: key lock BEFORE the content lock, same as every other
	// op (DeleteObject, CopyObject) — taking them the other way around
	// would ABBA-deadlock against a concurrent DeleteObject of this key.
	unlockRes := t.lockKey(id)
	// NOTE: no defer — this lock is released explicitly after the
	// registration section, before the old-content release (never hold two
	// content locks at once).

	t.mu.Lock()
	existing, exists := t.res[id]
	oldID := ""
	if old, ok := t.idx[fk]; ok {
		oldID = old.ID
	}
	t.mu.Unlock()

	now := t.now()
	if exists {
		// Dedup hit: the bytes are already registered; drop the copy we
		// just streamed and bump the refcount below.
		if err := hot.Delete(ctx, tmpKey); err != nil {
			log.Printf("tier: dedup tmp cleanup: %v", err)
		}
	} else {
		if err := renameInPool(hot, ctx, tmpKey, id); err != nil {
			unlockRes()
			return Entry{}, err
		}
		// Refs is set to 1 AT CONSTRUCTION: the row must never be visible
		// in the map with refs=0, or a sweep could take it for an orphan
		// (readers that skip the content lock — heal, touch — copy the
		// row wholesale and could record a stale 0 over a live object).
		existing = &resRow{
			Pool:         hot.Name(),
			Refs:         1,
			Size:         info.Size,
			ETag:         info.ETag,
			LastModified: now,
			LastAccess:   now,
		}
	}

	// Reference accounting, finalized while the new-id lock is still held
	// AND under t.mu. The row must never be mutated outside the mutex:
	// heal() copies the whole row and repoints it WITHOUT the content
	// lock (it only takes t.mu), so a lock-free Refs++ could have a stale
	// 0/old value written back over it — a live object swept as an orphan.
	// Discovery background: the previous version mutated existing.Refs
	// and read existing.Pool/Size/ETag for the Entry OUTSIDE t.mu (code
	// review + race analysis: heal, touch, transfer all touch the row
	// under t.mu; the Entry snapshot after unlocking could even observe
	// a transfer repointing the pool mid-read).
	// oldID == id (identical re-put / self-copy) replaces the key's own
	// reference — net zero, no increment. Discovery background: the old
	// code incremented unconditionally and skipped the release when
	// oldID == id, inflating refs by 1 on every identical re-PUT and
	// leaving unswept orphans.
	t.mu.Lock()
	if exists && oldID != id {
		// A refs=-1 row (refcount unknowable after an index rebuild)
		// becomes 1: THIS key now provably references the content.
		if existing.Refs < 0 {
			existing.Refs = 0
		}
		existing.Refs++
	} else if !exists {
		t.res[id] = existing
	}
	if oldID != id {
		if err := t.upsertResLocked(id, existing); err != nil {
			t.mu.Unlock()
			unlockRes()
			return Entry{}, err
		}
	}
	// Snapshot the resource facts for the Entry INSIDE the same critical
	// section: reading them after the unlock would race transfer/heal
	// repointing the row (torn Pool/Size/ETag in the response).
	e := Entry{
		ID:           id,
		Pool:         existing.Pool,
		Size:         existing.Size,
		ETag:         existing.ETag,
		ContentType:  opts.ContentType,
		StorageClass: opts.StorageClass,
		Metadata:     opts.Metadata,
		LastModified: now,
		LastAccess:   now,
	}
	t.mu.Unlock()

	// Release the new-id lock before touching the old content: never hold
	// two content locks (two concurrent overwrites swapping each other's
	// content would deadlock). releaseContent re-checks the map under the
	// old-id lock, so a concurrent re-registration of the old id either
	// bumps refs before us or blocks until our sweep is done.
	unlockRes()
	if oldID != "" && oldID != id {
		if err := t.releaseContent(ctx, oldID); err != nil {
			return Entry{}, err
		}
	}

	o := &objRow{
		ID:           id,
		ContentType:  opts.ContentType,
		StorageClass: opts.StorageClass,
		Metadata:     opts.Metadata,
		LastModified: now,
		LastAccess:   now,
	}
	if e.StorageClass == "" {
		e.StorageClass = "STANDARD"
		o.StorageClass = "STANDARD"
	}
	if e.ContentType == "" {
		e.ContentType = "application/octet-stream"
		o.ContentType = "application/octet-stream"
	}
	t.mu.Lock()
	t.idx[fk] = o
	t.buckets[bucket] = now
	if err := t.upsertObjLocked(fk, o); err != nil {
		t.mu.Unlock()
		return Entry{}, err
	}
	if err := t.upBucketLocked(bucket, now); err != nil {
		t.mu.Unlock()
		return Entry{}, err
	}
	t.mu.Unlock()
	return e, nil
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// renameInPool moves a temporary key to its final content-id key, using the
// plugin's native rename when available and copy+delete otherwise.
func renameInPool(p store.Store, ctx context.Context, fromKey, toKey string) error {
	if rn, ok := p.(store.Renamer); ok {
		return rn.Rename(ctx, fromKey, toKey)
	}
	res, err := p.Get(ctx, fromKey, store.Range{Start: 0, End: -1})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if _, err := p.Put(ctx, toKey, res.Body, res.Info.Size, res.Info.ContentType, store.PutOptions{}); err != nil {
		return err
	}
	return p.Delete(ctx, fromKey)
}

// sweepAll deletes the content id from every pool. Only releaseContent and
// sweepOrphans may call it: the caller must hold the content-id lock and
// have confirmed the resource row is gone, otherwise a concurrent
// re-registration of the same id (dedup) can have its fresh bytes deleted.
func (t *TieredStore) sweepAll(ctx context.Context, id string) {
	for _, p := range t.allPools() {
		if err := p.Delete(ctx, id); err != nil {
			log.Printf("tier: sweep %s %s: %v", p.Name(), id, err)
		}
	}
}

// releaseContent drops one reference of a content id and sweeps the bytes
// from every pool once the last reference is gone.
//
// The row is re-read and the sweep executed UNDER the content-id lock.
// Discovery background: the previous inline blocks decremented a row read
// under a DIFFERENT lock (the new content's) and swept without any lock, so
// a concurrent upload of the same bytes could re-register the id (fresh
// copy written to hot) while the sweep deleted "its" bytes — the new object
// 404'd forever, not even heal() could find the bytes. The lock serializes
// a re-registration behind our sweep; the re-read makes the decrement apply
// to the live row (a registration that slipped in earlier just bumps refs,
// and our decrement then lands correctly without sweeping).
func (t *TieredStore) releaseContent(ctx context.Context, id string) error {
	unlock := t.lockKey(id)
	defer unlock()

	t.mu.Lock()
	r, ok := t.res[id]
	if !ok {
		t.mu.Unlock()
		return nil
	}
	r.Refs--
	if r.Refs > 0 {
		err := t.upsertResLocked(id, r)
		t.mu.Unlock()
		return err
	}
	delete(t.res, id)
	if err := t.delResLocked(id); err != nil {
		t.mu.Unlock()
		return err
	}
	t.mu.Unlock()
	t.sweepAll(ctx, id)
	return nil
}

// reconcileRefs recomputes every resource's refcount from the name layer
// and repairs mismatches (in memory + DB). Called at startup, before any
// concurrent traffic. Discovery background: a crash between the name-layer
// delete and the content release leaves a phantom ref (refs>0 with no key);
// pre-fix bugs left inflated refs and refs=0 rows. Both make bytes
// unreachable-but-kept (disk leak) or, worse, keep sweeping away from
// correct releases. Exact recounting makes refs provably correct again;
// rows left at 0 become sweep candidates for sweepOrphans.
//
// refs=-1 rows (index rebuild) are deliberately skipped: their count is
// unknowable from the name layer — a first-boot rebuild has NO name rows,
// so counting would flatten every row to 0 and the next sweepOrphans
// would delete every rebuilt byte from every pool. Discovery background:
// 2026-08 review — the flattening was verified end-to-end (rebuild, then
// restart, then RunOnce: all bytes swept); the -1 marker survives only
// because Rebuild skips this pass, and it must survive a restart too.
func (t *TieredStore) reconcileRefs() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	counts := make(map[string]int, len(t.idx))
	for _, o := range t.idx {
		counts[o.ID]++
	}
	for id, r := range t.res {
		want := counts[id]
		if r.Refs == want || r.Refs < 0 {
			continue
		}
		log.Printf("tier: reconcile refs %s: %d -> %d", shortID(id), r.Refs, want)
		r.Refs = want
		if err := t.upsertResLocked(id, r); err != nil {
			return err
		}
	}
	return nil
}

// sweepOrphans reclaims bytes of resources with refs==0. Only the periodic
// scan calls it, and only a row that is STILL refs==0 under the content-id
// lock is swept — a concurrent registration of the same id either bumps the
// row before us (skipped) or blocks until our sweep finished. The content
// lock is held THROUGH sweepAll, exactly like releaseContent: dropping it
// before the deletes would let a registration land between the unlock and
// the completed sweep and have its fresh bytes deleted. Discovery
// background: 2026-08 review — a first version unlocked before sweepAll,
// opening that delete-sweep race on the orphan path (see
// TestReviewOrphanSweepRace). refs=-1 rows (index rebuild) are NEVER
// touched: their refcount is unknowable, and sweeping them would destroy
// the only copy of the bytes. Discovery background: code review — legacy/
// pre-fix states could leave refs=0 rows with bytes in the pools, and no
// mechanism reclaimed them.
func (t *TieredStore) sweepOrphans(ctx context.Context) {
	t.mu.Lock()
	orphans := make([]string, 0)
	for id, r := range t.res {
		if r.Refs == 0 {
			orphans = append(orphans, id)
		}
	}
	t.mu.Unlock()
	for _, id := range orphans {
		unlock := t.lockKey(id)
		t.mu.Lock()
		r, ok := t.res[id]
		if !ok || r.Refs != 0 {
			t.mu.Unlock()
			unlock()
			continue
		}
		delete(t.res, id)
		if err := t.delResLocked(id); err != nil {
			t.mu.Unlock()
			unlock()
			log.Printf("tier: orphan %s: %v", shortID(id), err)
			continue
		}
		t.mu.Unlock()
		t.sweepAll(ctx, id) // sweep under the content lock (see above)
		unlock()
		log.Printf("tier: swept orphan resource %s", shortID(id))
	}
}

// CopyObject (S3 CopyObject / the dedup fast path): the destination is a
// mapping insert referencing the source content id — zero bytes moved.
func (t *TieredStore) CopyObject(ctx context.Context, dstBucket, dstKey, srcBucket, srcKey string) (Entry, error) {
	fk := fullKey(dstBucket, dstKey)
	unlock := t.lockKey(fk)
	defer unlock()

	t.mu.Lock()
	src, ok := t.idx[fullKey(srcBucket, srcKey)]
	t.mu.Unlock()
	if !ok {
		return Entry{}, store.ErrNotFound
	}
	id := src.ID

	unlockRes := t.lockKey(id)
	// NOTE: no defer — released explicitly before the old-content release
	// (never hold two content locks at once).

	t.mu.Lock()
	cur, ok := t.res[id]
	if !ok {
		t.mu.Unlock()
		unlockRes()
		return Entry{}, store.ErrNotFound
	}
	now := t.now()
	oldID := ""
	if old, hasOld := t.idx[fk]; hasOld {
		oldID = old.ID
	}

	// Reference accounting, same rule as PutObject: only bump when the
	// destination key does not already reference this content
	// (oldID == id: identical overwrite/self-copy, net zero). Discovery
	// background: the old code bumped unconditionally and released the
	// old id inside the same critical section while holding the NEW id
	// lock — refcount inflation on self-copy plus an ABBA deadlock window
	// when two copies swap content.
	if oldID != id {
		// Same refs<0 promotion rule as PutObject: after an index rebuild
		// the refcount is unknowable (-1); this copy proves one reference.
		if cur.Refs < 0 {
			cur.Refs = 0
		}
		cur.Refs++
		if err := t.upsertResLocked(id, cur); err != nil {
			t.mu.Unlock()
			unlockRes()
			return Entry{}, err
		}
	}
	e := Entry{
		ID:           id,
		Pool:         cur.Pool,
		Size:         cur.Size,
		ETag:         cur.ETag,
		ContentType:  src.ContentType,
		StorageClass: src.StorageClass,
		Metadata:     src.Metadata,
		LastModified: now,
		LastAccess:   now,
	}
	o := &objRow{
		ID:           id,
		ContentType:  src.ContentType,
		StorageClass: src.StorageClass,
		Metadata:     src.Metadata,
		LastModified: now,
		LastAccess:   now,
	}
	if e.StorageClass == "" {
		e.StorageClass = "STANDARD"
		o.StorageClass = "STANDARD"
	}
	if e.ContentType == "" {
		e.ContentType = "application/octet-stream"
		o.ContentType = "application/octet-stream"
	}
	t.idx[fk] = o
	t.buckets[dstBucket] = now
	if err := t.upsertObjLocked(fk, o); err != nil {
		t.mu.Unlock()
		unlockRes()
		return Entry{}, err
	}
	t.mu.Unlock()

	// Drop the NEW id lock before releasing the OLD content (never hold
	// two content locks at once); releaseContent re-checks the map under
	// the old-id lock.
	unlockRes()
	if oldID != "" && oldID != id {
		if err := t.releaseContent(ctx, oldID); err != nil {
			return Entry{}, err
		}
	}
	return e, nil
}

// GetObject streams bytes of the resource behind a key. The per-key entry
// is resolved, the resource's pool serves the range; on a pool miss the
// index is healed by probing other pools (crashed migration recovery).
func (t *TieredStore) GetObject(ctx context.Context, bucket, key string, rng store.Range) (store.GetResult, Entry, error) {
	fk := fullKey(bucket, key)
	e, err := t.getEntry(fk)
	if err != nil {
		return store.GetResult{}, Entry{}, err
	}
	// The row's pool may have vanished from the config (pool removed while
	// the index still references it): nil-deref crash instead of a clean
	// error. Discovery background: 2026-08 review — reads had no guard,
	// unlike execCommand which checks `pool no longer configured`. The
	// heal probe below recovers such rows when the bytes exist elsewhere.
	pool, ok := t.pools[e.Pool]
	if !ok {
		return store.GetResult{}, e, fmt.Errorf("tier: pool %q no longer configured", e.Pool)
	}
	res, err := pool.Get(ctx, e.ID, rng)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return store.GetResult{}, e, err
		}
		healed, hErr := t.heal(ctx, e.ID)
		if hErr != nil {
			return store.GetResult{}, e, store.ErrNotFound
		}
		e.Pool = healed.Pool
		res, err = t.pools[healed.Pool].Get(ctx, e.ID, rng)
		if err != nil {
			return store.GetResult{}, e, err
		}
	}
	t.touch(fk, e.ID)
	promote := t.cfg.PromoteOnAccess
	if ov := t.ov.Load(); ov != nil && ov.PromoteOnAccess != nil {
		promote = *ov.PromoteOnAccess
	}
	if promote && e.Pool != t.cfg.Hot {
		// Detach from the request context: the client may hang up while
		// the read-through promotion is still copying, and a cancelled
		// promote never retries — the object would stay cold despite
		// PromoteOnAccess. Discovery background: code review — promote
		// inherited the request ctx, so a disconnect mid-transfer aborted
		// the copy every time.
		go t.promote(context.WithoutCancel(ctx), e.ID)
	}
	return res, e, nil
}

// HeadObject mirrors GetObject without a body.
func (t *TieredStore) HeadObject(ctx context.Context, bucket, key string) (Entry, error) {
	fk := fullKey(bucket, key)
	e, err := t.getEntry(fk)
	if err != nil {
		return e, err
	}
	// Same missing-pool guard as GetObject: a config change dropping a
	// pool must not nil-deref on the read path. Discovery background:
	// 2026-08 review.
	pool, ok := t.pools[e.Pool]
	if !ok {
		return e, fmt.Errorf("tier: pool %q no longer configured", e.Pool)
	}
	_, err = pool.Head(ctx, e.ID)
	if err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			return e, err
		}
		healed, hErr := t.heal(ctx, e.ID)
		if hErr != nil {
			return e, store.ErrNotFound
		}
		e.Pool = healed.Pool
	}
	t.touch(fk, e.ID)
	return e, nil
}

// DeleteObject removes the key (name layer) and releases its content
// (refcount--; bytes swept from every pool at zero).
func (t *TieredStore) DeleteObject(ctx context.Context, bucket, key string) error {
	fk := fullKey(bucket, key)
	unlock := t.lockKey(fk)
	defer unlock()

	t.mu.Lock()
	o, ok := t.idx[fk]
	if !ok {
		t.mu.Unlock()
		return nil // S3 delete is idempotent
	}
	id := o.ID
	delete(t.idx, fk)
	if err := t.delObjLocked(fk); err != nil {
		t.mu.Unlock()
		return err
	}
	t.mu.Unlock()

	// Name layer is gone; drop the content reference under the content-id
	// lock so the sweep cannot race a concurrent re-registration.
	return t.releaseContent(ctx, id)
}

func (t *TieredStore) getEntry(fk string) (Entry, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	o, ok := t.idx[fk]
	if !ok {
		return Entry{}, store.ErrNotFound
	}
	r, ok := t.res[o.ID]
	if !ok {
		// Mapping without content (lost resource row): surface as missing
		// and let reads heal or fail cleanly.
		return Entry{}, store.ErrNotFound
	}
	return Entry{
		ID:           o.ID,
		Pool:         r.Pool,
		Size:         r.Size,
		ETag:         r.ETag,
		ContentType:  o.ContentType,
		StorageClass: o.StorageClass,
		Metadata:     o.Metadata,
		LastModified: o.LastModified,
		LastAccess:   o.LastAccess,
	}, nil
}

// heal locates a content id in a pool other than the one the index records
// and repoints the resource row there. Called on a read miss after a crash
// interrupted a migration between index update and source deletion.
//
// Only the Pool field is flipped, and the live row is re-read under t.mu at
// write-back time. Discovery background: the old code copied the whole row
// lock-free and wrote the copy back wholesale — a concurrent PutObject
// bumping refs (or transfer/heal repointing) between copy and write-back
// got its change clobbered; a stale Refs=0 written back over a live object
// then made sweepOrphans destroy the bytes. This function deliberately
// does NOT take the content-id lock: it only mutates Pool under t.mu, and
// the same field is only ever touched under t.mu elsewhere.
func (t *TieredStore) heal(ctx context.Context, id string) (*resRow, error) {
	t.mu.Lock()
	cur, ok := t.res[id]
	t.mu.Unlock()
	if !ok {
		return nil, store.ErrNotFound
	}
	for _, p := range t.allPools() {
		if p.Name() == cur.Pool {
			continue
		}
		if _, err := p.Head(ctx, id); err == nil {
			t.mu.Lock()
			live, ok := t.res[id]
			if ok {
				live.Pool = p.Name()
				err := t.upsertResLocked(id, live)
				t.mu.Unlock()
				return live, err
			}
			t.mu.Unlock()
		}
	}
	return nil, store.ErrNotFound
}

// touch records a read access on the key and bubbles the max access time
// up to the content resource (dedup: any alias's access keeps the shared
// bytes warm). Write-through both rows.
func (t *TieredStore) touch(fk, id string) {
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	o, ok := t.idx[fk]
	if !ok {
		return
	}
	o.LastAccess = now
	if err := t.upsertObjLocked(fk, o); err != nil {
		log.Printf("tier: touch %s: %v", fk, err)
	}
	if r, ok := t.res[id]; ok && now.After(r.LastAccess) {
		r.LastAccess = now
		if err := t.upsertResLocked(id, r); err != nil {
			log.Printf("tier: touch res %s: %v", shortID(id), err)
		}
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

// ListObjects pages the name layer (the merged view across tiers — pools
// cannot group, and names only exist here).
func (t *TieredStore) ListObjects(ctx context.Context, p ListParams) (ListResult, error) {
	if p.MaxKeys <= 0 {
		p.MaxKeys = 1000
	}
	base := p.Bucket + "/"
	keyPrefix := base + p.Prefix
	var out ListResult

	t.mu.Lock()
	keys := make([]string, 0, len(t.idx))
	for fk := range t.idx {
		if strings.HasPrefix(fk, keyPrefix) {
			keys = append(keys, fk)
		}
	}
	sort.Strings(keys)
	t.mu.Unlock()

	emitted := 0
	lastEmitted := "" // deepest key consumed so far (monotonic)
	lastCP := ""      // previous grouped common prefix, dedupe consecutive
	truncated := false
	for _, fk := range keys {
		if p.StartAfter != "" && fk <= p.StartAfter {
			continue
		}
		// Prefix-stripped remainder; the emitted key keeps the prefix, the
		// common prefix is computed against the remainder after it.
		// Discovery background: grouping used to index the delimiter
		// inside the prefix itself, producing "dir/dir/"-style prefixes
		// and empty listings for subdirectories.
		rest := strings.TrimPrefix(fk, keyPrefix)
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
			if cp == p.Prefix+rest {
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
		e, err := t.getEntry(fk)
		if err != nil {
			continue
		}
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
	out.NextToken = lastEmitted
	return out, nil
}

// ---------------------------------------------------------------------------
// buckets

// ListBuckets returns every bucket the frontend created, oldest first.
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

// CreateBucket records the bucket and ensures the pools can accept writes
// (idempotent on the backends).
func (t *TieredStore) CreateBucket(ctx context.Context, bucket string) error {
	t.mu.Lock()
	_, exists := t.buckets[bucket]
	t.mu.Unlock()
	if exists {
		return store.ErrBucketExists
	}
	for _, p := range t.allPools() {
		if err := p.EnsureBucket(ctx, "data"); err != nil {
			return err
		}
	}
	t.mu.Lock()
	created := t.now()
	t.buckets[bucket] = created
	err := t.upBucketLocked(bucket, created)
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
	if !ok {
		t.mu.Unlock()
		return store.ErrNotFound
	}
	remaining := 0
	for fk := range t.idx {
		if b, _ := splitKey(fk); b == bucket {
			remaining++
		}
	}
	if remaining > 0 {
		t.mu.Unlock()
		return store.ErrNotEmpty
	}
	delete(t.buckets, bucket)
	err := t.delBucketLocked(bucket)
	t.mu.Unlock()
	return err
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
		// tens of GB before anyone noticed. Stops with ctx so a graceful
		// shutdown does not keep touching a closed pool.
		//
		// The age is NOT the scan interval: a .tmp is only cleared once it
		// has been untouched for tmpCleanupAge, and an in-flight upload's
		// .tmp keeps its mtime fresh as bytes stream in. Passing the
		// interval (the old behavior, discovered 2026-08 review) deleted
		// the staging file of any upload slower than one scan period
		// (multi-GB uploads over slow links finish well beyond 1h), killing
		// the transfer mid-body.
		go func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if err := local.RemoveStaleTemps(tmpCleanupAge); err != nil {
						log.Printf("tier: temp cleanup: %v", err)
					}
				}
			}
		}()
	}
	// Poller runs on its own goroutine — it is an endless loop and must
	// not block the migration loop below (a synchronous call here froze
	// RunOnce forever, caught by the auto-pause test).
	go t.pollControl(ctx)
	ticks := time.NewTicker(interval)
	defer ticks.Stop()
	// Log the pause state once per transition, not every tick.
	pausedLogged := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks.C:
			if !t.autoOn.Load() {
				if !pausedLogged {
					log.Printf("tier: auto migration paused by control (auto_enabled=0)")
					pausedLogged = true
				}
				continue
			}
			pausedLogged = false
			t.RunOnce()
		}
	}
}

// pollControl consumes external control input. Two triggers, no polling:
// filesystem events on the sentinel file (the admin process touches it
// after writing control/commands rows), and a slow stat fallback for lost
// events. The fallback is a stat(2) on the sentinel path — it never reads
// the DB, so a broken producer cannot cause hot-path DB traffic.
func (t *TieredStore) pollControl(ctx context.Context) {
	// Consume once on start: rows written before this process came up
	// (a paused maintenance window must survive a restart).
	t.consumeControl(ctx)
	t.consumeCommands(ctx)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("tier: sentinel watch unavailable, stat fallback only: %v", err)
		t.statFallback(ctx)
		return
	}
	defer watcher.Close()
	watcher.Add(t.sentinelPath)

	lastStat := int64(0)
	fallback := time.NewTicker(controlFallbackInterval)
	defer fallback.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-watcher.Events:
			if !ok {
				return
			}
			// Any event on the sentinel (create/write/chmod) means: an
			// admin wrote rows. Consume; repeated events just re-consume
			// (commands rows are deleted after execution, so this is
			// idempotent and cheap: zero rows = zero work).
			if ev.Name == t.sentinelPath {
				t.consumeControl(ctx)
				t.consumeCommands(ctx)
				lastStat = t.sentinelMtime()
			}
		case ev, ok := <-watcher.Errors:
			if !ok {
				return
			}
			// ev is the fs error; the out-of-scope `err` (from
			// NewWatcher) would print nil here. Discovery background:
			// code review — the log line referenced the wrong variable.
			log.Printf("tier: sentinel watch: %v", ev)
		case <-fallback.C:
			// stat-only safety net; catches touches the watcher missed.
			if m := t.sentinelMtime(); m != 0 && m != lastStat {
				t.consumeControl(ctx)
				t.consumeCommands(ctx)
				lastStat = m
			}
		}
	}
}

func (t *TieredStore) statFallback(ctx context.Context) {
	lastStat := int64(0)
	ticker := time.NewTicker(controlFallbackInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if m := t.sentinelMtime(); m != 0 && m != lastStat {
				t.consumeControl(ctx)
				t.consumeCommands(ctx)
				lastStat = m
			}
		}
	}
}

// sentinelMtime returns the sentinel file mtime in nanoseconds (0 when
// absent); a change is the "admin wrote rows" signal.
func (t *TieredStore) sentinelMtime() int64 {
	st, err := os.Stat(t.sentinelPath)
	if err != nil {
		return 0
	}
	return st.ModTime().UnixNano()
}

// controlFallbackInterval backs up the fs event path; stat-only, never a DB
// read.
const controlFallbackInterval = time.Minute

// tmpCleanupAge is how long a hot-pool .tmp file must sit untouched before
// the cleanup sweep removes it. Kept far above any realistic single-upload
// duration and well below the "leftovers accumulate" timescale: an
// interrupted PUT's staging file disappears within a day, a still-running
// multi-hour upload keeps its mtime fresh and survives. Discovery
// background: the sweep used to pass the scan interval, which could be
// minutes — every upload slower than that had its .tmp deleted mid-stream
// (2026-08 review).
const tmpCleanupAge = 24 * time.Hour

// consumeControl applies the control table to the running policy. The
// full table is re-read and a fresh overrides struct is published on every
// poll: DELETing a control row therefore reverts that knob to the config
// value immediately (fresh struct = nil fields = use Config).
func (t *TieredStore) consumeControl(ctx context.Context) {
	rows, err := t.db.QueryContext(ctx, `SELECT k, v FROM control`)
	if err != nil {
		log.Printf("tier: control read: %v", err)
		return
	}
	defer rows.Close()
	ov := &overrides{}
	// autoEnabledSeen tracks whether the table carries an auto_enabled
	// row: unlike the three overrides (whose absence means "use Config"
	// via the fresh struct), t.autoOn is a latch, so an absent row must
	// mean the config default (enabled) — otherwise `pause` followed by
	// `DELETE FROM control WHERE k='auto_enabled'` would stay paused
	// forever, contradicting the "delete reverts to default" contract.
	// Discovery background: found in the 2026-08 review.
	autoEnabledSeen := false
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			log.Printf("tier: control read: %v", err)
			return
		}
		switch k {
		case "auto_enabled":
			t.autoOn.Store(v == "1")
			autoEnabledSeen = true
		case "cold_after_ms":
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				d := time.Duration(ms) * time.Millisecond
				ov.ColdAfter = &d
			} else {
				log.Printf("tier: control cold_after_ms=%q: %v", v, err)
			}
		case "max_hot_bytes":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				ov.MaxHotBytes = &n
			} else {
				log.Printf("tier: control max_hot_bytes=%q: %v", v, err)
			}
		case "promote_on_access":
			b := v == "1"
			ov.PromoteOnAccess = &b
		default:
			log.Printf("tier: control: unknown key %q (ignored)", k)
		}
	}
	if err := rows.Err(); err != nil {
		log.Printf("tier: control read: %v", err)
		return
	}
	if !autoEnabledSeen {
		t.autoOn.Store(true)
	}
	t.ov.Store(ov)
}

// consumeCommands drains the commands queue, executing each verb exactly
// once. Commands are deleted after execution (success or failure — the log
// preserves the outcome; a failed command is not retried to avoid hanging
// the queue on a persistent error).
func (t *TieredStore) consumeCommands(ctx context.Context) {
	rows, err := t.db.QueryContext(ctx, `SELECT seq, verb, arg FROM commands ORDER BY seq`)
	if err != nil {
		log.Printf("tier: commands read: %v", err)
		return
	}
	var cmds []struct {
		seq  int64
		verb string
		arg  string
	}
	for rows.Next() {
		var c struct {
			seq  int64
			verb string
			arg  string
		}
		if err := rows.Scan(&c.seq, &c.verb, &c.arg); err != nil {
			rows.Close()
			log.Printf("tier: commands read: %v", err)
			return
		}
		cmds = append(cmds, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Printf("tier: commands read: %v", err)
		return
	}
	for _, c := range cmds {
		if err := t.execCommand(ctx, c.verb, c.arg); err != nil {
			log.Printf("tier: command %d (%s %q): %v", c.seq, c.verb, c.arg, err)
		}
		if _, err := t.db.ExecContext(ctx, `DELETE FROM commands WHERE seq = ?`, c.seq); err != nil {
			log.Printf("tier: cleanup command %d: %v", c.seq, err)
		}
	}
}

// execCommand runs one admin verb. arg is either "bucket/key" (name layer)
// or a bare content id (64 hex chars).
func (t *TieredStore) execCommand(ctx context.Context, verb, arg string) error {
	id, err := t.resolveCommandID(arg)
	if err != nil {
		return err
	}
	switch verb {
	case "migrate":
		t.mu.Lock()
		r, ok := t.res[id]
		t.mu.Unlock()
		if !ok {
			return store.ErrNotFound
		}
		if r.Pool != t.cfg.Hot {
			return fmt.Errorf("resource already in cold pool %q", r.Pool)
		}
		target := t.nextCold()
		if target == nil {
			return fmt.Errorf("no cold pool configured")
		}
		// transfer re-checks placement under the content lock, so a
		// concurrent auto-move yields a clean error instead of corruption.
		return t.transfer(ctx, id, t.hot(), target)
	case "promote":
		t.mu.Lock()
		r, ok := t.res[id]
		t.mu.Unlock()
		if !ok {
			return store.ErrNotFound
		}
		if r.Pool == t.cfg.Hot {
			return fmt.Errorf("resource already in hot pool")
		}
		from, ok := t.pools[r.Pool]
		if !ok {
			return fmt.Errorf("pool %q no longer configured", r.Pool)
		}
		return t.transfer(ctx, id, from, t.hot())
	default:
		return fmt.Errorf("unknown verb %q", verb)
	}
}

// resolveCommandID accepts "bucket/key" (looked up in the name layer) or a
// bare content id (trusted as-is; wrong ids fail later in transfer).
func (t *TieredStore) resolveCommandID(arg string) (string, error) {
	if strings.Contains(arg, "/") {
		t.mu.Lock()
		defer t.mu.Unlock()
		if o, ok := t.idx[arg]; ok {
			return o.ID, nil
		}
		return "", fmt.Errorf("key %q not found", arg)
	}
	return arg, nil
}

// runOnceTimeout bounds a single migration pass. Discovery background:
// code review — RunOnce used context.Background() everywhere, so a stuck
// cold pool (each request can take up to the configured per-request
// timeout) froze the migration loop for arbitrarily long; subsequent
// ticks queue behind it. The bound makes a slow pass give up early and
// resume next tick; in-flight transfers of the pass get cancelled.
const runOnceTimeout = 10 * time.Minute

// RunOnce evaluates the tiering policy once at the RESOURCE granularity:
// drains the hot pool down to the idle threshold and/or the byte quota,
// oldest-accessed first. Policy knobs come from the control overrides when
// present, else from Config (control rows are deleted to revert).
func (t *TieredStore) RunOnce() {
	// Bound the whole pass: a stuck cold pool must not freeze the
	// migration loop forever (see runOnceTimeout).
	ctx, cancel := context.WithTimeout(context.Background(), runOnceTimeout)
	defer cancel()

	// Reclaim bytes whose refcount reached 0 without a sweep (crash
	// windows, legacy states) before the migration pass below.
	t.sweepOrphans(ctx)

	ov := t.ov.Load()
	coldAfter := t.cfg.ColdAfter
	maxHot := t.cfg.MaxHotBytes
	if ov != nil {
		if ov.ColdAfter != nil {
			coldAfter = *ov.ColdAfter
		}
		if ov.MaxHotBytes != nil {
			maxHot = *ov.MaxHotBytes
		}
	}
	t.mu.Lock()
	now := t.now()
	var hotIDs []string
	var hotBytes int64
	for id, r := range t.res {
		if r.Pool != t.cfg.Hot {
			continue
		}
		hotIDs = append(hotIDs, id)
		hotBytes += r.Size
	}
	idle := make(map[string]bool)
	for _, id := range hotIDs {
		r := t.res[id]
		if coldAfter > 0 && now.Sub(r.LastAccess) >= coldAfter {
			idle[id] = true
		}
	}
	quota := make(map[string]bool)
	if maxHot > 0 {
		sort.Slice(hotIDs, func(i, j int) bool {
			ri, rj := t.res[hotIDs[i]], t.res[hotIDs[j]]
			if ri.LastAccess.Equal(rj.LastAccess) {
				return hotIDs[i] < hotIDs[j]
			}
			return ri.LastAccess.Before(rj.LastAccess)
		})
		for _, id := range hotIDs {
			if hotBytes <= maxHot {
				break
			}
			if idle[id] {
				continue // already scheduled via idle path
			}
			r := t.res[id]
			hotBytes -= r.Size
			quota[id] = true
		}
	}
	candidates := make([]string, 0, len(idle)+len(quota))
	for id := range idle {
		candidates = append(candidates, id)
	}
	for id := range quota {
		candidates = append(candidates, id)
	}
	sort.Strings(candidates)
	t.mu.Unlock()

	for _, id := range candidates {
		target := t.nextCold()
		if target == nil {
			log.Printf("tier: no cold pool configured, skip %s", shortID(id))
			return
		}
		from := t.hot()
		if err := t.transfer(ctx, id, from, target); err != nil {
			log.Printf("tier: migrate %s %s->%s: %v", shortID(id), from.Name(), target.Name(), err)
			continue
		}
		log.Printf("tier: moved %s %s -> %s", shortID(id), from.Name(), target.Name())
	}
}

// nextCold picks the drain target round-robin across all cold pools.
func (t *TieredStore) nextCold() store.Store {
	colds := t.colds()
	if len(colds) == 0 {
		return nil
	}
	i := t.rr.Add(1) - 1
	return colds[i%uint64(len(colds))]
}

// promote moves a cold resource back to hot after it was served,
// impersonating a cache read-through. Must NOT hold any key lock here:
// transfer() takes the content-id lock itself and lockKey is not reentrant
// (a second lock on the same id in this goroutine would deadlock forever —
// this exact bug shipped once; see git history of promote).
func (t *TieredStore) promote(ctx context.Context, id string) {
	t.mu.Lock()
	r, ok := t.res[id]
	t.mu.Unlock()
	if !ok || r.Pool == t.cfg.Hot {
		return
	}
	from, ok := t.pools[r.Pool]
	if !ok {
		// The row points at a pool the config no longer has; nothing to
		// move from. heal on the next read will repoint it. Discovery
		// background: 2026-08 review — a nil from here panicked transfer.
		log.Printf("tier: promote %s: pool %q no longer configured", shortID(id), r.Pool)
		return
	}
	if err := t.transfer(ctx, id, from, t.hot()); err != nil {
		log.Printf("tier: promote %s: %v", shortID(id), err)
	}
}

// transfer moves a resource between pools: copy, flip the resource row,
// drop the source. The row flip happens only after the target holds a
// complete copy, so readers always find valid bytes. A concurrent
// zero-refcount release serializes on the content-id lock; the re-check
// below prevents moving a resource that was deleted meanwhile.
func (t *TieredStore) transfer(ctx context.Context, id string, from, to store.Store) error {

	t.mu.Lock()
	r, ok := t.res[id]
	t.mu.Unlock()
	if !ok || r.Pool != from.Name() {
		return nil // gone or already moved
	}

	if err := to.EnsureBucket(ctx, "data"); err != nil {
		return err
	}
	res, err := from.Get(ctx, id, store.Range{Start: 0, End: -1})
	if err != nil {
		return err
	}
	defer res.Body.Close()
	info, err := to.Put(ctx, id, res.Body, res.Info.Size, res.Info.ContentType, store.PutOptions{StorageClass: res.Info.StorageClass})
	if err != nil {
		return err
	}

	unlock := t.lockKey(id)
	defer unlock()
	// Re-check under the id lock: a concurrent PutObject registered a NEW
	// resource for the same id, or the resource was released; do not
	// repoint the row or delete the fresh copy.
	t.mu.Lock()
	cur, ok := t.res[id]
	if !ok || cur.Pool != from.Name() {
		t.mu.Unlock()
		// Leave the stray copy at `to`; the next zero-refcount sweep
		// removes it, or a future transfer finds it via heal.
		return nil
	}
	cur.Pool = to.Name()
	if info.Size > 0 {
		cur.Size = info.Size
	}
	if info.ETag != "" {
		cur.ETag = info.ETag
	}
	err = t.upsertResLocked(id, cur)
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return from.Delete(ctx, id)
}
