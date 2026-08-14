package tier

// Regression tests for bugs found during the 2026-08 review. Each test
// documents how the bug was discovered (E2E debugging / code review) and
// what fix it protects — see the per-test Discovery background comments.

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"s3proxy/internal/store"
)

// blockStore wraps a MemStore and parks every Delete of a specific key
// until the test releases it. Lets a test hold a sweep mid-flight while
// another goroutine runs, to force the interleaving that lost data.
type blockStore struct {
	*store.MemStore
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockStore) Delete(ctx context.Context, key string) error {
	if key == b.key {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.MemStore.Delete(ctx, key)
}

func newBlockedTier(t *testing.T) (*TieredStore, *blockStore) {
	t.Helper()
	blocked := &blockStore{
		MemStore: store.NewMem("hot"),
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	tr, err := New([]store.Store{blocked}, Config{Hot: "hot"}, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })
	return tr, blocked
}

// TestReviewDeleteSweepRace — content re-registered concurrently with a
// refcount-zero sweep must not have its fresh bytes deleted.
//
// Discovery background: code review of the refcount release paths. The old
// DeleteObject decremented refs and then swept the pools WITHOUT holding
// the content-id lock (it unlocked before sweepAll), so a concurrent PUT of
// the same bytes re-registered the id (fresh copy written to hot) while the
// sweep was still deleting "its" bytes — the new object 404'd forever (even
// heal() could not find the bytes anywhere). Fix: sweep under the
// content-id lock via releaseContent, which also re-reads the row.
func TestReviewDeleteSweepRace(t *testing.T) {
	tr, blocked := newBlockedTier(t)
	e := put(t, tr, "bkt", "k1", "shared-bytes")
	blocked.key = e.ID

	// G1 deletes the only reference; its sweep parks inside pool.Delete.
	var wg sync.WaitGroup
	var g1err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		g1err = tr.DeleteObject(context.Background(), "bkt", "k1")
	}()
	<-blocked.entered

	// G2 re-uploads the same bytes under another key while the sweep is
	// mid-flight — the dangerous interleaving.
	putDone := make(chan error, 1)
	go func() {
		_, err := tr.PutObject(context.Background(), "bkt", "k2", strings.NewReader("shared-bytes"), int64(len("shared-bytes")), PutOpts{})
		putDone <- err
	}()
	// A fast completion means G2 registered during the sweep window (the
	// bug); a timeout means it is parked on the content lock (fixed).
	got := false
	var g2err error
	select {
	case g2err = <-putDone:
		got = true
	case <-time.After(200 * time.Millisecond):
	}
	close(blocked.release)
	wg.Wait()
	if !got {
		g2err = <-putDone
	}
	if g1err != nil || g2err != nil {
		t.Fatalf("g1=%v g2=%v", g1err, g2err)
	}
	if getBody(t, tr, "bkt", "k2") != "shared-bytes" {
		t.Fatal("sweep deleted the concurrently re-registered bytes")
	}
}

// TestReviewOverwriteSweepRace — same race through the overwrite path:
// PUT replacing a key's content releases the old id (refs 1->0) and sweeps
// it while a concurrent PUT of the old bytes registers a fresh copy.
//
// Discovery background: same code review as TestReviewDeleteSweepRace. The
// old PutObject swept the old content while holding the NEW content's lock
// (no protection for the old id at all), so a concurrent re-upload of the
// old bytes could be swept from under its resource row.
func TestReviewOverwriteSweepRace(t *testing.T) {
	tr, blocked := newBlockedTier(t)
	e := put(t, tr, "bkt", "k1", "shared-bytes")
	blocked.key = e.ID

	var wg sync.WaitGroup
	var g1err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Overwrite k1 with different content; releases "shared-bytes".
		_, g1err = tr.PutObject(context.Background(), "bkt", "k1", strings.NewReader("replacement-content"), int64(len("replacement-content")), PutOpts{})
	}()
	<-blocked.entered

	putDone := make(chan error, 1)
	go func() {
		_, err := tr.PutObject(context.Background(), "bkt", "k2", strings.NewReader("shared-bytes"), int64(len("shared-bytes")), PutOpts{})
		putDone <- err
	}()
	got := false
	var g2err error
	select {
	case g2err = <-putDone:
		got = true
	case <-time.After(200 * time.Millisecond):
	}
	close(blocked.release)
	wg.Wait()
	if !got {
		g2err = <-putDone
	}
	if g1err != nil || g2err != nil {
		t.Fatalf("g1=%v g2=%v", g1err, g2err)
	}
	if getBody(t, tr, "bkt", "k2") != "shared-bytes" {
		t.Fatal("overwrite's sweep deleted the re-registered duplicate")
	}
}

// TestReviewIdenticalReputDoesNotInflateRefs — PUTting identical bytes
// over the same key replaces the reference instead of adding one.
//
// Discovery background: code review of the refcount bookkeeping while
// fixing the sweep race. The old code incremented refs unconditionally and
// skipped the release only when oldID == id, so every identical re-PUT
// leaked +1 refcount and the content was never swept after the final
// delete (silent orphan bytes). Fix: the increment is skipped when the
// key's previous content is the same id (net zero).
func TestReviewIdenticalReputDoesNotInflateRefs(t *testing.T) {
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "same")
	put(t, tier, "bkt", "k", "same")
	tier.mu.Lock()
	r := tier.res[e.ID]
	tier.mu.Unlock()
	if r.Refs != 1 {
		t.Fatalf("refs = %d, want 1 (identical re-put must not add a reference)", r.Refs)
	}
	if err := tier.DeleteObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := hot.Head(context.Background(), e.ID); err != store.ErrNotFound {
		t.Fatalf("bytes not swept after delete: %v", err)
	}
}

// TestReviewSelfCopyDoesNotInflateRefs — copying an object onto itself
// (rclone's copy-of-self) must keep refcount 1.
//
// Discovery background: code review, same refcount accounting bug as
// TestReviewIdenticalReputDoesNotInflateRefs, via the CopyObject path. The
// comment in the api layer claimed self-copy is "idempotent in refcount
// terms" but the tier incremented anyway.
func TestReviewSelfCopyDoesNotInflateRefs(t *testing.T) {
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "data")
	dst, err := tier.CopyObject(context.Background(), "bkt", "k", "bkt", "k")
	if err != nil {
		t.Fatal(err)
	}
	if dst.ID != e.ID {
		t.Fatalf("self-copy diverged: %s vs %s", dst.ID, e.ID)
	}
	tier.mu.Lock()
	r := tier.res[e.ID]
	tier.mu.Unlock()
	if r.Refs != 1 {
		t.Fatalf("refs = %d, want 1 after self-copy", r.Refs)
	}
	if err := tier.DeleteObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := hot.Head(context.Background(), e.ID); err != store.ErrNotFound {
		t.Fatalf("bytes not swept after delete: %v", err)
	}
}

// TestReviewLockTableReclaimsIdleEntries — the content/name lock table must
// not keep one mutex per key ever touched.
//
// Discovery background: code review of TieredStore.keyLocks (sync.Map,
// entries never removed). A long-running proxy accumulated a live mutex per
// unique key/content id, unbounded memory growth. Fix: refcounted lockTable
// removes entries once nobody holds or waits on them.
func TestReviewLockTableReclaimsIdleEntries(t *testing.T) {
	lt := &lockTable{locks: make(map[string]*lockEntry)}
	for i := 0; i < 100; i++ {
		unlock := lt.lock(fmt.Sprintf("key-%d", i))
		unlock()
	}
	if n := len(lt.locks); n >= 10 {
		t.Fatalf("lock table retained %d idle entries", n)
	}
	unlock := lt.lock("key-0")
	unlock()
	if n := len(lt.locks); n != 0 {
		t.Fatalf("re-lock leaves %d entries, want 0 (reclaimed again)", n)
	}
}

// TestReviewLockTableConcurrent — the refcounted table must stay correct
// under concurrent lock/unlock of one name (removal must never race a
// waiter, or two goroutines would end up on different mutexes).
//
// Discovery background: written while implementing the lockTable that
// TestReviewLockTableReclaimsIdleEntries requires (the fix itself); no
// pre-existing bug drove it. The danger is specific to the refcounted
// design: an unlock that removes the map entry while another goroutine is
// about to block on that entry's mutex would let a third goroutine create a
// NEW entry for the same name — two goroutines "holding" the same lock on
// different mutexes. The refs-before-block ordering inside lock() prevents
// this; the test would fail loudly under -race if it regressed.
func TestReviewLockTableConcurrent(t *testing.T) {
	lt := &lockTable{locks: make(map[string]*lockEntry)}
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				unlock := lt.lock("shared-name")
				unlock()
			}
		}()
	}
	wg.Wait()
	if n := len(lt.locks); n != 0 {
		t.Fatalf("left %d entries after concurrent use", n)
	}
}

// TestReviewOrphanSweep — a resource whose refcount reached 0 without a
// sweep (crash between the name delete and the content release, or a
// legacy pre-fix state) must have its bytes reclaimed by the periodic scan.
//
// Discovery background: code review while fixing the sweep race. DeleteObject
// and the overwrite path run the name-layer delete and the content release
// as two steps; a crash in between leaves refs=0 (or inflated refs) rows
// with live bytes in the pools and no mechanism ever reclaimed them. Fix:
// sweepOrphans runs inside RunOnce and reclaims rows that are STILL refs=0
// under the content-id lock (a concurrent registration bumps refs and is
// skipped; refs=-1 rebuild rows are never touched).
func TestReviewOrphanSweep(t *testing.T) {
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "orphan-me")

	// Simulate the crash window: name row gone, resource row stuck at
	// refs=0, bytes still in the pool.
	tier.mu.Lock()
	delete(tier.idx, "bkt/k")
	if err := tier.delObjLocked("bkt/k"); err != nil {
		tier.mu.Unlock()
		t.Fatal(err)
	}
	r := tier.res[e.ID]
	r.Refs = 0
	if err := tier.upsertResLocked(e.ID, r); err != nil {
		tier.mu.Unlock()
		t.Fatal(err)
	}
	tier.mu.Unlock()

	tier.RunOnce()
	if _, err := hot.Head(context.Background(), e.ID); err != store.ErrNotFound {
		t.Fatalf("orphan bytes not reclaimed: %v", err)
	}
	tier.mu.Lock()
	_, still := tier.res[e.ID]
	tier.mu.Unlock()
	if still {
		t.Fatal("orphan resource row survived")
	}
}

// TestReviewReconcileRefs — at startup the refcounts must be rebuilt from
// the name layer: inflated refs (crash window) are corrected, and rows with
// no key references fall to 0 where the scan reclaims them.
//
// Discovery background: companion to TestReviewOrphanSweep. A crash after
// the name delete but before the refcount decrement leaves refs>0 with no
// key — bytes kept forever despite being unreachable. The reconcile pass
// recomputes exact counts from the name layer before the engine serves
// traffic.
func TestReviewReconcileRefs(t *testing.T) {
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "shared")
	put(t, tier, "bkt", "k2", "shared") // second reference: refs == 2

	// Inflate the refcount as a crash would leave it.
	tier.mu.Lock()
	r := tier.res[e.ID]
	r.Refs = 5
	if err := tier.upsertResLocked(e.ID, r); err != nil {
		tier.mu.Unlock()
		t.Fatal(err)
	}
	// A phantom resource with no keys at all.
	ghost := &resRow{Pool: "hot", Refs: 3, Size: 4, ETag: `"x"`, LastModified: tier.now(), LastAccess: tier.now()}
	tier.res["ghost-id"] = ghost
	if err := tier.upsertResLocked("ghost-id", ghost); err != nil {
		tier.mu.Unlock()
		t.Fatal(err)
	}
	tier.mu.Unlock()

	if err := tier.reconcileRefs(); err != nil {
		t.Fatal(err)
	}

	tier.mu.Lock()
	r = tier.res[e.ID]
	ghostRefs := -1
	if g, ok := tier.res["ghost-id"]; ok {
		ghostRefs = g.Refs
	}
	tier.mu.Unlock()
	if r.Refs != 2 {
		t.Fatalf("refs = %d, want 2 after reconcile", r.Refs)
	}
	if ghostRefs != 0 {
		t.Fatalf("phantom resource refs = %d, want 0 (sweep candidate)", ghostRefs)
	}

	tier.RunOnce()
	if _, err := hot.Head(context.Background(), "ghost-id"); err != store.ErrNotFound {
		t.Fatalf("phantom bytes not reclaimed: %v", err)
	}
}

// TestReviewRebuildKeepsOrphanBytes — after an index rebuild the refcount
// is unknowable (refs=-1); the scan must NOT reclaim those bytes, and a
// later upload of the same content must promote the row to refs=1.
//
// Discovery background: designed while adding refs=-1. The rebuild path
// loses the name layer; if the scan treated rebuilt rows as refs=0 it would
// destroy the only copy of every object's bytes. The -1 marker keeps them
// until a real reference is proven (re-upload) or the row is legitimately
// released.
func TestReviewRebuildKeepsOrphanBytes(t *testing.T) {
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "precious")

	if err := tier.Rebuild(); err != nil {
		t.Fatal(err)
	}
	tier.mu.Lock()
	r := tier.res[e.ID]
	tier.mu.Unlock()
	if r.Refs != -1 {
		t.Fatalf("refs = %d after rebuild, want -1", r.Refs)
	}

	// Scan must not touch refs=-1 rows.
	tier.RunOnce()
	if _, err := hot.Head(context.Background(), e.ID); err != nil {
		t.Fatalf("rebuild bytes swept by scan: %v", err)
	}

	// Re-uploading the same content proves one reference.
	put(t, tier, "bkt2", "k", "precious")
	tier.mu.Lock()
	r = tier.res[e.ID]
	tier.mu.Unlock()
	if r.Refs != 1 {
		t.Fatalf("refs = %d after re-upload, want 1", r.Refs)
	}
	if err := tier.DeleteObject(context.Background(), "bkt2", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := hot.Head(context.Background(), e.ID); err != store.ErrNotFound {
		t.Fatalf("bytes not swept after legit release: %v", err)
	}
}

// TestReviewRebuildMarkerSurvivesRestart — the refs=-1 marker written by a
// first-boot rebuild must survive a restart; the startup reconcile pass
// must not flatten it to 0, or the first RunOnce treats every rebuilt byte
// as an orphan and deletes it.
//
// Discovery background: found during the 2026-08 review. Boot 1 has pool
// bytes but no state db (first boot pointing at pre-existing buckets, or a
// wiped state dir): New() rebuilds the content layer, persisting refs=-1
// with an empty name layer. Boot 2 loads fine and reconcileRefs runs —
// with no name rows, counts[id]=0 rewrote -1 to 0, and the next RunOnce
// swept every rebuilt byte from every pool (verified by reproduction
// before the fix). The marker's contract is "unknowable: never sweep until
// a real reference is proven", which only survived in-process because
// Rebuild skips reconcile; the restart path broke it. Fix: reconcile skips
// negative refcounts.
func TestReviewRebuildMarkerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tier.db")
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")

	// Pre-populate the pools as a previous/migrated cluster would: bytes
	// exist, no state db on this host.
	h := contentID()
	h.Write([]byte("precious"))
	id := hex.EncodeToString(h.Sum(nil))
	if _, err := hot.Put(context.Background(), id, strings.NewReader("precious"), 8, "application/octet-stream", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	// Boot 1: no db -> rebuild from pools, refs=-1, name layer empty.
	tr, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	tr.mu.Lock()
	r, ok := tr.res[id]
	tr.mu.Unlock()
	if !ok {
		t.Fatal("rebuild did not find the pool bytes")
	}
	if r.Refs != -1 {
		t.Fatalf("refs after rebuild = %d, want -1", r.Refs)
	}
	tr.Close()

	// Boot 2: same pools, same state dir; reconcileRefs runs here.
	tr2, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer tr2.Close()
	tr2.mu.Lock()
	r, ok = tr2.res[id]
	tr2.mu.Unlock()
	if !ok {
		t.Fatal("resource row lost after restart")
	}
	if r.Refs != -1 {
		t.Fatalf("refs after restart = %d, want -1 (unknowable preserved)", r.Refs)
	}
	tr2.RunOnce()
	if _, err := hot.Head(context.Background(), id); err != nil {
		t.Fatalf("post-restart sweep deleted rebuilt bytes: %v", err)
	}
}

// TestReviewOrphanSweepRace — a resource swept by the periodic orphan scan
// must not have its bytes raced by a concurrent re-registration: the sweep
// holds the content-id lock through the pool deletes, so a PutObject of
// identical bytes either registers before the sweep (refs 0->1, skipped)
// or parks until the sweep is done.
//
// Discovery background: 2026-08 review — sweepOrphans released the
// content-id lock BEFORE sweeping the pools (only releaseContent kept it
// through the sweep), so an identical-content upload landing between the
// unlock and the completed sweepAll had its fresh copy deleted — the same
// delete-sweep race TestReviewDeleteSweepRace guards on the DeleteObject
// path, reachable here through RunOnce whenever an orphan row exists
// (crash window / legacy state). Fix: the sweep runs under the lock.
func TestReviewOrphanSweepRace(t *testing.T) {
	tr, blocked := newBlockedTier(t)
	e := put(t, tr, "bkt", "k", "shared-bytes")

	// Simulate the crash window that creates an orphan: name row gone,
	// resource row stuck at refs=0, bytes still in the pool.
	tr.mu.Lock()
	delete(tr.idx, "bkt/k")
	if err := tr.delObjLocked("bkt/k"); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	tr.res[e.ID].Refs = 0
	if err := tr.upsertResLocked(e.ID, tr.res[e.ID]); err != nil {
		tr.mu.Unlock()
		t.Fatal(err)
	}
	tr.mu.Unlock()

	blocked.key = e.ID
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tr.RunOnce()
	}()
	<-blocked.entered

	// Concurrent re-registration of the same bytes under a new key while
	// the orphan sweep is mid-delete: with the fix it parks on the
	// content-id lock; before the fix it registered and its fresh copy
	// was then swept away.
	putDone := make(chan error, 1)
	go func() {
		_, err := tr.PutObject(context.Background(), "bkt", "k2", strings.NewReader("shared-bytes"), int64(len("shared-bytes")), PutOpts{})
		putDone <- err
	}()
	got := false
	var g2err error
	select {
	case g2err = <-putDone:
		got = true
	case <-time.After(200 * time.Millisecond):
	}
	close(blocked.release)
	wg.Wait()
	if !got {
		g2err = <-putDone
	}
	if g2err != nil {
		t.Fatalf("re-registration: %v", g2err)
	}
	if getBody(t, tr, "bkt", "k2") != "shared-bytes" {
		t.Fatal("orphan sweep deleted the concurrently re-registered bytes")
	}
	if _, err := tr.hot().Head(context.Background(), e.ID); err != nil {
		t.Fatalf("bytes missing after orphan sweep + re-registration: %v", err)
	}
}

// headBlockStore wraps a MemStore and parks every Head of a specific key
// until the test releases it. Lets a test hold heal() mid-probe while
// another goroutine runs.
type headBlockStore struct {
	*store.MemStore
	key     string
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *headBlockStore) Head(ctx context.Context, key string) (store.ObjectInfo, error) {
	if key == b.key {
		b.once.Do(func() { close(b.entered) })
		<-b.release
	}
	return b.MemStore.Head(ctx, key)
}

// TestReviewHealDoesNotClobberRefcount — heal repointing a resource row
// must not overwrite a concurrent refcount change.
//
// Discovery background: race analysis during code review. heal() copied
// the whole resource row lock-free and wrote the copy back wholesale; a
// concurrent PutObject (dedup hit) bumping Refs between copy and
// write-back had its increment clobbered — a stale Refs=0 written back
// over a live object then let sweepOrphans destroy the only bytes. The
// regression test drives heal (parked mid-probe) concurrently with a dedup
// PUT and asserts the refcount and bytes survive; under -race the old
// lock-free copy/write-back is itself a detected race. Fix: heal re-reads
// the live row under t.mu and flips only the Pool field.
func TestReviewHealDoesNotClobberRefcount(t *testing.T) {
	hot := store.NewMem("hot")
	cold := &headBlockStore{
		MemStore: store.NewMem("cold"),
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
	tr, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tr.Close() })

	e := put(t, tr, "bkt", "k1", "data")
	// Simulate an interrupted migration: the index says hot, but the bytes
	// only exist in cold — the exact read pattern that triggers heal.
	if err := hot.Delete(context.Background(), e.ID); err != nil {
		t.Fatal(err)
	}
	cold.key = e.ID
	if _, err := cold.Put(context.Background(), e.ID, strings.NewReader("data"), int64(4), "application/octet-stream", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}

	// G1 reads k1: hot miss -> heal probes the other pools -> parks inside
	// cold.Head.
	var g1err error
	g1done := make(chan struct{})
	go func() {
		defer close(g1done)
		_, _, g1err = tr.GetObject(context.Background(), "bkt", "k1", store.Range{Start: 0, End: -1})
	}()
	<-cold.entered

	// G2 registers the same content under another key (dedup refs++) while
	// heal is mid-probe — the interleaving that used to lose the increment.
	put(t, tr, "bkt", "k2", "data")

	close(cold.release)
	<-g1done
	if g1err != nil {
		t.Fatal(g1err)
	}

	tr.mu.Lock()
	r := tr.res[e.ID]
	tr.mu.Unlock()
	if r.Refs != 2 {
		t.Fatalf("refs = %d after heal+dedup, want 2 (refcount clobbered)", r.Refs)
	}
	// The bytes live in cold in this scenario (hot lost them in the
	// simulated interrupted migration); they must still be there.
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatalf("bytes missing after heal: %v", err)
	}
}

// TestReviewConcurrentOverwriteSameKey — two concurrent PUTs of the same
// key must serialize on the key lock at REGISTRATION (not during the body
// stream) and end with exactly one mapping, correct refcounts, no leaked
// content.
//
// Discovery background: code review — the key lock used to wrap the whole
// body upload, serializing concurrent overwrites of one key for the entire
// transfer duration (a slow client's multi-GB PUT blocked every other
// write to that key). The lock was narrowed to the registration section.
// This test hammers the narrowed locking under -race: both writers stream
// in parallel, register one after the other, and the loser's content is
// released exactly once.
func TestReviewConcurrentOverwriteSameKey(t *testing.T) {
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	put(t, tier, "bkt", "k", "initial")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("writer-%d", i)
			_, errs[i] = tier.PutObject(context.Background(), "bkt", "k", strings.NewReader(body), int64(len(body)), PutOpts{})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("writer %d: %v", i, err)
		}
	}

	tier.mu.Lock()
	defer tier.mu.Unlock()
	o, ok := tier.idx["bkt/k"]
	if !ok {
		t.Fatal("mapping lost")
	}
	r, ok := tier.res[o.ID]
	if !ok || r.Refs != 1 {
		t.Fatalf("winner content refs = %+v, want 1", r)
	}
	if len(tier.res) != 1 {
		t.Fatalf("resources left: %d, want 1 (loser/initial content leaked)", len(tier.res))
	}
}

// TestReviewRoundRobinConcurrent — the cold-pool round-robin cursor must
// be safe under concurrent advance from the migration loop (RunOnce) and
// the external-command consumer (execCommand), who share it in production.
//
// Discovery background: 2026-08 review — t.rr was a plain uint64
// read-modified outside any critical section by nextCold(), called from
// both goroutines: an unsynchronized data race (benign in effect — a cold
// pool picked twice — but flagged by -race). Fix: atomic.Uint64. The test
// hammers nextCold concurrently; with the old field it fails under -race.
func TestReviewRoundRobinConcurrent(t *testing.T) {
	hot := store.NewMem("hot")
	coldA, coldB, coldC := store.NewMem("cold-a"), store.NewMem("cold-b"), store.NewMem("cold-c")
	tier, err := New([]store.Store{hot, coldA, coldB, coldC},
		Config{Hot: "hot", Cold: []string{"cold-a", "cold-b", "cold-c"}}, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tier.Close() })
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if tier.nextCold() == nil {
					t.Error("nextCold returned nil")
					return
				}
			}
		}()
	}
	wg.Wait()
	// 8*500 picks consumed; the atomic counter guarantees the next index
	// is exactly 4000 % 3 = 1 -> cold-b (a racy counter could derail it).
	if got := tier.nextCold().Name(); got != "cold-b" {
		t.Fatalf("round-robin drift: %q, want cold-b", got)
	}
}

// TestReviewControlAutoEnabledDeleteRestoresDefault — deleting the
// auto_enabled control row must resume the auto migration loop (the config
// default), not leave it latched at the last written value.
//
// Discovery background: 2026-08 review — the control-plane contract says
// "DELETE FROM control reverts a knob to its config default", but
// autoOn was only ever assigned while the row existed, so `pause` (row=0)
// followed by a row delete stayed paused forever. The other three knobs
// reverted because the overrides struct is rebuilt fresh each consume;
// autoOn is a latch and needed the same semantics. Fix: consumeControl
// resets autoOn to true when the table carries no auto_enabled row.
func TestReviewControlAutoEnabledDeleteRestoresDefault(t *testing.T) {
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	if _, err := tier.db.Exec(`INSERT INTO control (k, v) VALUES ('auto_enabled', '0')`); err != nil {
		t.Fatal(err)
	}
	tier.consumeControl(context.Background())
	if tier.autoOn.Load() {
		t.Fatal("auto migration still on after pause row")
	}
	if _, err := tier.db.Exec(`DELETE FROM control WHERE k = 'auto_enabled'`); err != nil {
		t.Fatal(err)
	}
	tier.consumeControl(context.Background())
	if !tier.autoOn.Load() {
		t.Fatal("auto migration stayed paused after control row deleted")
	}
}

// TestReviewMissingPoolReadsFailCleanly — a read whose resource row points
// at a pool that is no longer configured must return an error, never
// nil-deref on t.pools.
//
// Discovery background: 2026-08 review — GetObject/HeadObject/promote
// called methods on t.pools[e.Pool] without checking existence, while
// execCommand already had the "pool no longer configured" check. After a
// config change dropping a pool, every read of its leftover rows panicked
// (net/http recovers per-request by dropping the connection). Fix: explicit
// missing-pool errors on the read paths.
func TestReviewMissingPoolReadsFailCleanly(t *testing.T) {
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	put(t, tier, "bkt", "k", "data")
	delete(tier.pools, "hot") // simulate a config that removed the pool
	if _, _, err := tier.GetObject(context.Background(), "bkt", "k", store.Range{Start: 0, End: -1}); err == nil {
		t.Fatal("GET of a row pointing at a missing pool succeeded")
	}
	if _, err := tier.HeadObject(context.Background(), "bkt", "k"); err == nil {
		t.Fatal("HEAD of a row pointing at a missing pool succeeded")
	}
	// Restore so the test's Close() can release every pool without
	// nil-dereffing the removed entry (t.allPools walks the config names).
	tier.pools["hot"] = store.NewMem("hot")
}

// TestReviewCorrectMD5HexAccepted — a PutObject whose MD5Hex matches the
// streamed bytes must succeed; a mismatching one must fail without touching
// the previous object.
//
// Discovery background: same review as the api-level BadDigest tests. The
// tier compared MD5Hex (32 hex chars) against its own SHA-256 content id
// (64 hex chars) — never equal, so EVERY upload carrying a Content-MD5 was
// refused with ErrBadDigest, correct digest or not. Fix: the MD5 of the
// streamed bytes is computed in parallel (second tee) and compared against.
func TestReviewCorrectMD5HexAccepted(t *testing.T) {
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	if err := tier.CreateBucket(context.Background(), "bkt"); err != nil {
		t.Fatal(err)
	}

	body := "correct-md5"
	sum := md5.Sum([]byte(body))
	good := hex.EncodeToString(sum[:])
	e, err := tier.PutObject(context.Background(), "bkt", "k", strings.NewReader(body), int64(len(body)), PutOpts{MD5Hex: good})
	if err != nil {
		t.Fatalf("CORRECT MD5Hex rejected: %v", err)
	}
	if got := getBody(t, tier, "bkt", "k"); got != body {
		t.Fatalf("stored bytes mismatch: %q", got)
	}

	// A wrong digest on an overwrite must leave the old object intact.
	bad := hex.EncodeToString(make([]byte, 16))
	_, err = tier.PutObject(context.Background(), "bkt", "k", strings.NewReader("other"), int64(5), PutOpts{MD5Hex: bad})
	if !errors.Is(err, ErrBadDigest) {
		t.Fatalf("wrong MD5Hex: got %v, want ErrBadDigest", err)
	}
	if got := getBody(t, tier, "bkt", "k"); got != body {
		t.Fatalf("old object lost after bad-digest overwrite: %q", got)
	}
	if e.ID == "" {
		t.Fatal("empty content id")
	}
}
