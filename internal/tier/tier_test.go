package tier

// Tier engine tests over mem pools. Discovery backgrounds are documented
// per test; see tier.go for the two-layer (name + content) design.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"s3proxy/internal/store"
)

// clock is a test time source; injected via SetNow so idle/quota policy
// decisions are deterministic.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestTier builds hot+cold mem pools and a New() tier whose time is
// pinned by the returned clock. statePath is per-test (fresh DB).
func newTestTier(t *testing.T, cfg Config) (*TieredStore, *store.MemStore, *store.MemStore, *clock) {
	t.Helper()
	clk := &clock{t: time.Now()}
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	tier, err := New([]store.Store{hot, cold}, cfg, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	tier.SetNow(clk.now)
	t.Cleanup(func() { tier.Close() })
	return tier, hot, cold, clk
}

func put(t *testing.T, tier *TieredStore, bucket, key, body string) Entry {
	t.Helper()
	e, err := tier.PutObject(context.Background(), bucket, key, strings.NewReader(body), int64(len(body)), PutOpts{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func getBody(t *testing.T, tier *TieredStore, bucket, key string) string {
	t.Helper()
	res, _, err := tier.GetObject(context.Background(), bucket, key, store.Range{Start: 0, End: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustNotExist(t *testing.T, tier *TieredStore, bucket, key string) {
	t.Helper()
	if _, err := tier.HeadObject(context.Background(), bucket, key); err != store.ErrNotFound {
		t.Fatalf("expected %s/%s to be gone, got %v", bucket, key, err)
	}
}

func TestWritesLandInHot(t *testing.T) {
	// The buffer contract: every write ends up in the hot pool and the
	// resource row records it.
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "a", "hello")
	if e.Pool != "hot" {
		t.Fatalf("write landed in %q", e.Pool)
	}
	if _, err := hot.Head(context.Background(), e.ID); err != nil {
		t.Fatalf("bytes absent from hot pool at id %s", e.ID)
	}
	tier.mu.Lock()
	r := tier.res[e.ID]
	tier.mu.Unlock()
	if r == nil || r.Refs != 1 {
		t.Fatalf("resource refs = %+v", r)
	}
}

func TestDedupSharesContent(t *testing.T) {
	// Discovery background: the whole point of the two-layer design —
	// identical bytes uploaded under different keys must share one
	// resource in the pool. Draft implementation created one xref row per
	// key and leaked bytes; refcount semantics fix it.
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e1 := put(t, tier, "bkt", "a", "same")
	e2 := put(t, tier, "bkt", "b", "same")
	if e1.ID != e2.ID {
		t.Fatalf("same content produced different ids %s vs %s", e1.ID, e2.ID)
	}
	// One copy on disk, two references.
	if _, err := hot.Get(context.Background(), e1.ID, store.Range{Start: 0, End: -1}); err != nil {
		t.Fatal(err)
	}
	tier.mu.Lock()
	r := tier.res[e1.ID]
	tier.mu.Unlock()
	if r.Refs != 2 {
		t.Fatalf("refs = %d, want 2", r.Refs)
	}
	// Deleting one key keeps the bytes alive for the other.
	if err := tier.DeleteObject(context.Background(), "bkt", "a"); err != nil {
		t.Fatal(err)
	}
	if getBody(t, tier, "bkt", "b") != "same" {
		t.Fatal("shared content lost after deleting one alias")
	}
	if err := tier.DeleteObject(context.Background(), "bkt", "b"); err != nil {
		t.Fatal(err)
	}
	if _, err := hot.Head(context.Background(), e1.ID); err != store.ErrNotFound {
		t.Fatal("bytes not swept at refcount zero")
	}
}

func TestOverwriteReleasesOldContent(t *testing.T) {
	// PUT over an existing key: the old content's refcount returns to
	// zero (single reference) and its bytes disappear from the pool.
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e1 := put(t, tier, "bkt", "k", "old-data")
	put(t, tier, "bkt", "k", "new-data")
	if _, err := hot.Head(context.Background(), e1.ID); err != store.ErrNotFound {
		t.Fatal("old content still present after overwrite")
	}
	if getBody(t, tier, "bkt", "k") != "new-data" {
		t.Fatal("overwrite reads stale bytes")
	}
}

func TestCopyIsZeroByte(t *testing.T) {
	// CopyObject = mapping insert: the destination references the source
	// content id; nothing was copied in the pool, refs went 1 -> 2.
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	src := put(t, tier, "bkt", "src", "copy-me")
	dst, err := tier.CopyObject(context.Background(), "bkt", "dst", "bkt", "src")
	if err != nil {
		t.Fatal(err)
	}
	if dst.ID != src.ID {
		t.Fatalf("copy diverged content: %s vs %s", dst.ID, src.ID)
	}
	tier.mu.Lock()
	r := tier.res[src.ID]
	tier.mu.Unlock()
	if r.Refs != 2 {
		t.Fatalf("refs = %d, want 2", r.Refs)
	}
	// Delete source; destination still serves.
	if err := tier.DeleteObject(context.Background(), "bkt", "src"); err != nil {
		t.Fatal(err)
	}
	if getBody(t, tier, "bkt", "dst") != "copy-me" {
		t.Fatal("copied content lost after source delete")
	}
}

func TestColdMigrationDrainsHot(t *testing.T) {
	// Timeline: write both at T0, touch "fresh" at T0+1.5h, run at
	// T0+2.5h. cold.txt is idle 2.5h (>= 2h coldAfter) and must drain;
	// fresh.txt is idle 1h (< coldAfter) and must stay in the buffer.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: 2 * time.Hour,
	})
	e1 := put(t, tier, "bkt", "cold.txt", "idle-data")
	e2 := put(t, tier, "bkt", "fresh.txt", "just-wrote")
	clk.advance(90 * time.Minute)
	tier.GetObject(context.Background(), "bkt", "fresh.txt", store.Range{}) // touch: keep fresh
	clk.advance(time.Hour)
	tier.RunOnce()

	if _, err := hot.Head(context.Background(), e1.ID); err == nil {
		t.Fatal("idle resource not drained")
	}
	if _, err := cold.Head(context.Background(), e1.ID); err != nil {
		t.Fatal("drained resource missing in cold pool")
	}
	if _, err := hot.Head(context.Background(), e2.ID); err != nil {
		t.Fatal("recently-touched resource must stay hot")
	}
}

func TestQuotaEviction(t *testing.T) {
	// maxHotBytes caps the buffer: the oldest resources are evicted even
	// if they were touched recently, until the quota is met.
	tier, hot, _, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, MaxHotBytes: 5,
	})
	e1 := put(t, tier, "bkt", "a", "a") // 1 byte
	clk.advance(time.Minute)
	e2 := put(t, tier, "bkt", "b", "bb") // 2 bytes
	clk.advance(time.Minute)
	e3 := put(t, tier, "bkt", "c", "ccc") // 3 bytes; hot now 6 > 5

	tier.RunOnce()
	if _, err := hot.Head(context.Background(), e1.ID); err == nil {
		t.Fatal("oldest resource not evicted over quota")
	}
	if _, err := hot.Head(context.Background(), e2.ID); err != nil {
		t.Fatal("second oldest evicted while quota allowed keeping it")
	}
	if _, err := hot.Head(context.Background(), e3.ID); err != nil {
		t.Fatal("newest resource evicted")
	}
}

func TestReadThroughHealsIndex(t *testing.T) {
	// A crashed migration can leave the index pointing at a pool where
	// the bytes no longer exist. A read must probe other pools and repoint
	// the resource row (heal) instead of failing.
	tier, hot, cold, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "data")
	// Simulate: transfer happened, index says hot, but bytes only in cold.
	if err := hot.Delete(context.Background(), e.ID); err != nil {
		t.Fatal(err)
	}
	cold.Put(context.Background(), e.ID, strings.NewReader("data"), 4, "", store.PutOptions{})
	if got := getBody(t, tier, "bkt", "k"); got != "data" {
		t.Fatalf("healed read: %q", got)
	}
	tier.mu.Lock()
	pool := tier.res[e.ID].Pool
	tier.mu.Unlock()
	if pool != "cold" {
		t.Fatalf("index not healed to cold, pool=%q", pool)
	}
}

func TestPromoteOnAccess(t *testing.T) {
	// Read of a cold resource triggers async promotion back to hot; the
	// first read still serves from cold while the copy races.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour, PromoteOnAccess: true,
	})
	e := put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatal("resource should be cold now")
	}

	if got := getBody(t, tier, "bkt", "k"); got != "data" {
		t.Fatalf("cold read: %q", got)
	}
	// Promotion runs asynchronously; wait for it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := hot.Head(context.Background(), e.ID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cold resource not promoted back to hot")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestDeleteSweepsTiers(t *testing.T) {
	// Delete removes the resource bytes from every pool (covered copies
	// included), then the key.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour, PromoteOnAccess: true,
	})
	e := put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatal("setup: resource should be cold")
	}
	if err := tier.DeleteObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := cold.Head(context.Background(), e.ID); err == nil {
		t.Fatal("cold copy not swept by delete")
	}
	if _, err := hot.Head(context.Background(), e.ID); err == nil {
		t.Fatal("hot leftover after delete")
	}
	mustNotExist(t, tier, "bkt", "k")
	// Delete of a gone key is idempotent (S3 semantics).
	if err := tier.DeleteObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatalf("idempotent delete: %v", err)
	}
}

func TestListAcrossTiersWithDelimiter(t *testing.T) {
	tier, _, _, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	put(t, tier, "bkt", "a.txt", "1")
	put(t, tier, "bkt", "dir/b.txt", "2")
	put(t, tier, "bkt", "dir/sub/c.txt", "3")
	clk.advance(2 * time.Hour)
	tier.RunOnce()

	res, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Key != "a.txt" {
		t.Fatalf("delimiter contents: %+v", res.Entries)
	}
	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0] != "dir/" {
		t.Fatalf("delimiter prefixes: %+v", res.CommonPrefixes)
	}

	res, err = tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", Prefix: "dir/", Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Key != "dir/b.txt" {
		t.Fatalf("subdir contents: %+v", res.Entries)
	}
	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0] != "dir/sub/" {
		t.Fatalf("subdir prefixes: %+v", res.CommonPrefixes)
	}
}

func TestListPaginationToken(t *testing.T) {
	// The page token must advance past exactly the keys a page emitted:
	// a boundary hit on a key that was NOT returned must not skip it.
	// Discovery background: NextToken was once assigned before the
	// truncation check, so page2 skipped the first key of page1's next
	// page.
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	for _, k := range []string{"a", "b", "c"} {
		put(t, tier, "bkt", k, k)
	}
	res1, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", MaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !res1.IsTruncated {
		t.Fatalf("page1 not truncated: %+v", res1)
	}
	res2, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", MaxKeys: 2, StartAfter: res1.NextToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(res2.Entries) != 1 || res2.Entries[0].Key != "c" {
		t.Fatalf("page2: %+v (token %q)", res2.Entries, res1.NextToken)
	}
	if res2.IsTruncated {
		t.Fatalf("page2 truncated: %+v", res2)
	}
}

func TestRebuildRestoresContentLayer(t *testing.T) {
	// A lost DB cannot restore names (documented limitation), but the
	// content layer comes back from pool listings, and a future upload of
	// the same bytes dedups into the recovered resource instead of
	// duplicating.
	clk := &clock{t: time.Now()}
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	ctx := context.Background()
	// Pool keys are content ids; seed with the REAL sha256 of the bytes so
	// the dedup probe below can hit them.
	h := sha256.Sum256([]byte("dup2"))
	dupID := hex.EncodeToString(h[:])
	hot.Put(ctx, dupID, strings.NewReader("dup2"), 4, "", store.PutOptions{})
	hot.Put(ctx, "tmp/deadbeef", strings.NewReader("stray"), 5, "", store.PutOptions{})
	cold.Put(ctx, "only-cold-id", strings.NewReader("x"), 1, "", store.PutOptions{})

	tier, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, t.TempDir()+"/tier.db")
	if err != nil {
		t.Fatal(err)
	}
	defer tier.Close()
	tier.SetNow(clk.now)

	tier.mu.Lock()
	ids := make(map[string]*resRow)
	for id, r := range tier.res {
		ids[id] = r
	}
	tier.mu.Unlock()
	if _, ok := ids["tmp/deadbeef"]; ok {
		t.Fatal("stray temp key recovered as a resource")
	}
	for want, pool := range map[string]string{dupID: "hot", "only-cold-id": "cold"} {
		r, ok := ids[want]
		if !ok {
			t.Fatalf("resource %s missing after rebuild", want)
		}
		if r.Pool != pool {
			t.Fatalf("resource %s in pool %q, want %q", want, r.Pool, pool)
		}
	}

	// Same bytes uploaded again dedup into the recovered resource.
	e := put(t, tier, "bkt", "k", "dup2")
	if e.ID != dupID {
		t.Fatalf("dedup missed rebuilt content: id %s", e.ID)
	}
	tier.mu.Lock()
	got := tier.res[dupID]
	tier.mu.Unlock()
	if got.Refs != 1 {
		t.Fatalf("refs after dedup into rebuilt resource: %d", got.Refs)
	}
}

func TestDedupSurvivesRestart(t *testing.T) {
	// The index is durable: same content across a restart still lands on
	// one resource (refcount persisted).
	statePath := t.TempDir() + "/tier.db"
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	t1, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, statePath)
	if err != nil {
		t.Fatal(err)
	}
	e1 := put(t, t1, "bkt", "a", "x")
	t1.Close()

	t2, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()
	e2 := put(t, t2, "bkt", "b", "x")
	if e1.ID != e2.ID {
		t.Fatalf("ids diverged across restart: %s vs %s", e1.ID, e2.ID)
	}
	t2.mu.Lock()
	refs := t2.res[e2.ID].Refs
	t2.mu.Unlock()
	if refs != 2 {
		t.Fatalf("refs after restart = %d, want 2", refs)
	}
}

// setControl writes one control row directly into the DB, the same way an
// external admin process would (cmd/s3-admin / raw sqlite3).
func setControl(t *testing.T, tier *TieredStore, k, v string) {
	t.Helper()
	if _, err := tier.db.Exec(`INSERT INTO control (k, v) VALUES (?, ?)
		ON CONFLICT(k) DO UPDATE SET v = excluded.v`, k, v); err != nil {
		t.Fatal(err)
	}
}

func clearControl(t *testing.T, tier *TieredStore, k string) {
	t.Helper()
	if _, err := tier.db.Exec(`DELETE FROM control WHERE k = ?`, k); err != nil {
		t.Fatal(err)
	}
}

func queueCommand(t *testing.T, tier *TieredStore, verb, arg string) {
	t.Helper()
	if _, err := tier.db.Exec(`INSERT INTO commands (verb, arg) VALUES (?, ?)`, verb, arg); err != nil {
		t.Fatal(err)
	}
}

func TestControlAutoPause(t *testing.T) {
	// The Run loop gates RunOnce on the auto_enabled flag; the poll loop
	// publishes it. Run the real loops with a fast tick to prove pause
	// holds back migration and resume lets it through.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	e := put(t, tier, "bkt", "k", "data")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tier.Run(ctx, 10*time.Millisecond)

	clk.advance(2 * time.Hour)
	setControl(t, tier, "auto_enabled", "0")
	tier.consumeControl(context.Background())
	time.Sleep(80 * time.Millisecond) // several loop ticks
	if _, err := hot.Head(context.Background(), e.ID); err != nil {
		t.Fatal("resource migrated while auto migration is paused")
	}

	setControl(t, tier, "auto_enabled", "1")
	tier.consumeControl(context.Background())
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := cold.Head(context.Background(), e.ID); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("resource not migrated after resume")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestControlColdAfterOverride(t *testing.T) {
	// Discovery background: threshold must be changeable at runtime; the
	// DB row is the only channel, so RunOnce reads it through ov. Deleting
	// the row reverts to Config.
	tier, hot, _, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: 24 * time.Hour,
	})
	e := put(t, tier, "bkt", "k", "data")
	clk.advance(time.Hour)

	// Override cold_after to 30min: the 1h-old resource qualifies.
	setControl(t, tier, "cold_after_ms", "1800000")
	tier.consumeControl(context.Background())
	tier.RunOnce()
	if _, err := hot.Head(context.Background(), e.ID); err == nil {
		t.Fatal("resource not migrated with tightened cold_after override")
	}

	// Revert (delete row): Config's 24h applies again, nothing is hot to
	// move; also the override memory must be gone.
	clearControl(t, tier, "cold_after_ms")
	tier.consumeControl(context.Background())
	ov := tier.ov.Load()
	if ov.ColdAfter != nil {
		t.Fatal("deleted control row still overrides")
	}
}

func TestControlMaxHotBytesOverride(t *testing.T) {
	// Quota is a live knob too: config says unlimited, control sets 1
	// byte, the hot pool drains below it.
	tier, hot, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e1 := put(t, tier, "bkt", "a", "aa")
	e2 := put(t, tier, "bkt", "b", "bb")

	setControl(t, tier, "max_hot_bytes", "1")
	tier.consumeControl(context.Background())
	tier.RunOnce()
	// Both resources are 2 bytes > 1-byte quota: everything must drain.
	if _, err := hot.Head(context.Background(), e1.ID); err == nil {
		t.Fatal("resource a still hot above override quota")
	}
	if _, err := hot.Head(context.Background(), e2.ID); err == nil {
		t.Fatal("resource b still hot above override quota")
	}
}

func TestControlPromoteOverride(t *testing.T) {
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour, PromoteOnAccess: false,
	})
	e := put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce() // config: promote off, so get serves from cold and stays
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatal("setup: resource should be cold")
	}
	setControl(t, tier, "promote_on_access", "1")
	tier.consumeControl(context.Background())
	getBody(t, tier, "bkt", "k")
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := hot.Head(context.Background(), e.ID)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("promote_on_access override did not promote")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestControlForceMigrate(t *testing.T) {
	// Commands bypass the idle policy entirely: migrate by key name and by
	// bare content id.
	tier, hot, cold, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	e := put(t, tier, "bkt", "k", "data")

	queueCommand(t, tier, "migrate", "bkt/k")
	tier.consumeCommands(context.Background())
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatal("force migrate by key did not move the resource")
	}
	if _, err := hot.Head(context.Background(), e.ID); err == nil {
		t.Fatal("source copy left after forced migrate")
	}
	// Idempotent-ish: forcing a cold resource to migrate errors cleanly
	// (logged, never crashes the poller).
	queueCommand(t, tier, "migrate", e.ID)
	tier.consumeCommands(context.Background())
}

func TestControlForcePromote(t *testing.T) {
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	e := put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), e.ID); err != nil {
		t.Fatal("setup: resource should be cold")
	}
	queueCommand(t, tier, "promote", e.ID)
	tier.consumeCommands(context.Background())
	if _, err := hot.Head(context.Background(), e.ID); err != nil {
		t.Fatal("force promote did not move the resource back to hot")
	}
}

func TestControlUnknownKeyIgnored(t *testing.T) {
	// Foreign rows written by a confused admin must not wedge the poller.
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	setControl(t, tier, "bogus_knob", "42")
	queueCommand(t, tier, "explode", "everything")
	tier.consumeControl(context.Background())
	tier.consumeCommands(context.Background())
	if !tier.autoOn.Load() {
		t.Fatal("unknown row flipped the pause flag")
	}
}

func TestSentinelTriggersConsume(t *testing.T) {
	// The consumption contract: NOTHING is polled. Rows written to the DB
	// only take effect after the sentinel file is touched (or at startup).
	// Caveat that drove the design: the first control design polled the
	// DB every second; the sentinel event models "admin wrote rows" with
	// zero steady-state traffic and ~instant pickup.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	e := put(t, tier, "bkt", "k", "data")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go tier.Run(ctx, 10*time.Millisecond)

	waitTrue := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timeout waiting: %s", what)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Write a pause row WITHOUT consuming: must have no effect until the
	// sentinel is touched.
	setControl(t, tier, "auto_enabled", "0")
	if !tier.autoOn.Load() {
		t.Fatal("control row consumed before sentinel trigger")
	}
	os.Chtimes(tier.sentinelPath, time.Now(), time.Now())
	waitTrue(func() bool { return !tier.autoOn.Load() }, "pause consumed via sentinel")

	// While paused, a queued migrate must still run (commands are separate
	// from the pause gate and always honored).
	clk.advance(2 * time.Hour)
	queueCommand(t, tier, "migrate", "bkt/k")
	if _, err := hot.Head(context.Background(), e.ID); err != nil {
		t.Fatal("migrated before sentinel trigger")
	}
	os.Chtimes(tier.sentinelPath, time.Now(), time.Now())
	waitTrue(func() bool {
		_, err := cold.Head(context.Background(), e.ID)
		return err == nil
	}, "migrate command consumed via sentinel")
}

func TestStartupConsumesPersistedControl(t *testing.T) {
	// Rows written before the process starts are applied at New(): a
	// pause decided during the previous session must survive a restart.
	statePath := t.TempDir() + "/tier.db"
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	t1, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, statePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := t1.db.Exec(`INSERT INTO control (k, v) VALUES ('auto_enabled', '0')`); err != nil {
		t.Fatal(err)
	}
	t1.Close()

	t2, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()
	if t2.autoOn.Load() {
		t.Fatal("persisted pause not applied at startup")
	}
}
