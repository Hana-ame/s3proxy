package tier

// Tiering policy tests: buffer drain (hot -> cold by idle time and by byte
// quota), read-through of cold objects, optional promotion back to hot,
// index healing after a crashed migration, and stale-copy cleanup on
// overwrite. These pin the "one pool is another pool's buffer" behavior.

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"s3proxy/internal/store"
)

const (
	testAK = "ak"
	testSK = "sk"
)

// clock is an injectable time source so tests can age objects without
// sleeping.
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

func newTestTier(t *testing.T, cfg Config) (*TieredStore, *store.MemStore, *store.MemStore, *clock) {
	t.Helper()
	clk := &clock{t: time.Now()}
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	tier, err := New([]store.Store{hot, cold}, cfg, t.TempDir()+"/objects.json")
	if err != nil {
		t.Fatal(err)
	}
	tier.now = clk.now
	t.Cleanup(func() { tier.Close() })
	return tier, hot, cold, clk
}

func put(t *testing.T, tier *TieredStore, bucket, key, body string) Entry {
	t.Helper()
	e, err := tier.PutObject(context.Background(), bucket, key, strings.NewReader(body), int64(len(body)), PutOpts{ContentType: "text/plain"})
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

func TestWritesLandInHot(t *testing.T) {
	tier, hot, cold, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	put(t, tier, "bkt", "k.txt", "hello")
	if _, err := hot.Head(context.Background(), "bkt/k.txt"); err != nil {
		t.Fatal("object not in hot pool")
	}
	if _, err := cold.Head(context.Background(), "bkt/k.txt"); err == nil {
		t.Fatal("object leaked into cold pool")
	}
	if got := getBody(t, tier, "bkt", "k.txt"); got != "hello" {
		t.Fatalf("read back %q", got)
	}
}

func TestColdMigrationDrainsHot(t *testing.T) {
	// Discovery background: the whole point of the buffer policy —
	// uploaded objects must stop consuming the hot pool once idle.
	// Timeline: write both at T0, touch "fresh" at T0+1.5h, run at
	// T0+2.5h. cold.txt is idle 2.5h (>= 2h coldAfter) and must drain;
	// fresh.txt is idle 1h (< coldAfter) and must stay in the buffer.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: 2 * time.Hour,
	})
	put(t, tier, "bkt", "cold.txt", "idle-data")
	put(t, tier, "bkt", "fresh.txt", "just-wrote")
	clk.advance(90 * time.Minute)
	tier.GetObject(context.Background(), "bkt", "fresh.txt", store.Range{}) // touch: keep fresh
	clk.advance(time.Hour)
	tier.RunOnce()

	if _, err := cold.Head(context.Background(), "bkt/cold.txt"); err != nil {
		t.Fatal("idle object not migrated to cold")
	}
	if _, err := hot.Head(context.Background(), "bkt/cold.txt"); err == nil {
		t.Fatal("idle object still in hot after drain")
	}
	if _, err := hot.Head(context.Background(), "bkt/fresh.txt"); err != nil {
		t.Fatal("recently-touched object must stay hot")
	}
	if got := getBody(t, tier, "bkt", "cold.txt"); got != "idle-data" {
		t.Fatalf("cold read-back %q", got)
	}
}

func TestQuotaEviction(t *testing.T) {
	// maxHotBytes caps the buffer: the oldest objects are evicted even if
	// they were touched recently.
	tier, hot, _, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, MaxHotBytes: 5,
	})
	put(t, tier, "bkt", "a", "a") // 1 byte
	clk.advance(time.Minute)
	put(t, tier, "bkt", "b", "bb") // 2 bytes
	clk.advance(time.Minute)
	put(t, tier, "bkt", "c", "ccc") // 3 bytes; hot now 6 > 5

	tier.RunOnce()
	if _, err := hot.Head(context.Background(), "bkt/a"); err == nil {
		t.Fatal("oldest object not evicted over quota")
	}
	if _, err := hot.Head(context.Background(), "bkt/b"); err != nil {
		t.Fatal("second oldest evicted while quota allowed keeping it")
	}
	if _, err := hot.Head(context.Background(), "bkt/c"); err != nil {
		t.Fatal("newest object evicted")
	}
}

func TestReadThroughHealsIndex(t *testing.T) {
	// Simulate a crash between index update and source deletion: index
	// says hot, bytes live only in cold. A read must find the object and
	// repoint the index (this is what makes durability after restarts
	// work at all).
	tier, hot, cold, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	put(t, tier, "bkt", "k", "moved-away")
	// Manually move bytes cold-ward without touching the index.
	if _, err := cold.Put(context.Background(), "bkt/k", strings.NewReader("moved-away"), 10, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := hot.Delete(context.Background(), "bkt/k"); err != nil {
		t.Fatal(err)
	}

	if got := getBody(t, tier, "bkt", "k"); got != "moved-away" {
		t.Fatalf("read-through get: %q", got)
	}
	tier.mu.Lock()
	e := tier.idx["bkt/k"]
	tier.mu.Unlock()
	if e == nil || e.Pool != "cold" {
		t.Fatalf("index not healed to cold: %+v", e)
	}
}

func TestPromoteOnAccess(t *testing.T) {
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour, PromoteOnAccess: true,
	})
	put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), "bkt/k"); err != nil {
		t.Fatal("object should be cold now")
	}

	if got := getBody(t, tier, "bkt", "k"); got != "data" {
		t.Fatalf("cold read: %q", got)
	}
	// Promotion runs asynchronously; wait for it.
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, err := hot.Head(context.Background(), "bkt/k")
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cold object not promoted back to hot")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestOverwriteRemovesColdCopy(t *testing.T) {
	// A newer PUT must eliminate the stale cold copy, otherwise reads
	// after a subsequent migration could serve the old bytes.
	tier, hot, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	put(t, tier, "bkt", "k", "v1")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), "bkt/k"); err != nil {
		t.Fatal("setup: object should be cold")
	}

	put(t, tier, "bkt", "k", "v2-new")
	if _, err := hot.Head(context.Background(), "bkt/k"); err != nil {
		t.Fatal("overwrite should be in hot")
	}
	if _, err := cold.Head(context.Background(), "bkt/k"); err == nil {
		t.Fatal("stale cold copy survived the overwrite")
	}
	if got := getBody(t, tier, "bkt", "k"); got != "v2-new" {
		t.Fatalf("got %q", got)
	}
}

func TestDeleteSweepsTiers(t *testing.T) {
	tier, _, cold, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	put(t, tier, "bkt", "k", "data")
	clk.advance(2 * time.Hour)
	tier.RunOnce()
	if _, err := cold.Head(context.Background(), "bkt/k"); err != nil {
		t.Fatal("setup: object should be cold")
	}
	if err := tier.DeleteObject(context.Background(), "bkt", "k"); err != nil {
		t.Fatal(err)
	}
	if _, err := cold.Head(context.Background(), "bkt/k"); err == nil {
		t.Fatal("cold copy not swept by delete")
	}
	if _, _, err := tier.GetObject(context.Background(), "bkt", "k", store.Range{}); err != store.ErrNotFound {
		t.Fatalf("get after delete: %v", err)
	}
}

func TestListAcrossTiersWithDelimiter(t *testing.T) {
	tier, _, _, clk := newTestTier(t, Config{
		Hot: "hot", Cold: []string{"cold"}, ColdAfter: time.Hour,
	})
	put(t, tier, "bkt", "a.txt", "1")
	put(t, tier, "bkt", "dir/b.txt", "2")
	clk.advance(2 * time.Hour)
	tier.RunOnce()

	res, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", Delimiter: "/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Key != "a.txt" {
		t.Fatalf("entries: %+v", res.Entries)
	}
	if len(res.CommonPrefixes) != 1 || res.CommonPrefixes[0] != "dir/" {
		t.Fatalf("common prefixes: %v", res.CommonPrefixes)
	}

	res, err = tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", Prefix: "dir/"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 1 || res.Entries[0].Key != "dir/b.txt" {
		t.Fatalf("prefix list: %+v", res.Entries)
	}
}

func TestListPaginationToken(t *testing.T) {
	tier, _, _, _ := newTestTier(t, Config{Hot: "hot", Cold: []string{"cold"}})
	put(t, tier, "bkt", "a", "1")
	put(t, tier, "bkt", "b", "2")
	put(t, tier, "bkt", "c", "3")

	page1, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", MaxKeys: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !page1.IsTruncated || len(page1.Entries) != 2 {
		t.Fatalf("page1: %+v", page1)
	}
	page2, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", MaxKeys: 2, StartAfter: page1.NextToken})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2.Entries) != 1 || page2.Entries[0].Key != "c" {
		t.Fatalf("page2: %+v", page2.Entries)
	}
}

func TestRebuildFromPools(t *testing.T) {
	// Discovery background: index is the metadata source of truth; after a
	// crash or manual state deletion the index must be reproducible from
	// the pools alone, and duplicates (both copies present after an
	// interrupted migration) must collapse to one entry.
	clk := &clock{t: time.Now()}
	hot := store.NewMem("hot")
	cold := store.NewMem("cold")
	ctx := context.Background()
	hot.EnsureBucket(ctx, "bkt")
	cold.EnsureBucket(ctx, "bkt")
	// Object present in BOTH pools (crash refl); cold copy is newer.
	hot.Put(ctx, "bkt/dup", strings.NewReader("dup"), 3, "", store.PutOptions{})
	if _, err := cold.Put(ctx, "bkt/dup", strings.NewReader("dup2"), 4, "", store.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	// Object only in cold (post-migration crash).
	cold.Put(ctx, "bkt/only-cold", strings.NewReader("x"), 1, "", store.PutOptions{})
	// Object only in hot.
	hot.Put(ctx, "bkt/only-hot", strings.NewReader("y"), 1, "", store.PutOptions{})

	tier, err := New([]store.Store{hot, cold}, Config{Hot: "hot", Cold: []string{"cold"}}, t.TempDir()+"/objects.json")
	if err != nil {
		t.Fatal(err)
	}
	defer tier.Close()
	tier.now = clk.now

	res, err := tier.ListObjects(context.Background(), ListParams{Bucket: "bkt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 3 {
		t.Fatalf("expected 3 entries after rebuild, got %+v", res.Entries)
	}
	// The duplicate must resolve to the newer (cold) copy.
	res, _ = tier.ListObjects(context.Background(), ListParams{Bucket: "bkt", Prefix: "dup"})
	if got := getBody(t, tier, "bkt", "dup"); got != "dup2" {
		t.Fatalf("dup resolved to %q", got)
	}
	_ = res
}
