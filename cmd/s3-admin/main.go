package main

// Command s3-admin reads and drives the s3-proxy tiering engine directly
// through its SQLite index file — no HTTP admin port, no shared state
// beyond the DB. Two tables are the contract:
//
//   - control(k,v): runtime policy overrides + the auto-migration pause
//     flag. Written with INSERT OR REPLACE; DELETING a row reverts that
//     knob to the config file value.
//   - commands(seq,verb,arg): one-shot force operations, consumed (and
//     deleted) by the proxy's polling loop within ~1s.
//
// Everything else (resources, objects, buckets, v_cold_status) is a live
// mirror and may be read directly — never write those tables, the proxy's
// in-memory maps are authoritative and will overwrite foreign edits.
//
// Examples:
//
//	s3-admin state/tier.db status
//	s3-admin state/tier.db status --json
//	s3-admin state/tier.db pause
//	s3-admin state/tier.db resume
//	s3-admin state/tier.db set --cold-after=30m --max-hot-bytes=100GiB
//	s3-admin state/tier.db migrate mybucket/path/to/object
//	s3-admin state/tier.db promote <sha256hex-of-content>

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}
	dbPath, cmd := os.Args[1], os.Args[2]
	rest := os.Args[3:]

	db, err := sql.Open("sqlite", "file:"+dbPath+"?_pragma=busy_timeout(5000)")
	if err != nil {
		fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		fatal(fmt.Errorf("open %s: %w", dbPath, err))
	}

	switch cmd {
	case "status":
		fs := flag.NewFlagSet("status", flag.ExitOnError)
		asJSON := fs.Bool("json", false, "emit JSON")
		fs.Parse(rest)
		status(db, *asJSON)
	case "pause", "resume":
		v := "0"
		if cmd == "resume" {
			v = "1"
		}
		setControl(db, "auto_enabled", v)
		touchSentinel(dbPath)
		fmt.Printf("auto migration %s\n", map[string]string{"0": "paused", "1": "resumed"}[v])
	case "set":
		fs := flag.NewFlagSet("set", flag.ExitOnError)
		coldAfter := fs.String("cold-after", "", "idle threshold, e.g. 30m/1h")
		maxHot := fs.String("max-hot-bytes", "", "hot buffer quota, e.g. 10GiB/500MB")
		promote := fs.String("promote", "", "promote cold reads back to hot: 0|1")
		fs.Parse(rest)
		if *coldAfter != "" {
			d, err := time.ParseDuration(*coldAfter)
			if err != nil {
				fatal(fmt.Errorf("cold-after: %w", err))
			}
			setControl(db, "cold_after_ms", fmt.Sprint(d.Milliseconds()))
			fmt.Printf("cold_after_ms = %d\n", d.Milliseconds())
		}
		if *maxHot != "" {
			n, err := parseBytes(*maxHot)
			if err != nil {
				fatal(err)
			}
			setControl(db, "max_hot_bytes", fmt.Sprint(n))
			fmt.Printf("max_hot_bytes = %d\n", n)
		}
		if *promote != "" {
			if *promote != "0" && *promote != "1" {
				fatal(fmt.Errorf("promote must be 0 or 1"))
			}
			setControl(db, "promote_on_access", *promote)
			fmt.Printf("promote_on_access = %s\n", *promote)
		}
		if *coldAfter == "" && *maxHot == "" && *promote == "" {
			fatal(fmt.Errorf("set needs at least one of --cold-after/--max-hot-bytes/--promote"))
		}
		touchSentinel(dbPath)
	case "migrate", "promote":
		if len(rest) != 1 {
			fatal(fmt.Errorf("%s needs exactly one target: <bucket/key> or <content-id>", cmd))
		}
		if _, err := db.Exec(`INSERT INTO commands (verb, arg) VALUES (?, ?)`, cmd, rest[0]); err != nil {
			fatal(err)
		}
		touchSentinel(dbPath)
		fmt.Printf("queued %s %s (takes effect immediately)\n", cmd, rest[0])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage: s3-admin <tier.db> <command> [args]

commands:
  status [--json]                    resources + pools + control knobs
  pause | resume                     pause/resume the auto migration loop
  set --cold-after=1h --max-hot-bytes=10GiB --promote=0|1
                                     runtime policy overrides (empty = keep)
  migrate <bucket/key|id>            force one resource cold (immediately)
  promote <bucket/key|id>            force one resource back to hot
`)
}

func setControl(db *sql.DB, k, v string) {
	if _, err := db.Exec(`INSERT INTO control (k, v) VALUES (?, ?)
		ON CONFLICT(k) DO UPDATE SET v = excluded.v`, k, v); err != nil {
		fatal(err)
	}
}

// touchSentinel signals the proxy to consume the rows just written: the
// proxy watches this file's mtime instead of polling the DB.
func touchSentinel(dbPath string) {
	f, err := os.OpenFile(dbPath+".ctl", os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fatal(err)
	}
	if _, err := f.Write([]byte("1")); err != nil {
		fatal(err)
	}
	f.Close()
}

// parseBytes accepts bare bytes or KB/MB/GB/TB/KiB/MiB/GiB/TiB suffixes.
func parseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	mult := int64(1)
	upper := strings.ToUpper(s)
	for _, suf := range []struct {
		sfx string
		m   int64
	}{{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
		{"KB", 1000}, {"MB", 1000 * 1000}, {"GB", 1000 * 1000 * 1000}, {"TB", 1000 * 1000 * 1000 * 1000}} {
		if strings.HasSuffix(upper, suf.sfx) {
			mult = suf.m
			s = strings.TrimSpace(s[:len(s)-len(suf.sfx)])
			break
		}
	}
	var n int64
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return 0, fmt.Errorf("parse bytes %q: %w", s, err)
	}
	return n * mult, nil
}

type resStatus struct {
	ID          string `json:"id"`
	Pool        string `json:"pool"`
	Refs        int    `json:"refs"`
	Size        int64  `json:"size"`
	ETag        string `json:"etag"`
	LastAccess  string `json:"last_access"`
	IdleSeconds int64  `json:"idle_seconds"`
}

func status(db *sql.DB, asJSON bool) {
	rows, err := db.Query(`SELECT id, pool, refs, size, etag, last_access, idle_seconds
		FROM v_cold_status ORDER BY idle_seconds DESC`)
	if err != nil {
		fatal(err)
	}
	defer rows.Close()
	var res []resStatus
	for rows.Next() {
		var r resStatus
		if err := rows.Scan(&r.ID, &r.Pool, &r.Refs, &r.Size, &r.ETag, &r.LastAccess, &r.IdleSeconds); err != nil {
			fatal(err)
		}
		res = append(res, r)
	}
	if err := rows.Err(); err != nil {
		fatal(err)
	}

	// Per-pool byte/object totals.
	stats, err := db.Query(`SELECT pool, count(*), coalesce(sum(size), 0) FROM resources GROUP BY pool`)
	if err != nil {
		fatal(err)
	}
	type poolStat struct {
		Pool    string `json:"pool"`
		Objects int    `json:"objects"`
		Bytes   int64  `json:"bytes"`
	}
	var pools []poolStat
	for stats.Next() {
		var p poolStat
		if err := stats.Scan(&p.Pool, &p.Objects, &p.Bytes); err != nil {
			fatal(err)
		}
		pools = append(pools, p)
	}
	stats.Close()

	// Control knobs as seen by the polling loop.
	ctl, err := db.Query(`SELECT k, v FROM control`)
	if err != nil {
		fatal(err)
	}
	var knobs []string
	for ctl.Next() {
		var k, v string
		if err := ctl.Scan(&k, &v); err != nil {
			fatal(err)
		}
		knobs = append(knobs, k+"="+v)
	}
	ctl.Close()
	sort.Strings(knobs)

	if asJSON {
		out := struct {
			Resources []resStatus `json:"resources"`
			Pools     []poolStat  `json:"pools"`
			Control   []string    `json:"control"`
		}{res, pools, knobs}
		b, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(b))
		return
	}
	for _, p := range pools {
		fmt.Printf("pool %-12s %8d objects %15d bytes\n", p.Pool, p.Objects, p.Bytes)
	}
	if len(pools) == 0 {
		fmt.Println("(no resources)")
	}
	for _, k := range knobs {
		fmt.Printf("control %s\n", k)
	}
	for _, r := range res {
		fmt.Printf("%-12s refs=%-3d size=%-10d idle=%8ds %s\n",
			r.Pool, r.Refs, r.Size, r.IdleSeconds, r.ID)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "s3-admin:", err)
	os.Exit(1)
}
