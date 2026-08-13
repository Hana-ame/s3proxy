package main

// Command s3-proxy exposes a fully S3-compliant HTTP/HTTPS API in front of
// pluggable storage backends, with a buffer/tiering policy:
//
//   - every write lands in the "hot" pool (typically local disk)
//   - the background loop pushes objects idle for cold_after (or over the
//     hot byte quota) into one of the "cold" pools (any number of remote
//     S3-compatible backends, or additional local pools)
//   - reads of cold objects can promote them back to hot
//
// Configuration lives in a single JSON file, see example.json.

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"s3proxy/internal/api"
	"s3proxy/internal/store"
	"s3proxy/internal/store/local"
	"s3proxy/internal/store/s3store"
	"s3proxy/internal/tier"
)

type cred struct {
	AK string `json:"ak"`
	SK string `json:"sk"`
}

type duration struct{ time.Duration }

func (d *duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

// poolConfig declares one storage plugin instance.
type poolConfig struct {
	Name     string   `json:"name"`     // referenced by tiering.hot/cold
	Backend  string   `json:"backend"`  // "local" | "s3"
	Role     string   `json:"role"`     // "hot" | "cold" (informational; tiering below decides)
	Path     string   `json:"path"`     // local backend: data directory
	Endpoint string   `json:"endpoint"` // s3 backend: e.g. https://minio.internal:9000
	Region   string   `json:"region"`   // s3 backend
	Bucket   string   `json:"bucket"`   // s3 backend: prefix-mode bucket; empty = per-bucket mode
	AK       string   `json:"ak"`       // s3 backend
	SK       string   `json:"sk"`       // s3 backend
	Insecure bool     `json:"insecure"` // s3 backend: skip TLS verify
	Timeout  duration `json:"timeout"`  // s3 backend: per-request timeout
}

type tieringConfig struct {
	Hot             []string `json:"hot"`               // pool names receiving writes (exactly one)
	Cold            []string `json:"cold"`              // pool names drained to (zero or more)
	ColdAfter       duration `json:"cold_after"`        // idle time before an object qualifies as cold
	ScanInterval    duration `json:"scan_interval"`     // migration loop period
	MaxHotBytes     int64    `json:"max_hot_bytes"`     // 0 = unlimited buffer
	PromoteOnAccess bool     `json:"promote_on_access"` // move cold objects back to hot on read
}

type config struct {
	Listen      string        `json:"listen"`
	TLSCert     string        `json:"tls_cert"`
	TLSKey      string        `json:"tls_key"`
	Region      string        `json:"region"`
	BaseHost    string        `json:"base_host"` // optional: enables virtual-host style addressing
	StateDir    string        `json:"state_dir"` // index + multipart staging
	ClientCreds []cred        `json:"client_creds"`
	Pools       []poolConfig  `json:"pools"`
	Tiering     tieringConfig `json:"tiering"`
}

func loadConfig(path string) (config, error) {
	cfg := config{
		Listen: "0.0.0.0:9000",
		Region: "us-east-1",
		Tiering: tieringConfig{
			ColdAfter:    duration{168 * time.Hour},
			ScanInterval: duration{time.Hour},
		},
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Listen == "" {
		cfg.Listen = "0.0.0.0:9000"
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.StateDir == "" {
		cfg.StateDir = "state"
	}
	if cfg.Tiering.ScanInterval.Duration <= 0 {
		cfg.Tiering.ScanInterval.Duration = time.Hour
	}
	return cfg, nil
}

func (c config) validate() error {
	if len(c.ClientCreds) == 0 {
		return fmt.Errorf("client_creds must contain at least one ak/sk pair")
	}
	if len(c.Pools) == 0 {
		return fmt.Errorf("at least one pool is required")
	}
	if (c.TLSCert == "") != (c.TLSKey == "") {
		return fmt.Errorf("tls_cert and tls_key must be provided together")
	}
	seen := make(map[string]bool)
	for _, p := range c.Pools {
		if p.Name == "" {
			return fmt.Errorf("every pool needs a name")
		}
		if seen[p.Name] {
			return fmt.Errorf("duplicate pool name %q", p.Name)
		}
		seen[p.Name] = true
		switch p.Backend {
		case "local":
			if p.Path == "" {
				return fmt.Errorf("pool %q (local) needs path", p.Name)
			}
		case "s3":
			if p.AK == "" || p.SK == "" {
				return fmt.Errorf("pool %q (s3) needs ak and sk", p.Name)
			}
			// Content-addressed resources live in one fixed namespace
			// ("data" -> the configured bucket); per-bucket mode conflicts
			// with global dedup (an id would need to duplicate across
			// remotes). Require prefix mode.
			if p.Bucket == "" {
				return fmt.Errorf("pool %q (s3): bucket is required (prefix mode) — per-bucket mode is incompatible with content dedup", p.Name)
			}
		default:
			return fmt.Errorf("pool %q: unknown backend %q (want local or s3)", p.Name, p.Backend)
		}
	}
	if len(c.Tiering.Hot) != 1 {
		return fmt.Errorf("tiering.hot must name exactly one hot pool")
	}
	for _, name := range append(append([]string{}, c.Tiering.Hot...), c.Tiering.Cold...) {
		if !seen[name] {
			return fmt.Errorf("tiering references pool %q that is not defined", name)
		}
	}
	return nil
}

func main() {
	configPath := flag.String("config", "config.json", "path to JSON configuration file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		log.Fatal(err)
	}

	// Build the storage plugins; the tier engine treats roles purely
	// through tiering.hot/tiering.cold entries.
	pools := make([]store.Store, 0, len(cfg.Pools))
	for _, p := range cfg.Pools {
		var st store.Store
		switch p.Backend {
		case "local":
			st, err = local.New(p.Name, p.Path)
		case "s3":
			st, err = s3store.New(p.Name, s3store.Config{
				Endpoint: p.Endpoint,
				Region:   p.Region,
				Bucket:   p.Bucket,
				AK:       p.AK,
				SK:       p.SK,
				Insecure: p.Insecure,
				Timeout:  p.Timeout.Duration,
			})
		}
		if err != nil {
			log.Fatalf("pool %q: %v", p.Name, err)
		}
		pools = append(pools, st)
		log.Printf("pool %q: %s backend ready", p.Name, p.Backend)
	}

	creds := make(map[string]string, len(cfg.ClientCreds))
	for _, k := range cfg.ClientCreds {
		if k.AK != "" && k.SK != "" {
			creds[k.AK] = k.SK
		}
	}

	t, err := tier.New(pools, tier.Config{
		Hot:             cfg.Tiering.Hot[0],
		Cold:            cfg.Tiering.Cold,
		ColdAfter:       cfg.Tiering.ColdAfter.Duration,
		ScanInterval:    cfg.Tiering.ScanInterval.Duration,
		MaxHotBytes:     cfg.Tiering.MaxHotBytes,
		PromoteOnAccess: cfg.Tiering.PromoteOnAccess,
	}, filepath.Join(cfg.StateDir, "tier.db"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go t.Run(ctx, cfg.Tiering.ScanInterval.Duration)

	handler, err := api.New(t, creds, cfg.Region, cfg.BaseHost, cfg.StateDir)
	if err != nil {
		log.Fatal(err)
	}

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           handler,
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	log.Printf("s3-proxy listening on %s (region %s, hot=%v, cold=%v, cold_after=%s)",
		cfg.Listen, cfg.Region, cfg.Tiering.Hot, cfg.Tiering.Cold, cfg.Tiering.ColdAfter.Duration)
	switch {
	case cfg.TLSCert != "":
		log.Fatal(srv.ListenAndServeTLS(cfg.TLSCert, cfg.TLSKey))
	default:
		log.Fatal(srv.ListenAndServe())
	}
}
