// Command s3-store runs a local filesystem storage pool as a standalone
// S3-compatible HTTP endpoint. Mostly used as the development backfill for
// the s3 proxy: the proxy's "local" pool plugin embeds the same code
// in-process, and this binary is what you run when the pool lives on
// another machine.
package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"s3proxy/internal/store/local"
)

func main() {
	listen := flag.String("listen", "0.0.0.0:9002", "listen address")
	data := flag.String("data", "data", "data directory")
	flag.Parse()

	if err := os.MkdirAll(*data, 0o755); err != nil {
		log.Fatalf("cannot create data dir: %v", err)
	}
	s, err := local.New("local", *data)
	if err != nil {
		log.Fatalf("cannot open store: %v", err)
	}

	// Periodic cleanup of interrupted writes (<key>.tmp), mirroring what
	// the tier loop does when the pool is embedded.
	go func() {
		for range time.Tick(time.Minute) {
			if err := s.RemoveStaleTemps(time.Minute); err != nil {
				log.Printf("temp cleanup: %v", err)
			}
		}
	}()

	srv := &http.Server{
		Addr:              *listen,
		Handler:           local.NewHTTPHandler(s),
		ReadHeaderTimeout: 30 * time.Second,
		IdleTimeout:       5 * time.Minute,
	}
	log.Printf("s3-store listening on %s, data dir %s", *listen, *data)
	log.Fatal(srv.ListenAndServe())
}
