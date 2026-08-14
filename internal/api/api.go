// Package api implements the S3 REST API served by the proxy: SigV4
// authentication, bucket/object operations, listing, copying, multipart
// uploads and bulk deletes, all backed by the tier store (which owns
// placement across storage plugins).
package api

import (
	"crypto/rand"
	"encoding/hex"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"s3proxy/internal/tier"
)

// Server is the S3 frontend.
type Server struct {
	tier     *tier.TieredStore
	creds    map[string]string // ak -> sk accepted from clients
	region   string
	baseHost string // optional; enables virtual-host style addressing
	uploads  *uploadStore
	now      func() time.Time
}

// New wires the frontend to a tiered store. stateDir hosts transient upload
// part state.
func New(t *tier.TieredStore, creds map[string]string, region, baseHost, stateDir string) (*Server, error) {
	us, err := newUploadStore(stateDir)
	if err != nil {
		return nil, err
	}
	return &Server{
		tier:     t,
		creds:    creds,
		region:   region,
		baseHost: baseHost,
		uploads:  us,
		now:      time.Now,
	}, nil
}

func newRequestID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ServeHTTP is the S3 API entry point.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := newRequestID()
	s.serve(w, r, requestID)
}

func (s *Server) serve(w http.ResponseWriter, r *http.Request, requestID string) {
	// CORS preflight is answered before auth: browsers probing presigned
	// URLs send OPTIONS without credentials.
	if r.Method == http.MethodOptions {
		s.handleCORS(w, r, requestID)
		return
	}

	bucket, key, err := parseTarget(r, s.baseHost)
	if err != nil {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidRequest", message: err.Error()}, requestID)
		return
	}

	// Authentication is required for everything except CORS preflight.
	if _, ok := verifySigV4(r, s.creds, s.now()); !ok {
		writeError(w, r, &s3Err{
			status:  http.StatusForbidden,
			code:    "SignatureDoesNotMatch",
			message: "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		}, requestID)
		return
	}

	// Service level (no bucket in the target): ListBuckets.
	if bucket == "" {
		switch r.Method {
		case http.MethodGet:
			s.handleListBuckets(w, r, requestID)
		case http.MethodHead:
			// AWS answers bare HEAD with 200 and no body.
			w.WriteHeader(http.StatusOK)
		default:
			writeError(w, r, &s3Err{status: http.StatusMethodNotAllowed, code: "MethodNotAllowed", message: "The specified method is not allowed against this resource."}, requestID)
		}
		return
	}

	q := r.URL.Query()
	if key == "" {
		s.serveBucket(w, r, requestID, bucket, q)
		return
	}
	s.serveObject(w, r, requestID, bucket, key, q)
}

// parseTarget resolves the target bucket and key from either path-style
// (/{bucket}/{key}) or virtual-host style ({bucket}.{baseHost}/{key})
// requests. Returns bucket="" when the request addresses the service root.
//
// Keys are reconstructed from the escaped path so an encoded "%2F" inside a
// segment survives Go's automatic path unescaping as one key character.
func parseTarget(r *http.Request, baseHost string) (bucket, key string, err error) {
	// EscapedPath keeps %2F intact; split on real slashes only.
	segments := strings.Split(strings.TrimPrefix(r.URL.EscapedPath(), "/"), "/")
	bucketHost, hErr := vhostBucket(r.Host, baseHost)
	if hErr != nil {
		return "", "", hErr
	}
	if bucketHost != "" {
		// Virtual-host style: hostname carries the bucket, path is the key.
		bucket = bucketHost
		key = joinSegments(segments)
		return bucket, key, nil
	}
	if len(segments) == 1 && segments[0] == "" {
		return "", "", nil // service root
	}
	if len(segments) > 0 {
		bucket = segments[0]
		key = joinSegments(segments[1:])
	}
	if bucket == "" {
		return "", "", nil
	}
	if !validBucket(bucket) {
		return "", "", errBadBucket
	}
	return bucket, key, nil
}

// joinSegments decodes each escaped segment and rejoins with slashes,
// preserving keys containing literal slashes ("a%2Fb" stays one key with a
// slash inside) AND empty segments ("a//b" and a trailing slash "a/" are
// legal S3 keys, so they must round-trip verbatim). Discovery background:
// the old code skipped empty segments, silently rewriting "dir//file" into
// "dir/file" and turning a trailing-slash key into a plain key.
func joinSegments(segs []string) string {
	parts := make([]string, 0, len(segs))
	for _, seg := range segs {
		if dec, err := urlPathUnescape(seg); err == nil {
			seg = dec
		}
		parts = append(parts, seg)
	}
	return strings.Join(parts, "/")
}

func urlPathUnescape(s string) (string, error) {
	return url.PathUnescape(s)
}

// vhostBucket maps r.Host to a bucket name when virtual-host style is
// enabled; "" means the request is path-style.
func vhostBucket(host, baseHost string) (string, error) {
	if baseHost == "" {
		return "", nil
	}
	// r.Host always carries the port for non-default ports
	// ("bucket.s3.example.com:9000"); the suffix match must be done on
	// the bare hostname or every virtual-host request through a port
	// silently falls back to path-style (bucket parsed from the path,
	// signature mismatch -> 403). Discovery background: 2026-08 review —
	// vhost addressing worked only for default-port setups.
	host = hostOnly(host)
	baseHost = hostOnly(baseHost)
	if host == baseHost {
		return "", nil
	}
	if strings.HasSuffix(host, "."+baseHost) {
		b := strings.TrimSuffix(host, "."+baseHost)
		if b != "" && validBucket(b) {
			return b, nil
		}
		// A subdomain that is not a legal bucket is a bad request target,
		// not a fallback to path style.
		return "", errBadBucket
	}
	return "", nil
}

// hostOnly strips the port from a Host header value, keeping IPv6 literals
// intact (SplitHostPort requires brackets for them, and no port leaves the
// string untouched).
func hostOnly(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		return host
	}
	return h
}

// validBucket mirrors the S3 bucket naming rules loosely: 3-63 chars,
// lowercase alnum, dash and dot. Kept lenient (allows dots and leading
// chars) because several S3-compatible hosts accept more.
func validBucket(b string) bool {
	if len(b) < 3 || len(b) > 63 {
		return false
	}
	var hasLetter bool
	for i := 0; i < len(b); i++ {
		c := b[i]
		switch {
		case c >= 'a' && c <= 'z':
			hasLetter = true
		case c >= '0' && c <= '9':
		case c == '-' || c == '.':
		default:
			return false
		}
	}
	return hasLetter
}

var errBadBucket = errString("bucket name is not valid")

type errString string

func (e errString) Error() string { return string(e) }

// handleCORS answers preflight/per-request CORS permissively so browser
// presigned-URL access works out of the box.
func (s *Server) handleCORS(w http.ResponseWriter, r *http.Request, requestID string) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		w.WriteHeader(http.StatusOK)
		return
	}
	h := w.Header()
	method := r.Header.Get("Access-Control-Request-Method")
	if method == "" {
		method = r.Method
	}
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", "GET, PUT, HEAD, DELETE, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Content-MD5, Range, x-amz-content-sha256, x-amz-date, x-amz-copy-source, x-amz-meta-*")
	h.Set("Access-Control-Max-Age", "3600")
	w.WriteHeader(http.StatusOK)
}

// requestLocation builds the object Location URL used by multipart complete,
// preserving the external host and scheme (behind a TLS terminator the
// X-Forwarded-Proto header tells us the public scheme).
func requestLocation(r *http.Request, bucket, key string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd == "https" || fwd == "http" {
		scheme = fwd
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}
	return scheme + "://" + host + "/" + url.PathEscape(bucket) + "/" + url.PathEscape(key)
}
