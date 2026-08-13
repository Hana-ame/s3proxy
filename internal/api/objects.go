package api

// Object operations: GET/HEAD (with Range and conditional headers), PUT
// (plain and copy-source based), DELETE. Multipart sub-resources (uploadId
// / partNumber / uploads) are dispatched into multipart.go.

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"s3proxy/internal/store"
	"s3proxy/internal/tier"
)

// serveObject dispatches one object-scoped request.
func (s *Server) serveObject(w http.ResponseWriter, r *http.Request, requestID, bucket, key string, q url.Values) {
	switch r.Method {
	case http.MethodGet:
		if q.Has("uploadId") {
			// ListParts: GET /key?uploadId=... — the staged parts are the
			// subject, not the (not yet existing) final object.
			s.handleListParts(w, r, requestID, bucket, key, q.Get("uploadId"))
			return
		}
		s.handleGetObject(w, r, requestID, bucket, key)
	case http.MethodHead:
		s.handleHeadObject(w, r, requestID, bucket, key)
	case http.MethodPut:
		switch {
		case q.Has("partNumber") && q.Has("uploadId"):
			s.handleUploadPart(w, r, requestID, bucket, key, q)
		case r.Header.Get("x-amz-copy-source") != "":
			s.handleCopyObject(w, r, requestID, bucket, key)
		default:
			s.handlePutObject(w, r, requestID, bucket, key)
		}
	case http.MethodDelete:
		if q.Has("uploadId") {
			// AbortMultipartUpload. A second abort of the same upload must
			// yield NoSuchUpload (404), not a silent 204.
			if err := s.uploads.abort(r.Context(), q.Get("uploadId")); err != nil {
				if errors.Is(err, store.ErrNotFound) {
					writeError(w, r, &s3Err{status: http.StatusNotFound, code: "NoSuchUpload", message: "The specified multipart upload does not exist."}, requestID)
					return
				}
				writeError(w, r, fmtErr("%v", err), requestID)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if err := s.tier.DeleteObject(r.Context(), bucket, key); err != nil {
			writeError(w, r, fmtErr("%v", err), requestID)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		switch {
		case q.Has("uploadId"):
			s.handleCompleteMultipart(w, r, requestID, bucket, key, q)
		case q.Has("uploads"):
			// Initiate multipart: POST /key?uploads (flag-style param, no
			// value), so Has() not Get().
			s.handleCreateMultipartUpload(w, r, requestID, bucket, key)
		default:
			writeError(w, r, &s3Err{status: http.StatusNotImplemented, code: "NotImplemented", message: "POST is only supported for multipart completes."}, requestID)
		}
	default:
		writeError(w, r, &s3Err{status: http.StatusMethodNotAllowed, code: "MethodNotAllowed", message: "The specified method is not allowed against this resource."}, requestID)
	}
}

// parseRange parses an S3 Range header into a store.Range. Mirrors the
// s3-store implementation; sanity for start>=size is left to the backend's
// resolved span (which bounds-checkes against its own object size).
func parseRange(header string, size int64) (store.Range, bool, error) {
	if header == "" {
		return store.Range{}, false, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return store.Range{}, false, errors.New("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") || spec == "" {
		return store.Range{}, false, errors.New("only single ranges are supported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if strings.TrimSpace(parts[0]) == "" {
		suffix, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || suffix <= 0 {
			return store.Range{}, false, errors.New("invalid suffix range")
		}
		if suffix > size {
			suffix = size
		}
		return store.Range{Start: size - suffix, End: size - 1}, true, nil
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || start < 0 {
		return store.Range{}, false, errors.New("invalid start range")
	}
	if start >= size {
		return store.Range{}, false, errors.New("start beyond object size")
	}
	if strings.TrimSpace(parts[1]) == "" {
		return store.Range{Start: start, End: -1}, true, nil
	}
	end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil || end < start {
		return store.Range{}, false, errors.New("invalid end range")
	}
	if end >= size {
		end = size - 1
	}
	return store.Range{Start: start, End: end}, true, nil
}

// checkConditional evaluates the S3 conditional GET headers against the
// entry; returns an *s3Err to send (nil = proceed). Note that GET with
// If-None-Match matching returns 304 Not Modified, not 412.
func checkConditional(r *http.Request, e *tier.Entry) *s3Err {
	if im := r.Header.Get("If-Match"); im != "" {
		if !etagMatches(im, e.ETag) {
			return &s3Err{status: http.StatusPreconditionFailed, code: "PreconditionFailed", message: "At least one of the pre-conditions you specified did not hold."}
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if etagMatches(inm, e.ETag) {
			return &s3Err{status: http.StatusNotModified, code: "NotModified", message: "Not Modified"}
		}
	}
	if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && e.LastModified.After(t) {
			return &s3Err{status: http.StatusPreconditionFailed, code: "PreconditionFailed", message: "At least one of the pre-conditions you specified did not hold."}
		}
	}
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !e.LastModified.After(t) {
			return &s3Err{status: http.StatusNotModified, code: "NotModified", message: "Not Modified"}
		}
	}
	return nil
}

// etagMatches compares a client-supplied etag list (comma separated, each
// may be quoted or a bare hex and * is a wildcard) with our stored etag.
func etagMatches(list, etag string) bool {
	ours := strings.Trim(etag, `"`)
	valid := ours != ""
	for _, item := range strings.Split(list, ",") {
		t := strings.TrimSpace(strings.Trim(item, `"`))
		if t == "*" || (valid && strings.EqualFold(t, ours)) {
			return true
		}
	}
	return false
}

// writeObjectHeaders emits the common metadata headers for GET/HEAD.
func writeObjectHeaders(w http.ResponseWriter, e *tier.Entry, size int64) {
	h := w.Header()
	if e.ETag != "" {
		h.Set("ETag", e.ETag)
	}
	if e.ContentType != "" {
		h.Set("Content-Type", e.ContentType)
	}
	if !e.LastModified.IsZero() {
		h.Set("Last-Modified", e.LastModified.UTC().Format(http.TimeFormat))
	}
	for k, v := range e.Metadata {
		h.Set("x-amz-meta-"+k, v)
	}
	sc := e.StorageClass
	if sc == "" {
		sc = "STANDARD"
	}
	if sc != "STANDARD" {
		h.Set("x-amz-storage-class", sc)
	}
	h.Set("Accept-Ranges", "bytes")
	_ = size
}

func (s *Server) handleGetObject(w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	e, err := s.tier.HeadObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNotFoundKey(bucket, key), requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if cerr := checkConditional(r, &e); cerr != nil {
		if cerr.code == "NotModified" {
			writeObjectHeaders(w, &e, e.Size)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeError(w, r, cerr, requestID)
		return
	}
	rng, isRange, rngErr := parseRange(r.Header.Get("Range"), e.Size)
	if rngErr != nil && isRange {
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(e.Size, 10))
		writeError(w, r, &s3Err{status: http.StatusRequestedRangeNotSatisfiable, code: "InvalidRange", message: "The requested range is not satisfiable."}, requestID)
		return
	}
	if !isRange {
		// parseRange's zero value is {0,0} (a one-byte slice), which would
		// make an unconditional GET return a single byte. Normalize a
		// rangeless request to the full object here so the backend never
		// has to guess between "no range" and "first byte".
		rng = store.Range{Start: 0, End: -1}
	}
	res, _, err := s.tier.GetObject(r.Context(), bucket, key, rng)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNotFoundKey(bucket, key), requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	defer res.Body.Close()

	writeObjectHeaders(w, &e, e.Size)
	span := res.Span
	status := http.StatusOK
	if isRange && span.Start >= 0 && span.Start < e.Size {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(span.Start, 10)+"-"+strconv.FormatInt(span.End, 10)+"/"+strconv.FormatInt(e.Size, 10))
		w.Header().Set("Content-Length", strconv.FormatInt(span.End-span.Start+1, 10))
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(e.Size, 10))
	}
	// response-content-* overrides used by presigned downloads.
	if ct := r.URL.Query().Get("response-content-type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	if cd := r.URL.Query().Get("response-content-disposition"); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		io.Copy(w, res.Body)
	}
}

func (s *Server) handleHeadObject(w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	e, err := s.tier.HeadObject(r.Context(), bucket, key)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNotFoundKey(bucket, key), requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if cerr := checkConditional(r, &e); cerr != nil {
		if cerr.code == "NotModified" {
			writeObjectHeaders(w, &e, e.Size)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		writeError(w, r, cerr, requestID)
		return
	}
	writeObjectHeaders(w, &e, e.Size)
	w.Header().Set("Content-Length", strconv.FormatInt(e.Size, 10))
	w.WriteHeader(http.StatusOK)
}

// handlePutObject ingests a plain object write. Content-MD5 is verified
// against a tee'd hash of the request body (a client-side integrity check
// S3 supports; a mismatch must fail with 400 BadDigest).
func (s *Server) handlePutObject(w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	md5Hex := r.Header.Get("Content-MD5")
	var body io.Reader = r.Body
	var hasher *hashTee
	if md5Hex != "" {
		hasher = &hashTee{r: r.Body, h: md5.New()}
		body = hasher
	}

	e, err := s.tier.PutObject(r.Context(), bucket, key, body, r.ContentLength, tier.PutOpts{
		ContentType:  contentTypeOf(r),
		Metadata:     userMetadata(r),
		StorageClass: r.Header.Get("x-amz-storage-class"),
	})
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if hasher != nil {
		got := hex.EncodeToString(hasher.h.Sum(nil))
		if !strings.EqualFold(got, md5Hex) {
			// Best-effort cleanup of the just-written object; S3 leaves
			// nothing behind on BadDigest.
			s.tier.DeleteObject(r.Context(), bucket, key)
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "BadDigest", message: "The Content-MD5 you specified did not match what we received."}, requestID)
			return
		}
	}
	w.Header().Set("ETag", e.ETag)
	w.WriteHeader(http.StatusOK)
}

// hashTee passes through the body while hashing it.
type hashTee struct {
	r io.Reader
	h hash.Hash
}

func (t *hashTee) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.h.Write(p[:n])
	}
	return n, err
}

// contentTypeOf normalizes an absent Content-Type header to
// application/octet-stream, matching S3's default.
func contentTypeOf(r *http.Request) string {
	if ct := r.Header.Get("Content-Type"); ct != "" {
		return ct
	}
	return "application/octet-stream"
}

// userMetadata extracts x-amz-meta-* headers from a request.
func userMetadata(r *http.Request) map[string]string {
	var meta map[string]string
	for k, v := range r.Header {
		lk := strings.ToLower(k)
		if strings.HasPrefix(lk, "x-amz-meta-") && len(v) > 0 {
			if meta == nil {
				meta = make(map[string]string)
			}
			meta[strings.TrimPrefix(lk, "x-amz-meta-")] = v[0]
		}
	}
	return meta
}

// handleCopyObject implements PUT with x-amz-copy-source. The source is
// streamed through the tier (which may promote it from a cold pool), then
// written back through the hot pool: this keeps copies correct across any
// tier combination without server-side-copy support in the plugins.
func (s *Server) handleCopyObject(w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	src := r.Header.Get("x-amz-copy-source")
	srcBucket, srcKey, ok := parseCopySource(src)
	if !ok {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidArgument", message: "Invalid copy source."}, requestID)
		return
	}
	if srcBucket == bucket && srcKey == key {
		// S3 replaces the object with itself; rclone's copy of an object
		// onto itself hits this path. We re-stream anyway (harmless).
	}

	e, err := s.tier.HeadObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNotFoundKey(srcBucket, srcKey), requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	res, _, err := s.tier.GetObject(r.Context(), srcBucket, srcKey, store.Range{Start: 0, End: -1})
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	defer res.Body.Close()
	put, err := s.tier.PutObject(r.Context(), bucket, key, res.Body, res.Info.Size, tier.PutOpts{
		ContentType:  e.ContentType,
		Metadata:     e.Metadata,
		StorageClass: r.Header.Get("x-amz-storage-class"),
	})
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if put.ContentType == "" {
		put.ContentType = e.ContentType
	}
	var copyRes struct {
		XMLName      xml.Name `xml:"CopyObjectResult"`
		Xmlns        string   `xml:"xmlns,attr"`
		ETag         string   `xml:"ETag"`
		LastModified string   `xml:"LastModified"`
	}
	copyRes.Xmlns = s3Namespace
	copyRes.ETag = put.ETag
	copyRes.LastModified = put.LastModified.UTC().Format(time.RFC3339)
	writeXML(w, http.StatusOK, requestID, copyRes)
}

// parseCopySource splits "/bucket/key[?versionId=...]" into bucket+key.
func parseCopySource(src string) (bucket, key string, ok bool) {
	src = strings.TrimPrefix(src, "/")
	if i := strings.Index(src, "?"); i >= 0 {
		src = src[:i] // drop versionId/other query params — versioning is off
	}
	b, rest, found := strings.Cut(src, "/")
	if !found || b == "" || rest == "" {
		return "", "", false
	}
	// The source is URL-encoded by most SDKs (aws-cli sends the raw key).
	if dec, err := url.QueryUnescape(rest); err == nil && dec != "" {
		rest = dec
	}
	return b, rest, true
}
