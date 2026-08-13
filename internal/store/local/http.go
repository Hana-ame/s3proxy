package local

// S3-shaped HTTP adapter over the local Store. Lets a local pool serve as a
// standalone S3-compatible endpoint (cmd/s3-store) and doubles as the test
// double for the remote S3 plugin (internal/store/s3store tests point it at
// this handler over httptest).
//
// Implements the subset the plugin clients use: Put/Get/Head/Delete object,
// buckets, ListObjectsV2 with prefix/continuation tokens, byte ranges.

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"s3proxy/internal/store"
)

const ns = "http://s3.amazonaws.com/doc/2006-03-01/"

// NewHTTPHandler exposes s as an HTTP S3 endpoint.
func NewHTTPHandler(s *Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveHTTPStore(w, r, s)
	})
	return mux
}

func splitRequestPath(p string) (bucket, key string, valid bool) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", "", false
	}
	b, rest, _ := strings.Cut(p, "/")
	return b, rest, true
}

func writeXMLResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	body := fmt.Sprintf("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>%s</Code><Message>%s</Message></Error>",
		code, xmlEscape(msg))
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	io.WriteString(w, body)
}

func xmlEscape(s string) string {
	var b strings.Builder
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func serveHTTPStore(w http.ResponseWriter, r *http.Request, s *Store) {
	bucket, key, ok := splitRequestPath(r.URL.Path)
	if !ok {
		writeError(w, http.StatusBadRequest, "InvalidRequest", "missing bucket")
		return
	}
	ctx := r.Context()
	switch r.Method {
	case http.MethodPut:
		if key == "" {
			if err := s.EnsureBucket(ctx, bucket); err != nil {
				writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		info, err := s.Put(ctx, bucket+"/"+key, r.Body, r.ContentLength, r.Header.Get("Content-Type"), store.PutOptions{})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		w.Header().Set("ETag", info.ETag)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if key == "" {
			if !handleList(w, r, s, bucket) {
				writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
			}
			return
		}
		handleGet(w, r, s, bucket, key)
	case http.MethodHead:
		if key == "" {
			ok, err := s.BucketExists(ctx, bucket)
			if err != nil || !ok {
				writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		info, err := s.Head(ctx, bucket+"/"+key)
		if err != nil {
			writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
			return
		}
		w.Header().Set("ETag", info.ETag)
		w.Header().Set("Content-Type", info.ContentType)
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.Header().Set("Last-Modified", info.LastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if key == "" {
			if _, err := os.Stat(s.objectPath(bucket)); err != nil {
				writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
				return
			}
			if err := os.RemoveAll(s.objectPath(bucket)); err != nil {
				writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		s.Delete(ctx, bucket+"/"+key)
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "MethodNotAllowed", "method not allowed")
	}
}

func handleGet(w http.ResponseWriter, r *http.Request, s *Store, bucket, key string) {
	ctx := r.Context()
	info, err := s.Head(ctx, bucket+"/"+key)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	rng := store.Range{Start: 0, End: -1}
	parseRangeHeader(r.Header.Get("Range"), info.Size, &rng)

	res, err := s.Get(ctx, bucket+"/"+key, rng)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
		return
	}
	defer res.Body.Close()
	h := w.Header()
	h.Set("ETag", res.Info.ETag)
	h.Set("Content-Type", res.Info.ContentType)
	h.Set("Last-Modified", res.Info.LastModified.UTC().Format(http.TimeFormat))
	h.Set("Accept-Ranges", "bytes")
	status := http.StatusOK
	if res.Span.Start > 0 || res.Span.End < res.Info.Size-1 {
		status = http.StatusPartialContent
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", res.Span.Start, res.Span.End, res.Info.Size))
		h.Set("Content-Length", strconv.FormatInt(res.Span.End-res.Span.Start+1, 10))
	} else {
		h.Set("Content-Length", strconv.FormatInt(res.Info.Size, 10))
	}
	w.WriteHeader(status)
	io.Copy(w, res.Body)
}

// parseRangeHeader resolves an RFC 7233 range into rng (best-effort: any
// parse failure leaves the range untouched = full object; the local plugin
// bounds-checks against the object size anyway).
func parseRangeHeader(header string, size int64, rng *store.Range) {
	if !strings.HasPrefix(header, "bytes=") {
		return
	}
	spec := strings.TrimPrefix(header, "bytes=")
	if strings.Contains(spec, ",") {
		return
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return
	}
	if strings.TrimSpace(parts[0]) == "" {
		suffix, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil || suffix <= 0 {
			return
		}
		if suffix > size {
			suffix = size
		}
		rng.Start, rng.End = size-suffix, size-1
		return
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil || start < 0 || start >= size {
		return
	}
	rng.Start = start
	rng.End = -1
	if strings.TrimSpace(parts[1]) != "" {
		if end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64); err == nil && end >= start {
			rng.End = end
			if rng.End >= size {
				rng.End = size - 1
			}
		}
	}
}

type listResponse struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	Xmlns       string   `xml:"xmlns,attr"`
	Name        string   `xml:"Name"`
	Prefix      string   `xml:"Prefix"`
	KeyCount    int      `xml:"KeyCount"`
	MaxKeys     int      `xml:"MaxKeys"`
	IsTruncated bool     `xml:"IsTruncated"`
	Contents    []struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	} `xml:"Contents"`
	NextToken string `xml:"NextContinuationToken,omitempty"`
}

// handleList serves ListObjectsV2 with prefix + continuation tokens, the
// pagination style the S3 plugin client uses in big buckets.
func handleList(w http.ResponseWriter, r *http.Request, s *Store, bucket string) bool {
	q := r.URL.Query()
	prefix := q.Get("prefix")
	startAfter := q.Get("continuation-token")
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 1000 {
			maxKeys = n
		}
	}
	pg, err := s.List(r.Context(), bucket+"/"+prefix, startAfter, maxKeys)
	if err != nil {
		return false
	}
	out := listResponse{
		Xmlns:       ns,
		Name:        bucket,
		Prefix:      prefix,
		MaxKeys:     maxKeys,
		IsTruncated: pg.IsTruncated,
		NextToken:   pg.NextToken,
	}
	for _, e := range pg.Entries {
		out.Contents = append(out.Contents, struct {
			Key          string `xml:"Key"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
			StorageClass string `xml:"StorageClass"`
		}{
			Key:          strings.TrimPrefix(e.Key, bucket+"/"),
			LastModified: e.LastModified.UTC().Format(time.RFC3339),
			ETag:         e.ETag,
			Size:         e.Size,
			StorageClass: "STANDARD",
		})
	}
	out.KeyCount = len(out.Contents)
	writeXMLResponse(w, http.StatusOK, out)
	return true
}
