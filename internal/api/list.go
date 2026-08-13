package api

// ListObjects V1/V2 responses served from the tier index, and the bulk
// delete (POST ?delete) used by rclone/aws-cli rm.

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"s3proxy/internal/store"
	"s3proxy/internal/tier"
)

type listEntryXML struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixXML struct {
	Prefix string `xml:"Prefix"`
}

func (s *Server) handleListObjects(w http.ResponseWriter, r *http.Request, requestID, bucket string) {
	q := r.URL.Query()
	maxKeys := 1000
	if v := q.Get("max-keys"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxKeys = n
		}
	}
	params := tier.ListParams{
		Bucket:    bucket,
		Prefix:    q.Get("prefix"),
		Delimiter: q.Get("delimiter"),
		MaxKeys:   maxKeys,
	}
	v2 := q.Get("list-type") == "2"
	if v2 {
		// ContinuationToken wins over start-after (SDKs always send the
		// token; start-after is used by fresh listings).
		if tok := q.Get("continuation-token"); tok != "" {
			// Tokens carry the absolute "bucket/key" position the index
			// uses; the request-scoped form is just the key.
			if key, ok := absoluteKey(bucket, tok); ok {
				params.StartAfter = key
			}
		} else if sa := q.Get("start-after"); sa != "" {
			if key, ok := absoluteKey(bucket, sa); ok {
				params.StartAfter = key
			}
		}
	} else if marker := q.Get("marker"); marker != "" {
		if key, ok := absoluteKey(bucket, marker); ok {
			params.StartAfter = key
		}
	}

	if err := s.tier.HeadBucket(r.Context(), bucket); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, r, errNotFoundBucket(bucket), requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	res, err := s.tier.ListObjects(r.Context(), params)
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}

	// Next-page pointers: the index token is absolute ("bucket/key"); S3
	// clients expect a plain key (v2) or the same for the marker (v1).
	nextToken := ""
	if tok := res.NextToken; tok != "" {
		if _, k, ok := strings.Cut(tok, "/"); ok {
			nextToken = k
		}
	}

	if v2 {
		s.writeListV2(w, requestID, bucket, q, maxKeys, &res, nextToken)
		return
	}
	s.writeListV1(w, requestID, bucket, q, maxKeys, &res, nextToken)
}

// absoluteKey rebases a request-scoped key ("dir/file") onto the index's
// absolute form ("bucket/dir/file") so continuation tokens address the same
// space in every page.
func absoluteKey(bucket, key string) (string, bool) {
	if key == "" {
		return "", false
	}
	return bucket + "/" + key, true
}

type listV2Result struct {
	XMLName         xml.Name          `xml:"ListBucketResult"`
	Xmlns           string            `xml:"xmlns,attr"`
	Name            string            `xml:"Name"`
	Prefix          string            `xml:"Prefix"`
	KeyCount        int               `xml:"KeyCount"`
	MaxKeys         int               `xml:"MaxKeys"`
	Delimiter       string            `xml:"Delimiter,omitempty"`
	IsTruncated     bool              `xml:"IsTruncated"`
	Contents        []listEntryXML    `xml:"Contents"`
	CommonPrefixes  []commonPrefixXML `xml:"CommonPrefixes"`
	NextToken       string            `xml:"NextContinuationToken,omitempty"`
	StartAfter      string            `xml:"StartAfter,omitempty"`
	ContinuationTok string            `xml:"ContinuationToken,omitempty"`
}

func (s *Server) writeListV2(w http.ResponseWriter, requestID, bucket string, q url.Values, maxKeys int, res *tier.ListResult, nextToken string) {
	out := listV2Result{
		Xmlns:           s3Namespace,
		Name:            bucket,
		Prefix:          q.Get("prefix"),
		MaxKeys:         maxKeys,
		Delimiter:       q.Get("delimiter"),
		IsTruncated:     res.IsTruncated,
		StartAfter:      q.Get("start-after"),
		ContinuationTok: q.Get("continuation-token"),
	}
	for _, e := range res.Entries {
		out.Contents = append(out.Contents, listEntryXML{
			Key:          e.Key,
			LastModified: e.LastModified.UTC().Format(time.RFC3339Nano),
			ETag:         e.ETag,
			Size:         e.Size,
			StorageClass: orDefault(e.StorageClass, "STANDARD"),
		})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: p})
	}
	out.KeyCount = len(out.Contents) + len(out.CommonPrefixes)
	if res.IsTruncated {
		out.NextToken = nextToken
	}
	writeXML(w, http.StatusOK, requestID, out)
}

type listV1Result struct {
	XMLName        xml.Name          `xml:"ListBucketResult"`
	Xmlns          string            `xml:"xmlns,attr"`
	Name           string            `xml:"Name"`
	Prefix         string            `xml:"Prefix"`
	Marker         string            `xml:"Marker"`
	NextMarker     string            `xml:"NextMarker,omitempty"`
	MaxKeys        int               `xml:"MaxKeys"`
	Delimiter      string            `xml:"Delimiter,omitempty"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Contents       []listEntryXML    `xml:"Contents"`
	CommonPrefixes []commonPrefixXML `xml:"CommonPrefixes"`
}

func (s *Server) writeListV1(w http.ResponseWriter, requestID, bucket string, q url.Values, maxKeys int, res *tier.ListResult, nextToken string) {
	out := listV1Result{
		Xmlns:       s3Namespace,
		Name:        bucket,
		Prefix:      q.Get("prefix"),
		Marker:      q.Get("marker"),
		MaxKeys:     maxKeys,
		Delimiter:   q.Get("delimiter"),
		IsTruncated: res.IsTruncated,
	}
	for _, e := range res.Entries {
		out.Contents = append(out.Contents, listEntryXML{
			Key:          e.Key,
			LastModified: e.LastModified.UTC().Format(time.RFC3339Nano),
			ETag:         e.ETag,
			Size:         e.Size,
			StorageClass: orDefault(e.StorageClass, "STANDARD"),
		})
	}
	for _, p := range res.CommonPrefixes {
		out.CommonPrefixes = append(out.CommonPrefixes, commonPrefixXML{Prefix: p})
	}
	if res.IsTruncated {
		out.NextMarker = nextToken
	}
	writeXML(w, http.StatusOK, requestID, out)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// --- bulk delete -----------------------------------------------------------

type deleteRequestXML struct {
	XMLName xml.Name `xml:"Delete"`
	Objects []struct {
		Key string `xml:"Key"`
	} `xml:"Object"`
	Quiet bool `xml:"Quiet"`
}

type deleteResultXML struct {
	XMLName xml.Name `xml:"DeleteResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Deleted []struct {
		Key string `xml:"Key"`
	} `xml:"Deleted"`
	Errors []struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}

// handleDeleteObjects implements POST ?delete, the multi-object delete
// protocol rclone and aws s3api use for bulk removal.
func (s *Server) handleDeleteObjects(w http.ResponseWriter, r *http.Request, requestID, bucket string) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	var req deleteRequestXML
	if err := xml.Unmarshal(body, &req); err != nil {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "MalformedXML", message: "The XML you provided was not well-formed."}, requestID)
		return
	}
	if len(req.Objects) == 0 {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "MalformedXML", message: "The XML you provided was not well-formed."}, requestID)
		return
	}
	var out deleteResultXML
	out.Xmlns = s3Namespace
	for _, o := range req.Objects {
		key := o.Key
		if key == "" || strings.ContainsRune(key, 0) {
			out.Errors = append(out.Errors, struct {
				Key     string `xml:"Key"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}{Key: key, Code: "InvalidArgument", Message: "Invalid key"})
			continue
		}
		if err := s.tier.DeleteObject(r.Context(), bucket, key); err != nil {
			out.Errors = append(out.Errors, struct {
				Key     string `xml:"Key"`
				Code    string `xml:"Code"`
				Message string `xml:"Message"`
			}{Key: key, Code: "InternalError", Message: err.Error()})
			continue
		}
		if !req.Quiet {
			out.Deleted = append(out.Deleted, struct {
				Key string `xml:"Key"`
			}{Key: key})
		}
	}
	writeXML(w, http.StatusOK, requestID, out)
}
