package api

// S3-compliant XML error bodies, matching the format AWS clients parse
// (Code/Message/Resource/Bucket/Key/RequestId).

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// s3Err describes one S3 error.
type s3Err struct {
	status  int
	code    string
	message string
	bucket  string
	key     string
}

// notFoundKey / notFoundBucket are the two most common error templates.
func errNotFoundKey(bucket, key string) *s3Err {
	return &s3Err{status: http.StatusNotFound, code: "NoSuchKey", message: "The specified key does not exist.", bucket: bucket, key: key}
}

func errNotFoundBucket(bucket string) *s3Err {
	return &s3Err{status: http.StatusNotFound, code: "NoSuchBucket", message: "The specified bucket does not exist.", bucket: bucket}
}

type errorXML struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	Bucket    string   `xml:"BucketName,omitempty"`
	Key       string   `xml:"Key,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// writeError renders an S3 error response.
func writeError(w http.ResponseWriter, r *http.Request, e *s3Err, requestID string) {
	res := errorXML{
		Code:      e.code,
		Message:   xmlEscape(e.message),
		Resource:  r.URL.Path,
		Bucket:    xmlEscape(e.bucket),
		Key:       xmlEscape(e.key),
		RequestID: requestID,
	}
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(e.status)
	io.WriteString(w, xml.Header)
	enc := xml.NewEncoder(w)
	enc.Encode(res)
}

// writeXML renders an XML result document, preserving the header+namespace
// convention S3 clients expect.
func writeXML(w http.ResponseWriter, status int, requestID string, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-amz-request-id", requestID)
	w.WriteHeader(status)
	io.WriteString(w, xml.Header)
	enc := xml.NewEncoder(w)
	enc.Indent("", "  ")
	enc.Encode(v)
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	// xml.EscapeText also escapes quotes to &quot;, which S3 does not do in
	// message text; it is safe enough for client-side parsing and matches
	// MinIO behavior.
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func fmtErr(f string, a ...any) *s3Err {
	return &s3Err{status: http.StatusInternalServerError, code: "InternalError", message: fmt.Sprintf(f, a...)}
}
