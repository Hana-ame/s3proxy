package api

// SigV4 request verification for client requests. Supports both header-based
// Authorization and presigned-query signatures (browser download URLs),
// matching what AWS SDKs, rclone and s3cmd produce against a path-style
// endpoint.

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	awsAlgorithm    = "AWS4-HMAC-SHA256"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	iso8601Basic    = "20060102T150405Z"
	yyyymmdd        = "20060102"
	timeSkewWindow  = 15 * time.Minute
)

// verifySigV4 validates the SigV4 signature on r using the shared client
// credentials. It supports both header-based Authorization and presigned
// query signatures (used by browser download URLs). The returned access key
// id identifies the authenticated client.
func verifySigV4(r *http.Request, creds map[string]string, now time.Time) (string, bool) {
	auth := r.Header.Get("Authorization")
	if auth != "" {
		return verifyHeaderSig(r, auth, creds, now)
	}
	if r.URL.Query().Get("X-Amz-Algorithm") != "" {
		return verifyQuerySig(r, creds, now)
	}
	return "", false
}

func verifyHeaderSig(r *http.Request, auth string, creds map[string]string, now time.Time) (string, bool) {
	parts := strings.SplitN(auth, " ", 2)
	if len(parts) != 2 || parts[0] != awsAlgorithm {
		return "", false
	}
	fields := map[string]string{}
	for _, kv := range strings.Split(parts[1], ",") {
		k, v, ok := strings.Cut(kv, "=")
		if ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	credential := fields["Credential"]
	signature := fields["Signature"]
	signedHeaders := fields["SignedHeaders"]
	if credential == "" || signature == "" || signedHeaders == "" {
		return "", false
	}
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 {
		return "", false
	}
	ak, date, region, service := credParts[0], credParts[1], credParts[2], credParts[3]
	sk, ok := creds[ak]
	if !ok {
		return "", false
	}
	signingTime, ok := parseSigningTime(r.Header.Get("X-Amz-Date"))
	if !ok || absDur(now.Sub(signingTime)) > timeSkewWindow || date != signingTime.UTC().Format(yyyymmdd) {
		return "", false
	}

	canonicalHeaders, ok := buildCanonicalHeaders(r, signedHeaders)
	if !ok {
		return "", false
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}
	canonical := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonicalQuery(r.URL),
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		awsAlgorithm,
		signingTime.UTC().Format(iso8601Basic),
		scope,
		hex.EncodeToString(sha256sum([]byte(canonical))),
	}, "\n")
	expected := signatureHex(sk, date, region, service, stringToSign)
	if !hmac.Equal([]byte(expected), []byte(strings.ToLower(signature))) {
		return "", false
	}
	return ak, true
}

func verifyQuerySig(r *http.Request, creds map[string]string, now time.Time) (string, bool) {
	q := r.URL.Query()
	credential := q.Get("X-Amz-Credential")
	signature := q.Get("X-Amz-Signature")
	signedHeaders := q.Get("X-Amz-SignedHeaders")
	dateStr := q.Get("X-Amz-Date")
	expiresStr := q.Get("X-Amz-Expires")
	if credential == "" || signature == "" || signedHeaders == "" || dateStr == "" {
		return "", false
	}
	credParts := strings.Split(credential, "/")
	if len(credParts) != 5 {
		return "", false
	}
	ak, date, region, service := credParts[0], credParts[1], credParts[2], credParts[3]
	sk, ok := creds[ak]
	if !ok {
		return "", false
	}
	signingTime, err := time.Parse(iso8601Basic, dateStr)
	if err != nil {
		return "", false
	}
	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil {
		return "", false
	}
	if now.Before(signingTime.Add(-timeSkewWindow)) || now.After(signingTime.Add(time.Duration(expires)*time.Second)) {
		return "", false
	}

	// Canonical query excludes X-Amz-Signature but includes every other
	// query parameter.
	canonical, err := canonicalQueryString(r.URL, "X-Amz-Signature")
	if err != nil {
		return "", false
	}
	canonicalHeaders, ok := buildCanonicalHeaders(r, signedHeaders)
	if !ok {
		return "", false
	}
	payloadHash := unsignedPayload
	if v := q.Get("X-Amz-Content-Sha256"); v != "" {
		payloadHash = v
	}
	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL),
		canonical,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	scope := strings.Join([]string{date, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		awsAlgorithm,
		signingTime.UTC().Format(iso8601Basic),
		scope,
		hex.EncodeToString(sha256sum([]byte(canonicalRequest))),
	}, "\n")
	expected := signatureHex(sk, date, region, service, stringToSign)
	if !hmac.Equal([]byte(expected), []byte(strings.ToLower(signature))) {
		return "", false
	}
	return ak, true
}

func parseSigningTime(v string) (time.Time, bool) {
	if t, err := time.Parse(iso8601Basic, v); err == nil {
		return t, true
	}
	if t, err := http.ParseTime(v); err == nil {
		return t, true
	}
	return time.Time{}, false
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

// buildCanonicalHeaders renders the canonical header block for the headers
// listed in signedHeaders, reading values from the request.
func buildCanonicalHeaders(r *http.Request, signedHeaders string) (string, bool) {
	var b strings.Builder
	for _, name := range strings.Split(signedHeaders, ";") {
		if name == "" {
			continue
		}
		var values []string
		switch name {
		case "host":
			host := r.Host
			if host == "" {
				host = r.URL.Host
			}
			values = []string{host}
		case "content-length":
			// Go's server moves Content-Length out of r.Header into
			// r.ContentLength, but the signer may include it in the
			// signed headers.
			values = []string{strconv.FormatInt(r.ContentLength, 10)}
		default:
			h, ok := r.Header[http.CanonicalHeaderKey(name)]
			if !ok {
				return "", false
			}
			values = h
		}
		b.WriteString(strings.ToLower(name))
		b.WriteString(":")
		b.WriteString(strings.Join(values, ","))
		b.WriteString("\n")
	}
	return b.String(), true
}

// canonicalURI returns the AWS-canonical form of the request path.
func canonicalURI(u *url.URL) string {
	p := u.EscapedPath()
	if p == "" {
		return "/"
	}
	return p
}

func canonicalQuery(u *url.URL) string {
	s, err := canonicalQueryString(u, "")
	if err != nil {
		return ""
	}
	return s
}

func canonicalQueryString(u *url.URL, exclude string) (string, error) {
	vals := u.Query()
	if exclude != "" {
		vals.Del(exclude)
	}
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		for _, v := range vals[k] {
			parts = append(parts, awsEncode(k)+"="+awsEncode(v))
		}
	}
	return strings.Join(parts, "&"), nil
}

// awsEncode percent-encodes per SigV4 rules: unreserved characters and ~ are
// left untouched, everything else becomes %XX.
func awsEncode(s string) string {
	const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if strings.IndexByte(unreserved, c) >= 0 {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func sha256sum(b []byte) []byte {
	h := sha256.Sum256(b)
	return h[:]
}

func signatureHex(sk, date, region, service, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+sk), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
