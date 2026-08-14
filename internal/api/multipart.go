package api

// Virtual multipart uploads. Parts are staged as files under the state dir
// (not inside any storage pool — they are transient and would pollute the
// buffer), and CompleteMultipartUpload streams them through the tiered store
// as one ordered object. The composite S3 etag ("md5(concat(part-md5s))-N")
// is computed from the recorded part digests, matching what SDKs validate.
//
// Cheats noted: ListMultipartUploads returns everything without paging
// (IsTruncated=false) — real clients tolerate it.

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"s3proxy/internal/store"
	"s3proxy/internal/tier"
)

type partMeta struct {
	PartNumber int    `json:"part_number"`
	Size       int64  `json:"size"`
	MD5        []byte `json:"md5"` // raw 16 bytes, concatenated for the composite etag
}

type uploadMeta struct {
	UploadID     string            `json:"upload_id"`
	Bucket       string            `json:"bucket"`
	Key          string            `json:"key"`
	ContentType  string            `json:"content_type"`
	Metadata     map[string]string `json:"metadata"`
	StorageClass string            `json:"storage_class"`
	Initiated    time.Time         `json:"initiated"`
	Parts        []partMeta        `json:"parts"`
}

// staleUploadTTL is the age past which an upload (and its staged parts) is
// treated as abandoned and removed. Discovery background: code review — the
// staging area had no lifecycle: an interrupted upload left its parts on
// disk forever (unbounded disk growth, easy DoS). S3 aborts idle uploads
// after 24h, so the same bound applies here.
const staleUploadTTL = 24 * time.Hour

// uploadStore owns the on-disk staging area for in-flight uploads.
type uploadStore struct {
	root string // <stateDir>/uploads

	mu    sync.Mutex
	locks uploadLockTable // per-upload lock, so parallel part uploads of one upload serialize on the manifest only

	lastCleanup time.Time // throttle: cleanupStale runs at most once per cleanupMinInterval
}

// uploadLockTable is a refcounted mutex table (the same design as the tier
// package's lockTable): entries are dropped once nobody holds or waits on
// them. Discovery background: 2026-08 review — the original
// map[string]*sync.Mutex never removed entries. In-flight uploads are
// bounded, but upload ids are random 32-hex, so over months of operation
// the map grew monotonically by one mutex per historically initiated
// upload — the exact unbounded-memory pattern the tier lockTable was
// written to fix.
type uploadLockTable struct {
	mu    sync.Mutex
	locks map[string]*uploadLockEntry
}

type uploadLockEntry struct {
	mu   sync.Mutex
	refs int
}

// lock acquires the lock for id and returns the release func. Waiters
// increment refs BEFORE blocking on the mutex, so an entry is removed only
// when nobody holds or waits on it — a later lock for the same id can never
// race a removal (which would split one logical lock into two mutexes).
func (lt *uploadLockTable) lock(id string) func() {
	lt.mu.Lock()
	e := lt.locks[id]
	if e == nil {
		e = &uploadLockEntry{}
		lt.locks[id] = e
	}
	e.refs++
	lt.mu.Unlock()

	e.mu.Lock()
	return func() {
		e.mu.Unlock()
		lt.mu.Lock()
		e.refs--
		if e.refs == 0 {
			delete(lt.locks, id)
		}
		lt.mu.Unlock()
	}
}

// cleanupMinInterval bounds how often cleanupStale walks the whole uploads
// directory. Discovery background: cleanup ran on every initiate/part
// request, an O(in-flight uploads) directory scan per upload op — a busy
// proxy with many concurrent uploads burned the disk listing on every
// part. A once-per-minute sweep is ample (staleness is measured in hours).
const cleanupMinInterval = time.Minute

func newUploadStore(stateDir string) (*uploadStore, error) {
	root := filepath.Join(stateDir, "uploads")
	if err := os.MkdirAll(filepath.Join(root, "parts"), 0o755); err != nil {
		return nil, err
	}
	return &uploadStore{root: root, locks: uploadLockTable{locks: make(map[string]*uploadLockEntry)}}, nil
}

// cleanupStale removes uploads older than staleUploadTTL together with
// their staged parts. The manifest is re-loaded UNDER the per-upload lock
// and the removal happens under that same lock. Discovery background: a
// first version checked staleness under the lock but removed the files
// AFTER unlocking — a part upload that started right after the check could
// land mid-removal and have its fresh part directory wiped (and the
// manifest saved over a removed one), contradicting "an active upload
// wins". Holding the lock through the removal makes an upload either
// finish before the removal (then it is removed — it is >24h old by its
// own initiation time, S3 semantics) or fail cleanly with NoSuchUpload.
func (u *uploadStore) cleanupStale(now time.Time) {
	u.mu.Lock()
	if now.Sub(u.lastCleanup) < cleanupMinInterval {
		u.mu.Unlock()
		return
	}
	u.lastCleanup = now
	u.mu.Unlock()

	entries, err := os.ReadDir(u.root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		unlock := u.lock(id)
		m, err := u.load(id)
		if err != nil {
			// Unreadable manifest: drop the staging dir defensively AND
			// remove the manifest itself, or every future sweep re-
			// discovers this id, re-reads the same broken file and
			// re-removes the (already gone) part dir forever — a
			// permanent tombstone with no benefit. Discovery background:
			// 2026-08 review — the .json was left behind.
			os.RemoveAll(u.partDir(id))
			os.Remove(u.manifestPath(id))
			unlock()
			continue
		}
		if now.Sub(m.Initiated) >= staleUploadTTL {
			// Remove dir first (cheap failure) then the manifest itself.
			os.RemoveAll(u.partDir(id))
			os.Remove(u.manifestPath(id))
		}
		unlock()
	}
}

func (u *uploadStore) manifestPath(id string) string { return filepath.Join(u.root, id+".json") }
func (u *uploadStore) partDir(id string) string      { return filepath.Join(u.root, "parts", id) }

func (u *uploadStore) lock(id string) func() { return u.locks.lock(id) }

func (u *uploadStore) load(id string) (*uploadMeta, error) {
	data, err := os.ReadFile(u.manifestPath(id))
	if err != nil {
		return nil, err
	}
	var m uploadMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (u *uploadStore) save(m *uploadMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := u.manifestPath(m.UploadID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, u.manifestPath(m.UploadID))
}

func (u *uploadStore) listMetas() ([]*uploadMeta, error) {
	entries, err := os.ReadDir(u.root)
	if err != nil {
		return nil, err
	}
	var out []*uploadMeta
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			id := strings.TrimSuffix(e.Name(), ".json")
			m, err := u.load(id)
			if err == nil {
				out = append(out, m)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].UploadID < out[j].UploadID
	})
	return out, nil
}

func newUploadID() string {
	var b [16]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// validUploadID rejects anything but the exact shape newUploadID generates
// (32 lowercase hex chars). uploadId arrives from the query string and is
// joined into filesystem paths below, so an unvalidated id (e.g.
// "../../escape") would read/write outside <stateDir>/uploads — path
// traversal. Discovery background: 2026-08 review — every uploadId
// consumer (part upload, list parts, complete, abort) joined the raw
// string into filepath.Join; exploiting required a crafted JSON manifest
// readable at the escaped location, but the read/write primitive existed.
func validUploadID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// --- HTTP handlers ---------------------------------------------------------

type initiateResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// handleCreateMultipartUpload: POST /bucket/key?uploads
func (s *Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, requestID, bucket, key string) {
	s.uploads.cleanupStale(time.Now())
	id := newUploadID()
	m := &uploadMeta{
		UploadID:     id,
		Bucket:       bucket,
		Key:          key,
		ContentType:  contentTypeOf(r),
		Metadata:     userMetadata(r),
		StorageClass: r.Header.Get("x-amz-storage-class"),
		Initiated:    time.Now(),
	}
	if err := s.uploads.save(m); err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if err := os.MkdirAll(s.uploads.partDir(id), 0o755); err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	writeXML(w, http.StatusOK, requestID, initiateResult{
		Xmlns: s3Namespace, Bucket: bucket, Key: key, UploadID: id,
	})
}

// handleUploadPart: PUT /bucket/key?partNumber=N&uploadId=ID
func (s *Server) handleUploadPart(w http.ResponseWriter, r *http.Request, requestID, bucket, key string, q url.Values) {
	s.uploads.cleanupStale(time.Now())
	uploadID := q.Get("uploadId")
	partStr := q.Get("partNumber")
	partNum, err := strconv.Atoi(partStr)
	if err != nil || partNum < 1 || partNum > 10000 {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidPart", message: "One or more of the specified parts could not be found."}, requestID)
		return
	}
	unlock := s.uploads.lock(uploadID)
	defer unlock()
	m, err := s.uploads.load(uploadID)
	if err != nil {
		writeError(w, r, &s3Err{status: http.StatusNotFound, code: "NoSuchUpload", message: "The specified multipart upload does not exist."}, requestID)
		return
	}
	if m.Bucket != bucket || m.Key != key {
		writeError(w, r, &s3Err{status: http.StatusNotFound, code: "NoSuchUpload", message: "The specified multipart upload does not exist."}, requestID)
		return
	}

	// Ranged copy (UploadPartCopy) reuses this path with the source in the
	// headers instead of the body. The source body is closed on return:
	// Discovery background — the old code never closed it, leaking one
	// file descriptor (local pool) / HTTP connection (s3 pool) per
	// UploadPartCopy.
	var body io.Reader = r.Body
	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		srcBody, copyErr := s.copySourceReader(r, src)
		if copyErr != nil {
			writeError(w, r, fmtErr("%v", copyErr), requestID)
			return
		}
		body = srcBody
		defer srcBody.Close()
	}

	h := md5.New()
	tee := &hashTee{r: body, h: h}
	partPath := filepath.Join(s.uploads.partDir(uploadID), strconv.Itoa(partNum)+".bin")
	if err := os.MkdirAll(filepath.Dir(partPath), 0o755); err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	// Write via temp file so a failed part never leaves a visible part.
	tmp := partPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	n, err := io.Copy(f, tee)
	cerr := f.Close()
	if err != nil {
		os.Remove(tmp)
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	if cerr != nil {
		os.Remove(tmp)
		writeError(w, r, fmtErr("%v", cerr), requestID)
		return
	}

	// Content-MD5 is verified against the staged bytes BEFORE the temp
	// file is renamed into place. A rejected part must leave the staging
	// dir exactly as it was — in particular, when this part number
	// already existed, deleting `partPath` here would destroy the OLD
	// part's .bin while the manifest still points at it, and Complete
	// would later fail with "staged part lost". Discovery background:
	// 2026-08 review — the digest check ran after os.Rename and removed
	// partPath on failure.
	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	if raw := r.Header.Get("Content-MD5"); raw != "" {
		// Content-MD5 is base64 of the 16-byte digest (RFC 1864), same as
		// the plain-PUT path. Discovery background: the old code compared
		// the hex part digest directly against the raw base64 header —
		// different encodings of the same bytes can never be equal, so
		// EVERY part upload carrying Content-MD5 failed with BadDigest
		// (boto3/aws-cli send it by default on large uploads).
		sum, err := base64.StdEncoding.DecodeString(strings.TrimSpace(raw))
		if err != nil {
			os.Remove(tmp)
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidDigest", message: "The Content-MD5 you specified is not valid."}, requestID)
			return
		}
		got := strings.Trim(etag, `"`)
		if !strings.EqualFold(got, hex.EncodeToString(sum)) {
			os.Remove(tmp)
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "BadDigest", message: "The Content-MD5 you specified did not match what we received."}, requestID)
			return
		}
	}
	// The part is now verified; commit it into the visible name.
	if err := os.Rename(tmp, partPath); err != nil {
		os.Remove(tmp)
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	// Replace-or-insert this part number in the manifest.
	replaced := false
	for i := range m.Parts {
		if m.Parts[i].PartNumber == partNum {
			m.Parts[i].Size = n
			m.Parts[i].MD5 = h.Sum(nil)
			replaced = true
			break
		}
	}
	if !replaced {
		m.Parts = append(m.Parts, partMeta{PartNumber: partNum, Size: n, MD5: h.Sum(nil)})
	}
	if err := s.uploads.save(m); err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	w.Header().Set("ETag", etag)
	w.WriteHeader(http.StatusOK)
}

// copySourceReader builds a reader over the x-amz-copy-source object,
// honoring x-amz-copy-source-range for partial part copies. The caller
// must Close the returned body.
func (s *Server) copySourceReader(r *http.Request, src string) (io.ReadCloser, error) {
	srcBucket, srcKey, ok := parseCopySource(src)
	if !ok {
		return nil, errors.New("invalid copy source")
	}
	rng := store.Range{Start: 0, End: -1}
	if cr := r.Header.Get("x-amz-copy-source-range"); cr != "" {
		var e tier.Entry
		var err error
		e, err = s.tier.HeadObject(r.Context(), srcBucket, srcKey)
		if err != nil {
			return nil, err
		}
		if r, ok, err := parseRange(cr, e.Size); err == nil && ok {
			rng = r
		} else {
			return nil, fmt.Errorf("invalid copy range")
		}
	}
	res, _, err := s.tier.GetObject(r.Context(), srcBucket, srcKey, rng)
	if err != nil {
		return nil, err
	}
	return res.Body, nil
}

type listPartsResult struct {
	XMLName              xml.Name `xml:"ListPartsResult"`
	Xmlns                string   `xml:"xmlns,attr"`
	Bucket               string   `xml:"Bucket"`
	Key                  string   `xml:"Key"`
	UploadID             string   `xml:"UploadId"`
	PartNumberMarker     int      `xml:"PartNumberMarker"`
	NextPartNumberMarker int      `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int      `xml:"MaxParts"`
	IsTruncated          bool     `xml:"IsTruncated"`
	Initiator            ownerXML `xml:"Initiator"`
	Owner                ownerXML `xml:"Owner"`
	StorageClass         string   `xml:"StorageClass"`
	Parts                []struct {
		PartNumber   int    `xml:"PartNumber"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
	} `xml:"Part"`
}

// handleListParts: GET /bucket/key?uploadId=ID
func (s *Server) handleListParts(w http.ResponseWriter, r *http.Request, requestID, bucket, key, uploadID string) {
	m, err := s.uploads.load(uploadID)
	if err != nil {
		writeError(w, r, &s3Err{status: http.StatusNotFound, code: "NoSuchUpload", message: "The specified multipart upload does not exist."}, requestID)
		return
	}
	q := r.URL.Query()
	marker := 0
	if v := q.Get("part-number-marker"); v != "" {
		marker, _ = strconv.Atoi(v)
	}
	maxParts := 1000
	if v := q.Get("max-parts"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxParts = n
		}
	}
	var parts []partMeta
	for _, p := range m.Parts {
		if p.PartNumber > marker {
			parts = append(parts, p)
		}
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].PartNumber < parts[j].PartNumber })
	truncated := len(parts) > maxParts
	if truncated {
		parts = parts[:maxParts]
	}
	out := listPartsResult{
		Xmlns: s3Namespace, Bucket: m.Bucket, Key: m.Key, UploadID: uploadID,
		PartNumberMarker: marker,
		MaxParts:         maxParts,
		IsTruncated:      truncated,
		Initiator:        ownerXML{ID: "s3proxy", DisplayName: "s3proxy"},
		Owner:            ownerXML{ID: "s3proxy", DisplayName: "s3proxy"},
		StorageClass:     orDefault(m.StorageClass, "STANDARD"),
	}
	if truncated {
		out.NextPartNumberMarker = parts[len(parts)-1].PartNumber
	}
	for _, p := range parts {
		out.Parts = append(out.Parts, struct {
			PartNumber   int    `xml:"PartNumber"`
			LastModified string `xml:"LastModified"`
			ETag         string `xml:"ETag"`
			Size         int64  `xml:"Size"`
		}{
			PartNumber:   p.PartNumber,
			LastModified: m.Initiated.UTC().Format(time.RFC3339),
			ETag:         `"` + hex.EncodeToString(p.MD5) + `"`,
			Size:         p.Size,
		})
	}
	writeXML(w, http.StatusOK, requestID, out)
}

type listUploadsResult struct {
	XMLName            xml.Name `xml:"ListMultipartUploadsResult"`
	Xmlns              string   `xml:"xmlns,attr"`
	Bucket             string   `xml:"Bucket"`
	KeyMarker          string   `xml:"KeyMarker"`
	UploadIDMarker     string   `xml:"UploadIdMarker"`
	NextKeyMarker      string   `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string   `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads         int      `xml:"MaxUploads"`
	IsTruncated        bool     `xml:"IsTruncated"`
	Upload             []struct {
		Key          string   `xml:"Key"`
		UploadID     string   `xml:"UploadId"`
		Initiator    ownerXML `xml:"Initiator"`
		Owner        ownerXML `xml:"Owner"`
		StorageClass string   `xml:"StorageClass"`
		Initiated    string   `xml:"Initiated"`
	} `xml:"Upload"`
}

// handleListMultipartUploads: GET /bucket?uploads
func (s *Server) handleListMultipartUploads(w http.ResponseWriter, r *http.Request, requestID, bucket string) {
	metas, err := s.uploads.listMetas()
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	out := listUploadsResult{Xmlns: s3Namespace, Bucket: bucket, MaxUploads: 1000}
	for _, m := range metas {
		// Only the requested bucket's uploads, like AWS. Discovery
		// background: code review — every in-flight upload on the whole
		// state dir leaked into the response, so one tenant's uploads
		// showed up in another bucket's listing.
		if m.Bucket != bucket {
			continue
		}
		out.Upload = append(out.Upload, struct {
			Key          string   `xml:"Key"`
			UploadID     string   `xml:"UploadId"`
			Initiator    ownerXML `xml:"Initiator"`
			Owner        ownerXML `xml:"Owner"`
			StorageClass string   `xml:"StorageClass"`
			Initiated    string   `xml:"Initiated"`
		}{
			Key: m.Key, UploadID: m.UploadID,
			Initiator:    ownerXML{ID: "s3proxy", DisplayName: "s3proxy"},
			Owner:        ownerXML{ID: "s3proxy", DisplayName: "s3proxy"},
			StorageClass: orDefault(m.StorageClass, "STANDARD"),
			Initiated:    m.Initiated.UTC().Format(time.RFC3339),
		})
	}
	writeXML(w, http.StatusOK, requestID, out)
}

type completeRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type completeResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// handleCompleteMultipart: POST /bucket/key?uploadId=ID
func (s *Server) handleCompleteMultipart(w http.ResponseWriter, r *http.Request, requestID, bucket, key string, q url.Values) {
	uploadID := q.Get("uploadId")
	unlock := s.uploads.lock(uploadID)
	defer unlock()
	m, err := s.uploads.load(uploadID)
	if err != nil {
		writeError(w, r, &s3Err{status: http.StatusNotFound, code: "NoSuchUpload", message: "The specified multipart upload does not exist."}, requestID)
		return
	}
	var req completeRequest
	if err := xml.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&req); err != nil {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "MalformedXML", message: "The XML you provided was not well-formed."}, requestID)
		return
	}

	// Validate: parts must cover 1..N contiguously and their etags must
	// match what we staged (S3 rejects mismatches with InvalidPart).
	byNum := make(map[int]partMeta, len(m.Parts))
	for _, p := range m.Parts {
		byNum[p.PartNumber] = p
	}
	var ordered []partMeta
	var sum int64
	h := md5.New()
	prev := 0
	for _, reqPart := range req.Parts {
		p, ok := byNum[reqPart.PartNumber]
		if !ok || reqPart.PartNumber != prev+1 {
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidPart", message: "One or more of the specified parts could not be found. The part might not have been uploaded."}, requestID)
			return
		}
		if want := `"` + hex.EncodeToString(p.MD5) + `"`; reqPart.ETag != "" && !strings.EqualFold(strings.Trim(reqPart.ETag, `"`), strings.Trim(want, `"`)) {
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidPart", message: "One or more of the specified parts could not be found. The part might not have been uploaded."}, requestID)
			return
		}
		h.Write(p.MD5)
		ordered = append(ordered, p)
		sum += p.Size
		prev = reqPart.PartNumber
	}
	if len(ordered) == 0 {
		writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "InvalidPart", message: "One or more of the specified parts could not be found."}, requestID)
		return
	}
	composite := hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(ordered))
	if m.ContentType == "" {
		m.ContentType = "application/octet-stream"
	}

	// Stream the staged part files through the tiered store in order.
	readers := make([]io.Reader, 0, len(ordered))
	closers := make([]io.Closer, 0, len(ordered))
	for _, p := range ordered {
		f, err := os.Open(filepath.Join(s.uploads.partDir(uploadID), strconv.Itoa(p.PartNumber)+".bin"))
		if err != nil {
			for _, c := range closers {
				c.Close()
			}
			writeError(w, r, &s3Err{status: http.StatusInternalServerError, code: "InternalError", message: "staged part lost"}, requestID)
			return
		}
		readers = append(readers, f)
		closers = append(closers, f)
	}
	stream := io.MultiReader(readers...)
	e, err := s.tier.PutObject(r.Context(), bucket, key, stream, sum, tier.PutOpts{
		ContentType:  m.ContentType,
		Metadata:     m.Metadata,
		StorageClass: m.StorageClass,
		ETag:         `"` + composite + `"`,
	})
	for _, c := range closers {
		c.Close()
	}
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}

	// Consume the upload state; a crash before this leaves a stale upload
	// that Abort cleanup picks up later (acceptable leak).
	os.RemoveAll(s.uploads.partDir(uploadID))
	os.Remove(s.uploads.manifestPath(uploadID))
	if e.ETag == "" {
		e.ETag = `"` + composite + `"`
	}
	writeXML(w, http.StatusOK, requestID, completeResult{
		Xmlns: s3Namespace, Location: requestLocation(r, bucket, key), Bucket: bucket, Key: key, ETag: e.ETag,
	})
}

// abort removes the staged state of an upload, idempotently. A missing
// upload is an error (NoSuchUpload) so the api layer can report it.
func (u *uploadStore) abort(ctx context.Context, uploadID string) error {
	unlock := u.lock(uploadID)
	defer unlock()
	if _, err := u.load(uploadID); err != nil {
		return store.ErrNotFound
	}
	os.RemoveAll(u.partDir(uploadID))
	os.Remove(u.manifestPath(uploadID))
	return nil
}
