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

// uploadStore owns the on-disk staging area for in-flight uploads.
type uploadStore struct {
	root string // <stateDir>/uploads

	mu    sync.Mutex
	locks map[string]*sync.Mutex // per-upload lock, so parallel part uploads of one upload serialize on the manifest only
}

func newUploadStore(stateDir string) (*uploadStore, error) {
	root := filepath.Join(stateDir, "uploads")
	if err := os.MkdirAll(filepath.Join(root, "parts"), 0o755); err != nil {
		return nil, err
	}
	return &uploadStore{root: root, locks: make(map[string]*sync.Mutex)}, nil
}

func (u *uploadStore) manifestPath(id string) string { return filepath.Join(u.root, id+".json") }
func (u *uploadStore) partDir(id string) string      { return filepath.Join(u.root, "parts", id) }

func (u *uploadStore) lock(id string) func() {
	u.mu.Lock()
	m, ok := u.locks[id]
	if !ok {
		m = &sync.Mutex{}
		u.locks[id] = m
	}
	u.mu.Unlock()
	m.Lock()
	return m.Unlock
}

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
	// headers instead of the body.
	var body io.Reader = r.Body
	var copyErr error
	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		body, copyErr = s.copySourceReader(r, src)
		if copyErr != nil {
			writeError(w, r, fmtErr("%v", copyErr), requestID)
			return
		}
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
	if err := os.Rename(tmp, partPath); err != nil {
		os.Remove(tmp)
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}

	etag := `"` + hex.EncodeToString(h.Sum(nil)) + `"`
	if md5Hex := r.Header.Get("Content-MD5"); md5Hex != "" {
		got := strings.Trim(etag, `"`)
		if !strings.EqualFold(got, md5Hex) {
			os.Remove(partPath)
			writeError(w, r, &s3Err{status: http.StatusBadRequest, code: "BadDigest", message: "The Content-MD5 you specified did not match what we received."}, requestID)
			return
		}
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
// honoring x-amz-copy-source-range for partial part copies.
func (s *Server) copySourceReader(r *http.Request, src string) (io.Reader, error) {
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
