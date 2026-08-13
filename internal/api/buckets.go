package api

// Bucket-level operations: ListBuckets, CreateBucket, HeadBucket,
// DeleteBucket, plus the small GET sub-resources (location, versioning) that
// clients probe during setup.

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"

	"s3proxy/internal/store"
)

type listBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   ownerXML `xml:"Owner"`
	Buckets struct {
		Bucket []bucketXML `xml:"Bucket"`
	} `xml:"Buckets"`
}

type ownerXML struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type bucketXML struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

func (s *Server) handleListBuckets(w http.ResponseWriter, r *http.Request, requestID string) {
	buckets, err := s.tier.ListBuckets(r.Context())
	if err != nil {
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	var res listBucketsResult
	res.Xmlns = s3Namespace
	res.Owner = ownerXML{ID: "s3proxy", DisplayName: "s3proxy"}
	for _, b := range buckets {
		res.Buckets.Bucket = append(res.Buckets.Bucket, bucketXML{
			Name:         b.Name,
			CreationDate: b.Created.UTC().Format("2006-01-02T15:04:05.000Z"),
		})
	}
	writeXML(w, http.StatusOK, requestID, res)
}

// serveBucket dispatches bucket-scoped operations (path is exactly /bucket).
func (s *Server) serveBucket(w http.ResponseWriter, r *http.Request, requestID, bucket string, q url.Values) {
	switch r.Method {
	case http.MethodGet:
		switch {
		case q.Has("location"):
			writeXML(w, http.StatusOK, requestID, struct {
				XMLName xml.Name `xml:"LocationConstraint"`
				Xmlns   string   `xml:"xmlns,attr"`
				Value   string   `xml:",chardata"`
			}{Xmlns: s3Namespace, Value: s.region})
		case q.Has("versioning"):
			// Versioning is not implemented; report Suspended so clients
			// that probe it (aws cli configure, rclone) proceed.
			writeXML(w, http.StatusOK, requestID, struct {
				XMLName xml.Name `xml:"VersioningConfiguration"`
				Xmlns   string   `xml:"xmlns,attr"`
				Status  string   `xml:"Status"`
			}{Xmlns: s3Namespace, Status: "Suspended"})
		case q.Has("uploads"):
			s.handleListMultipartUploads(w, r, requestID, bucket)
		case q.Has("versions") || q.Has("tagging") || q.Has("acl") || q.Has("policy") || q.Has("cors") || q.Has("lifecycle"):
			// Unimplemented sub-resources get a clean S3 error instead of
			// a silent fallthrough to list.
			writeError(w, r, &s3Err{status: http.StatusNotImplemented, code: "NotImplemented", message: "A header you provided implies functionality that is not implemented."}, requestID)
		default:
			s.handleListObjects(w, r, requestID, bucket)
		}
	case http.MethodPut:
		s.handleCreateBucket(w, r, requestID, bucket)
	case http.MethodHead:
		if err := s.tier.HeadBucket(r.Context(), bucket); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeError(w, r, errNotFoundBucket(bucket), requestID)
				return
			}
			writeError(w, r, fmtErr("%v", err), requestID)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := s.tier.DeleteBucket(r.Context(), bucket); err != nil {
			switch {
			case errors.Is(err, store.ErrNotFound):
				writeError(w, r, errNotFoundBucket(bucket), requestID)
			case errors.Is(err, store.ErrNotEmpty):
				writeError(w, r, &s3Err{status: http.StatusConflict, code: "BucketNotEmpty", message: "The bucket you tried to delete is not empty."}, requestID)
			default:
				writeError(w, r, fmtErr("%v", err), requestID)
			}
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		// POST ?delete is a flag-style parameter with no value, so Has()
		// not Get() — Get would return "" and reject every batch delete.
		if !q.Has("delete") {
			writeError(w, r, &s3Err{status: http.StatusNotImplemented, code: "NotImplemented", message: "Only POST ?delete is supported on a bucket."}, requestID)
			return
		}
		s.handleDeleteObjects(w, r, requestID, bucket)
	default:
		writeError(w, r, &s3Err{status: http.StatusMethodNotAllowed, code: "MethodNotAllowed", message: "The specified method is not allowed against this resource."}, requestID)
	}
}

func (s *Server) handleCreateBucket(w http.ResponseWriter, r *http.Request, requestID, bucket string) {
	// The request body may carry a CreateBucketConfiguration; we honor the
	// region field for AWS-style responses but ignore its location
	// constraint (all buckets live on the configured pools).
	if err := s.tier.CreateBucket(r.Context(), bucket); err != nil {
		if errors.Is(err, store.ErrBucketExists) {
			writeError(w, r, &s3Err{status: http.StatusConflict, code: "BucketAlreadyExists", message: "The requested bucket name is not available."}, requestID)
			return
		}
		writeError(w, r, fmtErr("%v", err), requestID)
		return
	}
	// AWS returns 200 with a Location header on successful creation.
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}
