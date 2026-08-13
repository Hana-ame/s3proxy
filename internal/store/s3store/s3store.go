// Package s3store implements the Store interface against any
// S3-compatible endpoint (AWS S3, Cloudflare R2, MinIO, ...). One instance
// per configured pool; the tier layer can drive several of them.
//
// Two addressing modes, chosen by whether the pool config sets "bucket":
//
//   - prefix mode (bucket set): every frontend bucket is stored inside the
//     single configured remote bucket, keyed "<frontbucket>/<key>". This is
//     the default for providers like R2 with limited bucket quotas.
//   - per-bucket mode (bucket empty): the remote bucket name equals the
//     frontend bucket name.
package s3store

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"

	"s3proxy/internal/store"
)

// Config describes one remote S3 pool.
type Config struct {
	Endpoint string // e.g. https://minio.internal:9000 or https://<acct>.r2.cloudflarestorage.com
	Region   string // "us-east-1", "auto", ...
	Bucket   string // prefix-mode bucket; empty = per-bucket mode
	AK       string
	SK       string
	// Insecure skips TLS verification; use only for internal plain-HTTP
	// endpoints or self-signed MinIO certs.
	Insecure bool
	// Timeout bounds each individual remote request.
	Timeout time.Duration
}

// Store is one S3-compatible pool.
type Store struct {
	name string
	cfg  Config
	cli  *s3.Client
	hc   *http.Client
}

// New builds the client. HTTP client is shared per store so connection
// pooling works across concurrent tier operations.
func New(name string, cfg Config) (*Store, error) {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Minute
	}
	hc := &http.Client{
		Timeout: cfg.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 16,
			IdleConnTimeout:     90 * time.Second,
			DisableCompression:  true,
			// TLSClientConfig set only when Insecure, to keep the
			// transport zero-config otherwise.
		},
	}
	if cfg.Insecure {
		t, ok := hc.Transport.(*http.Transport)
		if ok {
			t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // explicit opt-in via config
		}
	}
	awsCfg := aws.Config{
		Region:      cfg.Region,
		Credentials: aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.AK, cfg.SK, "")),
		HTTPClient:  hc,
	}
	cli := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// Path-style addressing is required by MinIO and R2; AWS S3
			// accepts it too. Virtual-host mode would also require the
			// hostname to match the bucket, which breaks prefix mode.
			o.UsePathStyle = true
		}
	})
	return &Store{name: name, cfg: cfg, cli: cli, hc: hc}, nil
}

func (s *Store) Name() string { return s.name }

func (s *Store) Close() error {
	s.hc.CloseIdleConnections()
	return nil
}

// splitKey maps a plugin key "bucket/key" (or "bucket") to the physical
// remote bucket + key.
func (s *Store) splitKey(key string) (bucket string, objKey string) {
	if s.cfg.Bucket != "" {
		return s.cfg.Bucket, key
	}
	b, rest, _ := strings.Cut(key, "/")
	return b, rest
}

// mapErr converts an aws-sdk error into store.ErrNotFound when the remote
// reports 404 (NoSuchKey / NoSuchBucket / plain 404 from HeadObject), so the
// tier read-through probing treats it as a miss rather than a failure.
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "404", "NoSuchKey", "NoSuchBucket", "NotFound":
			return store.ErrNotFound
		}
	}
	return err
}

func (s *Store) Put(ctx context.Context, key string, r io.Reader, size int64, contentType string, opts store.PutOptions) (store.ObjectInfo, error) {
	bucket, objKey := s.splitKey(key)
	if objKey == "" {
		return store.ObjectInfo{}, fmt.Errorf("s3store: key %q has no object part", key)
	}
	in := &s3.PutObjectInput{
		Bucket:        aws.String(bucket),
		Key:           aws.String(objKey),
		Body:          r,
		ContentLength: aws.Int64(size),
		Metadata:      opts.RequestedMetadata,
	}
	if contentType != "" {
		in.ContentType = aws.String(contentType)
	}
	if opts.StorageClass != "" {
		in.StorageClass = types.StorageClass(opts.StorageClass)
	}
	// The v2 SDK computes a CRC32 checksum by default; do not set one
	// explicitly so we stay compatible with backends that only accept the
	// SDK default (MinIO, R2, AWS all accept it).
	out, err := s.cli.PutObject(ctx, in)
	if err != nil {
		return store.ObjectInfo{}, err
	}
	etag := ""
	if out.ETag != nil {
		etag = *out.ETag
	}
	return store.ObjectInfo{
		Key:          key,
		Size:         size,
		ETag:         etag,
		ContentType:  contentType,
		LastModified: time.Now(),
		StorageClass: orDefault(opts.StorageClass, "STANDARD"),
	}, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func (s *Store) Get(ctx context.Context, key string, rng store.Range) (store.GetResult, error) {
	bucket, objKey := s.splitKey(key)
	in := &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(objKey)}
	if rng.Start >= 0 {
		// AWS range format "bytes=start-end"; open-ended range (End<0)
		// becomes "bytes=start-".
		rs := "bytes=" + strconv.FormatInt(rng.Start, 10) + "-"
		if rng.End >= 0 {
			rs += strconv.FormatInt(rng.End, 10)
		}
		in.Range = aws.String(rs)
	}
	out, err := s.cli.GetObject(ctx, in)
	if err != nil {
		return store.GetResult{}, mapErr(err)
	}
	info := store.ObjectInfo{
		Key:         key,
		Size:        deref(out.ContentLength, 0),
		ETag:        derefStr(out.ETag),
		ContentType: derefStr(out.ContentType),
		// LastModified absent on some backends for ranged reads? It is
		// always present on GetObject; keep zero value fallback safe.
		StorageClass: "STANDARD",
	}
	if out.LastModified != nil {
		info.LastModified = *out.LastModified
	}
	if out.StorageClass != "" {
		info.StorageClass = string(out.StorageClass)
	}
	span := store.Range{Start: 0, End: info.Size - 1}
	if cr := derefStr(out.ContentRange); cr != "" {
		// "bytes 0-4/10" → resolved span of a ranged read.
		if st, en, ok := parseContentRange(cr); ok {
			span = store.Range{Start: st, End: en}
		}
	}
	return store.GetResult{Body: out.Body, Info: info, Span: span}, nil
}

// parseContentRange parses "bytes 0-4/10" into inclusive span 0..4.
func parseContentRange(cr string) (start, end int64, ok bool) {
	rest, found := strings.CutPrefix(cr, "bytes ")
	if !found {
		return 0, 0, false
	}
	span, _, found := strings.Cut(rest, "/")
	if !found {
		return 0, 0, false
	}
	se := strings.SplitN(span, "-", 2)
	if len(se) != 2 {
		return 0, 0, false
	}
	st, err1 := strconv.ParseInt(se[0], 10, 64)
	en, err2 := strconv.ParseInt(se[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return st, en, true
}

func (s *Store) Head(ctx context.Context, key string) (store.ObjectInfo, error) {
	bucket, objKey := s.splitKey(key)
	out, err := s.cli.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(objKey)})
	if err != nil {
		return store.ObjectInfo{}, mapErr(err)
	}
	info := store.ObjectInfo{
		Key:         key,
		Size:        deref(out.ContentLength, 0),
		ETag:        derefStr(out.ETag),
		ContentType: derefStr(out.ContentType),
	}
	if out.LastModified != nil {
		info.LastModified = *out.LastModified
	}
	if out.StorageClass != "" {
		info.StorageClass = string(out.StorageClass)
	}
	return info, nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	bucket, objKey := s.splitKey(key)
	if objKey == "" {
		return nil
	}
	_, err := s.cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(objKey)})
	return mapErr(err)
}

func (s *Store) List(ctx context.Context, keyPrefix string, startAfter string, maxKeys int) (store.ListOutput, error) {
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.remoteBucketForPrefix(keyPrefix)),
		Prefix:  aws.String(keyPrefix),
		MaxKeys: aws.Int32(int32(maxKeys)),
	}
	if startAfter != "" {
		in.ContinuationToken = aws.String(startAfter)
	}
	out, err := s.cli.ListObjectsV2(ctx, in)
	if err != nil {
		return store.ListOutput{}, mapErr(err)
	}
	res := store.ListOutput{IsTruncated: out.IsTruncated != nil && *out.IsTruncated}
	if out.NextContinuationToken != nil {
		res.NextToken = *out.NextContinuationToken
	}
	for _, obj := range out.Contents {
		if obj.Key == nil {
			continue
		}
		info := store.ObjectInfo{
			Key:          *obj.Key,
			Size:         deref(obj.Size, 0),
			ETag:         derefStr(obj.ETag),
			StorageClass: "STANDARD",
		}
		if obj.LastModified != nil {
			info.LastModified = *obj.LastModified
		}
		if obj.StorageClass != "" {
			info.StorageClass = string(obj.StorageClass)
		}
		res.Entries = append(res.Entries, info)
	}
	return res, nil
}

// remoteBucketForPrefix derives the physical remote bucket used for a List
// of the given key prefix ("bucket/..."): prefix mode returns the configured
// bucket, per-bucket mode the leading segment of the prefix.
func (s *Store) remoteBucketForPrefix(keyPrefix string) string {
	if s.cfg.Bucket != "" {
		return s.cfg.Bucket
	}
	b, _, _ := strings.Cut(keyPrefix, "/")
	return b
}

func (s *Store) EnsureBucket(ctx context.Context, bucket string) error {
	target := bucket
	if s.cfg.Bucket != "" {
		target = s.cfg.Bucket
	}
	if _, err := s.cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(target)}); err == nil {
		return nil
	}
	// NotFound (or any error like 403 with permission to create) — attempt
	// creation; if it already appeared concurrently, creation errors out
	// and we accept BucketAlreadyOwnedByYou.
	_, err := s.cli.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(target),
		// AWS requires LocationConstraint for any region other than
		// us-east-1; MinIO/R2 ignore it.
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.cfg.Region),
		},
	})
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "BucketAlreadyOwnedByYou" || apiErr.ErrorCode() == "BucketAlreadyExists") {
			return nil
		}
		return err
	}
	return nil
}

func (s *Store) BucketExists(ctx context.Context, bucket string) (bool, error) {
	target := bucket
	if s.cfg.Bucket != "" {
		target = s.cfg.Bucket
	}
	_, err := s.cli.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(target)})
	if err != nil {
		if mapErr(err) == store.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) Buckets(ctx context.Context) ([]string, error) {
	if s.cfg.Bucket != "" {
		return []string{s.cfg.Bucket}, nil
	}
	out, err := s.cli.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	var res []string
	for _, b := range out.Buckets {
		if b.Name != nil {
			res = append(res, *b.Name)
		}
	}
	return res, nil
}

// Rename renames an object within the same remote bucket. S3 has no server
// rename, so this is a CopyObject (same bucket, no data movement server-side)
// followed by a delete of the source. Used by the tier layer to finalize
// content-addressed writes (temporary key -> sha256 name).
func (s *Store) Rename(ctx context.Context, fromKey, toKey string) error {
	fromBucket, fromObj := s.splitKey(fromKey)
	toBucket, toObj := s.splitKey(toKey)
	if fromBucket != toBucket {
		return fmt.Errorf("s3store: rename across buckets %q -> %q", fromKey, toKey)
	}
	if fromObj == "" || toObj == "" {
		return fmt.Errorf("s3store: rename %q -> %q: missing object part", fromKey, toKey)
	}
	_, err := s.cli.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(toBucket),
		Key:        aws.String(toObj),
		CopySource: aws.String(fromBucket + "/" + fromObj),
	})
	if err != nil {
		return mapErr(err)
	}
	_, err = s.cli.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(fromBucket), Key: aws.String(fromObj)})
	return mapErr(err)
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func deref(p *int64, def int64) int64 {
	if p == nil {
		return def
	}
	return *p
}

var _ store.Store = (*Store)(nil)
var _ store.Renamer = (*Store)(nil)
