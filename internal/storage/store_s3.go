package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3StoreConfig configures an S3-compatible object storage backend.
// Works with AWS S3, MinIO, Cloudflare R2, DigitalOcean Spaces, and any
// other S3-compatible store via Endpoint.
type S3StoreConfig struct {
	Bucket   string // required
	Prefix   string // key prefix, no trailing slash (e.g. "amber")
	Region   string // AWS region; defaults to AWS_DEFAULT_REGION env var
	Endpoint string // custom endpoint for MinIO/R2/etc., empty = AWS
	LocalDir string // local cache dir; downloaded segments land here
}

// S3Store implements SegmentStore backed by S3-compatible object storage.
// Sealed segments are uploaded on Put and served from local cache on Get.
// The local cache directory is the same as the data directory, so reads
// that already exist locally are zero-cost. When a file is missing locally
// (e.g. after a node restart), Get downloads it from S3 before returning.
type S3Store struct {
	client   *s3.Client
	cfg      S3StoreConfig
	localDir string
}

// NewS3Store creates an S3Store using the default AWS credential chain
// (env vars → ~/.aws/credentials → IAM instance role).
func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("s3store: bucket is required")
	}
	if cfg.LocalDir == "" {
		return nil, errors.New("s3store: local cache dir is required")
	}

	awsOpts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		awsOpts = append(awsOpts, config.WithRegion(cfg.Region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("s3store: load aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			// Path-style addressing required for MinIO and most S3-compatible stores.
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3Store{
		client:   client,
		cfg:      cfg,
		localDir: cfg.LocalDir,
	}, nil
}

func (s *S3Store) key(name string) string {
	if s.cfg.Prefix == "" {
		return name
	}
	return s.cfg.Prefix + "/" + name
}

// Put uploads the named file to S3. For LocalStore the file already exists
// on disk; for S3Store we read from r and upload. The caller (seal callback)
// passes an open reader over the already-written local file.
func (s *S3Store) Put(name string, r io.Reader) error {
	_, err := s.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(name)),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("s3store: put %s: %w", name, err)
	}
	return nil
}

// Get returns a reader for name. If the file exists in the local cache it is
// returned directly (zero S3 cost). Otherwise it is downloaded from S3 and
// saved to the local cache before returning.
func (s *S3Store) Get(name string) (io.ReadCloser, error) {
	localPath := filepath.Join(s.localDir, name)

	if f, err := os.Open(localPath); err == nil {
		return f, nil
	}

	out, err := s.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(name)),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("s3store: get %s: %w", name, os.ErrNotExist)
		}
		return nil, fmt.Errorf("s3store: get %s: %w", name, err)
	}
	defer out.Body.Close()

	// Write to local cache so subsequent reads are free.
	if err := os.MkdirAll(s.localDir, 0750); err != nil {
		return nil, fmt.Errorf("s3store: mkdir cache: %w", err)
	}
	f, err := os.CreateTemp(s.localDir, name+".tmp.*")
	if err != nil {
		return nil, fmt.Errorf("s3store: create tmp: %w", err)
	}
	if _, err := io.Copy(f, out.Body); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("s3store: download %s: %w", name, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("s3store: close tmp: %w", err)
	}
	if err := os.Rename(f.Name(), localPath); err != nil {
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("s3store: rename to cache: %w", err)
	}

	return os.Open(localPath)
}

// Delete removes the file from S3 and the local cache. Used for terminal
// retention where the segment is gone for good. For local-only eviction
// (keeping the S3 copy intact), use DeleteLocal.
func (s *S3Store) Delete(name string) error {
	_, err := s.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(s.cfg.Bucket),
		Key:    aws.String(s.key(name)),
	})
	if err != nil {
		return fmt.Errorf("s3store: delete %s: %w", name, err)
	}
	localPath := filepath.Join(s.localDir, name)
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("s3store: delete local cache %s: %w", name, err)
	}
	return nil
}

// DeleteLocal removes the file from the local cache only, leaving the S3
// object intact. A subsequent Get will re-fetch from S3 on demand. Used by
// local-tier retention to free disk while keeping the durable copy.
func (s *S3Store) DeleteLocal(name string) error {
	localPath := filepath.Join(s.localDir, name)
	if err := os.Remove(localPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("s3store: delete local cache %s: %w", name, err)
	}
	return nil
}

// List returns base names of all segment data files (*.alog) in the S3 prefix.
func (s *S3Store) List() ([]string, error) {
	prefix := s.cfg.Prefix
	if prefix != "" {
		prefix += "/"
	}

	var names []string
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.cfg.Bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("s3store: list: %w", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			base := strings.TrimPrefix(key, prefix)
			if strings.HasSuffix(base, ".alog") {
				names = append(names, base)
			}
		}
	}
	return names, nil
}
