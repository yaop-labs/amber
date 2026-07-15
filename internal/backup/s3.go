package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const maxRemoteManifestBytes = 64 << 20

// S3TransportConfig configures snapshot transport to S3-compatible storage.
type S3TransportConfig struct {
	Bucket   string
	Prefix   string
	Region   string
	Endpoint string
}

// S3Transport uploads and downloads complete directory snapshots. It does not
// define the snapshot consistency boundary; Create, Verify, and Restore do.
type S3Transport struct {
	objects objectStore
	prefix  string
}

type objectStore interface {
	Put(context.Context, string, io.Reader, int64, map[string]string) error
	Get(context.Context, string) (io.ReadCloser, error)
}

type awsObjectStore struct {
	client *s3.Client
	bucket string
}

// NewS3Transport creates a transport using the standard AWS credential chain.
func NewS3Transport(ctx context.Context, cfg S3TransportConfig) (*S3Transport, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("backup s3: bucket is required")
	}
	options := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		options = append(options, awsconfig.WithRegion(cfg.Region))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("backup s3: load AWS config: %w", err)
	}
	s3Options := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Options = append(s3Options, func(options *s3.Options) {
			options.BaseEndpoint = aws.String(cfg.Endpoint)
			options.UsePathStyle = true
		})
	}
	return &S3Transport{
		objects: &awsObjectStore{client: s3.NewFromConfig(awsCfg, s3Options...), bucket: cfg.Bucket},
		prefix:  strings.Trim(cfg.Prefix, "/"),
	}, nil
}

// Upload verifies snapshotDir and publishes its COMPLETE object last.
func (t *S3Transport) Upload(ctx context.Context, snapshotDir string) (Verification, error) {
	verified, err := Verify(ctx, snapshotDir)
	if err != nil {
		return Verification{}, err
	}
	root, err := filepath.Abs(snapshotDir)
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: resolve snapshot: %w", err)
	}
	dataRoot := filepath.Join(root, DataDirectoryName)
	for _, entry := range verified.Manifest.Files {
		if err := ctx.Err(); err != nil {
			return Verification{}, err
		}
		filePath, err := secureSnapshotPath(dataRoot, entry.Path)
		if err != nil {
			return Verification{}, err
		}
		file, err := os.Open(filePath) //nolint:gosec
		if err != nil {
			return Verification{}, fmt.Errorf("backup s3: open %s: %w", entry.Path, err)
		}
		key := t.snapshotKey(verified.Checkpoint, DataDirectoryName, entry.Path)
		putErr := t.objects.Put(ctx, key, file, entry.Size, map[string]string{"sha256": entry.SHA256})
		closeErr := file.Close()
		if putErr != nil {
			return Verification{}, fmt.Errorf("backup s3: upload %s: %w", entry.Path, putErr)
		}
		if closeErr != nil {
			return Verification{}, fmt.Errorf("backup s3: close %s: %w", entry.Path, closeErr)
		}
	}

	manifest, err := readRegularFile(filepath.Join(root, ManifestFileName))
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: read manifest: %w", err)
	}
	if err := t.objects.Put(ctx, t.snapshotKey(verified.Checkpoint, ManifestFileName), strings.NewReader(string(manifest)), int64(len(manifest)), map[string]string{"sha256": verified.Checkpoint}); err != nil {
		return Verification{}, fmt.Errorf("backup s3: upload manifest: %w", err)
	}
	completion := verified.Checkpoint + "\n"
	if err := t.objects.Put(ctx, t.snapshotKey(verified.Checkpoint, CompletionFileName), strings.NewReader(completion), int64(len(completion)), map[string]string{"sha256": verified.Checkpoint}); err != nil {
		return Verification{}, fmt.Errorf("backup s3: publish completion marker: %w", err)
	}
	return verified, nil
}

// Download fetches a completed remote snapshot into a new local directory.
// Every object is authenticated before the directory is atomically published.
func (t *S3Transport) Download(ctx context.Context, checkpoint, snapshotDir string) (Verification, error) {
	if err := validateCheckpointDigest(checkpoint); err != nil {
		return Verification{}, err
	}
	destination, err := filepath.Abs(snapshotDir)
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: resolve destination: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return Verification{}, fmt.Errorf("backup s3: snapshot destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Verification{}, fmt.Errorf("backup s3: inspect destination: %w", err)
	}

	completion, err := t.readSmallObject(ctx, t.snapshotKey(checkpoint, CompletionFileName), 128)
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: read completion marker: %w", err)
	}
	if string(completion) != checkpoint+"\n" {
		return Verification{}, errors.New("backup s3: remote completion marker does not match checkpoint")
	}
	manifestPayload, err := t.readSmallObject(ctx, t.snapshotKey(checkpoint, ManifestFileName), maxRemoteManifestBytes)
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: read manifest: %w", err)
	}
	if digestBytes(manifestPayload) != checkpoint {
		return Verification{}, errors.New("backup s3: remote manifest checksum does not match checkpoint")
	}
	manifest, err := parseManifest(manifestPayload)
	if err != nil {
		return Verification{}, err
	}

	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return Verification{}, fmt.Errorf("backup s3: create destination parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".amber-s3-download-*.tmp")
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: create staging: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	dataRoot := filepath.Join(staging, DataDirectoryName)
	if err := os.Mkdir(dataRoot, 0o750); err != nil {
		return Verification{}, fmt.Errorf("backup s3: create data directory: %w", err)
	}
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return Verification{}, err
		}
		target := filepath.Join(dataRoot, filepath.FromSlash(entry.Path))
		if err := t.downloadFile(ctx, t.snapshotKey(checkpoint, DataDirectoryName, entry.Path), target, entry); err != nil {
			return Verification{}, err
		}
	}
	if err := writeSyncedFile(filepath.Join(staging, ManifestFileName), manifestPayload); err != nil {
		return Verification{}, err
	}
	if err := writeSyncedFile(filepath.Join(staging, CompletionFileName), completion); err != nil {
		return Verification{}, err
	}
	verified, err := Verify(ctx, staging)
	if err != nil {
		return Verification{}, fmt.Errorf("backup s3: verify downloaded snapshot: %w", err)
	}
	if err := syncDirectoryTree(staging); err != nil {
		return Verification{}, fmt.Errorf("backup s3: sync downloaded snapshot: %w", err)
	}
	if err := os.Rename(staging, destination); err != nil {
		return Verification{}, fmt.Errorf("backup s3: publish downloaded snapshot: %w", err)
	}
	published = true
	if err := syncDir(parent); err != nil {
		return Verification{}, fmt.Errorf("backup s3: sync destination parent: %w", err)
	}
	return verified, nil
}

func (t *S3Transport) snapshotKey(checkpoint string, parts ...string) string {
	all := make([]string, 0, 3+len(parts))
	all = append(all, t.prefix, "snapshots", checkpoint)
	all = append(all, parts...)
	return path.Join(all...)
}

func (t *S3Transport) readSmallObject(ctx context.Context, key string, limit int64) ([]byte, error) {
	body, err := t.objects.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	payload, readErr := io.ReadAll(io.LimitReader(body, limit+1))
	closeErr := body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if int64(len(payload)) > limit {
		return nil, fmt.Errorf("object %s exceeds %d bytes", key, limit)
	}
	return payload, nil
}

func (t *S3Transport) downloadFile(ctx context.Context, key, target string, entry FileEntry) error {
	body, err := t.objects.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("backup s3: download %s: %w", entry.Path, err)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		_ = body.Close()
		return err
	}
	file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(entry.Mode)) //nolint:gosec
	if err != nil {
		_ = body.Close()
		return err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), io.LimitReader(body, entry.Size+1))
	bodyCloseErr := body.Close()
	if copyErr == nil {
		copyErr = file.Sync()
	}
	fileCloseErr := file.Close()
	if copyErr != nil {
		return fmt.Errorf("backup s3: write %s: %w", entry.Path, copyErr)
	}
	if bodyCloseErr != nil {
		return fmt.Errorf("backup s3: close remote %s: %w", entry.Path, bodyCloseErr)
	}
	if fileCloseErr != nil {
		return fmt.Errorf("backup s3: close local %s: %w", entry.Path, fileCloseErr)
	}
	if written != entry.Size || hex.EncodeToString(hash.Sum(nil)) != entry.SHA256 {
		return fmt.Errorf("backup s3: object integrity mismatch for %s", entry.Path)
	}
	return nil
}

func validateCheckpointDigest(checkpoint string) error {
	if len(checkpoint) != 64 {
		return errors.New("backup s3: checkpoint must be a lowercase SHA-256 digest")
	}
	for _, char := range checkpoint {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return errors.New("backup s3: checkpoint must be a lowercase SHA-256 digest")
		}
	}
	return nil
}

func (s *awsObjectStore) Put(ctx context.Context, key string, body io.Reader, size int64, metadata map[string]string) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), Body: body,
		ContentLength: aws.Int64(size), Metadata: metadata,
	})
	return err
}

func (s *awsObjectStore) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, fmt.Errorf("%s: %w", key, os.ErrNotExist)
		}
		return nil, err
	}
	return output.Body, nil
}
