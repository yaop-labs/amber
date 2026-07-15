package backup

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestS3TransportMinIOIntegration(t *testing.T) {
	endpoint := os.Getenv("AMBER_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("AMBER_TEST_S3_ENDPOINT is not set")
	}
	bucket := os.Getenv("AMBER_TEST_S3_BUCKET")
	if bucket == "" {
		bucket = "amber-backup-integration"
	}
	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-east-1"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	transport, err := NewS3Transport(ctx, S3TransportConfig{
		Bucket: bucket, Prefix: "integration", Region: region, Endpoint: endpoint,
	})
	if err != nil {
		t.Fatalf("NewS3Transport: %v", err)
	}
	awsStore, ok := transport.objects.(*awsObjectStore)
	if !ok {
		t.Fatal("S3 transport is not backed by awsObjectStore")
	}
	if _, err := awsStore.client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	payload := bytes.Repeat([]byte("amber-s3-integration\n"), 64*1024)
	if err := os.WriteFile(filepath.Join(source, "payload.bin"), payload, 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	createEmptyOTLPJournal(t, source)
	localSnapshot := filepath.Join(root, "snapshot")
	created, err := Create(ctx, source, localSnapshot)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	uploaded, err := transport.Upload(ctx, localSnapshot)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploaded.Checkpoint != created.Checkpoint {
		t.Fatalf("uploaded checkpoint = %s, want %s", uploaded.Checkpoint, created.Checkpoint)
	}

	downloadedSnapshot := filepath.Join(root, "downloaded")
	downloaded, err := transport.Download(ctx, created.Checkpoint, downloadedSnapshot)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if downloaded.Checkpoint != created.Checkpoint {
		t.Fatalf("downloaded checkpoint = %s, want %s", downloaded.Checkpoint, created.Checkpoint)
	}
	restored := filepath.Join(root, "restored")
	if _, err := Restore(ctx, downloadedSnapshot, restored); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(restored, "payload.bin"))
	if err != nil {
		t.Fatalf("read restored payload: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("restored payload length = %d, want %d", len(got), len(payload))
	}

	workspace := filepath.Join(root, "drill")
	drilled, err := transport.Drill(ctx, created.Checkpoint, DrillOptions{
		Workspace:          workspace,
		ExpectedDatabaseID: created.Manifest.Database.ID,
		Probe: func(_ context.Context, restoredRoot string, verified Verification) (SemanticProbe, error) {
			got, err := os.ReadFile(filepath.Join(restoredRoot, "payload.bin"))
			if err != nil {
				return SemanticProbe{}, err
			}
			if !bytes.Equal(got, payload) {
				return SemanticProbe{}, errors.New("drill restored a different payload")
			}
			return SemanticProbe{
				Ready:             true,
				DatabaseID:        verified.Manifest.Database.ID,
				RestoreCheckpoint: verified.Checkpoint,
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("Drill: %v", err)
	}
	if drilled.Verification.Checkpoint != created.Checkpoint || !drilled.Probe.Ready {
		t.Fatalf("drill result = %+v", drilled)
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drill workspace was not removed: %v", err)
	}
}
