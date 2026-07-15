package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type memoryObjectStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
}

func newMemoryObjectStore() *memoryObjectStore {
	return &memoryObjectStore{objects: make(map[string][]byte)}
}

func (s *memoryObjectStore) Put(_ context.Context, key string, body io.Reader, size int64, _ map[string]string) error {
	payload, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if int64(len(payload)) != size {
		return fmt.Errorf("size %d, want %d", len(payload), size)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = bytes.Clone(payload)
	s.puts = append(s.puts, key)
	return nil
}

func (s *memoryObjectStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	payload, ok := s.objects[key]
	if !ok {
		return nil, os.ErrNotExist
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(payload))), nil
}

func TestS3TransportUploadDownloadRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("remote backup"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	createEmptyOTLPJournal(t, source)
	snapshot := filepath.Join(root, "snapshot")
	created, err := Create(context.Background(), source, snapshot)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	objects := newMemoryObjectStore()
	transport := &S3Transport{objects: objects, prefix: "amber/prod"}
	uploaded, err := transport.Upload(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if uploaded.Checkpoint != created.Checkpoint {
		t.Fatalf("uploaded checkpoint = %s, want %s", uploaded.Checkpoint, created.Checkpoint)
	}
	completionKey := transport.snapshotKey(created.Checkpoint, CompletionFileName)
	if len(objects.puts) == 0 || objects.puts[len(objects.puts)-1] != completionKey {
		t.Fatalf("put order = %v, completion must be last", objects.puts)
	}
	if !strings.HasPrefix(completionKey, "amber/prod/snapshots/"+created.Checkpoint+"/") {
		t.Fatalf("completion key = %s", completionKey)
	}

	downloadedDir := filepath.Join(root, "downloaded")
	downloaded, err := transport.Download(context.Background(), created.Checkpoint, downloadedDir)
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if downloaded.Checkpoint != created.Checkpoint {
		t.Fatalf("downloaded checkpoint = %s, want %s", downloaded.Checkpoint, created.Checkpoint)
	}
	payload, err := os.ReadFile(filepath.Join(downloadedDir, DataDirectoryName, "payload"))
	if err != nil {
		t.Fatalf("read downloaded payload: %v", err)
	}
	if string(payload) != "remote backup" {
		t.Fatalf("downloaded payload = %q", payload)
	}
}

func TestS3TransportRejectsCorruptObjectWithoutPublishing(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("original"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	createEmptyOTLPJournal(t, source)
	snapshot := filepath.Join(root, "snapshot")
	created, err := Create(context.Background(), source, snapshot)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	objects := newMemoryObjectStore()
	transport := &S3Transport{objects: objects}
	if _, err := transport.Upload(context.Background(), snapshot); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	objects.objects[transport.snapshotKey(created.Checkpoint, DataDirectoryName, "payload")] = []byte("corrupt")
	destination := filepath.Join(root, "downloaded")
	if _, err := transport.Download(context.Background(), created.Checkpoint, destination); err == nil {
		t.Fatal("Download accepted corrupt object")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("corrupt download published destination: %v", err)
	}
}

func TestS3TransportRejectsInvalidCheckpoint(t *testing.T) {
	transport := &S3Transport{objects: newMemoryObjectStore()}
	if _, err := transport.Download(context.Background(), "not-a-checkpoint", filepath.Join(t.TempDir(), "snapshot")); err == nil {
		t.Fatal("Download accepted invalid checkpoint")
	}
}

func TestS3TransportDrillRestoresProbesAndCleansWorkspace(t *testing.T) {
	transport, created := uploadedDrillSnapshot(t)
	workspace := filepath.Join(t.TempDir(), "drill")
	result, err := transport.Drill(context.Background(), created.Checkpoint, DrillOptions{
		Workspace:          workspace,
		ExpectedDatabaseID: created.Manifest.Database.ID,
		Probe: func(_ context.Context, restoredRoot string, verified Verification) (SemanticProbe, error) {
			payload, err := os.ReadFile(filepath.Join(restoredRoot, "payload"))
			if err != nil {
				return SemanticProbe{}, err
			}
			if string(payload) != "drill payload" {
				return SemanticProbe{}, fmt.Errorf("payload = %q", payload)
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
	if result.Verification.Checkpoint != created.Checkpoint || result.DataBytes <= 0 || !result.Probe.Ready {
		t.Fatalf("drill result = %+v", result)
	}
	if result.TotalElapsed <= 0 {
		t.Fatalf("total elapsed = %s", result.TotalElapsed)
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drill workspace was not removed: %v", err)
	}
}

func TestS3TransportDrillRejectsWrongIdentityAndCleansWorkspace(t *testing.T) {
	transport, created := uploadedDrillSnapshot(t)
	workspace := filepath.Join(t.TempDir(), "drill")
	probeCalled := false
	_, err := transport.Drill(context.Background(), created.Checkpoint, DrillOptions{
		Workspace:          workspace,
		ExpectedDatabaseID: "01J00000000000000000000000",
		Probe: func(context.Context, string, Verification) (SemanticProbe, error) {
			probeCalled = true
			return SemanticProbe{}, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "does not match expected") {
		t.Fatalf("Drill error = %v", err)
	}
	if probeCalled {
		t.Fatal("semantic probe ran for the wrong database identity")
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drill workspace was not removed: %v", err)
	}
}

func TestS3TransportDrillCleansWorkspaceAfterProbeFailure(t *testing.T) {
	transport, created := uploadedDrillSnapshot(t)
	workspace := filepath.Join(t.TempDir(), "drill")
	_, err := transport.Drill(context.Background(), created.Checkpoint, DrillOptions{
		Workspace: workspace,
		Probe: func(context.Context, string, Verification) (SemanticProbe, error) {
			return SemanticProbe{}, errors.New("probe failed")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "probe failed") {
		t.Fatalf("Drill error = %v", err)
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drill workspace was not removed: %v", err)
	}
}

func TestS3TransportDrillRejectsProbeIdentity(t *testing.T) {
	transport, created := uploadedDrillSnapshot(t)
	workspace := filepath.Join(t.TempDir(), "drill")
	_, err := transport.Drill(context.Background(), created.Checkpoint, DrillOptions{
		Workspace: workspace,
		Probe: func(_ context.Context, _ string, verified Verification) (SemanticProbe, error) {
			return SemanticProbe{
				Ready:             true,
				DatabaseID:        "different-database",
				RestoreCheckpoint: verified.Checkpoint,
			}, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "different database identity") {
		t.Fatalf("Drill error = %v", err)
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("drill workspace was not removed: %v", err)
	}
}

func TestS3TransportDrillPreservesExistingWorkspace(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "drill")
	if err := os.Mkdir(workspace, 0o750); err != nil {
		t.Fatalf("mkdir workspace: %v", err)
	}
	sentinel := filepath.Join(workspace, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	transport := &S3Transport{objects: newMemoryObjectStore()}
	_, err := transport.Drill(context.Background(), strings.Repeat("a", 64), DrillOptions{
		Workspace: workspace,
		Probe: func(context.Context, string, Verification) (SemanticProbe, error) {
			return SemanticProbe{}, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "workspace already exists") {
		t.Fatalf("Drill error = %v", err)
	}
	if payload, err := os.ReadFile(sentinel); err != nil || string(payload) != "keep" {
		t.Fatalf("sentinel = %q, err=%v", payload, err)
	}
}

func TestS3TransportDrillCancellationDoesNotCreateWorkspace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	workspace := filepath.Join(t.TempDir(), "drill")
	transport := &S3Transport{objects: newMemoryObjectStore()}
	_, err := transport.Drill(ctx, strings.Repeat("a", 64), DrillOptions{
		Workspace: workspace,
		Probe: func(context.Context, string, Verification) (SemanticProbe, error) {
			return SemanticProbe{}, nil
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Drill error = %v, want context.Canceled", err)
	}
	if _, err := os.Lstat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled drill created workspace: %v", err)
	}
}

func uploadedDrillSnapshot(t *testing.T) (*S3Transport, Verification) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("drill payload"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	createEmptyOTLPJournal(t, source)
	created, err := Create(context.Background(), source, filepath.Join(root, "snapshot"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	transport := &S3Transport{objects: newMemoryObjectStore(), prefix: "drill-test"}
	if _, err := transport.Upload(context.Background(), filepath.Join(root, "snapshot")); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	return transport, created
}
