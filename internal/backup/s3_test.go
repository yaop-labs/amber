package backup

import (
	"bytes"
	"context"
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
