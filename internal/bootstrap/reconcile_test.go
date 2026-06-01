package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/storage"
)

// memStore is a SegmentStore backed by an in-memory map. Mimics S3 behavior
// enough for reconcile tests: List returns only *.alog names, Get writes
// to the local cache (like S3Store does) so the reconcile path's footer
// read can open the downloaded file.
type memStore struct {
	mu       sync.Mutex
	objects  map[string][]byte
	localDir string
}

func newMemStore(localDir string) *memStore {
	return &memStore{
		objects:  make(map[string][]byte),
		localDir: localDir,
	}
}

func (m *memStore) Put(name string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[name] = data
	m.mu.Unlock()
	return nil
}

func (m *memStore) Get(name string) (io.ReadCloser, error) {
	m.mu.Lock()
	data, ok := m.objects[name]
	m.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	// Mirror S3Store: write to local cache so subsequent local reads work.
	if err := os.MkdirAll(m.localDir, 0750); err != nil {
		return nil, err
	}
	localPath := filepath.Join(m.localDir, name)
	if err := os.WriteFile(localPath, data, 0600); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *memStore) Delete(name string) error {
	m.mu.Lock()
	delete(m.objects, name)
	m.mu.Unlock()
	_ = os.Remove(filepath.Join(m.localDir, name))
	return nil
}

func (m *memStore) DeleteLocal(name string) error {
	_ = os.Remove(filepath.Join(m.localDir, name))
	return nil
}

func (m *memStore) List() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for k := range m.objects {
		if strings.HasSuffix(k, ".alog") {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	return names, nil
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// uploadSegmentToMemStore copies a segment's data file and every sidecar
// extension from the source dir into the mem store, mimicking what the
// background uploader would do after seal.
func uploadSegmentToMemStore(t *testing.T, srcDir, fileName string, store *memStore) {
	t.Helper()
	for _, ext := range storage.SegmentSidecarExts {
		path := filepath.Join(srcDir, fileName+ext)
		data, err := os.ReadFile(path) //nolint:gosec
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			t.Fatalf("read %s: %v", path, err)
		}
		if err := store.Put(fileName+ext, bytes.NewReader(data)); err != nil {
			t.Fatalf("put %s: %v", fileName+ext, err)
		}
	}
}

func TestReconcile_AdoptsRemoteSegments(t *testing.T) {
	// Stage 1: produce a sealed segment on node A.
	srcDir := t.TempDir()
	smA, err := storage.OpenSegmentManager(srcDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager A: %v", err)
	}
	if err := smA.Write([]byte("hello"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := smA.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	sealed := smA.Segments()
	if len(sealed) != 1 {
		t.Fatalf("expected 1 sealed segment, got %d", len(sealed))
	}
	fileName := sealed[0].FileName
	if err := smA.Close(); err != nil {
		t.Fatalf("Close A: %v", err)
	}

	// Stage 2: node B starts on a fresh dir; mem store has the segment from A.
	dstDir := t.TempDir()
	store := newMemStore(dstDir)
	uploadSegmentToMemStore(t, srcDir, fileName, store)

	smB, err := storage.OpenSegmentManager(dstDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager B: %v", err)
	}
	defer smB.Close()

	if len(smB.Segments()) != 0 {
		t.Fatalf("fresh node B should have no segments, got %d", len(smB.Segments()))
	}

	n, err := ReconcileFromRemote(context.Background(), smB, store, dstDir, quietLogger())
	if err != nil {
		t.Fatalf("ReconcileFromRemote: %v", err)
	}
	if n != 1 {
		t.Errorf("adopted: got %d, want 1", n)
	}

	adopted := smB.Segments()
	if len(adopted) != 1 {
		t.Fatalf("after reconcile: got %d segments, want 1", len(adopted))
	}
	if adopted[0].FileName != fileName {
		t.Errorf("filename: got %q, want %q", adopted[0].FileName, fileName)
	}
	if adopted[0].UploadState != storage.UploadStateUploaded {
		t.Errorf("UploadState: got %d, want UploadStateUploaded", adopted[0].UploadState)
	}
	if adopted[0].RecordCount == 0 {
		t.Errorf("RecordCount: got 0; footer not parsed?")
	}
}

func TestReconcile_IdempotentOnRerun(t *testing.T) {
	srcDir := t.TempDir()
	smA, err := storage.OpenSegmentManager(srcDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager A: %v", err)
	}
	if err := smA.Write([]byte("a"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := smA.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	fileName := smA.Segments()[0].FileName
	_ = smA.Close()

	dstDir := t.TempDir()
	store := newMemStore(dstDir)
	uploadSegmentToMemStore(t, srcDir, fileName, store)

	smB, err := storage.OpenSegmentManager(dstDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager B: %v", err)
	}
	defer smB.Close()

	n1, err := ReconcileFromRemote(context.Background(), smB, store, dstDir, quietLogger())
	if err != nil || n1 != 1 {
		t.Fatalf("first reconcile: n=%d err=%v", n1, err)
	}
	n2, err := ReconcileFromRemote(context.Background(), smB, store, dstDir, quietLogger())
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if n2 != 0 {
		t.Errorf("second reconcile should adopt nothing: got %d", n2)
	}
}

// Compile-time interface check.
var _ storage.SegmentStore = (*memStore)(nil)
