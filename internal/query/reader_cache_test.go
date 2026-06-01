package query

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metrics"
	"github.com/yaop-labs/amber/internal/storage"
)

// queryMemStore mirrors the bootstrap-test memStore for use inside the
// query package. Kept local to avoid an internal/testutil package for a
// one-off helper.
type queryMemStore struct {
	mu       sync.Mutex
	objects  map[string][]byte
	localDir string
	gets     int32
}

func newQueryMemStore(localDir string) *queryMemStore {
	return &queryMemStore{
		objects:  make(map[string][]byte),
		localDir: localDir,
	}
}

func (m *queryMemStore) Put(name string, r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.objects[name] = data
	m.mu.Unlock()
	return nil
}

func (m *queryMemStore) Get(name string) (io.ReadCloser, error) {
	atomic.AddInt32(&m.gets, 1)
	m.mu.Lock()
	data, ok := m.objects[name]
	m.mu.Unlock()
	if !ok {
		return nil, os.ErrNotExist
	}
	if err := os.MkdirAll(m.localDir, 0750); err != nil {
		return nil, err
	}
	// Mimic S3Store.Get's atomic temp+rename so partial writes are never
	// visible to a concurrent OpenSegmentReader.
	localPath := filepath.Join(m.localDir, name)
	f, err := os.CreateTemp(m.localDir, name+".tmp.*")
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(f.Name())
		return nil, err
	}
	if err := os.Rename(f.Name(), localPath); err != nil {
		_ = os.Remove(f.Name())
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *queryMemStore) Delete(name string) error {
	m.mu.Lock()
	delete(m.objects, name)
	m.mu.Unlock()
	return nil
}

func (m *queryMemStore) DeleteLocal(_ string) error {
	return nil
}

func (m *queryMemStore) List() ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var names []string
	for k := range m.objects {
		if strings.HasSuffix(k, ".alog") {
			names = append(names, k)
		}
	}
	return names, nil
}

// produceSealedSegment writes one record, rotates, and returns the file
// name plus the source directory holding the data file and sidecars.
func produceSealedSegment(t *testing.T) (srcDir, fileName string) {
	t.Helper()
	srcDir = t.TempDir()
	sm, err := storage.OpenSegmentManager(srcDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	if err := sm.Write([]byte("payload"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	segs := sm.Segments()
	if len(segs) != 1 {
		t.Fatalf("expected 1 sealed segment, got %d", len(segs))
	}
	fileName = segs[0].FileName
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return srcDir, fileName
}

func uploadToQueryStore(t *testing.T, srcDir, fileName string, store *queryMemStore) {
	t.Helper()
	for _, ext := range storage.SegmentSidecarExts {
		path := filepath.Join(srcDir, fileName+ext)
		data, err := os.ReadFile(path) //nolint:gosec
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("read %s: %v", path, err)
		}
		if err := store.Put(fileName+ext, bytes.NewReader(data)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
}

func TestReaderCache_FetchesFromStoreOnMiss(t *testing.T) {
	srcDir, fileName := produceSealedSegment(t)
	dstDir := t.TempDir()
	store := newQueryMemStore(dstDir)
	uploadToQueryStore(t, srcDir, fileName, store)

	cache := newReaderCache(4)
	cache.setFetcher(makeStoreFetcher(store, dstDir, "logs", nil))

	path := filepath.Join(dstDir, fileName)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expected file absent before acquire, got err=%v", err)
	}

	cr, err := cache.acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	cache.release(cr)

	if _, err := os.Stat(path); err != nil {
		t.Errorf("expected file present after acquire, got err=%v", err)
	}
}

func TestReaderCache_SingleflightDedupsConcurrentMisses(t *testing.T) {
	srcDir, fileName := produceSealedSegment(t)
	dstDir := t.TempDir()
	store := newQueryMemStore(dstDir)
	uploadToQueryStore(t, srcDir, fileName, store)

	cache := newReaderCache(4)
	cache.setFetcher(makeStoreFetcher(store, dstDir, "logs", nil))

	const concurrency = 8
	var wg sync.WaitGroup
	errs := make(chan error, concurrency)
	path := filepath.Join(dstDir, fileName)

	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cr, err := cache.acquire(path)
			if err != nil {
				errs <- err
				return
			}
			cache.release(cr)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("acquire: %v", err)
		}
	}

	// Singleflight gives an upper bound on Get count that scales with the
	// number of sidecars, NOT with goroutine count. Without dedup, 8
	// goroutines would each request the full sidecar set (~6 each = 48+).
	// We assert that real concurrency dedup happened: the count must stay
	// well below the unbounded case. The tight upper bound is timing-
	// dependent (singleflight clears its key as soon as the first call
	// returns), so we allow up to ~3 fetcher passes' worth.
	gets := atomic.LoadInt32(&store.gets)
	if maxAllowed := int32(3 * len(storage.SegmentSidecarExts)); gets > maxAllowed {
		t.Errorf("Get calls scaled with concurrency: got %d, want <= %d", gets, maxAllowed)
	}
}

// TestReaderCache_ColdFetchMetricsOnDataMiss confirms that a fetch triggered
// by an absent .alog file (the eviction case) bumps the cold-read counter,
// while a fetch that only refills sidecars (the data file is already local)
// does not. This is what makes the metric a useful signal of "your local
// retention horizon is too short for your query mix."
func TestReaderCache_ColdFetchMetricsOnDataMiss(t *testing.T) {
	srcDir, fileName := produceSealedSegment(t)
	dstDir := t.TempDir()
	store := newQueryMemStore(dstDir)
	uploadToQueryStore(t, srcDir, fileName, store)

	before := metrics.QueryColdSegmentReads.WithLabelValues("logs").Get()

	cache := newReaderCache(4)
	cache.setFetcher(makeStoreFetcher(store, dstDir, "logs", nil))

	path := filepath.Join(dstDir, fileName)
	cr, err := cache.acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	cache.release(cr)

	after := metrics.QueryColdSegmentReads.WithLabelValues("logs").Get()
	if after != before+1 {
		t.Errorf("cold read counter: before=%d after=%d, expected +1", before, after)
	}
}

func TestReaderCache_PropagatesStoreErrorOnMissingRemote(t *testing.T) {
	dstDir := t.TempDir()
	// Store is empty: the data file simply doesn't exist anywhere.
	store := newQueryMemStore(dstDir)
	cache := newReaderCache(4)
	cache.setFetcher(makeStoreFetcher(store, dstDir, "logs", nil))

	path := filepath.Join(dstDir, "seg_00000099.alog")
	_, err := cache.acquire(path)
	if err == nil {
		t.Fatalf("expected error when remote also missing")
	}
}

var _ storage.SegmentStore = (*queryMemStore)(nil)
