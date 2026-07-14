package query

import (
	"container/list"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
)

// segmentFetcher materializes a segment and its required sidecars locally.
type segmentFetcher func(fileName string) error

type readerCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List

	// fetcher pulls missing segments from remote storage. Nil disables remote
	// fetches.
	fetcher segmentFetcher
	// flight deduplicates concurrent remote fetches of the same segment.
	flight singleflight.Group
}

// cachedReader tracks one shared SegmentReader. SegmentReader scans are
// position-stateless (pread), so concurrent queries share one reader without
// serializing; refs/evicted (guarded by readerCache.mu) only defer Close
// until the last in-flight scan releases.
type cachedReader struct {
	path   string
	reader *storage.SegmentReader

	refs    int
	evicted bool
}

type readerCacheEntry struct {
	path   string
	cached *cachedReader
}

func newReaderCache(capacity int) *readerCache {
	if capacity < 1 {
		capacity = 1
	}
	return &readerCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

// setFetcher wires a remote-fetch fallback.
func (c *readerCache) setFetcher(f segmentFetcher) {
	c.fetcher = f
}

// makeStoreFetcher builds a fetcher backed by a SegmentStore.
// Missing optional sidecars are tolerated; the data file is required.
func makeStoreFetcher(store storage.SegmentStore, localDir, kind string, log *slog.Logger) segmentFetcher {
	return func(fileName string) error {
		dataMissing := false
		if _, err := os.Stat(filepath.Join(localDir, fileName)); err != nil && os.IsNotExist(err) {
			dataMissing = true
		}

		start := time.Now()
		for _, ext := range storage.SegmentSidecarExts {
			name := fileName + ext
			if _, err := os.Stat(filepath.Join(localDir, name)); err == nil {
				continue
			}
			rc, err := store.Get(name)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					if ext == "" {
						return err
					}
					continue
				}
				if ext == "" {
					return err
				}
				continue
			}
			_ = rc.Close()
		}
		if dataMissing {
			elapsed := time.Since(start)
			selfobs.QueryColdSegmentReads.WithLabelValues(kind).Inc()
			selfobs.QueryColdSegmentFetchDur.WithLabelValues(kind).Observe(elapsed.Seconds())
			if log != nil {
				log.Info("cold segment fetch",
					"kind", kind,
					"segment", fileName,
					"duration", elapsed,
				)
			}
		}
		return nil
	}
}

func (c *readerCache) acquire(path string) (*cachedReader, error) {
	c.mu.Lock()
	if el, ok := c.items[path]; ok {
		c.order.MoveToFront(el)
		cr := el.Value.(*readerCacheEntry).cached
		cr.refs++
		c.mu.Unlock()
		return cr, nil
	}
	c.mu.Unlock()

	sr, err := storage.OpenSegmentReader(path, nil)
	if err != nil && c.fetcher != nil && (errors.Is(err, os.ErrNotExist) || errors.Is(err, storage.ErrSegmentCorrupted)) {
		if errors.Is(err, storage.ErrSegmentCorrupted) {
			if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
				return nil, removeErr
			}
		}
		// Local miss: pull from remote store under singleflight so concurrent
		// queriers don't each pay the network cost. After fetch, retry the
		// open - store.Get writes the data file atomically via temp+rename.
		fileName := filepath.Base(path)
		_, ferr, _ := c.flight.Do(path, func() (any, error) {
			return nil, c.fetcher(fileName)
		})
		if ferr != nil {
			return nil, ferr
		}
		sr, err = storage.OpenSegmentReader(path, nil)
	}
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if el, ok := c.items[path]; ok {
		cr := el.Value.(*readerCacheEntry).cached
		c.order.MoveToFront(el)
		cr.refs++
		c.mu.Unlock()
		_ = sr.Close()
		return cr, nil
	}

	cr := &cachedReader{path: path, reader: sr, refs: 1}
	ent := &readerCacheEntry{path: path, cached: cr}
	el := c.order.PushFront(ent)
	c.items[path] = el

	var closeReader *storage.SegmentReader
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			oldEnt := oldest.Value.(*readerCacheEntry)
			c.order.Remove(oldest)
			delete(c.items, oldEnt.path)
			closeReader = evictLocked(oldEnt.cached)
		}
	}
	c.mu.Unlock()

	if closeReader != nil {
		_ = closeReader.Close()
	}
	return cr, nil
}

// refreshCorrupt drops a cached reader and corrupt local data file, then
// materializes one fresh remote copy. Callers retry the scan at most once.
func (c *readerCache) refreshCorrupt(path string) error {
	if c.fetcher == nil {
		return errors.New("reader cache: remote fetch disabled")
	}
	c.invalidate(path)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	_, err, _ := c.flight.Do(path+"#corrupt", func() (any, error) {
		return nil, c.fetcher(filepath.Base(path))
	})
	return err
}

// evictLocked marks the entry evicted and, when no scan holds it, detaches
// the reader for the caller to close. Caller holds c.mu.
func evictLocked(cr *cachedReader) *storage.SegmentReader {
	cr.evicted = true
	if cr.refs > 0 || cr.reader == nil {
		return nil
	}
	r := cr.reader
	cr.reader = nil
	return r
}

func (c *readerCache) release(cr *cachedReader) {
	c.mu.Lock()
	cr.refs--
	var closeReader *storage.SegmentReader
	if cr.evicted && cr.refs == 0 && cr.reader != nil {
		closeReader = cr.reader
		cr.reader = nil
	}
	c.mu.Unlock()
	if closeReader != nil {
		_ = closeReader.Close()
	}
}

func (c *readerCache) invalidate(path string) {
	c.mu.Lock()
	el, ok := c.items[path]
	if !ok {
		c.mu.Unlock()
		return
	}
	ent := el.Value.(*readerCacheEntry)
	c.order.Remove(el)
	delete(c.items, path)
	closeReader := evictLocked(ent.cached)
	c.mu.Unlock()

	if closeReader != nil {
		_ = closeReader.Close()
	}
}

func (c *readerCache) close() {
	c.mu.Lock()
	var closeReaders []*storage.SegmentReader
	for _, el := range c.items {
		ent := el.Value.(*readerCacheEntry)
		if r := evictLocked(ent.cached); r != nil {
			closeReaders = append(closeReaders, r)
		}
	}
	c.items = make(map[string]*list.Element)
	c.order.Init()
	c.mu.Unlock()
	for _, r := range closeReaders {
		_ = r.Close()
	}
}
