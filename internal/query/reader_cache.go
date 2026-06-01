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

	"github.com/yaop-labs/amber/internal/metrics"
	"github.com/yaop-labs/amber/internal/storage"
)

// segmentFetcher pulls a segment data file and all its sidecars from a
// remote SegmentStore into the local cache directory. fileName is the
// data-file base name (e.g. "seg_00000001.alog"); sidecars are derived
// from storage.SegmentSidecarExts. Returning nil means every required
// file is now present at filepath.Join(localDir, fileName + ext) for
// the data ext; missing optional sidecars are tolerated.
type segmentFetcher func(fileName string) error

type readerCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List

	// fetcher, if non-nil, is called when OpenSegmentReader fails with
	// os.ErrNotExist. It pulls the segment from remote storage and the
	// open is retried. Nil disables remote fetch (default for local-only
	// deployments).
	fetcher segmentFetcher
	// flight deduplicates concurrent remote fetches of the same segment.
	// Without it N concurrent queries against an evicted segment would
	// each pay N S3 GETs.
	flight singleflight.Group
}

type cachedReader struct {
	mu     sync.Mutex
	path   string
	reader *storage.SegmentReader
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

// setFetcher wires a remote-fetch fallback. Pass nil to disable. The cache
// itself stays unaware of S3 specifics; the fetcher closure encapsulates
// store + local dir.
func (c *readerCache) setFetcher(f segmentFetcher) {
	c.fetcher = f
}

// makeStoreFetcher builds a segmentFetcher that pulls every sidecar for
// fileName from store, materializing them under localDir. Missing remote
// sidecars (except the data file itself) are tolerated — bootstrap or
// on-demand build rebuilds them. Atomic write is delegated to store.Get,
// which writes through a temp file and renames.
//
// kind is the storage stream tag ("logs"|"spans") used for cold-read metrics
// and the single observability log emitted per fetch. log may be nil.
func makeStoreFetcher(store storage.SegmentStore, localDir, kind string, log *slog.Logger) segmentFetcher {
	return func(fileName string) error {
		// Treat the absence of the data file as the cold-read trigger:
		// sidecar-only refetches (rare, only when bidx/fidx/etc. went missing
		// but .alog stayed) shouldn't inflate the cold-fetch counter.
		dataMissing := false
		if _, err := os.Stat(filepath.Join(localDir, fileName)); err != nil && os.IsNotExist(err) {
			dataMissing = true
		}

		start := time.Now()
		for _, ext := range storage.SegmentSidecarExts {
			name := fileName + ext
			// Skip if already present locally — store.Get does this check
			// too, but avoiding the call saves a lock acquire in singleflight.
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
				// Non-fatal sidecar fetch failure; carry on.
				continue
			}
			_ = rc.Close()
		}
		if dataMissing {
			elapsed := time.Since(start)
			metrics.QueryColdSegmentReads.WithLabelValues(kind).Inc()
			metrics.QueryColdSegmentFetchDur.WithLabelValues(kind).Observe(elapsed.Seconds())
			// One log per cold fetch is bounded by segment size (sealed
			// segments are tens of MB), so this stays quiet even when the
			// query plan scans many segments.
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
		c.mu.Unlock()
		cr.mu.Lock()
		return cr, nil
	}
	c.mu.Unlock()

	sr, err := storage.OpenSegmentReader(path, nil)
	if err != nil && c.fetcher != nil && errors.Is(err, os.ErrNotExist) {
		// Local miss: pull from remote store under singleflight so concurrent
		// queriers don't each pay the network cost. After fetch, retry the
		// open — store.Get writes the data file atomically via temp+rename.
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
		c.mu.Unlock()
		_ = sr.Close()
		cr.mu.Lock()
		return cr, nil
	}

	cr := &cachedReader{path: path, reader: sr}
	ent := &readerCacheEntry{path: path, cached: cr}
	el := c.order.PushFront(ent)
	c.items[path] = el

	var evicted *cachedReader
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			oldEnt := oldest.Value.(*readerCacheEntry)
			c.order.Remove(oldest)
			delete(c.items, oldEnt.path)
			evicted = oldEnt.cached
		}
	}
	c.mu.Unlock()

	if evicted != nil {

		go func(e *cachedReader) {
			e.mu.Lock()
			if e.reader != nil {
				_ = e.reader.Close()
				e.reader = nil
			}
			e.mu.Unlock()
		}(evicted)
	}

	cr.mu.Lock()
	return cr, nil
}

func (c *readerCache) release(cr *cachedReader) {
	cr.mu.Unlock()
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
	c.mu.Unlock()

	go func(e *cachedReader) {
		e.mu.Lock()
		if e.reader != nil {
			_ = e.reader.Close()
			e.reader = nil
		}
		e.mu.Unlock()
	}(ent.cached)
}

func (c *readerCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, el := range c.items {
		ent := el.Value.(*readerCacheEntry)
		ent.cached.mu.Lock()
		if ent.cached.reader != nil {
			_ = ent.cached.reader.Close()
			ent.cached.reader = nil
		}
		ent.cached.mu.Unlock()
	}
	c.items = make(map[string]*list.Element)
	c.order.Init()
}
