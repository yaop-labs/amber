package store

import "github.com/yaop-labs/amber/internal/metricsengine/block"

// dirCache is a byte-budgeted cache of decoded block directories. It replaces
// a fixed count bound (was: 8 entries): a directory's retained footprint
// scales with its series count, so a count bound either thrashes on large
// blocks or wastes memory on small ones. A byte budget gives a predictable
// ceiling regardless of block shape - the campaign query path re-decodes
// every in-window directory it cannot hold, and at ~30 blocks the re-decode
// dominated both CPU and allocations (see rate_query_bench_test.go).
//
// All methods require the caller to hold the owning store's lock (the cache
// piggybacks on s.mu like the map it replaced).
type dirCache struct {
	entries map[string]dirCacheEntry
	bytes   int64
	budget  int64
}

type dirCacheEntry struct {
	dir   block.Directory
	bytes int64
}

// defaultDirCacheBudget bounds cached directory memory. ~320MiB holds eight
// 100k-series campaign blocks (~40MiB decoded each) - matching the worst case
// of the old count=8 bound - while letting many more small blocks stay warm.
// Sized to stay clear of the metrics store's GOMEMLIMIT-class soft limit so
// caching does not reintroduce the GC-assist pathology that motivated the
// original bound.
const defaultDirCacheBudget = 320 << 20

func newDirCache(budget int64) *dirCache {
	if budget <= 0 {
		budget = defaultDirCacheBudget
	}
	return &dirCache{entries: make(map[string]dirCacheEntry), budget: budget}
}

func (c *dirCache) get(path string) (block.Directory, bool) {
	e, ok := c.entries[path]
	return e.dir, ok
}

// put inserts dir under path, evicting other entries (random map order, as the
// old bound did) until the total stays within budget. A directory larger than
// the whole budget is still cached alone so a single query can make progress.
func (c *dirCache) put(path string, dir block.Directory) {
	if _, ok := c.entries[path]; ok {
		return
	}
	sz := directoryRetainedBytes(dir)
	for key, e := range c.entries {
		if c.bytes+sz <= c.budget {
			break
		}
		delete(c.entries, key)
		c.bytes -= e.bytes
	}
	c.entries[path] = dirCacheEntry{dir: dir, bytes: sz}
	c.bytes += sz
}

// reset drops every cached directory. Used when the manifest changes wholesale
// (reopen, compaction) and old paths may no longer exist.
func (c *dirCache) reset() {
	c.entries = make(map[string]dirCacheEntry)
	c.bytes = 0
}

// directoryRetainedBytes estimates the heap a decoded Directory holds: the
// entry array plus the per-block label and aggregate-bucket arenas. Label
// strings are interned within the block, so their content is a small constant
// amortised into the per-label term rather than counted per occurrence.
func directoryRetainedBytes(dir block.Directory) int64 {
	const (
		perEntry  = 176 // DirectoryEntry struct (numeric fields + two slice headers)
		perLabel  = 48  // model.Label: two string headers + amortised interned bytes
		perBucket = 56  // AggregateBucket struct
	)
	var labels, buckets int
	for i := range dir.Series {
		labels += len(dir.Series[i].Labels)
		buckets += len(dir.Series[i].AggregateBuckets)
	}
	return int64(len(dir.Series))*perEntry + int64(labels)*perLabel + int64(buckets)*perBucket
}
