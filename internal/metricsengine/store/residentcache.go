package store

import "github.com/yaop-labs/amber/internal/metricsengine/block"

// residentCache is a byte-budgeted cache of compact resident block indexes
// (block.ResidentBlock). It mirrors dirCache but holds the resident form, which
// is ~3.5× smaller than a decoded Directory (~12 vs ~40 MiB for a 100k-series
// block), so every in-window block of a campaign-scale range-step query fits at
// once — the directory decode that was ~43% of the qm2 query happens once per
// block instead of every query (see metrics-query-redesign.md, phase 2).
//
// All methods require the caller to hold the owning store's lock, like dirCache.
type residentCache struct {
	entries map[string]residentCacheEntry
	bytes   int64
	budget  int64
}

type residentCacheEntry struct {
	rb    *block.ResidentBlock
	bytes int64
}

// defaultResidentCacheBudget bounds resident-index memory. ~384 MiB holds the
// ~30 blocks of a campaign store (~12 MiB each). Sized to coexist with the
// store's GOMEMLIMIT-class soft limit: the resident form is steady-state (built
// once, not churned), so it does not reintroduce the GC-assist pathology that a
// recurring per-query decode caused.
const defaultResidentCacheBudget = 384 << 20

func newResidentCache(budget int64) *residentCache {
	if budget <= 0 {
		budget = defaultResidentCacheBudget
	}
	return &residentCache{entries: make(map[string]residentCacheEntry), budget: budget}
}

func (c *residentCache) get(path string) (*block.ResidentBlock, bool) {
	e, ok := c.entries[path]
	return e.rb, ok
}

func (c *residentCache) put(path string, rb *block.ResidentBlock) {
	if _, ok := c.entries[path]; ok {
		return
	}
	sz := rb.ResidentRetainedBytes()
	for key, e := range c.entries {
		if c.bytes+sz <= c.budget {
			break
		}
		delete(c.entries, key)
		c.bytes -= e.bytes
	}
	c.entries[path] = residentCacheEntry{rb: rb, bytes: sz}
	c.bytes += sz
}

func (c *residentCache) reset() {
	c.entries = make(map[string]residentCacheEntry)
	c.bytes = 0
}
