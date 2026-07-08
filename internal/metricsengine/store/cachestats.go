package store

// splitCacheBudget divides the combined Options.CacheBudget between the
// directory cache and the resident cache, preserving the historical 320:384
// proportion of the per-cache defaults. Zero (or negative) means "no combined
// budget configured": both caches fall back to their compiled-in defaults
// inside their constructors.
func splitCacheBudget(budget int64) (dirBudget, residentBudget int64) {
	if budget <= 0 {
		return 0, 0
	}
	// Split in two terms so the exact defaultDirCacheBudget:total proportion
	// holds without overflowing int64 on multi-GiB budgets: for total t,
	// budget*dir/t == (budget/t)*dir + (budget%t)*dir/t, and both products stay
	// well under MaxInt64. The naive budget*defaultDirCacheBudget wrapped
	// negative above ~25.6 GiB, handing the resident cache more than the whole
	// budget while the dir cache fell back to its default.
	const total = defaultDirCacheBudget + defaultResidentCacheBudget
	dirBudget = budget/total*defaultDirCacheBudget + budget%total*defaultDirCacheBudget/total
	// A positive budget must never round the dir share to 0, or newDirCache
	// would read it as "unset" and restore its full compiled-in default.
	if dirBudget == 0 {
		dirBudget = 1
	}
	return dirBudget, budget - dirBudget
}

// CacheStats is a point-in-time snapshot of the two block caches.
// Hit/miss/eviction counters are cumulative since open; bytes/budget are
// current. A nonzero eviction rate under a steady block set means the budget
// is smaller than the working set.
type CacheStats struct {
	DirBytes     int64
	DirBudget    int64
	DirHits      int64
	DirMisses    int64
	DirEvictions int64

	ResidentBytes     int64
	ResidentBudget    int64
	ResidentHits      int64
	ResidentMisses    int64
	ResidentEvictions int64
}

// CacheStats returns the current snapshot of both block caches.
func (s *Store) CacheStats() CacheStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return CacheStats{
		DirBytes:     s.directoryCache.bytes,
		DirBudget:    s.directoryCache.budget,
		DirHits:      s.directoryCache.hits.Load(),
		DirMisses:    s.directoryCache.misses.Load(),
		DirEvictions: s.directoryCache.evictions.Load(),

		ResidentBytes:     s.residentCache.bytes,
		ResidentBudget:    s.residentCache.budget,
		ResidentHits:      s.residentCache.hits.Load(),
		ResidentMisses:    s.residentCache.misses.Load(),
		ResidentEvictions: s.residentCache.evictions.Load(),
	}
}
