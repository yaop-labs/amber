package store

import (
	"time"
)

// startEvictionSweep launches the background goroutine that periodically
// evicts cold series from the index registry and persists those evictions
// to the catalog log. Lifecycle owned by Store: startEvictionSweep is
// called once from OpenWithOptions; stopEvictionSweep is called from
// Close. Disabled (no goroutine launched) when opts.Retention <= 0.
//
// INDEX_EVICTION_SPEC_v0.md §3+§4:
//   - sweep cadence = retention/6 (clamped to [1s, 60s]) — small enough to
//     bound the lag between "series becomes cold" and "sweep evicts it",
//     large enough to keep per-tick work cheap;
//   - eviction uses Registry.Sweep which holds the index write lock for
//     the duration of one expired-bucket walk; the bucket size is bounded
//     by churn rate × granularity, so held-lock duration is bounded;
//   - concurrent-read invariant for v0: readers RLock the registry during
//     Match; sweep WLocks during eviction; the RW-mutex queues them — no
//     torn read. Grace period for in-flight queries is implicit via the
//     lock, not explicit. RCU/epoch is a later-iteration upgrade (spec §4).
//
// Crash safety: in-memory eviction happens BEFORE the catalog log EVICT
// record is written. If we crash between them the series is gone from
// memory but still present in the log; recovery re-Imports it, the next
// sweep re-evicts. Idempotent.
func (s *Store) startEvictionSweep() {
	if s.opts.Retention <= 0 {
		return
	}
	interval := s.opts.Retention / 6
	if interval < time.Second {
		interval = time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	s.stopSweep = make(chan struct{})
	s.sweepDone = make(chan struct{})
	go s.runEvictionSweep(interval)
}

func (s *Store) stopEvictionSweep() {
	if s.stopSweep == nil {
		return
	}
	close(s.stopSweep)
	<-s.sweepDone
	s.stopSweep = nil
}

func (s *Store) runEvictionSweep(interval time.Duration) {
	defer close(s.sweepDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopSweep:
			return
		case <-ticker.C:
			s.evictOnce()
		}
	}
}

// evictOnce runs one sweep iteration. Returns the number of series
// evicted (used by tests to assert the gate condition).
func (s *Store) evictOnce() int {
	now := s.clock().UnixMilli()
	evicted := s.engine.Registry().Sweep(now)
	if len(evicted) == 0 {
		return 0
	}
	// Persist evictions to the catalog log. We do this AFTER the
	// registry write lock has released (Sweep returned) — log fsync is
	// far too slow to hold the index lock for.
	//
	// If the catalog log is unavailable (boot path didn't open it, or
	// disk is full), we still report the eviction count — the in-memory
	// eviction already happened. The next sweep's idempotent recovery
	// will not re-introduce these series unless they were re-Imported
	// from a snapshot/log in between (which they won't be without a
	// restart).
	if s.catalogLog != nil {
		ts := now
		for _, id := range evicted {
			if err := s.catalogLog.AppendEvict(uint64(id), ts); err != nil {
				// Record but do not stop. The series is already gone
				// from memory; persistence will catch up next sweep
				// if the error was transient.
				s.setBackgroundError(err)
				break
			}
		}
	}
	// Drop evicted ids from the in-memory s.catalog.Series slice so the
	// next ensureCatalog scan is consistent with the registry.
	evictedSet := make(map[uint64]struct{}, len(evicted))
	for _, id := range evicted {
		evictedSet[uint64(id)] = struct{}{}
	}
	s.mu.Lock()
	kept := s.catalog.Series[:0]
	for _, entry := range s.catalog.Series {
		if _, gone := evictedSet[entry.ID]; gone {
			continue
		}
		kept = append(kept, entry)
	}
	s.catalog.Series = kept
	s.mu.Unlock()
	return len(evicted)
}
