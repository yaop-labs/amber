package store

import (
	"time"
)

// startEvictionSweep launches background eviction for cold series.
// The sweep is disabled when retention is not configured.
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

// evictOnce runs one sweep iteration and returns the number of evicted series.
func (s *Store) evictOnce() int {
	now := s.clock().UnixMilli()
	evicted := s.engine.Registry().Sweep(now)
	if len(evicted) == 0 {
		return 0
	}
	// Persist evictions after the registry lock has been released.
	if s.catalogLog != nil {
		ts := now
		for _, id := range evicted {
			if err := s.catalogLog.AppendEvict(uint64(id), ts); err != nil {
				s.setBackgroundError(err)
				break
			}
		}
	}
	// Keep the in-memory catalog consistent with the registry.
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
