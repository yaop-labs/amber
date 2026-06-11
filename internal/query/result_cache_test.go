package query

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
)

func cacheKeyFor(b byte) [32]byte {
	var k [32]byte
	k[0] = b
	return k
}

// TestQueryCache_InvalidateRangeScoped pins the P1-3 contract: an ingest
// batch drops only cached results whose query window overlaps the batch's
// event-time range, so historical windows survive steady writes.
func TestQueryCache_InvalidateRangeScoped(t *testing.T) {
	c := newQueryCache(16, time.Minute)
	result := &LogResult{Entries: []model.LogEntry{{}}}

	histKey := cacheKeyFor(1)
	recentKey := cacheKeyFor(2)
	unboundedKey := cacheKeyFor(3)

	now := time.Now().UnixNano()
	hour := time.Hour.Nanoseconds()

	// Yesterday's window, a window covering "now", and an unbounded query.
	c.putLog(histKey, result, now-25*hour, now-24*hour)
	c.putLog(recentKey, result, now-hour, now+hour)
	c.putLog(unboundedKey, result, 0, int64(^uint64(0)>>1))

	// Batch written at ~now.
	c.invalidateRange(now, now)

	if _, ok := c.getLog(histKey); !ok {
		t.Fatal("historical window evicted by a non-overlapping batch")
	}
	if _, ok := c.getLog(recentKey); ok {
		t.Fatal("overlapping window survived the batch")
	}
	if _, ok := c.getLog(unboundedKey); ok {
		t.Fatal("unbounded window survived the batch")
	}

	// A backfill batch with old event timestamps must evict the historical
	// window too: overlap is judged on event time, not arrival time.
	c.putLog(recentKey, result, now-hour, now+hour)
	c.invalidateRange(now-25*hour, now-24*hour)
	if _, ok := c.getLog(histKey); ok {
		t.Fatal("historical window survived an overlapping backfill batch")
	}
	if _, ok := c.getLog(recentKey); !ok {
		t.Fatal("recent window evicted by a non-overlapping backfill batch")
	}
}
