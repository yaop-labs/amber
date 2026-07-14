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
	gen := c.logGenerationSnapshot()

	// Yesterday's window, a window covering "now", and an unbounded query.
	c.putLog(histKey, result, now-25*hour, now-24*hour, gen)
	c.putLog(recentKey, result, now-hour, now+hour, gen)
	c.putLog(unboundedKey, result, 0, int64(^uint64(0)>>1), gen)

	// Batch written at ~now.
	c.invalidateLogRange(now, now)

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
	c.putLog(recentKey, result, now-hour, now+hour, c.logGenerationSnapshot())
	c.invalidateLogRange(now-25*hour, now-24*hour)
	if _, ok := c.getLog(histKey); ok {
		t.Fatal("historical window survived an overlapping backfill batch")
	}
	if _, ok := c.getLog(recentKey); !ok {
		t.Fatal("recent window evicted by a non-overlapping backfill batch")
	}
}

func TestQueryCacheRejectsPublishFromStaleGeneration(t *testing.T) {
	c := newQueryCache(16, time.Minute)
	key := cacheKeyFor(1)
	result := &LogResult{Entries: []model.LogEntry{{Body: "stale"}}}
	startedAt := c.logGenerationSnapshot()

	// Models ingest/retention completing while the query is scanning.
	c.invalidateLogRange(100, 200)
	if c.putLog(key, result, 0, 300, startedAt) {
		t.Fatal("cache published a result scanned under an old log generation")
	}
	if _, ok := c.getLog(key); ok {
		t.Fatal("stale result is observable")
	}
}

func TestQueryCacheDeepCopiesResultsAndAttributes(t *testing.T) {
	c := newQueryCache(16, time.Minute)
	logKey := cacheKeyFor(1)
	logResult := &LogResult{Entries: []model.LogEntry{{Body: "original", Attrs: []model.Attr{{Key: "env", Value: "prod"}}}}}
	if !c.putLog(logKey, logResult, 0, 100, c.logGenerationSnapshot()) {
		t.Fatal("putLog failed")
	}
	logResult.Entries[0].Body = "mutated source"
	logResult.Entries[0].Attrs[0].Value = "mutated source attr"
	first, ok := c.getLog(logKey)
	if !ok || first.Entries[0].Body != "original" || first.Entries[0].Attrs[0].Value != "prod" {
		t.Fatalf("cache shares source ownership: %+v", first)
	}
	first.Entries[0].Body = "mutated caller"
	first.Entries[0].Attrs[0].Value = "mutated caller attr"
	second, _ := c.getLog(logKey)
	if second.Entries[0].Body != "original" || second.Entries[0].Attrs[0].Value != "prod" {
		t.Fatalf("cache shares returned ownership: %+v", second)
	}

	spanKey := cacheKeyFor(2)
	spanResult := &SpanResult{Spans: []model.SpanEntry{{Operation: "original", Attrs: []model.Attr{{Key: "db", Value: "amber"}}}}}
	if !c.putSpan(spanKey, spanResult, 0, 100, c.spanGenerationSnapshot()) {
		t.Fatal("putSpan failed")
	}
	gotSpan, _ := c.getSpan(spanKey)
	gotSpan.Spans[0].Operation = "mutated"
	gotSpan.Spans[0].Attrs[0].Value = "mutated"
	again, _ := c.getSpan(spanKey)
	if again.Spans[0].Operation != "original" || again.Spans[0].Attrs[0].Value != "amber" {
		t.Fatalf("span cache shares returned ownership: %+v", again)
	}
}

func TestQueryCacheGenerationsAreStreamScoped(t *testing.T) {
	c := newQueryCache(16, time.Minute)
	spanGeneration := c.spanGenerationSnapshot()
	c.invalidateLogRange(1, 2)
	spanResult := &SpanResult{Spans: []model.SpanEntry{{Operation: "span"}}}
	if !c.putSpan(cacheKeyFor(3), spanResult, 0, 10, spanGeneration) {
		t.Fatal("log invalidation incorrectly advanced span generation")
	}
}
