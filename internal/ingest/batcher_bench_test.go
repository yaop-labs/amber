package ingest

// Batcher hot-path benchmarks. White-box so we can poke `breakerThreshold`
// and `logFailures` directly to drive the log breaker into "open" without
// staging real WriteBatch failures.
//
// The pending question (cleanup-2 deferred #5) is: how much do
// IsBreakerOpen + guard.Check cost per entry? The Prelude_* benchmarks
// answer that in isolation; the Send_* ones include channel-send overhead.

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/indexer"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

func benchLogEntry() model.LogEntry {
	return model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: time.Now(),
		Level:     model.LevelInfo,
		Service:   "api-gateway",
		Host:      "host-001",
		Body:      "GET /api/v1/users 200 success latency 45ms",
		Attrs: []model.Attr{
			{Key: "env", Value: "prod"},
			{Key: "region", Value: "us-east-1"},
			{Key: "version", Value: "v1.42.0"},
		},
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newDrainBatcher returns a Batcher whose queue is drained by a background
// goroutine that does no work — measures pure SendLog prelude + channel send
// without storage cost. Returned stop func ensures the drain exits.
func newDrainBatcher(b *testing.B, guard *CardinalityGuard, breakerOpen bool) (*Batcher, func()) {
	b.Helper()
	cfg := Config{
		BatchSize:    1024,
		BatchTimeout: time.Second,
		QueueSize:    4096,
		// Threshold=1 makes breaker controllable via logFailures.
		BreakerThreshold: 1,
	}
	bt := NewBatcher(Deps{
		Guard:  guard,
		Logger: discardLogger(),
	}, cfg)
	if breakerOpen {
		bt.logFailures.Store(1)
	}
	// Tight-loop drain: `for range` is faster than select{<-done, <-queue}
	// because there's no extra case-comparison per recv. Stop by closing
	// the queue — drain exits when channel closed and drained. Caller must
	// not SendLog after stop().
	go func() {
		for range bt.logQueue {
		}
	}()
	stop := func() { close(bt.logQueue) }
	return bt, stop
}

// BenchmarkBatcher_Prelude_NoGuard isolates IsBreakerOpen() — guard is nil, so
// guard.Check returns "" immediately. This is the floor cost of every SendLog.
func BenchmarkBatcher_Prelude_NoGuard(b *testing.B) {
	bt := NewBatcher(Deps{Logger: discardLogger()}, Config{BreakerThreshold: 10})
	entry := benchLogEntry()

	b.ReportAllocs()
	for b.Loop() {
		_ = bt.IsBreakerOpen()
		_ = bt.guard.Check(entry.Service, entry.Attrs)
	}
}

// BenchmarkBatcher_Prelude_WithGuard adds a guard with 3 attrs to validate.
// Each Check walks attrs + checks per-service key set.
func BenchmarkBatcher_Prelude_WithGuard(b *testing.B) {
	guard := NewCardinalityGuard(64, 4096, 1024)
	bt := NewBatcher(Deps{Guard: guard, Logger: discardLogger()}, Config{BreakerThreshold: 10})
	entry := benchLogEntry()
	// Warm the guard's per-service key set so we measure the steady-state
	// "already-seen" path, not the first-insert allocations.
	for range 16 {
		_ = bt.guard.Check(entry.Service, entry.Attrs)
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = bt.IsBreakerOpen()
		_ = bt.guard.Check(entry.Service, entry.Attrs)
	}
}

// BenchmarkBatcher_SendLog_NoGuard end-to-end SendLog with drained queue.
// Includes channel send. Floor cost of SendLog without guard.
//
// Note: with a single drain goroutine the consumer can fall behind the
// producer under steady load — a fraction of iterations take the
// queue_full path (Warn + metric inc, heavier than success). We tolerate
// this and report the mixed cost. The Prelude_* benchmarks isolate the
// pre-channel work; this one captures real SendLog under load.
func BenchmarkBatcher_SendLog_NoGuard(b *testing.B) {
	bt, stop := newDrainBatcher(b, nil, false)
	defer stop()
	entry := benchLogEntry()

	b.ReportAllocs()
	for b.Loop() {
		_ = bt.SendLog(entry)
	}
}

// BenchmarkBatcher_SendLog_WithGuard end-to-end SendLog with cardinality guard
// in the warm path. Compare against _NoGuard for the marginal cost. Same
// queue-full caveat as _NoGuard.
func BenchmarkBatcher_SendLog_WithGuard(b *testing.B) {
	guard := NewCardinalityGuard(64, 4096, 1024)
	bt, stop := newDrainBatcher(b, guard, false)
	defer stop()
	entry := benchLogEntry()
	for range 16 {
		_ = bt.guard.Check(entry.Service, entry.Attrs)
	}

	b.ReportAllocs()
	for b.Loop() {
		_ = bt.SendLog(entry)
	}
}

// BenchmarkBatcher_SendLog_BreakerOpen short-circuits before guard or send.
// This is the cheapest path; if it's expensive, the breaker is more harm
// than help.
func BenchmarkBatcher_SendLog_BreakerOpen(b *testing.B) {
	bt, stop := newDrainBatcher(b, NewCardinalityGuard(64, 4096, 1024), true)
	defer stop()
	entry := benchLogEntry()

	b.ReportAllocs()
	for b.Loop() {
		_ = bt.SendLog(entry)
	}
}

// BenchmarkBatcher_ProcessBatch_e2e drives processBatch directly with a real
// SegmentManager, sparse index, and ActiveIndex. Measures the steady-state
// path that the background goroutine runs: serialize → WriteBatch → sparse
// touch → IndexLogEntries → Flush.
//
// b.N is the number of *batches* processed, each of size batchSize.
//
// Per-iteration ID/timestamp churn is pre-generated outside the timed loop
// because MustNewEntryID + time.Now together dominate CPU profile of a
// naive setup — we'd be measuring ULID generation, not processBatch.
func BenchmarkBatcher_ProcessBatch_e2e(b *testing.B) {
	const batchSize = 256
	dir := b.TempDir()

	mgr, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	defer mgr.Close()
	spanMgr, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager spans: %v", err)
	}
	defer spanMgr.Close()

	bt := NewBatcher(Deps{
		LogManager:  mgr,
		SpanManager: spanMgr,
		LogSparse:   index.NewSparseIndex(),
		SpanSparse:  index.NewSparseIndex(),
		Indexer:     indexer.New(mgr, spanMgr),
		Logger:      discardLogger(),
	}, Config{BatchSize: batchSize, BatchTimeout: time.Second, QueueSize: batchSize})

	// Synthesize IDs via counter encoding instead of MustNewEntryID. ULID
	// generation walks crypto/rand + time.Now and at b.N≈10k×256 entries
	// dominates CPU profile of setup — masking the real processBatch hot
	// path. We just need unique 16-byte IDs; encode the counter in the low
	// 8 bytes.
	ids := make([]model.EntryID, b.N*batchSize)
	for i := range ids {
		binary.BigEndian.PutUint64(ids[i][8:], uint64(i))
	}
	baseTS := time.Now().UnixNano()

	// One backing array of LogEntry values, pointers held by `batch`. The
	// timed loop only mutates ID + Timestamp fields — cheap.
	entries := make([]model.LogEntry, batchSize)
	batch := make([]item, batchSize)
	template := benchLogEntry()
	for j := range entries {
		entries[j] = template
		batch[j] = item{log: &entries[j]}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		base := i * batchSize
		for j := range entries {
			entries[j].ID = ids[base+j]
			entries[j].Timestamp = time.Unix(0, baseTS+int64(base+j))
		}
		bt.processBatch(context.Background(), batch)
	}
	b.ReportMetric(float64(b.N)*float64(batchSize)/b.Elapsed().Seconds(), "entries/sec")
	_ = bytes.NewBuffer(nil)
}
