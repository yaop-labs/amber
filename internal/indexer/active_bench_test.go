package indexer

// ActiveIndex.IndexLogEntries benchmarks. White-box so we can reset the
// internal bitmap between iterations — otherwise the bitmap grows unboundedly
// across b.N iterations and we end up measuring memory pressure on the
// roaring backing store, not the per-batch cost.
//
// Each iteration measures the "first batch into a fresh active segment" path,
// which is what processBatch sees right after a Rotate. Steady-state growth
// of the same bitmap is cheaper (no first-alloc) but harder to bench cleanly
// without time skew from the runaway dataset.

import (
	"fmt"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

func benchEntries(n int) []*model.LogEntry {
	services := []string{"api-gateway", "auth-service", "payment", "worker", "scheduler"}
	levels := []model.Level{model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError}
	now := time.Now()
	entries := make([]*model.LogEntry, n)
	for i := range n {
		entries[i] = &model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: now.Add(time.Duration(i) * time.Microsecond),
			Level:     levels[i%len(levels)],
			Service:   services[i%len(services)],
			Host:      fmt.Sprintf("host-%03d", i%10),
			Body:      "ignored — body not indexed by IndexLogEntries",
		}
	}
	return entries
}

func setupActiveIndex(b *testing.B) *ActiveIndex {
	b.Helper()
	dir := b.TempDir()
	policy := storage.RotationPolicy{MaxRecords: 1_000_000_000, MaxBytes: 64 << 30}
	mgr, err := storage.OpenSegmentManager(dir+"/logs", policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(dir+"/spans", policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager spans: %v", err)
	}
	b.Cleanup(func() { mgr.Close(); spanMgr.Close() })

	// IndexLogEntries calls ensure() which calls mgr.ActiveSegmentMeta() —
	// that returns ok=false until at least one record exists. Write one
	// throwaway record so an active segment exists.
	if err := mgr.WriteBatch([]storage.BatchItem{{Data: []byte("seed"), TS: time.Now().UnixNano()}}); err != nil {
		b.Fatalf("seed WriteBatch: %v", err)
	}
	return New(mgr, spanMgr)
}

func runIndexLogEntries(b *testing.B, batchSize int) {
	a := setupActiveIndex(b)
	entries := benchEntries(batchSize)

	// Prime the slot once so b.N iterations all hit the steady-state path
	// (no first-ensure allocations). Then reset bitmap per iteration to
	// keep memory bounded and measure cold-add cost.
	a.IndexLogEntries(entries)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		a.mu.Lock()
		a.log.bitmap = index.NewMultiFieldIndex()
		a.mu.Unlock()
		b.StartTimer()
		a.IndexLogEntries(entries)
	}
	b.ReportMetric(float64(b.N)*float64(batchSize)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkActiveIndex_IndexLogEntries_100(b *testing.B)   { runIndexLogEntries(b, 100) }
func BenchmarkActiveIndex_IndexLogEntries_1000(b *testing.B)  { runIndexLogEntries(b, 1000) }
func BenchmarkActiveIndex_IndexLogEntries_10000(b *testing.B) { runIndexLogEntries(b, 10_000) }

// BenchmarkActiveIndex_IndexLogEntry_Single is the per-call path used by
// IngestLog (one entry at a time). The processBatch path goes through
// IndexLogEntries; the direct HTTP/OTLP path through IndexLogEntry. Compare
// per-entry cost between the two to see whether bulk grouping pays off.
func BenchmarkActiveIndex_IndexLogEntry_Single(b *testing.B) {
	a := setupActiveIndex(b)
	entry := *benchEntries(1)[0]
	a.IndexLogEntry(entry) // prime

	b.ReportAllocs()
	for b.Loop() {
		a.IndexLogEntry(entry)
	}
}
