package query

// Executor benchmarks. White-box so we can clear resultCache between
// iterations — otherwise benchmarks that reuse the same *LogQuery measure
// map+mutex lookup cost on iterations 2..N, not real execution.
//
// The integration benchmarks under /benchmarks all have this skew. Trust the
// numbers there only for the very first call of a query; everything after is
// cached. The dedicated _ResultCacheHit benchmark below quantifies what that
// skew actually buys you.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// buildSealedDataset writes numSegments sealed segments, each containing
// recsPerSeg records spread linearly across one hour of synthetic time, with
// services/levels round-robin'd from the fixed pools. Each segment gets a
// registered bitmap index and a sparse entry. The returned executor is ready
// for ExecLog calls.
//
// Synthetic time placement: segment k covers [base + k*hour, base + (k+1)*hour)
// so time-range pruning benchmarks can target specific windows.
func buildSealedDataset(b *testing.B, numSegments, recsPerSeg int) (*Executor, func()) {
	b.Helper()
	dir := b.TempDir()
	logDir := dir + "/logs"
	spanDir := dir + "/spans"

	// MaxRecords / MaxBytes set high so we control rotation manually via
	// Rotate(). Rotation policy thresholds would conflict with the explicit
	// numSegments × recsPerSeg layout.
	policy := storage.RotationPolicy{MaxRecords: 1_000_000_000, MaxBytes: 64 << 30}
	mgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager logs: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager spans: %v", err)
	}

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	services := []string{"api-gateway", "auth-service", "payment", "worker", "scheduler"}
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	bodies := []string{
		"connection refused to postgres:5432 error",
		"timeout waiting for redis response",
		"panic: nil pointer dereference in handler",
		"GET /api/v1/users 200 success latency 45ms",
		"failed to process payment transaction error 500",
		"worker processed job successfully queue empty",
	}

	base := time.Now().Add(-time.Duration(numSegments) * time.Hour).UnixNano()
	hourNs := int64(time.Hour)
	step := hourNs / int64(recsPerSeg)
	if step == 0 {
		step = 1
	}

	buf := &bytes.Buffer{}
	for seg := range numSegments {
		segStart := base + int64(seg)*hourNs
		batch := make([]storage.BatchItem, 0, recsPerSeg)
		for i := range recsPerSeg {
			idx := seg*recsPerSeg + i
			lvl, _ := model.LevelFromString(levels[idx%len(levels)])
			entry := model.LogEntry{
				ID:        model.MustNewEntryID(),
				Timestamp: time.Unix(0, segStart+int64(i)*step),
				Level:     lvl,
				Service:   services[idx%len(services)],
				Host:      fmt.Sprintf("host-%03d", idx%10),
				Body:      bodies[idx%len(bodies)],
				Attrs:     []model.Attr{{Key: "env", Value: "prod"}},
			}
			buf.Reset()
			entry.WriteTo(buf)
			data := make([]byte, buf.Len())
			copy(data, buf.Bytes())
			batch = append(batch, storage.BatchItem{Data: data, TS: entry.Timestamp.UnixNano()})
		}
		if err := mgr.WriteBatch(batch); err != nil {
			b.Fatalf("WriteBatch seg %d: %v", seg, err)
		}
		if active, ok := mgr.ActiveSegmentMeta(); ok {
			sparse.TouchRange(active.ID, active.FileName, segStart, segStart+hourNs-1)
		}
		if err := mgr.Rotate(); err != nil {
			b.Fatalf("Rotate seg %d: %v", seg, err)
		}
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	exec := NewExecutor(mgr, spanMgr, sparse, spanSparse)
	for _, seg := range mgr.Segments() {
		segPath := logDir + "/" + seg.FileName
		if idx, err := index.BuildLogBitmapIndex(segPath, log); err == nil {
			exec.RegisterBitmapIndex(seg.FileName, idx)
		}
	}

	cleanup := func() { mgr.Close(); spanMgr.Close() }
	return exec, cleanup
}

// runExecLogBench drives ExecLog b.N times with cache cleared between calls
// so each iteration pays the full plan + scan cost. Reports ns/op + queries/sec.
func runExecLogBench(b *testing.B, exec *Executor, q *LogQuery) {
	ctx := context.Background()
	// Warm any one-shot init (reader cache miss on first segment etc.) so
	// the first measured iteration is not an outlier.
	if _, err := exec.ExecLog(ctx, q); err != nil {
		b.Fatalf("warmup: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exec.resultCache.clear()
		if _, err := exec.ExecLog(ctx, q); err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkExecLog_Sealed_1seg_10k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 10_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Limit: 100})
}

func BenchmarkExecLog_Sealed_1seg_100k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 100_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Limit: 100})
}

func BenchmarkExecLog_Sealed_10seg_10k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 10, 10_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Limit: 100})
}

func BenchmarkExecLog_Sealed_100seg_1k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 100, 1_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Limit: 100})
}

// BenchmarkExecLog_ServiceFilter_Bitmap_10seg_10k measures the bitmap
// fast-path: planner emits StepBitmapFilter, we hit a pre-built per-segment
// MultiFieldIndex. Compared to _Sealed_10seg_10k this isolates how much
// filtered scans save versus full scan + post-filter.
func BenchmarkExecLog_ServiceFilter_Bitmap_10seg_10k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 10, 10_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Services: []string{"api-gateway"}, Limit: 100})
}

// BenchmarkExecLog_TimeRangePruning_100seg targets only 2 of 100 segments via
// q.From/q.To. Number to watch: SegScanned in the LogResult — should be ~2,
// not 100. If sparse pruning regresses this benchmark times out or the
// queries/sec drops by ~50x.
func BenchmarkExecLog_TimeRangePruning_100seg(b *testing.B) {
	const numSeg = 100
	exec, cleanup := buildSealedDataset(b, numSeg, 1_000)
	defer cleanup()

	base := time.Now().Add(-time.Duration(numSeg) * time.Hour)
	q := &LogQuery{
		From:  base.Add(50 * time.Hour),
		To:    base.Add(52 * time.Hour),
		Limit: 100,
	}
	runExecLogBench(b, exec, q)
}

// BenchmarkExecLog_ResultCacheHit measures the warm path that the existing
// /benchmarks suite accidentally measures everywhere: identical query, cache
// stays populated. The delta versus _Sealed_10seg_10k is the value of the
// cache. Keep this benchmark — if cache lookup ever gets more expensive than
// a small scan, that's a signal to revisit the cache design.
func BenchmarkExecLog_ResultCacheHit(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 10, 10_000)
	defer cleanup()

	ctx := context.Background()
	q := &LogQuery{Limit: 100}
	if _, err := exec.ExecLog(ctx, q); err != nil {
		b.Fatalf("prime: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		if !r.CacheHit {
			b.Fatal("expected cache hit on every iteration")
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}
