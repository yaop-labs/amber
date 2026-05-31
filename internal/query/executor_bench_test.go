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
// templatedBodies are the handful of recurring log lines used by most
// benchmarks — realistic in that production logs are highly templated.
var templatedBodies = []string{
	"connection refused to postgres:5432 error",
	"timeout waiting for redis response",
	"panic: nil pointer dereference in handler",
	"GET /api/v1/users 200 success latency 45ms",
	"failed to process payment transaction error 500",
	"worker processed job successfully queue empty",
}

func defaultBody(idx int) string { return templatedBodies[idx%len(templatedBodies)] }

func buildSealedDataset(b *testing.B, numSegments, recsPerSeg int) (*Executor, func()) {
	return buildSealedDatasetBodied(b, numSegments, recsPerSeg, defaultBody)
}

func buildSealedDatasetBodied(b *testing.B, numSegments, recsPerSeg int, bodyFn func(idx int) string) (*Executor, func()) {
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
				Body:      bodyFn(idx),
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

// BenchmarkExecLog_FullTextScanFallback_* measure the active-segment full-text
// fallback: buildSealedDataset registers no FTS index, so a FullText query
// takes the per-record TokenizeFTS body match (needScanFTS). Compare ns/op
// against BenchmarkExecLog_Sealed_1seg_* (identical scan, no full text) to read
// the tokenization overhead. This is the worst case — no bitmap pre-filter, so
// every record in the segment is tokenized.
func BenchmarkExecLog_FullTextScanFallback_1seg_10k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 10_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{FullText: "error", Limit: 100})
}

func BenchmarkExecLog_FullTextScanFallback_1seg_100k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 100_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{FullText: "error", Limit: 100})
}

// With a service filter the bitmap pre-filter shrinks the candidate set before
// the body match, so far fewer records are tokenized.
func BenchmarkExecLog_FullTextScanFallback_ServiceFilter_1seg_100k(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 100_000)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{Services: []string{"api-gateway"}, FullText: "error", Limit: 100})
}

// uniqueBody keeps the templated prefix (so the "error" token recurs at the
// same rate) but appends a unique id, mimicking real logs that embed request
// IDs in the message. The per-scan body memo gets no hits here — this is the
// worst case for the memo optimization.
func uniqueBody(idx int) string {
	return fmt.Sprintf("%s req=%d id=%08x", templatedBodies[idx%len(templatedBodies)], idx, idx*2654435761)
}

func BenchmarkExecLog_FullTextScanFallback_UniqueBodies_1seg_100k(b *testing.B) {
	exec, cleanup := buildSealedDatasetBodied(b, 1, 100_000, uniqueBody)
	defer cleanup()
	runExecLogBench(b, exec, &LogQuery{FullText: "error", Limit: 100})
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

// BenchmarkExecLog_LazyDecode_TimeRangeOnly drives a query that has no
// service/host/level filter, so the bitmap pre-filter contributes nothing
// and every record reaches the decode-then-time-check path. Used to size
// the upper bound of a lazy-decode win: the gap between this and
// _Sealed_1seg_100k (which has Limit-bounded scan termination) tells us
// roughly how much decode work goes into records that fail the time
// check. To make the time range cut a substantial fraction of records
// without help from the sparse index, we ask for a slice in the middle
// of the segment's hour.
func BenchmarkExecLog_LazyDecode_TimeRangeOnly(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 100_000)
	defer cleanup()

	// The segment covers exactly 1 hour. Ask for a 30-minute window starting
	// 15 minutes in: half the records pass time-range, half don't, all are
	// currently decoded in full.
	base := time.Now().Add(-time.Hour)
	q := &LogQuery{
		From:  base.Add(15 * time.Minute),
		To:    base.Add(45 * time.Minute),
		Limit: 1_000_000, // disable early termination
	}
	runExecLogBench(b, exec, q)
}

// BenchmarkExecLog_LazyDecode_SelectiveService combines a service filter
// (bitmap pre-filter ~1/5 of records) with a wide time range. Hot path:
// allowedIDs bitmap test → DecodeBytes → matchesAttrs. The wasted work
// is decoding records that pass the bitmap but fail attrs (which here
// is the `env=prod` attr — set on every record, so this should be 0%
// waste in this dataset). The dataset is intentionally homogeneous so
// we can isolate decode cost from filter cost.
func BenchmarkExecLog_LazyDecode_SelectiveService(b *testing.B) {
	exec, cleanup := buildSealedDataset(b, 1, 100_000)
	defer cleanup()
	q := &LogQuery{
		Services: []string{"api-gateway"},
		Limit:    1_000_000,
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
