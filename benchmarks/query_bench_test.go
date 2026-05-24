package benchmarks

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
)

func setupBenchQueryBatched(b *testing.B, n int) (*query.Executor, *index.SparseIndex, *storage.SegmentManager, func()) {
	b.Helper()
	dir := b.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logDir := dir + "/logs"
	spanDir := dir + "/spans"
	policy := storage.RotationPolicy{MaxRecords: 10_000_000, MaxBytes: 4 << 30}
	manager, _ := storage.OpenSegmentManager(logDir, policy)
	spanManager, _ := storage.OpenSegmentManager(spanDir, policy)
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

	base := time.Now().Add(-24 * time.Hour).UnixNano()
	step := int64(24*time.Hour) / int64(n)

	const batchSize = 10_000
	batch := make([]storage.BatchItem, 0, batchSize)
	buf := &bytes.Buffer{}

	for i := 0; i < n; i++ {
		lvl, _ := model.LevelFromString(levels[i%len(levels)])
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Unix(0, base+int64(i)*step),
			Level:     lvl,
			Service:   services[i%len(services)],
			Host:      fmt.Sprintf("host-%03d", i%10),
			Body:      bodies[i%len(bodies)],
			Attrs:     []model.Attr{{Key: "env", Value: "prod"}},
		}
		buf.Reset()
		entry.WriteTo(buf)
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		batch = append(batch, storage.BatchItem{Data: data, TS: entry.Timestamp.UnixNano()})

		if len(batch) >= batchSize {
			if err := manager.WriteBatch(batch); err != nil {
				b.Fatalf("WriteBatch: %v", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := manager.WriteBatch(batch); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}

	if active, ok := manager.ActiveSegmentMeta(); ok {
		sparse.TouchRange(active.ID, active.FileName, base, base+int64(n)*step)
	}

	if err := manager.Rotate(); err != nil {
		b.Fatalf("setup Rotate: %v", err)
	}

	exec := query.NewExecutor(manager, spanManager, sparse, spanSparse)
	for _, seg := range manager.Segments() {
		segPath := logDir + "/" + seg.FileName
		if idx, err := index.BuildLogBitmapIndex(segPath, log); err == nil {
			exec.RegisterBitmapIndex(seg.FileName, idx)
		}
	}

	cleanup := func() { manager.Close(); spanManager.Close() }
	return exec, sparse, manager, cleanup
}

func setupBenchQuery(b *testing.B, n int) (*query.Executor, *index.SparseIndex, *storage.SegmentManager, func()) {
	b.Helper()
	dir := b.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logDir := dir + "/logs"
	spanDir := dir + "/spans"
	manager, _ := storage.OpenSegmentManager(logDir, storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(spanDir, storage.DefaultRotationPolicy)
	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	handler := ingest.NewHandler(manager, spanManager, sparse, spanSparse, nil, log)
	ctx := context.Background()

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

	base := time.Now().Add(-24 * time.Hour).UnixNano()
	step := int64(24*time.Hour) / int64(n)

	for i := 0; i < n; i++ {
		lvl, _ := model.LevelFromString(levels[i%len(levels)])
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Unix(0, base+int64(i)*step),
			Level:     lvl,
			Service:   services[i%len(services)],
			Host:      fmt.Sprintf("host-%03d", i%10),
			Body:      bodies[i%len(bodies)],
			Attrs:     []model.Attr{{Key: "env", Value: "prod"}},
		}
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("setup IngestLog: %v", err)
		}
	}

	if err := manager.Rotate(); err != nil {
		b.Fatalf("setup Rotate: %v", err)
	}

	exec := query.NewExecutor(manager, spanManager, sparse, spanSparse)
	for _, seg := range manager.Segments() {
		segPath := logDir + "/" + seg.FileName
		if idx, err := index.BuildLogBitmapIndex(segPath, log); err == nil {
			exec.RegisterBitmapIndex(seg.FileName, idx)
		}
	}

	cleanup := func() { manager.Close(); spanManager.Close() }
	return exec, sparse, manager, cleanup
}

func setupBenchQueryActive(b *testing.B, n int) (*query.Executor, func()) {
	b.Helper()
	dir := b.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logDir := dir + "/logs"
	spanDir := dir + "/spans"
	manager, _ := storage.OpenSegmentManager(logDir, storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(spanDir, storage.DefaultRotationPolicy)
	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	handler := ingest.NewHandler(manager, spanManager, sparse, spanSparse, nil, log)
	ctx := context.Background()

	services := []string{"api-gateway", "auth-service", "payment", "worker", "scheduler"}
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

	base := time.Now().Add(-1 * time.Hour).UnixNano()
	step := int64(time.Hour) / int64(n)

	for i := 0; i < n; i++ {
		lvl, _ := model.LevelFromString(levels[i%len(levels)])
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Unix(0, base+int64(i)*step),
			Level:     lvl,
			Service:   services[i%len(services)],
			Host:      fmt.Sprintf("host-%03d", i%10),
			Body:      fmt.Sprintf("request #%d processed", i),
			Attrs:     []model.Attr{{Key: "env", Value: "prod"}},
		}
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("setup IngestLog: %v", err)
		}
	}

	if err := manager.Flush(); err != nil {
		b.Fatalf("setup Flush: %v", err)
	}

	exec := query.NewExecutor(manager, spanManager, sparse, spanSparse)

	cleanup := func() { manager.Close(); spanManager.Close() }
	return exec, cleanup
}

func BenchmarkQuery_FullScan_1k(b *testing.B) {
	benchQueryFullScan(b, 1_000)
}

func BenchmarkQuery_FullScan_10k(b *testing.B) {
	benchQueryFullScan(b, 10_000)
}

func BenchmarkQuery_FullScan_100k(b *testing.B) {
	benchQueryFullScanBatched(b, 100_000)
}

func BenchmarkQuery_FullScan_1M(b *testing.B) {
	benchQueryFullScanBatched(b, 1_000_000)
}

func benchQueryFullScanBatched(b *testing.B, n int) {
	exec, _, _, cleanup := setupBenchQueryBatched(b, n)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{Limit: 100}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		if _, err := exec.ExecLog(ctx, q); err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_ServiceFilter_Bitmap_100k(b *testing.B) {
	benchQueryServiceFilterBatched(b, 100_000)
}

func BenchmarkQuery_ServiceFilter_Bitmap_1M(b *testing.B) {
	benchQueryServiceFilterBatched(b, 1_000_000)
}

func benchQueryServiceFilterBatched(b *testing.B, n int) {
	exec, _, _, cleanup := setupBenchQueryBatched(b, n)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{Services: []string{"api-gateway"}, Limit: 100}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		if _, err := exec.ExecLog(ctx, q); err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkFTSIndex_Search_100k(b *testing.B) {
	benchFTSSearch(b, 100_000)
}

func BenchmarkFTSIndex_Search_1M(b *testing.B) {
	benchFTSSearch(b, 1_000_000)
}

func benchQueryFullScan(b *testing.B, n int) {
	exec, _, _, cleanup := setupBenchQuery(b, n)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{Limit: 100}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_TimeRange_1k(b *testing.B) {
	exec, _, _, cleanup := setupBenchQuery(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		From:  time.Now().Add(-12 * time.Hour),
		To:    time.Now(),
		Limit: 100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_ServiceFilter_Bitmap_1k(b *testing.B) {
	exec, _, _, cleanup := setupBenchQuery(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		Services: []string{"api-gateway"},
		Limit:    100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		if len(result.Entries) == 0 {
			b.Fatal("expected results for service filter")
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_LevelFilter_Bitmap_1k(b *testing.B) {
	exec, _, _, cleanup := setupBenchQuery(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		Levels: []string{"ERROR"},
		Limit:  100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		if len(result.Entries) == 0 {
			b.Fatal("expected results for level filter")
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_ServiceAndLevel_Bitmap_1k(b *testing.B) {
	exec, _, _, cleanup := setupBenchQuery(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		Services: []string{"api-gateway"},
		Levels:   []string{"ERROR"},
		Limit:    100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_ServiceFilter_PostFilter_1k(b *testing.B) {
	exec, cleanup := setupBenchQueryActive(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		Services: []string{"api-gateway"},
		Limit:    100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		if len(result.Entries) == 0 {
			b.Fatal("expected results for service post-filter")
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_LevelFilter_PostFilter_1k(b *testing.B) {
	exec, cleanup := setupBenchQueryActive(b, 1_000)
	defer cleanup()

	ctx := context.Background()
	q := &query.LogQuery{
		Levels: []string{"ERROR"},
		Limit:  100,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		exec.ClearResultCache()
		result, err := exec.ExecLog(ctx, q)
		if err != nil {
			b.Fatalf("ExecLog: %v", err)
		}
		if len(result.Entries) == 0 {
			b.Fatal("expected results for level post-filter")
		}
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

func BenchmarkQuery_SegmentPruning(b *testing.B) {
	sparse := index.NewSparseIndex()

	base := time.Now().Add(-100 * time.Hour).UnixNano()
	hour := int64(time.Hour)
	for i := uint32(1); i <= 100; i++ {
		sparse.Add(index.SegmentTimeRange{
			SegmentID: i,
			FileName:  fmt.Sprintf("seg_%08d.alog", i),
			MinTS:     base + int64(i-1)*hour,
			MaxTS:     base + int64(i)*hour,
		})
	}

	from := time.Now().Add(-2 * time.Hour).UnixNano()
	to := time.Now().UnixNano()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := sparse.Lookup(from, to)
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "lookups/sec")
}

func BenchmarkFTSIndex_Search_1k(b *testing.B) {
	benchFTSSearch(b, 1_000)
}

func BenchmarkFTSIndex_Search_10k(b *testing.B) {
	benchFTSSearch(b, 10_000)
}

func benchFTSSearch(b *testing.B, n int) {
	ctx := context.Background()
	fts := index.NewFTSIndex()

	bodies := []string{
		"connection refused to postgres database error",
		"timeout waiting for redis response latency",
		"panic nil pointer dereference runtime error",
		"payment transaction failed insufficient funds",
		"worker job processed queue empty scheduler",
	}

	for i := 0; i < n; i++ {
		id := model.MustNewEntryID()
		body := bodies[i%len(bodies)]
		fts.Index(ctx, model.EntryIDToUint64(id), body)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		ids, err := fts.Search(ctx, "connection refused", 100)
		if err != nil {
			b.Fatalf("Search: %v", err)
		}
		_ = ids
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "searches/sec")
}

func BenchmarkBitmapFilter_1k(b *testing.B) {
	benchBitmapFilter(b, 1_000)
}

func BenchmarkBitmapFilter_100k(b *testing.B) {
	benchBitmapFilter(b, 100_000)
}

func benchBitmapFilter(b *testing.B, n int) {
	m := index.NewMultiFieldIndex()

	levels := []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
	services := []string{"api", "worker", "scheduler", "payment", "auth"}

	for i := 0; i < n; i++ {
		id := uint64(i)
		m.Add("level", levels[i%len(levels)], id)
		m.Add("service", services[i%len(services)], id)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result := m.Filter(map[string]string{
			"level":   "ERROR",
			"service": "api",
		})
		_ = result
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "filters/sec")
}

func BenchmarkSegmentReader_Scan(b *testing.B) {
	path := b.TempDir() + "/bench.alog"
	sw, _ := storage.OpenSegmentWriter(path)

	entry := makeLogEntry("api", "INFO", makeBody(200))
	data := serializeEntry(entry)
	ts := time.Now().UnixNano()

	const records = 10_000
	for i := 0; i < records; i++ {
		sw.WriteRecord(data, ts+int64(i))
	}
	sw.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		sr, _ := storage.OpenSegmentReader(path, nil)
		count := 0
		sr.Scan(func([]byte) error {
			count++
			return nil
		})
		sr.Close()
	}

	b.ReportMetric(float64(records)*float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}

func serializeEntry(entry model.LogEntry) []byte {
	var buf bytes.Buffer
	entry.WriteTo(&buf)
	return buf.Bytes()
}
