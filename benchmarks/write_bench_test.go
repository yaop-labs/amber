package benchmarks

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/storage"
)

func setupBenchStore(b *testing.B) (*ingest.Handler, func()) {
	b.Helper()
	dir := b.TempDir()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	handler := ingest.NewHandler(logManager, spanManager, sparse, spanSparse, nil, log)

	cleanup := func() {
		logManager.Close()
		spanManager.Close()
	}

	return handler, cleanup
}

func BenchmarkIngestLog_Small(b *testing.B) {
	handler, cleanup := setupBenchStore(b)
	defer cleanup()

	ctx := context.Background()
	entry := makeLogEntry("api-gateway", "ERROR", "connection refused")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.ID = model.MustNewEntryID()
		entry.Timestamp = time.Now()
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("IngestLog: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkIngestLog_Medium(b *testing.B) {
	handler, cleanup := setupBenchStore(b)
	defer cleanup()

	ctx := context.Background()
	body := makeBody(500)
	entry := makeLogEntry("api-gateway", "ERROR", body)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.ID = model.MustNewEntryID()
		entry.Timestamp = time.Now()
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("IngestLog: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkIngestLog_Large(b *testing.B) {
	handler, cleanup := setupBenchStore(b)
	defer cleanup()

	ctx := context.Background()
	body := makeBody(5000)
	entry := makeLogEntry("api-gateway", "ERROR", body)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry.ID = model.MustNewEntryID()
		entry.Timestamp = time.Now()
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("IngestLog: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkIngestLog_Parallel(b *testing.B) {
	dir := b.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	ctx, cancel := context.WithCancel(context.Background())
	batcher := ingest.NewBatcher(ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: sparse, SpanSparse: spanSparse, Logger: log}, ingest.Config{BatchSize: 500, BatchTimeout: 50 * time.Millisecond, QueueSize: 10000})
	batcher.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	lvl, _ := model.LevelFromString("INFO")
	template := model.LogEntry{
		Timestamp: time.Now(),
		Level:     lvl,
		Service:   "worker",
		Host:      "bench-host-001",
		Body:      "parallel ingest benchmark record",
		Attrs: []model.Attr{
			{Key: "env", Value: "bench"},
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		entry := template
		for pb.Next() {
			entry.ID = model.MustNewEntryID()
			batcher.SendLog(entry)
		}
	})

	b.StopTimer()

	cancel()
	batcher.Wait()

	logManager.Close()
	spanManager.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func setupBenchStoreIndexed(b *testing.B) (*ingest.Handler, func()) {
	b.Helper()
	dir := b.TempDir()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	exec := query.NewExecutor(logManager, spanManager, sparse, spanSparse)
	handler := ingest.NewHandler(logManager, spanManager, sparse, spanSparse, exec, log)

	cleanup := func() {
		logManager.Close()
		spanManager.Close()
	}

	return handler, cleanup
}

func BenchmarkIngestLog_Indexed(b *testing.B) {
	handler, cleanup := setupBenchStoreIndexed(b)
	defer cleanup()

	ctx := context.Background()
	lvl, _ := model.LevelFromString("INFO")
	template := model.LogEntry{
		Level:   lvl,
		Service: "api-gateway",
		Host:    "bench-host-001",
		Body:    "connection refused to postgres timeout",
		Attrs: []model.Attr{
			{Key: "env", Value: "bench"},
			{Key: "version", Value: "1.0.0"},
		},
	}
	var traceID model.TraceID
	for i := range traceID {
		traceID[i] = byte(i + 1)
	}
	template.TraceID = traceID

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := template
		entry.ID = model.MustNewEntryID()
		entry.Timestamp = time.Now()
		if err := handler.IngestLog(ctx, entry); err != nil {
			b.Fatalf("IngestLog: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkIngestLog_Parallel_Indexed(b *testing.B) {
	dir := b.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	exec := query.NewExecutor(logManager, spanManager, sparse, spanSparse)

	ctx, cancel := context.WithCancel(context.Background())
	batcher := ingest.NewBatcher(ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: sparse, SpanSparse: spanSparse, Indexer: exec, Logger: log}, ingest.Config{BatchSize: 500, BatchTimeout: 50 * time.Millisecond, QueueSize: 10000})
	batcher.Start(ctx)

	b.ResetTimer()
	b.ReportAllocs()

	lvl, _ := model.LevelFromString("INFO")
	var traceID model.TraceID
	for i := range traceID {
		traceID[i] = byte(i + 1)
	}
	template := model.LogEntry{
		Timestamp: time.Now(),
		Level:     lvl,
		Service:   "worker",
		Host:      "bench-host-001",
		Body:      "parallel ingest benchmark record",
		TraceID:   traceID,
		Attrs: []model.Attr{
			{Key: "env", Value: "bench"},
		},
	}

	b.RunParallel(func(pb *testing.PB) {
		entry := template
		for pb.Next() {
			entry.ID = model.MustNewEntryID()
			batcher.SendLog(entry)
		}
	})

	b.StopTimer()

	cancel()
	batcher.Wait()

	logManager.Close()
	spanManager.Close()

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
}

func BenchmarkSegmentWriter_Raw(b *testing.B) {
	path := b.TempDir() + "/bench.alog"
	sw, err := storage.OpenSegmentWriter(path)
	if err != nil {
		b.Fatalf("OpenSegmentWriter: %v", err)
	}
	defer sw.Close()

	data := []byte(makeBody(200))
	ts := time.Now().UnixNano()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := sw.WriteRecord(data, ts); err != nil {
			b.Fatalf("WriteRecord: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkWAL_Write(b *testing.B) {
	wal, err := storage.OpenWAL(b.TempDir())
	if err != nil {
		b.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	payload := []byte(makeBody(200))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if _, err := wal.Write(payload); err != nil {
			b.Fatalf("WAL.Write: %v", err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "writes/sec")
}

func BenchmarkEntryIDToUint64(b *testing.B) {
	id := model.MustNewEntryID()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = model.EntryIDToUint64(id)
	}
}

func BenchmarkLogEntry_Serialize(b *testing.B) {
	entry := makeLogEntry("api-gateway", "ERROR", makeBody(200))
	buf := make([]byte, 0, 512)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf = buf[:0]
		w := bytesWriter{buf: &buf}
		entry.WriteTo(&w)
	}
}

func makeLogEntry(service, level, body string) model.LogEntry {
	lvl, _ := model.LevelFromString(level)
	return model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: time.Now(),
		Level:     lvl,
		Service:   service,
		Host:      "bench-host-001",
		Body:      body,
		Attrs: []model.Attr{
			{Key: "env", Value: "bench"},
			{Key: "version", Value: "1.0.0"},
		},
	}
}

func makeBody(size int) string {
	tokens := []string{
		"connection", "refused", "timeout", "error", "database",
		"postgres", "redis", "kafka", "request", "response",
		"handler", "middleware", "panic", "nil", "pointer",
	}
	result := make([]byte, 0, size)
	i := 0
	for len(result) < size {
		token := tokens[i%len(tokens)]
		result = append(result, token...)
		result = append(result, ' ')
		i++
	}
	return string(result[:size])
}

type bytesWriter struct{ buf *[]byte }

func (w *bytesWriter) Write(p []byte) (int, error) {
	*w.buf = append(*w.buf, p...)
	return len(p), nil
}

var _ = fmt.Sprintf // prevent unused import
