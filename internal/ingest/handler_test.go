package ingest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

func setupTestHandler(t *testing.T) (*Handler, *storage.SegmentManager, *storage.SegmentManager, func()) {
	t.Helper()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()
	handler := NewHandler(logManager, spanManager, sparse, spanSparse, nil, log)

	cleanup := func() {
		logManager.Close()
		spanManager.Close()
	}
	return handler, logManager, spanManager, cleanup
}

func TestHandler_IngestLog(t *testing.T) {
	handler, logManager, _, cleanup := setupTestHandler(t)
	defer cleanup()

	ctx := context.Background()
	entry := model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: time.Now(),
		Level:     model.LevelError,
		Service:   "test-service",
		Host:      "test-host",
		Body:      "something went wrong",
	}

	if err := handler.IngestLog(ctx, entry); err != nil {
		t.Fatalf("IngestLog: %v", err)
	}

	if logManager.ActiveRecordCount() != 1 {
		t.Errorf("expected 1 record, got %d", logManager.ActiveRecordCount())
	}
}

func TestHandler_IngestSpan(t *testing.T) {
	handler, _, spanManager, cleanup := setupTestHandler(t)
	defer cleanup()

	ctx := context.Background()
	var traceID model.TraceID
	var spanID model.SpanID
	traceID[0] = 0x01
	spanID[0] = 0x02

	span := model.SpanEntry{
		ID:        model.MustNewEntryID(),
		TraceID:   traceID,
		SpanID:    spanID,
		Service:   "test-service",
		Operation: "GET /api/test",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(50 * time.Millisecond),
		Status:    model.SpanStatusOK,
	}

	if err := handler.IngestSpan(ctx, span); err != nil {
		t.Fatalf("IngestSpan: %v", err)
	}

	if spanManager.ActiveRecordCount() != 1 {
		t.Errorf("expected 1 span, got %d", spanManager.ActiveRecordCount())
	}
}

func TestHandler_IngestMultiple(t *testing.T) {
	handler, logManager, _, cleanup := setupTestHandler(t)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Now(),
			Level:     model.LevelInfo,
			Service:   "svc",
			Body:      "msg",
		}
		if err := handler.IngestLog(ctx, entry); err != nil {
			t.Fatalf("IngestLog %d: %v", i, err)
		}
	}

	if logManager.ActiveRecordCount() != 100 {
		t.Errorf("expected 100 records, got %d", logManager.ActiveRecordCount())
	}
}

func TestBatcher_SendAndDrain(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, _ := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	defer logManager.Close()
	defer spanManager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	batcher := NewBatcher(Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: sparse, SpanSparse: spanSparse, Logger: log}, Config{BatchSize: 50, BatchTimeout: 10 * time.Millisecond, QueueSize: 1000})
	batcher.Start(ctx)

	for i := 0; i < 200; i++ {
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Now(),
			Level:     model.LevelInfo,
			Service:   "batcher-test",
			Body:      "batch message",
		}
		if err := batcher.SendLog(entry); err != nil {
			t.Fatalf("SendLog %d: %v", i, err)
		}
	}

	cancel()
	batcher.Wait()

	if logManager.ActiveRecordCount() != 200 {
		t.Errorf("expected 200 records after drain, got %d", logManager.ActiveRecordCount())
	}
}

func TestBatcher_RotationTouchesWrittenSegmentSparseIndex(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.RotationPolicy{MaxRecords: 1, MaxBytes: 128 << 20})
	if err != nil {
		t.Fatalf("open log manager: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open span manager: %v", err)
	}
	defer logManager.Close()
	defer spanManager.Close()

	logSparse := index.NewSparseIndex()
	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: logSparse, SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 1, BatchTimeout: time.Hour, QueueSize: 16},
	)
	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)
	defer func() {
		cancel()
		batcher.Wait()
	}()

	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "rotate"}
	if err := batcher.SendLog(entry); err != nil {
		t.Fatalf("SendLog: %v", err)
	}

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if len(logManager.Segments()) == 1 && len(logSparse.All()) == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for rotation; sealed=%d sparse=%d", len(logManager.Segments()), len(logSparse.All()))
		case <-tick.C:
		}
	}

	sealed := logManager.Segments()[0]
	ranges := logSparse.All()
	if ranges[0].SegmentID != sealed.ID || ranges[0].FileName != sealed.FileName {
		t.Fatalf("sparse range = %+v, want sealed segment id=%d file=%s", ranges[0], sealed.ID, sealed.FileName)
	}
}

func TestBatcher_LogQueueFullDoesNotBlockSpan(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, _ := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	defer logManager.Close()
	defer spanManager.Close()

	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 10, BatchTimeout: time.Second, QueueSize: 1},
	)

	entry1 := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "one"}
	entry2 := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "two"}
	if err := batcher.SendLog(entry1); err != nil {
		t.Fatalf("first SendLog: %v", err)
	}
	if err := batcher.SendLog(entry2); err != ErrQueueFull {
		t.Fatalf("second SendLog error = %v, want ErrQueueFull", err)
	}

	span := model.SpanEntry{
		ID:        model.MustNewEntryID(),
		Service:   "traces",
		Operation: "GET /ok",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Millisecond),
		Status:    model.SpanStatusOK,
	}
	if err := batcher.SendSpan(span); err != nil {
		t.Fatalf("SendSpan after log queue full: %v", err)
	}
	if batcher.LogQueueLen() != 1 {
		t.Fatalf("LogQueueLen = %d, want 1", batcher.LogQueueLen())
	}
	if batcher.SpanQueueLen() != 1 {
		t.Fatalf("SpanQueueLen = %d, want 1", batcher.SpanQueueLen())
	}
}

func TestBatcher_LogBreakerDoesNotBlockSpan(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	logManager, _ := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	defer spanManager.Close()

	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 1, BatchTimeout: time.Hour, QueueSize: 16, BreakerThreshold: 1},
	)

	_ = logManager.Close()
	ctx, cancel := context.WithCancel(context.Background())
	batcher.Start(ctx)
	defer func() {
		cancel()
		batcher.Wait()
	}()

	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "trip"}
	if err := batcher.SendLog(entry); err != nil {
		t.Fatalf("SendLog: %v", err)
	}

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !batcher.IsLogBreakerOpen() {
		select {
		case <-deadline:
			t.Fatal("log breaker did not open")
		case <-tick.C:
		}
	}
	if batcher.IsSpanBreakerOpen() {
		t.Fatal("span breaker opened from log failure")
	}

	span := model.SpanEntry{
		ID:        model.MustNewEntryID(),
		Service:   "traces",
		Operation: "GET /ok",
		StartTime: time.Now(),
		EndTime:   time.Now().Add(time.Millisecond),
		Status:    model.SpanStatusOK,
	}
	if err := batcher.SendSpan(span); err != nil {
		t.Fatalf("SendSpan after log breaker open: %v", err)
	}
}
