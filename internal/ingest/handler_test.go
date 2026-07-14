package ingest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
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

	ctx := context.Background()
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

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := batcher.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}

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
	ctx := context.Background()
	batcher.Start(ctx)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = batcher.Close(closeCtx)
	}()

	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "rotate"}
	if err := batcher.SendLog(entry); err != nil {
		t.Fatalf("SendLog: %v", err)
	}

	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(logManager.Segments()) != 1 || len(logSparse.All()) != 1 {
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
	ctx := context.Background()
	batcher.Start(ctx)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = batcher.Close(closeCtx)
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

func TestBatcherFlushReturnsCoveredWriteError(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer spanManager.Close()

	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 100, BatchTimeout: time.Hour, QueueSize: 16},
	)
	batcher.Start(context.Background())

	// Closing the manager injects a deterministic covered storage failure.
	if err := logManager.Close(); err != nil {
		t.Fatal(err)
	}
	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "must fail"}
	if err := batcher.SendLog(entry); err != nil {
		t.Fatalf("SendLog: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := batcher.Flush(flushCtx); err == nil {
		t.Fatal("Flush returned nil after covered storage failure")
	}
	_ = batcher.Close(flushCtx)
}

func TestBatcherRejectsAdmissionAfterClose(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	logManager, _ := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	defer logManager.Close()
	defer spanManager.Close()

	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 100, BatchTimeout: time.Hour, QueueSize: 16},
	)
	batcher.Start(context.Background())
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := batcher.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "logs", Body: "late"}
	if err := batcher.SendLog(entry); !errors.Is(err, ErrClosed) {
		t.Fatalf("SendLog after Close = %v, want ErrClosed", err)
	}
	if err := batcher.Flush(closeCtx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Flush after Close = %v, want ErrClosed", err)
	}
}

func TestBatcherCloseDrainsEveryConcurrentlyAcceptedEntry(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer logManager.Close()
	defer spanManager.Close()

	batcher := NewBatcher(
		Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Logger: log},
		Config{BatchSize: 32, BatchTimeout: time.Hour, QueueSize: 1024},
	)
	batcher.Start(context.Background())

	var accepted atomic.Uint64
	start := make(chan struct{})
	var senders sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		senders.Add(1)
		go func(worker int) {
			defer senders.Done()
			<-start
			for i := 0; i < 500; i++ {
				entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Unix(0, int64(worker*500+i+1)), Level: model.LevelInfo, Service: "close-race", Body: "accepted must drain"}
				if err := batcher.SendLog(entry); err == nil {
					accepted.Add(1)
				} else if errors.Is(err, ErrClosed) {
					return
				} else if !errors.Is(err, ErrQueueFull) {
					t.Errorf("SendLog: %v", err)
					return
				}
			}
		}(worker)
	}
	close(start)
	deadline := time.Now().Add(2 * time.Second)
	for accepted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := batcher.Close(closeCtx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	senders.Wait()
	if got, want := logManager.ActiveRecordCount(), accepted.Load(); got != want {
		t.Fatalf("records after Close = %d, accepted = %d", got, want)
	}
}
