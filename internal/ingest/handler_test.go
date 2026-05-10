package ingest

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/storage"
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
