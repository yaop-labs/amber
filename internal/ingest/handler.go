package ingest

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// ActiveIndexer indexes individual entries into the active segment's index.
type ActiveIndexer interface {
	IndexLogEntry(entry model.LogEntry)
	IndexSpanEntry(span model.SpanEntry)
	IndexLogEntries(entries []*model.LogEntry)
	IndexSpanEntries(spans []*model.SpanEntry)
}

// CacheInvalidator drops cached query results affected by newly written entries.
type CacheInvalidator interface {
	// InvalidateResultRange drops cached query results whose window overlaps
	// the written batch's event-time range (unixnano), so steady ingest only
	// evicts results the new records can actually change.
	InvalidateResultRange(from, to int64)
}

// Handler is a synchronous ingest path: each IngestLog/IngestSpan writes one
// entry through to storage and the index before returning. Production uses the
// asynchronous Batcher; Handler exists for the write-path microbenchmarks,
// which measure per-record cost without the batcher's queue and group commit.
type Handler struct {
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	indexer     ActiveIndexer
	log         *slog.Logger
}

// NewHandler builds a synchronous ingest handler. A nil indexer disables
// active-segment indexing.
func NewHandler(
	logManager *storage.SegmentManager,
	spanManager *storage.SegmentManager,
	logSparse *index.SparseIndex,
	spanSparse *index.SparseIndex,
	indexer ActiveIndexer,
	log *slog.Logger,
) *Handler {
	return &Handler{
		logManager:  logManager,
		spanManager: spanManager,
		logSparse:   logSparse,
		spanSparse:  spanSparse,
		indexer:     indexer,
		log:         log,
	}
}

// IngestLog writes one log entry synchronously: serialize, append to the
// segment, touch the sparse index, and index it.
func (h *Handler) IngestLog(_ context.Context, entry model.LogEntry) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if _, err := entry.WriteTo(buf); err != nil {
		return fmt.Errorf("ingest: serialize log: %w", err)
	}

	ts := entry.Timestamp.UnixNano()
	targetMeta, hasTarget := h.logManager.ActiveSegmentMeta()
	if err := h.logManager.Write(buf.Bytes(), ts); err != nil {
		return fmt.Errorf("ingest: write log: %w", err)
	}

	if hasTarget {
		h.logSparse.Touch(targetMeta.ID, targetMeta.FileName, ts)
	}
	if h.indexer != nil && segmentStillActive(h.logManager, targetMeta, hasTarget) {
		h.indexer.IndexLogEntry(entry)
	}
	return nil
}

// IngestSpan writes one span synchronously, mirroring IngestLog.
func (h *Handler) IngestSpan(_ context.Context, span model.SpanEntry) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if _, err := span.WriteTo(buf); err != nil {
		return fmt.Errorf("ingest: serialize span: %w", err)
	}

	ts := span.StartTime.UnixNano()
	targetMeta, hasTarget := h.spanManager.ActiveSegmentMeta()
	if err := h.spanManager.Write(buf.Bytes(), ts); err != nil {
		return fmt.Errorf("ingest: write span: %w", err)
	}

	if hasTarget {
		h.spanSparse.Touch(targetMeta.ID, targetMeta.FileName, ts)
	}
	if h.indexer != nil && segmentStillActive(h.spanManager, targetMeta, hasTarget) {
		h.indexer.IndexSpanEntry(span)
	}
	return nil
}

func segmentStillActive(manager *storage.SegmentManager, meta storage.SegmentMeta, ok bool) bool {
	if !ok {
		return false
	}
	active, activeOK := manager.ActiveSegmentMeta()
	return activeOK && active.ID == meta.ID && active.FileName == meta.FileName
}
