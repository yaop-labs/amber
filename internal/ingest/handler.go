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

type ActiveIndexer interface {
	IndexLogEntry(entry model.LogEntry)
	IndexSpanEntry(span model.SpanEntry)
	IndexLogEntries(entries []*model.LogEntry)
	IndexSpanEntries(spans []*model.SpanEntry)
}

type Handler struct {
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	indexer     ActiveIndexer
	log         *slog.Logger
}

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

func (h *Handler) IngestLog(_ context.Context, entry model.LogEntry) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if _, err := entry.WriteTo(buf); err != nil {
		return fmt.Errorf("ingest: serialize log: %w", err)
	}

	ts := entry.Timestamp.UnixNano()
	if err := h.logManager.Write(buf.Bytes(), ts); err != nil {
		return fmt.Errorf("ingest: write log: %w", err)
	}

	h.updateSparse(h.logSparse, h.logManager, ts)
	if h.indexer != nil {
		h.indexer.IndexLogEntry(entry)
	}
	return nil
}

func (h *Handler) IngestSpan(_ context.Context, span model.SpanEntry) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if _, err := span.WriteTo(buf); err != nil {
		return fmt.Errorf("ingest: serialize span: %w", err)
	}

	ts := span.StartTime.UnixNano()
	if err := h.spanManager.Write(buf.Bytes(), ts); err != nil {
		return fmt.Errorf("ingest: write span: %w", err)
	}

	h.updateSparse(h.spanSparse, h.spanManager, ts)
	if h.indexer != nil {
		h.indexer.IndexSpanEntry(span)
	}
	return nil
}

func (h *Handler) updateSparse(sparse *index.SparseIndex, manager *storage.SegmentManager, ts int64) {
	activeMeta, ok := manager.ActiveSegmentMeta()
	if !ok {
		return
	}
	sparse.Touch(activeMeta.ID, activeMeta.FileName, ts)
}
