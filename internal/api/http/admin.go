package http

import (
	"log/slog"
	"net/http"
	"runtime"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/storage"
)

// AdminHandler serves the admin endpoints (stats and segment listing).
type AdminHandler struct {
	manager     *storage.SegmentManager
	sparse      *index.SparseIndex
	spanManager *storage.SegmentManager
	spanSparse  *index.SparseIndex
	batcher     *ingest.Batcher
	log         *slog.Logger
}

// NewAdminHandler builds the admin handler. manager/sparse are the log
// stream's; spanManager/spanSparse are the trace stream's and may be nil,
// in which case Stats omits the "spans_segments" section instead of
// reporting a zeroed manager as if it were real state.
func NewAdminHandler(manager *storage.SegmentManager, sparse *index.SparseIndex, spanManager *storage.SegmentManager, spanSparse *index.SparseIndex, batcher *ingest.Batcher, log *slog.Logger) *AdminHandler {
	return &AdminHandler{manager: manager, sparse: sparse, spanManager: spanManager, spanSparse: spanSparse, batcher: batcher, log: log}
}

// Stats serves runtime, segment, and ingest-queue statistics as JSON. Segment
// totals are authoritative (unlike a query's TotalHits).
func (h *AdminHandler) Stats(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	resp := map[string]any{
		"segments": segmentStats(h.manager),
		"sparse_index": map[string]any{
			"segments": h.sparse.Size(),
		},
		"ingest": h.ingestStats(),
		"memory": map[string]any{
			"heap_alloc_mb":  memStats.HeapAlloc / 1024 / 1024,
			"heap_inuse_mb":  memStats.HeapInuse / 1024 / 1024,
			"heap_objects":   memStats.HeapObjects,
			"total_alloc_mb": memStats.TotalAlloc / 1024 / 1024,
		},
	}

	// spans_segments mirrors segments for the trace stream. It is omitted
	// (rather than emitted zeroed) when no span manager was wired in, so a
	// caller polling this endpoint over time can distinguish "no trace
	// storage on this deployment" from "zero trace segments so far".
	if h.spanManager != nil {
		resp["spans_segments"] = segmentStats(h.spanManager)
	}
	if h.spanSparse != nil {
		resp["spans_sparse_index"] = map[string]any{
			"segments": h.spanSparse.Size(),
		}
	}

	writeJSON(w, http.StatusOK, resp)
}

// segmentStats builds the {sealed_count,total_records,total_bytes,total_mb,
// active} shape shared by the log and span sections of Stats, so the two
// streams' storage growth can be diffed without special-casing which signal
// is being read.
func segmentStats(manager *storage.SegmentManager) map[string]any {
	segments := manager.Segments()

	var totalRecords uint64
	var totalBytes int64
	for _, s := range segments {
		totalRecords += s.RecordCount
		totalBytes += s.SizeBytes
	}

	activeMeta, hasActive := manager.ActiveSegmentMeta()
	activeInfo := map[string]any{"exists": false}
	if hasActive {
		activeRecords := manager.ActiveRecordCount()
		totalRecords += activeRecords
		activeInfo = map[string]any{
			"exists":       true,
			"file":         activeMeta.FileName,
			"id":           activeMeta.ID,
			"record_count": activeRecords,
		}
	}

	return map[string]any{
		"sealed_count":  len(segments),
		"total_records": totalRecords,
		"total_bytes":   totalBytes,
		"total_mb":      totalBytes / 1024 / 1024,
		"active":        activeInfo,
	}
}

func (h *AdminHandler) ingestStats() map[string]any {
	if h.batcher == nil {
		return map[string]any{
			"logs":  map[string]any{"queue_len": 0, "breaker_open": false},
			"spans": map[string]any{"queue_len": 0, "breaker_open": false},
		}
	}
	return map[string]any{
		"logs": map[string]any{
			"queue_len":    h.batcher.LogQueueLen(),
			"breaker_open": h.batcher.IsLogBreakerOpen(),
		},
		"spans": map[string]any{
			"queue_len":    h.batcher.SpanQueueLen(),
			"breaker_open": h.batcher.IsSpanBreakerOpen(),
		},
	}
}

// Segments serves the per-segment metadata list as JSON.
func (h *AdminHandler) Segments(w http.ResponseWriter, r *http.Request) {
	segments := h.manager.Segments()
	writeJSON(w, http.StatusOK, map[string]any{
		"segments": segments,
		"count":    len(segments),
	})
}
