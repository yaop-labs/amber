package http

import (
	"log/slog"
	"net/http"
	"runtime"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/storage"
)

type AdminHandler struct {
	manager *storage.SegmentManager
	sparse  *index.SparseIndex
	log     *slog.Logger
}

func NewAdminHandler(manager *storage.SegmentManager, sparse *index.SparseIndex, log *slog.Logger) *AdminHandler {
	return &AdminHandler{manager: manager, sparse: sparse, log: log}
}

func (h *AdminHandler) Stats(w http.ResponseWriter, r *http.Request) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	segments := h.manager.Segments()

	var totalRecords uint64
	var totalBytes int64
	for _, s := range segments {
		totalRecords += s.RecordCount
		totalBytes += s.SizeBytes
	}

	activeMeta, hasActive := h.manager.ActiveSegmentMeta()
	activeInfo := map[string]any{"exists": false}
	if hasActive {
		activeRecords := h.manager.ActiveRecordCount()
		totalRecords += activeRecords
		activeInfo = map[string]any{
			"exists":       true,
			"file":         activeMeta.FileName,
			"id":           activeMeta.ID,
			"record_count": activeRecords,
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"segments": map[string]any{
			"sealed_count":  len(segments),
			"total_records": totalRecords,
			"total_bytes":   totalBytes,
			"total_mb":      totalBytes / 1024 / 1024,
			"active":        activeInfo,
		},
		"sparse_index": map[string]any{
			"segments": h.sparse.Size(),
		},
		"memory": map[string]any{
			"heap_alloc_mb":  memStats.HeapAlloc / 1024 / 1024,
			"heap_inuse_mb":  memStats.HeapInuse / 1024 / 1024,
			"heap_objects":   memStats.HeapObjects,
			"total_alloc_mb": memStats.TotalAlloc / 1024 / 1024,
		},
	})
}

func (h *AdminHandler) Segments(w http.ResponseWriter, r *http.Request) {
	segments := h.manager.Segments()
	writeJSON(w, http.StatusOK, map[string]any{
		"segments": segments,
		"count":    len(segments),
	})
}
