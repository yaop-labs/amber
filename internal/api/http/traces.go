package http

import (
	"encoding/hex"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/yaop-labs/amber/internal/query"
)

// TracesHandler serves the trace search endpoint, filtering by service,
// operation, time range, and duration.
type TracesHandler struct {
	exec *query.Executor
	log  *slog.Logger
}

// NewTracesHandler builds the trace search handler.
func NewTracesHandler(exec *query.Executor, log *slog.Logger) *TracesHandler {
	return &TracesHandler{exec: exec, log: log}
}

type traceSummary struct {
	TraceID    string    `json:"trace_id"`
	Service    string    `json:"service"`
	Operation  string    `json:"operation"`
	StartTime  time.Time `json:"start_time"`
	DurationMs int64     `json:"duration_ms"`
	SpanCount  int       `json:"span_count"`
	HasErrors  bool      `json:"has_errors"`
}

const traceSummaryMaxSpans = 100_000

func (h *TracesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	sq := query.SpanQuery{}

	if v := q.Get("service"); v != "" {
		sq.Services = splitComma(v)
	}
	if v := q.Get("operation"); v != "" {
		sq.Operations = splitComma(v)
	}
	if v := q.Get("min_duration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'min_duration': "+err.Error())
			return
		}
		sq.MinDuration = d
	}
	if v := q.Get("max_duration"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'max_duration': "+err.Error())
			return
		}
		sq.MaxDuration = d
	}
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'from': "+err.Error())
			return
		}
		sq.From = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid 'to': "+err.Error())
			return
		}
		sq.To = t
	}

	limit := 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = n
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 1_000_000 {
			writeError(w, http.StatusBadRequest, "offset must be between 0 and 1000000")
			return
		}
		offset = n
	}
	if offset > 1_000_000-limit {
		writeError(w, http.StatusBadRequest, "offset plus limit is too large")
		return
	}

	// Covering .cidx projections answer service-filtered summaries without
	// decompressing row blocks: SpanTraceSummaries folds covering rows straight
	// into per-trace rollups newest-first and stops once it has the requested
	// page, falling back to the row scan for segments without a .cidx.
	rollups, total, truncated, err := h.exec.SpanTraceSummaries(r.Context(), &sq, offset+limit, traceSummaryMaxSpans)
	if err != nil {
		h.log.Error("traces list query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	var summaries []traceSummary
	if offset < len(rollups) {
		page := rollups[offset:]
		if len(page) > limit {
			page = page[:limit]
		}
		summaries = make([]traceSummary, len(page))
		for i, s := range page {
			summaries[i] = traceSummary{
				TraceID:    hex.EncodeToString(s.TraceID[:]),
				Service:    s.Service,
				Operation:  s.Operation,
				StartTime:  s.Start,
				DurationMs: s.End.Sub(s.Start).Milliseconds(),
				SpanCount:  s.SpanCount,
				HasErrors:  s.HasErrors,
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"traces":    summaries,
		"total":     total,
		"truncated": truncated,
	})
}
