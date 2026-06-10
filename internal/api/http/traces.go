package http

import (
	"encoding/hex"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
)

type TracesHandler struct {
	exec *query.Executor
	log  *slog.Logger
}

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

const traceSummaryPageSize = 2000
const traceSummaryMaxSpans = 100_000

func (h *TracesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	sq := query.SpanQuery{}

	if v := q.Get("service"); v != "" {
		sq.Services = splitComma(v)
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
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	summaries, total, truncated, err := collectTraceSummaries(
		func(q *query.SpanQuery) (*query.SpanResult, error) {
			return h.exec.ExecSpan(r.Context(), q)
		},
		sq,
	)
	if err != nil {
		h.log.Error("traces list query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}
	if offset >= total {
		summaries = nil
	} else {
		summaries = summaries[offset:]
		if len(summaries) > limit {
			summaries = summaries[:limit]
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"traces":    summaries,
		"total":     total,
		"truncated": truncated,
	})
}

func collectTraceSummaries(
	fetch func(*query.SpanQuery) (*query.SpanResult, error),
	base query.SpanQuery,
) ([]traceSummary, int, bool, error) {
	page := base
	page.Limit = traceSummaryPageSize
	page.Cursor = ""

	var allSpans []model.SpanEntry
	truncated := false
	for {
		result, err := fetch(&page)
		if err != nil {
			return nil, 0, false, err
		}
		remaining := traceSummaryMaxSpans - len(allSpans)
		if remaining <= 0 {
			truncated = true
			break
		}
		if len(result.Spans) > remaining {
			allSpans = append(allSpans, result.Spans[:remaining]...)
			truncated = true
			break
		}
		allSpans = append(allSpans, result.Spans...)

		if result.NextCursor == "" || len(result.Spans) == 0 {
			break
		}
		page.Cursor = result.NextCursor
	}

	summaries := buildTraceSummaries(allSpans)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].StartTime.After(summaries[j].StartTime)
	})
	return summaries, len(summaries), truncated, nil
}

func buildTraceSummaries(spans []model.SpanEntry) []traceSummary {
	type traceGroup struct {
		root      *model.SpanEntry
		start     time.Time
		end       time.Time
		spanCount int
		hasErrors bool
	}

	groups := make(map[model.TraceID]*traceGroup)

	for i := range spans {
		sp := &spans[i]
		g, ok := groups[sp.TraceID]
		if !ok {
			g = &traceGroup{start: sp.StartTime, end: sp.EndTime}
			groups[sp.TraceID] = g
		}
		g.spanCount++
		if sp.StartTime.Before(g.start) {
			g.start = sp.StartTime
		}
		if sp.EndTime.After(g.end) {
			g.end = sp.EndTime
		}
		if sp.Status == model.SpanStatusError {
			g.hasErrors = true
		}
		if sp.IsRoot() || g.root == nil {
			g.root = sp
		}
	}

	summaries := make([]traceSummary, 0, len(groups))
	for traceID, g := range groups {
		service := ""
		operation := ""
		if g.root != nil {
			service = g.root.Service
			operation = g.root.Operation
		}
		summaries = append(summaries, traceSummary{
			TraceID:    hex.EncodeToString(traceID[:]),
			Service:    service,
			Operation:  operation,
			StartTime:  g.start,
			DurationMs: g.end.Sub(g.start).Milliseconds(),
			SpanCount:  g.spanCount,
			HasErrors:  g.hasErrors,
		})
	}
	return summaries
}
