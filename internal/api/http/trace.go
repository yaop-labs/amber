package http

import (
	"bytes"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	json "github.com/goccy/go-json"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
)

type TraceHandler struct {
	exec  *query.Executor
	log   *slog.Logger
	cache *httpCache
}

func NewTraceHandler(exec *query.Executor, log *slog.Logger) *TraceHandler {
	return &TraceHandler{
		exec:  exec,
		log:   log,
		cache: newHTTPCache(256, 5*time.Second),
	}
}

type spanNode struct {
	Span     model.SpanEntry  `json:"span"`
	Logs     []model.LogEntry `json:"logs"`
	Children []*spanNode      `json:"children"`
}

func (h *TraceHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	traceIDStr := strings.TrimPrefix(path, "/api/v1/traces/")
	if traceIDStr == "" {
		writeError(w, http.StatusBadRequest, "missing trace_id")
		return
	}

	if body, ok := h.cache.get(traceIDStr); ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(body)
		return
	}

	traceIDBytes, err := hex.DecodeString(traceIDStr)
	if err != nil || len(traceIDBytes) != 16 {
		writeError(w, http.StatusBadRequest, "invalid trace_id: must be 32 hex chars")
		return
	}

	var traceID model.TraceID
	copy(traceID[:], traceIDBytes)

	start := time.Now()

	q := &query.SpanQuery{
		TraceID: traceID,
		Limit:   10_000,
	}

	result, err := h.exec.ExecSpan(r.Context(), q)
	if err != nil {
		h.log.Error("trace query failed", "trace_id", traceIDStr, "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	lq := &query.LogQuery{
		TraceID: traceID,
		Limit:   100_000,
	}

	var logs []model.LogEntry
	logResult, err := h.exec.ExecLog(r.Context(), lq)
	if err != nil {
		h.log.Warn("trace log query failed", "trace_id", traceIDStr, "err", err)
	} else {
		logs = logResult.Entries
	}

	tree := buildSpanTree(result.Spans, logs)

	buf := bytes.NewBuffer(make([]byte, 0, 8192))
	if err := json.NewEncoder(buf).Encode(map[string]any{
		"trace_id":   traceIDStr,
		"span_count": len(result.Spans),
		"log_count":  len(logs),
		"tree":       tree,
		"took_ms":    time.Since(start).Milliseconds(),
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "encode error")
		return
	}
	body := buf.Bytes()
	h.cache.put(traceIDStr, body)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func buildSpanTree(spans []model.SpanEntry, logs []model.LogEntry) []*spanNode {
	nodes := make(map[model.SpanID]*spanNode, len(spans))
	for i := range spans {
		nodes[spans[i].SpanID] = &spanNode{Span: spans[i], Logs: []model.LogEntry{}}
	}

	for i := range logs {
		if node, ok := nodes[logs[i].SpanID]; ok {
			node.Logs = append(node.Logs, logs[i])
		}
	}

	var roots []*spanNode
	for i := range spans {
		node := nodes[spans[i].SpanID]
		if spans[i].IsRoot() {
			roots = append(roots, node)
		} else {
			if parent, ok := nodes[spans[i].ParentID]; ok {
				parent.Children = append(parent.Children, node)
			} else {
				roots = append(roots, node)
			}
		}
	}

	return roots
}
