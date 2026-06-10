package http

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
)

type logQueryResponse struct {
	Entries    []model.LogEntry `json:"entries"`
	TotalHits  int              `json:"total_hits"`
	Truncated  bool             `json:"truncated"`
	NextCursor string           `json:"next_cursor,omitempty"`
	TookMs     int64            `json:"took_ms"`
	SegTotal   int              `json:"seg_total,omitempty"`
	SegScanned int              `json:"seg_scanned,omitempty"`
	CacheHit   bool             `json:"cache_hit,omitempty"`
}

type httpCacheEntry struct {
	body    []byte
	expires int64
}

type httpCache struct {
	mu      sync.Mutex
	entries map[string]httpCacheEntry
	ttl     time.Duration
	maxSize int
}

func newHTTPCache(maxSize int, ttl time.Duration) *httpCache {
	return &httpCache{
		entries: make(map[string]httpCacheEntry, maxSize),
		ttl:     ttl,
		maxSize: maxSize,
	}
}

func (c *httpCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()
	if !ok || time.Now().UnixNano() > e.expires {
		return nil, false
	}
	return e.body, true
}

func (c *httpCache) put(key string, body []byte) {
	c.mu.Lock()
	if len(c.entries) >= c.maxSize {
		now := time.Now().UnixNano()
		for k, e := range c.entries {
			if e.expires < now {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= c.maxSize {
			c.entries = make(map[string]httpCacheEntry, c.maxSize)
		}
	}
	c.entries[key] = httpCacheEntry{
		body:    body,
		expires: time.Now().Add(c.ttl).UnixNano(),
	}
	c.mu.Unlock()
}

type QueryHandler struct {
	exec  *query.Executor
	log   *slog.Logger
	cache *httpCache
}

func NewQueryHandler(exec *query.Executor, log *slog.Logger) *QueryHandler {
	return &QueryHandler{
		exec: exec,
		log:  log,
	}
}

func wantsNDJSON(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/x-ndjson")
}

func (h *QueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ndjson := wantsNDJSON(r)

	cacheKey := r.URL.RawQuery
	if ndjson {
		cacheKey = "ndjson:" + cacheKey
	}

	if h.cache != nil {
		if body, ok := h.cache.get(cacheKey); ok {
			if ndjson {
				w.Header().Set("Content-Type", "application/x-ndjson")
			} else {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(http.StatusOK)
			w.Write(body)
			return
		}
	}

	q, err := parseLogQuery(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	start := time.Now()
	result, err := h.exec.ExecLog(r.Context(), q)
	if err != nil {
		h.log.Error("query failed", "err", err)
		writeError(w, http.StatusInternalServerError, "query failed")
		return
	}

	elapsed := time.Since(start)
	h.log.Info("query executed",
		"took_ms", elapsed.Milliseconds(),
		"hits", result.TotalHits,
		"truncated", result.Truncated,
		"cache_hit", result.CacheHit,
		"seg_total", result.SegTotal,
		"seg_scanned", result.SegScanned,
		"service", r.URL.Query().Get("service"),
		"q", r.URL.Query().Get("q"),
		"from", r.URL.Query().Get("from"),
		"to", r.URL.Query().Get("to"),
	)

	var body []byte
	var contentType string

	if ndjson {
		contentType = "application/x-ndjson"
		body = encodeNDJSON(result.Entries)
	} else {
		contentType = "application/json"
		resp := logQueryResponse{
			Entries:    result.Entries,
			TotalHits:  result.TotalHits,
			Truncated:  result.Truncated,
			NextCursor: result.NextCursor,
			TookMs:     elapsed.Milliseconds(),
			SegTotal:   result.SegTotal,
			SegScanned: result.SegScanned,
			CacheHit:   result.CacheHit,
		}
		buf := bytes.NewBuffer(make([]byte, 0, 8192))
		if err := json.NewEncoder(buf).Encode(resp); err != nil {
			writeError(w, http.StatusInternalServerError, "encode error")
			return
		}
		body = buf.Bytes()
	}

	if h.cache != nil {
		h.cache.put(cacheKey, body)
	}

	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func encodeNDJSON(entries []model.LogEntry) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, len(entries)*256))
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	for i := range entries {
		enc.Encode(ndjsonEntry(&entries[i]))
	}
	return buf.Bytes()
}

func ndjsonEntry(e *model.LogEntry) map[string]string {
	m := map[string]string{
		"_time":   e.Timestamp.Format(time.RFC3339Nano),
		"level":   e.Level.String(),
		"service": e.Service,
		"host":    e.Host,
		"_msg":    e.Body,
	}
	if !model.IsZeroTraceID(e.TraceID) {
		m["trace_id"] = hex.EncodeToString(e.TraceID[:])
	}
	if !model.IsZeroSpanID(e.SpanID) {
		m["span_id"] = hex.EncodeToString(e.SpanID[:])
	}
	for _, a := range e.Attrs {
		m[a.Key] = a.Value
	}
	return m
}

func parseLogQuery(r *http.Request) (*query.LogQuery, error) {
	q := r.URL.Query()
	lq := &query.LogQuery{}

	if v := q.Get("service"); v != "" {
		lq.Services = splitComma(v)
	}
	if v := q.Get("level"); v != "" {
		lq.Levels = splitComma(v)
	}
	if v := q.Get("host"); v != "" {
		lq.Hosts = splitComma(v)
	}
	if v := q.Get("q"); v != "" {
		lq.FullText = v
	}
	if v := q.Get("from"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, fmt.Errorf("invalid 'from' time: %w", err)
		}
		lq.From = t
	}
	if v := q.Get("to"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return nil, fmt.Errorf("invalid 'to' time: %w", err)
		}
		lq.To = t
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid 'limit': %w", err)
		}
		lq.Limit = n
	}
	if v := q.Get("cursor"); v != "" {
		lq.Cursor = v
	}

	for key, vals := range q {
		if len(key) > 5 && key[:5] == "attr." {
			if lq.Attrs == nil {
				lq.Attrs = make(map[string]string)
			}
			lq.Attrs[key[5:]] = vals[0]
		}
	}

	return lq, nil
}
