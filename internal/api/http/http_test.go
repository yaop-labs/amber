package http

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/storage"
)

func TestAPIKeyMiddleware_EmptyKey(t *testing.T) {
	handler := APIKeyMiddleware("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("empty key should pass all requests, got %d", rec.Code)
	}
}

func TestAPIKeyMiddleware_ValidKey(t *testing.T) {
	handler := APIKeyMiddleware("secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("valid key should pass, got %d", rec.Code)
	}
}

func TestAPIKeyMiddleware_InvalidKey(t *testing.T) {
	handler := APIKeyMiddleware("secret", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	cases := []struct {
		name string
		auth string
	}{
		{"no header", ""},
		{"wrong key", "Bearer wrong"},
		{"no bearer prefix", "secret"},
		{"basic auth", "Basic c2VjcmV0"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if c.auth != "" {
				req.Header.Set("Authorization", c.auth)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401, got %d", rec.Code)
			}
		})
	}
}

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusOK, map[string]string{"status": "ok"})

	if rec.Code != http.StatusOK {
		t.Errorf("status: got %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type: got %q", ct)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("body: got %v", body)
	}
}

func TestWriteError(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, http.StatusBadRequest, "bad input")

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status: got %d", rec.Code)
	}

	var body map[string]string
	json.NewDecoder(rec.Body).Decode(&body)
	if body["error"] != "bad input" {
		t.Errorf("error message: got %q", body["error"])
	}
}

func TestSplitComma(t *testing.T) {
	cases := []struct {
		input string
		want  int
	}{
		{"api,worker,auth", 3},
		{"single", 1},
		{"", 0},
		{" a , b , c ", 3},
		{",,,", 0},
	}

	for _, c := range cases {
		got := splitComma(c.input)
		if len(got) != c.want {
			t.Errorf("splitComma(%q): got %d items, want %d", c.input, len(got), c.want)
		}
	}
}

func TestParseLogQuery(t *testing.T) {
	req := httptest.NewRequest("GET",
		"/api/v1/logs?service=api,worker&level=ERROR&host=node-01&q=timeout&limit=50&offset=10&attr.env=prod",
		nil)

	q, err := parseLogQuery(req)
	if err != nil {
		t.Fatalf("parseLogQuery: %v", err)
	}

	if len(q.Services) != 2 || q.Services[0] != "api" {
		t.Errorf("Services: %v", q.Services)
	}
	if len(q.Levels) != 1 || q.Levels[0] != "ERROR" {
		t.Errorf("Levels: %v", q.Levels)
	}
	if len(q.Hosts) != 1 || q.Hosts[0] != "node-01" {
		t.Errorf("Hosts: %v", q.Hosts)
	}
	if q.FullText != "timeout" {
		t.Errorf("FullText: %q", q.FullText)
	}
	if q.Limit != 50 {
		t.Errorf("Limit: %d", q.Limit)
	}
	if q.Offset != 10 {
		t.Errorf("Offset: %d", q.Offset)
	}
	if q.Attrs["env"] != "prod" {
		t.Errorf("Attrs: %v", q.Attrs)
	}
}

func TestParseLogQuery_TimeRange(t *testing.T) {
	from := time.Now().Add(-1 * time.Hour).UTC().Format(time.RFC3339)
	to := time.Now().UTC().Format(time.RFC3339)

	req := httptest.NewRequest("GET", "/api/v1/logs?from="+from+"&to="+to, nil)
	q, err := parseLogQuery(req)
	if err != nil {
		t.Fatalf("parseLogQuery: %v", err)
	}

	if q.From.IsZero() {
		t.Error("From should be set")
	}
	if q.To.IsZero() {
		t.Error("To should be set")
	}
}

func TestParseLogQuery_InvalidTime(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/logs?from=not-a-date", nil)
	_, err := parseLogQuery(req)
	if err == nil {
		t.Error("expected error for invalid time")
	}
}

func TestParseLogQuery_InvalidLimit(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/v1/logs?limit=abc", nil)
	_, err := parseLogQuery(req)
	if err == nil {
		t.Error("expected error for invalid limit")
	}
}

func TestIngestHandler_EmptyArray(t *testing.T) {
	h := &IngestHandler{batcher: &ingest.Batcher{}, log: nil}
	req := httptest.NewRequest("POST", "/api/v1/logs", strings.NewReader("[]"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("expected 202 for empty array, got %d", rec.Code)
	}
}

func TestIngestHandler_InvalidJSON(t *testing.T) {
	h := &IngestHandler{batcher: &ingest.Batcher{}, log: nil}
	req := httptest.NewRequest("POST", "/api/v1/logs", strings.NewReader("{invalid"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for invalid JSON, got %d", rec.Code)
	}
}

func TestIngestHandler_Returns503WhenQueueIsFull(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	defer logManager.Close()

	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}
	defer spanManager.Close()

	batcher := ingest.NewBatcher(logManager, spanManager, index.NewSparseIndex(), index.NewSparseIndex(), nil, 10, time.Second, 1, 0, log)
	h := NewIngestHandler(batcher, log)

	req := httptest.NewRequest("POST", "/api/v1/logs", strings.NewReader(`[
		{"level":"INFO","service":"a","body":"one"},
		{"level":"INFO","service":"b","body":"two"}
	]`))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when queue is full, got %d", rec.Code)
	}

	var body map[string]int
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["accepted"] != 1 || body["rejected"] != 1 {
		t.Fatalf("unexpected counts: %v", body)
	}
}

func TestBatcher_SendLogReturnsQueueFull(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	logManager, err := storage.OpenSegmentManager(dir+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	defer logManager.Close()

	spanManager, err := storage.OpenSegmentManager(dir+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}
	defer spanManager.Close()

	batcher := ingest.NewBatcher(logManager, spanManager, index.NewSparseIndex(), index.NewSparseIndex(), nil, 10, time.Second, 1, 0, log)
	entry1, _ := model.NewLogEntry(model.LevelInfo, "a", "", "one")
	entry2, _ := model.NewLogEntry(model.LevelInfo, "b", "", "two")

	if err := batcher.SendLog(entry1); err != nil {
		t.Fatalf("first SendLog() error = %v", err)
	}
	if err := batcher.SendLog(entry2); !errors.Is(err, ingest.ErrQueueFull) {
		t.Fatalf("second SendLog() error = %v, want ErrQueueFull", err)
	}
}
