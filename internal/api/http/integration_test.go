package http

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
)

type apiHarness struct {
	mux         *http.ServeMux
	batcher     *ingest.Batcher
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	cancel      context.CancelFunc
}

func setupAPIHarness(t *testing.T) *apiHarness {
	t.Helper()

	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	spanDir := filepath.Join(dir, "spans")
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	logManager, err := storage.OpenSegmentManager(logDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open log manager: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(spanDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open span manager: %v", err)
	}

	logSparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()
	exec := query.NewExecutorWithCache(logManager, spanManager, logSparse, spanSparse, logDir, spanDir, 32)

	ctx, cancel := context.WithCancel(context.Background())
	batcher := ingest.NewBatcher(ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: logSparse, SpanSparse: spanSparse, Indexer: exec.ActiveIndex(), Logger: log}, ingest.Config{BatchSize: 16, BatchTimeout: 2 * time.Millisecond, QueueSize: 256})
	batcher.Start(ctx)

	mux := http.NewServeMux()
	var ready atomic.Bool
	ready.Store(true)
	RegisterRoutes(mux, RoutesDeps{Batcher: batcher, Executor: exec, LogManager: logManager, LogSparse: logSparse, IsReady: ready.Load, Logger: log}, RoutesConfig{APIKeys: []config.NamedAPIKey{{Name: "default", Key: "secret"}}, MaxRequestBytes: 32 << 20})

	t.Cleanup(func() {
		cancel()
		batcher.Wait()
		_ = logSparse.Save(logDir)
		_ = spanSparse.Save(spanDir)
		_ = logManager.Close()
		_ = spanManager.Close()
	})

	return &apiHarness{
		mux:         mux,
		batcher:     batcher,
		logManager:  logManager,
		spanManager: spanManager,
		logSparse:   logSparse,
		spanSparse:  spanSparse,
		cancel:      cancel,
	}
}

func (h *apiHarness) do(t *testing.T, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Authorization") == "" {
		req.Header.Set("Authorization", "Bearer secret")
	}
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	return rec
}

func (h *apiHarness) flush() {
	h.cancel()
	h.batcher.Wait()
}

func TestRoutes_JSONIngestQueryServicesAndAdmin(t *testing.T) {
	h := setupAPIHarness(t)

	traceID := "00112233445566778899aabbccddeeff"
	spanID := "0102030405060708"
	payload := []byte(`[
		{"level":"ERROR","service":"api","host":"node-01","body":"connection refused","trace_id":"` + traceID + `","span_id":"` + spanID + `","attrs":{"env":"prod"}},
		{"level":"INFO","service":"worker","host":"node-02","body":"job complete"}
	]`)

	rec := h.do(t, http.MethodPost, "/api/v1/logs", payload, map[string]string{"Content-Type": "application/json"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /api/v1/logs status = %d, want 202", rec.Code)
	}

	h.flush()

	queryRec := h.do(t, http.MethodGet, "/api/v1/logs?service=api&level=ERROR&limit=10&attr.env=prod", nil, nil)
	if queryRec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/logs status = %d, want 200", queryRec.Code)
	}
	var logsResp struct {
		Entries []struct {
			Service string `json:"service"`
			Body    string `json:"body"`
			Level   string `json:"level"`
		} `json:"entries"`
		TotalHits int `json:"total_hits"`
	}
	if err := json.NewDecoder(queryRec.Body).Decode(&logsResp); err != nil {
		t.Fatalf("decode log response: %v", err)
	}
	if logsResp.TotalHits != 1 || len(logsResp.Entries) != 1 {
		t.Fatalf("log query hits = %d entries = %d, want 1/1", logsResp.TotalHits, len(logsResp.Entries))
	}
	if logsResp.Entries[0].Service != "api" || logsResp.Entries[0].Body != "connection refused" || logsResp.Entries[0].Level != "ERROR" {
		t.Fatalf("unexpected log entry: %+v", logsResp.Entries[0])
	}

	ndjsonRec := h.do(t, http.MethodGet, "/api/v1/logs?service=api&limit=10", nil, map[string]string{"Accept": "application/x-ndjson"})
	if ndjsonRec.Code != http.StatusOK {
		t.Fatalf("NDJSON query status = %d, want 200", ndjsonRec.Code)
	}
	if got := ndjsonRec.Header().Get("Content-Type"); got != "application/x-ndjson" {
		t.Fatalf("NDJSON content-type = %q, want application/x-ndjson", got)
	}
	if !bytes.Contains(ndjsonRec.Body.Bytes(), []byte(`"trace_id":"`+traceID+`"`)) {
		t.Fatalf("NDJSON response missing trace_id: %s", ndjsonRec.Body.String())
	}

	servicesRec := h.do(t, http.MethodGet, "/api/v1/services", nil, nil)
	if servicesRec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/services status = %d, want 200", servicesRec.Code)
	}
	var servicesResp struct {
		Services []string `json:"services"`
	}
	if err := json.NewDecoder(servicesRec.Body).Decode(&servicesResp); err != nil {
		t.Fatalf("decode services: %v", err)
	}
	if len(servicesResp.Services) != 2 || servicesResp.Services[0] != "api" || servicesResp.Services[1] != "worker" {
		t.Fatalf("unexpected services list: %v", servicesResp.Services)
	}

	statsRec := h.do(t, http.MethodGet, "/api/v1/admin/stats", nil, nil)
	if statsRec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/admin/stats status = %d, want 200", statsRec.Code)
	}
	var statsResp struct {
		Segments struct {
			TotalRecords uint64 `json:"total_records"`
		} `json:"segments"`
		Ingest struct {
			Logs struct {
				QueueLen int `json:"queue_len"`
			} `json:"logs"`
			Spans struct {
				QueueLen int `json:"queue_len"`
			} `json:"spans"`
		} `json:"ingest"`
	}
	if err := json.NewDecoder(statsRec.Body).Decode(&statsResp); err != nil {
		t.Fatalf("decode admin stats: %v", err)
	}
	if statsResp.Segments.TotalRecords != 2 {
		t.Fatalf("admin total_records = %d, want 2", statsResp.Segments.TotalRecords)
	}
	if statsResp.Ingest.Logs.QueueLen != 0 || statsResp.Ingest.Spans.QueueLen != 0 {
		t.Fatalf("admin ingest queues = logs:%d spans:%d, want drained",
			statsResp.Ingest.Logs.QueueLen, statsResp.Ingest.Spans.QueueLen)
	}
}

func TestRoutes_OTLPTraceListAndTraceDetail(t *testing.T) {
	h := setupAPIHarness(t)

	traceID := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	rootSpanID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	childSpanID := []byte{8, 7, 6, 5, 4, 3, 2, 1}

	traceReq := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{stringAttr("service.name", "checkout")}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{
					{
						TraceId:           traceID,
						SpanId:            rootSpanID,
						Name:              "GET /checkout",
						StartTimeUnixNano: uint64(time.Unix(100, 0).UnixNano()),
						EndTimeUnixNano:   uint64(time.Unix(100, 150*time.Millisecond.Nanoseconds()).UnixNano()),
						Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
					},
					{
						TraceId:           traceID,
						SpanId:            childSpanID,
						ParentSpanId:      rootSpanID,
						Name:              "db lookup",
						StartTimeUnixNano: uint64(time.Unix(100, 10*time.Millisecond.Nanoseconds()).UnixNano()),
						EndTimeUnixNano:   uint64(time.Unix(100, 40*time.Millisecond.Nanoseconds()).UnixNano()),
						Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR},
					},
				},
			}},
		}},
	}
	traceBody, err := proto.Marshal(traceReq)
	if err != nil {
		t.Fatalf("marshal trace req: %v", err)
	}
	traceRec := h.do(t, http.MethodPost, "/v1/traces", traceBody, map[string]string{"Content-Type": "application/x-protobuf"})
	if traceRec.Code != http.StatusOK {
		t.Fatalf("POST /v1/traces status = %d, want 200", traceRec.Code)
	}

	logReq := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				stringAttr("service.name", "checkout"),
				stringAttr("host.name", "node-a"),
			}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{
					TimeUnixNano: uint64(time.Unix(100, 20*time.Millisecond.Nanoseconds()).UnixNano()),
					SeverityText: "ERROR",
					Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "db timeout"}},
					TraceId:      traceID,
					SpanId:       childSpanID,
					Attributes:   []*commonpb.KeyValue{stringAttr("env", "staging")},
				}},
			}},
		}},
	}
	logBody, err := proto.Marshal(logReq)
	if err != nil {
		t.Fatalf("marshal log req: %v", err)
	}
	logRec := h.do(t, http.MethodPost, "/v1/logs", logBody, map[string]string{"Content-Type": "application/x-protobuf"})
	if logRec.Code != http.StatusOK {
		t.Fatalf("POST /v1/logs status = %d, want 200", logRec.Code)
	}

	h.flush()

	listRec := h.do(t, http.MethodGet, "/api/v1/traces?service=checkout&limit=10", nil, nil)
	if listRec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/traces status = %d, want 200", listRec.Code)
	}
	var listResp struct {
		Traces []struct {
			TraceID   string `json:"trace_id"`
			Service   string `json:"service"`
			Operation string `json:"operation"`
			SpanCount int    `json:"span_count"`
			HasErrors bool   `json:"has_errors"`
		} `json:"traces"`
		Total int `json:"total"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&listResp); err != nil {
		t.Fatalf("decode traces list: %v", err)
	}
	if listResp.Total != 1 || len(listResp.Traces) != 1 {
		t.Fatalf("trace list total=%d len=%d, want 1/1", listResp.Total, len(listResp.Traces))
	}
	traceIDHex := hex.EncodeToString(traceID)
	if listResp.Traces[0].TraceID != traceIDHex {
		t.Fatalf("trace_id = %s, want %s", listResp.Traces[0].TraceID, traceIDHex)
	}
	if listResp.Traces[0].Service != "checkout" || listResp.Traces[0].Operation != "GET /checkout" {
		t.Fatalf("unexpected trace summary: %+v", listResp.Traces[0])
	}
	if listResp.Traces[0].SpanCount != 2 || !listResp.Traces[0].HasErrors {
		t.Fatalf("unexpected trace counts/errors: %+v", listResp.Traces[0])
	}

	detailRec := h.do(t, http.MethodGet, "/api/v1/traces/"+traceIDHex, nil, nil)
	if detailRec.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/traces/<id> status = %d, want 200", detailRec.Code)
	}
	var detailResp struct {
		TraceID   string `json:"trace_id"`
		SpanCount int    `json:"span_count"`
		LogCount  int    `json:"log_count"`
		Tree      []struct {
			Span struct {
				Operation string `json:"operation"`
			} `json:"span"`
			Logs []struct {
				Body string `json:"body"`
			} `json:"logs"`
			Children []struct {
				Span struct {
					Operation string `json:"operation"`
				} `json:"span"`
				Logs []struct {
					Body string `json:"body"`
				} `json:"logs"`
			} `json:"children"`
		} `json:"tree"`
	}
	if err := json.NewDecoder(detailRec.Body).Decode(&detailResp); err != nil {
		t.Fatalf("decode trace detail: %v", err)
	}
	if detailResp.TraceID != traceIDHex || detailResp.SpanCount != 2 || detailResp.LogCount != 1 {
		t.Fatalf("unexpected trace detail header: %+v", detailResp)
	}
	if len(detailResp.Tree) != 1 || detailResp.Tree[0].Span.Operation != "GET /checkout" {
		t.Fatalf("unexpected trace tree root: %+v", detailResp.Tree)
	}
	if len(detailResp.Tree[0].Children) != 1 || detailResp.Tree[0].Children[0].Span.Operation != "db lookup" {
		t.Fatalf("unexpected trace tree children: %+v", detailResp.Tree[0].Children)
	}
	if len(detailResp.Tree[0].Children[0].Logs) != 1 || detailResp.Tree[0].Children[0].Logs[0].Body != "db timeout" {
		t.Fatalf("unexpected child logs: %+v", detailResp.Tree[0].Children[0].Logs)
	}
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{StringValue: value},
		},
	}
}
