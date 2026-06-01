package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

// metricsHarness is a slimmed-down API harness that wires the metricsengine
// store into RoutesDeps so /v1/metrics is actually serviceable. It reuses the
// existing log/span scaffold because RegisterRoutes still expects them.
type metricsHarness struct {
	mux         *http.ServeMux
	metricStore *metricsengine.Store
}

func setupMetricsHarness(t *testing.T) *metricsHarness {
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
	batcher := ingest.NewBatcher(
		ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: logSparse, SpanSparse: spanSparse, Indexer: exec.ActiveIndex(), Logger: log},
		ingest.Config{BatchSize: 16, BatchTimeout: 2 * time.Millisecond, QueueSize: 256},
	)
	batcher.Start(ctx)

	metricStore, err := metricsengine.OpenStore(filepath.Join(dir, "metrics"))
	if err != nil {
		t.Fatalf("open metric store: %v", err)
	}

	mux := http.NewServeMux()
	var ready atomic.Bool
	ready.Store(true)
	RegisterRoutes(mux, RoutesDeps{
		Batcher: batcher, Executor: exec,
		LogManager: logManager, LogSparse: logSparse,
		MetricStore: metricStore,
		IsReady:     ready.Load, Logger: log,
	}, RoutesConfig{APIKeys: []config.NamedAPIKey{{Name: "default", Key: "secret"}}, MaxRequestBytes: 32 << 20})

	t.Cleanup(func() {
		cancel()
		batcher.Wait()
		_ = metricStore.Close()
		_ = logSparse.Save(logDir)
		_ = spanSparse.Save(spanDir)
		_ = logManager.Close()
		_ = spanManager.Close()
	})

	return &metricsHarness{mux: mux, metricStore: metricStore}
}

func (h *metricsHarness) post(t *testing.T, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	return rec
}

// TestOTLPMetrics_CounterRoundTrip POSTs a tiny OTLP Sum metric and verifies
// that the stored sample shows up via RateByLabelRange on the same store.
// This is the end-to-end equivalent of the embedded smoke test, but driven
// through the HTTP receiver.
func TestOTLPMetrics_CounterRoundTrip(t *testing.T) {
	h := setupMetricsHarness(t)

	t0 := time.Now().Add(-time.Minute).UnixNano()
	t1 := time.Now().UnixNano()

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					{Key: "service.name", Value: stringValue("api")},
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "http_requests_total",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
						DataPoints: []*metricspb.NumberDataPoint{
							{
								TimeUnixNano: uint64(t0),
								Attributes:   []*commonpb.KeyValue{{Key: "job", Value: stringValue("api")}},
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 1},
							},
							{
								TimeUnixNano: uint64(t1),
								Attributes:   []*commonpb.KeyValue{{Key: "job", Value: stringValue("api")}},
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 61},
							},
						},
					}},
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := h.post(t, "/v1/metrics", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Accepted    int `json:"accepted"`
		Rejected    int `json:"rejected"`
		Unsupported int `json:"unsupported"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 2 || resp.Rejected != 0 || resp.Unsupported != 0 {
		t.Fatalf("accepted/rejected/unsupported = %d/%d/%d, want 2/0/0", resp.Accepted, resp.Rejected, resp.Unsupported)
	}

	rs := metricsengine.RangeSelector{
		Selector: metricsengine.NewSelector(metricsengine.MetricName("http_requests_total")),
		Window:   2 * time.Minute,
	}
	rates, err := h.metricStore.RateByLabelRange(rs, t1/1_000_000, "job")
	if err != nil {
		t.Fatalf("RateByLabelRange: %v", err)
	}
	if got := rates["api"]; got <= 0 {
		t.Fatalf("rates[api] = %v, want > 0", got)
	}
}

// TestOTLPMetrics_HistogramReportedUnsupported posts a Histogram metric and
// expects the handler to record it as unsupported (skipped, not rejected).
// metricsengine v0 has no query path for histograms; accepting them at ingest
// would just write WAL bytes nothing can ever read.
func TestOTLPMetrics_HistogramReportedUnsupported(t *testing.T) {
	h := setupMetricsHarness(t)

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "http_request_duration_seconds",
					Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
						DataPoints: []*metricspb.HistogramDataPoint{{
							TimeUnixNano:   uint64(time.Now().UnixNano()),
							Count:          10,
							Sum:            proto.Float64(1.5),
							BucketCounts:   []uint64{1, 2, 3, 4},
							ExplicitBounds: []float64{0.1, 0.5, 1.0},
						}},
					}},
				}},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := h.post(t, "/v1/metrics", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Accepted    int `json:"accepted"`
		Rejected    int `json:"rejected"`
		Unsupported int `json:"unsupported"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 0 || resp.Unsupported != 1 {
		t.Fatalf("accepted/unsupported = %d/%d, want 0/1", resp.Accepted, resp.Unsupported)
	}
}

// TestOTLPMetrics_NoStoreReturns503 verifies the route stays alive when
// metrics are disabled but returns a clear 503 rather than silently dropping.
func TestOTLPMetrics_NoStoreReturns503(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	spanDir := filepath.Join(dir, "spans")
	logManager, _ := storage.OpenSegmentManager(logDir, storage.DefaultRotationPolicy)
	spanManager, _ := storage.OpenSegmentManager(spanDir, storage.DefaultRotationPolicy)
	t.Cleanup(func() {
		_ = logManager.Close()
		_ = spanManager.Close()
	})
	logSparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()
	exec := query.NewExecutorWithCache(logManager, spanManager, logSparse, spanSparse, logDir, spanDir, 32)
	ctx, cancel := context.WithCancel(context.Background())
	batcher := ingest.NewBatcher(
		ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: logSparse, SpanSparse: spanSparse, Indexer: exec.ActiveIndex(), Logger: log},
		ingest.Config{BatchSize: 16, BatchTimeout: 2 * time.Millisecond, QueueSize: 256},
	)
	batcher.Start(ctx)
	t.Cleanup(func() {
		cancel()
		batcher.Wait()
	})
	var ready atomic.Bool
	ready.Store(true)
	RegisterRoutes(mux, RoutesDeps{Batcher: batcher, Executor: exec, LogManager: logManager, LogSparse: logSparse, IsReady: ready.Load, Logger: log}, RoutesConfig{MaxRequestBytes: 32 << 20})

	body, _ := proto.Marshal(&collectormetrics.ExportMetricsServiceRequest{})
	req := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func stringValue(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}
