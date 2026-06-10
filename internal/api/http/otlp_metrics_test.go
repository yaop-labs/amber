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
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

type metricsHarness struct {
	mux         *http.ServeMux
	metricStore *metricsengine.Store
	histStore   *histogram.Store
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
	histStore, err := histogram.OpenStore(filepath.Join(dir, "metrics", "histograms"))
	if err != nil {
		t.Fatalf("open histogram store: %v", err)
	}

	mux := http.NewServeMux()
	var ready atomic.Bool
	ready.Store(true)
	RegisterRoutes(mux, RoutesDeps{
		Batcher: batcher, Executor: exec,
		LogManager: logManager, LogSparse: logSparse,
		MetricStore:    metricStore,
		HistogramStore: histStore,
		IsReady:        ready.Load, Logger: log,
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

	return &metricsHarness{mux: mux, metricStore: metricStore, histStore: histStore}
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

func TestOTLPMetrics_NonStringAttributesPreserveSeriesIdentity(t *testing.T) {
	h := setupMetricsHarness(t)

	t0 := time.Now().Add(-time.Minute).UnixNano()
	t1 := time.Now().UnixNano()
	point := func(ts int64, status int64, value int64) *metricspb.NumberDataPoint {
		return &metricspb.NumberDataPoint{
			TimeUnixNano: uint64(ts),
			Attributes: []*commonpb.KeyValue{
				{Key: "status_code", Value: intValue(status)},
				{Key: "canary", Value: boolValue(status == 200)},
			},
			Value: &metricspb.NumberDataPoint_AsInt{AsInt: value},
		}
	}
	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{{Key: "service.name", Value: stringValue("api")}},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "http_responses_total",
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
						DataPoints: []*metricspb.NumberDataPoint{
							point(t0, 200, 1),
							point(t1, 200, 11),
							point(t0, 500, 2),
							point(t1, 500, 22),
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

	rs := metricsengine.RangeSelector{
		Selector: metricsengine.NewSelector(metricsengine.MetricName("http_responses_total")),
		Window:   2 * time.Minute,
	}
	rates, err := h.metricStore.RateByLabelRange(rs, t1/1_000_000, "status_code")
	if err != nil {
		t.Fatalf("RateByLabelRange: %v", err)
	}
	if rates["200"] <= 0 || rates["500"] <= 0 {
		t.Fatalf("rates by int label = %v, want positive 200 and 500 keys", rates)
	}
	if _, collapsed := rates[""]; collapsed {
		t.Fatalf("rates include collapsed empty label key: %v", rates)
	}
}

func TestOTLPMetrics_ExplicitHistogramAccepted(t *testing.T) {
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
	if resp.Accepted != 1 || resp.Unsupported != 0 || resp.Rejected != 0 {
		t.Fatalf("accepted/rejected/unsupported = %d/%d/%d, want 1/0/0",
			resp.Accepted, resp.Rejected, resp.Unsupported)
	}
}

func TestOTLPMetrics_ExponentialHistogramAccepted(t *testing.T) {
	h := setupMetricsHarness(t)

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "rpc_latency_seconds",
					Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
						DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
							TimeUnixNano: uint64(time.Now().UnixNano()),
							Count:        4,
							Sum:          proto.Float64(0.4),
							Scale:        2,
							Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
								Offset:       0,
								BucketCounts: []uint64{1, 1, 1, 1},
							},
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
	if resp.Accepted != 1 || resp.Unsupported != 0 || resp.Rejected != 0 {
		t.Fatalf("accepted/rejected/unsupported = %d/%d/%d, want 1/0/0",
			resp.Accepted, resp.Rejected, resp.Unsupported)
	}
}

func TestOTLPMetrics_HistogramUnsupportedWhenHistStoreNil(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	metricStore, err := metricsengine.OpenStore(filepath.Join(dir, "metrics"))
	if err != nil {
		t.Fatalf("open metric store: %v", err)
	}
	t.Cleanup(func() { _ = metricStore.Close() })
	mux := http.NewServeMux()
	mux.Handle("POST /v1/metrics", NewOTLPHandler(noopBatcher(t), metricStore, nil, log))

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "h",
					Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
						DataPoints: []*metricspb.HistogramDataPoint{{
							TimeUnixNano:   uint64(time.Now().UnixNano()),
							BucketCounts:   []uint64{1},
							ExplicitBounds: []float64{},
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
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httpReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Accepted, Rejected, Unsupported int
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Unsupported != 1 {
		t.Fatalf("unsupported = %d, want 1; full=%+v", resp.Unsupported, resp)
	}
}

func TestOTLPMetrics_IgnoresLogSpanBreaker(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir := t.TempDir()
	metricStore, err := metricsengine.OpenStore(filepath.Join(dir, "metrics"))
	if err != nil {
		t.Fatalf("open metric store: %v", err)
	}
	t.Cleanup(func() { _ = metricStore.Close() })

	mux := http.NewServeMux()
	mux.Handle("POST /v1/metrics", NewOTLPHandler(openLogSpanBreakerBatcher(t), metricStore, nil, log))

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "lane_boundary_gauge",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: uint64(time.Now().UnixNano()),
							Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 1},
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

	httpReq := httptest.NewRequest(http.MethodPost, "/v1/metrics", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httpReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Accepted, Rejected, Unsupported int
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Accepted != 1 || resp.Rejected != 0 || resp.Unsupported != 0 {
		t.Fatalf("accepted/rejected/unsupported = %d/%d/%d, want 1/0/0",
			resp.Accepted, resp.Rejected, resp.Unsupported)
	}
}

func noopBatcher(t *testing.T) *ingest.Batcher {
	t.Helper()
	dir := t.TempDir()
	logManager, err := storage.OpenSegmentManager(filepath.Join(dir, "logs"), storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open log manager: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(filepath.Join(dir, "spans"), storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open span manager: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	exec := query.NewExecutorWithCache(logManager, spanManager, index.NewSparseIndex(), index.NewSparseIndex(), filepath.Join(dir, "logs"), filepath.Join(dir, "spans"), 32)
	b := ingest.NewBatcher(
		ingest.Deps{LogManager: logManager, SpanManager: spanManager, LogSparse: index.NewSparseIndex(), SpanSparse: index.NewSparseIndex(), Indexer: exec.ActiveIndex(), Logger: log},
		ingest.Config{BatchSize: 16, BatchTimeout: 2 * time.Millisecond, QueueSize: 256},
	)
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	t.Cleanup(func() {
		cancel()
		b.Wait()
		_ = logManager.Close()
		_ = spanManager.Close()
	})
	return b
}

func openLogSpanBreakerBatcher(t *testing.T) *ingest.Batcher {
	t.Helper()
	dir := t.TempDir()
	logDir := filepath.Join(dir, "logs")
	spanDir := filepath.Join(dir, "spans")
	logManager, err := storage.OpenSegmentManager(logDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open log manager: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(spanDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("open span manager: %v", err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := ingest.NewBatcher(
		ingest.Deps{
			LogManager:  logManager,
			SpanManager: spanManager,
			LogSparse:   index.NewSparseIndex(),
			SpanSparse:  index.NewSparseIndex(),
			Logger:      log,
		},
		ingest.Config{
			BatchSize:        1,
			BatchTimeout:     time.Hour,
			QueueSize:        16,
			BreakerThreshold: 1,
		},
	)

	_ = logManager.Close()

	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	entry, err := model.NewLogEntry(model.LevelInfo, "api", "", "trip breaker")
	if err != nil {
		t.Fatalf("new log entry: %v", err)
	}
	if err := b.SendLog(entry); err != nil {
		t.Fatalf("send log to trip breaker: %v", err)
	}
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !b.IsLogBreakerOpen() {
		select {
		case <-deadline:
			t.Fatal("log breaker did not open")
		case <-tick.C:
		}
	}
	t.Cleanup(func() {
		cancel()
		b.Wait()
		_ = spanManager.Close()
	})
	return b
}

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

func intValue(v int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}}
}

func boolValue(v bool) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: v}}
}
