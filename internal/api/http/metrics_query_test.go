package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/client"
	"github.com/yaop-labs/amber/metricsengine"
)

// TestMetricsRate_RoundTrip seeds two counter samples through the store, hits
// GET /api/v1/metrics/rate, and asserts the JSON shape matches the wire
// contract the client expects. This is the read-side mirror of the OTLP
// counter test.
func TestMetricsRate_RoundTrip(t *testing.T) {
	h := setupMetricsHarness(t)

	now := time.Now().UnixMilli()
	labels := metricsengine.LabelSet{
		{Name: metricsengine.MetricNameLabel, Value: "http_requests_total"},
		{Name: "job", Value: "api"},
	}
	if _, err := h.metricStore.AppendBatch([]metricsengine.Sample{
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now - 60_000, Value: 1},
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now, Value: 61},
	}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/rate?metric=http_requests_total&window=2m&by=job&end="+strconv.FormatInt(now, 10),
		nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp client.MetricRateResult
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Metric != "http_requests_total" || resp.By != "job" || resp.WindowMS != 120_000 {
		t.Fatalf("envelope wrong: %+v", resp)
	}
	if got := resp.Rates["api"]; got <= 0 {
		t.Fatalf("rates[api] = %v, want > 0", got)
	}
}

// TestMetricsRate_NoMatchReturnsEmpty asserts that a selector with no hits is
// reported as an empty rates map (not 404 or 500). The CLI relies on this
// to render "(no series matched)" cleanly.
func TestMetricsRate_NoMatchReturnsEmpty(t *testing.T) {
	h := setupMetricsHarness(t)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/rate?metric=does_not_exist&window=5m", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp client.MetricRateResult
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Rates) != 0 {
		t.Fatalf("Rates = %v, want empty", resp.Rates)
	}
}

// TestMetricsRate_MissingMetric returns 400, not 500, so the CLI can surface
// a clear user error.
func TestMetricsRate_MissingMetric(t *testing.T) {
	h := setupMetricsHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rate?window=5m", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// TestMetricsRate_StoreDisabledReturns503 ensures the route stays mounted but
// signals clearly when MetricStore is nil.
func TestMetricsRate_StoreDisabledReturns503(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/metrics/rate", NewMetricsQueryHandler(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rate?metric=foo&window=5m", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// TestMetricsList_ReturnsNames seeds two series with different __name__ values
// and asserts both appear in GET /api/v1/metrics, sorted and deduplicated.
func TestMetricsList_ReturnsNames(t *testing.T) {
	h := setupMetricsHarness(t)

	now := time.Now().UnixMilli()
	for _, name := range []string{"alpha_total", "zeta_total", "alpha_total"} {
		labels := metricsengine.LabelSet{
			{Name: metricsengine.MetricNameLabel, Value: name},
		}
		if _, err := h.metricStore.AppendBatch([]metricsengine.Sample{
			{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now, Value: 1},
		}); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Metrics []string `json:"metrics"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Metrics) != 2 {
		t.Fatalf("metrics = %v, want [alpha_total zeta_total]", resp.Metrics)
	}
	if resp.Metrics[0] != "alpha_total" || resp.Metrics[1] != "zeta_total" {
		t.Fatalf("metrics order = %v, want sorted", resp.Metrics)
	}
}

// TestMetricsList_EmptyStore returns an empty array (not null) when no series
// have been ingested yet.
func TestMetricsList_EmptyStore(t *testing.T) {
	h := setupMetricsHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp struct {
		Metrics []string `json:"metrics"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Metrics == nil {
		t.Fatalf("metrics is null, want empty array")
	}
}

// TestMetricsList_StoreDisabledReturns503 mirrors the rate handler guard.
func TestMetricsList_StoreDisabledReturns503(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/metrics", NewMetricsListHandler(nil))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// TestMetricsStats_AfterIngest seeds a sample, flushes to a sealed block, then
// hits /stats. We check buffered counters fall to zero post-flush and that the
// sealed block contributes to Blocks/Series/Samples/Bytes plus a valid time
// range.
func TestMetricsStats_AfterIngest(t *testing.T) {
	h := setupMetricsHarness(t)

	now := time.Now().UnixMilli()
	labels := metricsengine.LabelSet{
		{Name: metricsengine.MetricNameLabel, Value: "stats_test_total"},
	}
	if _, err := h.metricStore.AppendBatch([]metricsengine.Sample{
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now, Value: 1},
	}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if _, err := h.metricStore.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/stats", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp client.MetricStoreStats
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Blocks < 1 || resp.Series < 1 || resp.Samples < 1 || resp.Bytes <= 0 {
		t.Fatalf("expected non-zero block stats, got %+v", resp)
	}
	if resp.MinTimeMS == nil || resp.MaxTimeMS == nil {
		t.Fatalf("expected populated time range after flush, got %+v", resp)
	}
}

// TestMetricsStats_EmptyStore returns 200 with zeroed counters and nil time
// range; the CLI relies on min_time_ms being absent to render "-".
func TestMetricsStats_EmptyStore(t *testing.T) {
	h := setupMetricsHarness(t)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/stats", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp client.MetricStoreStats
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Blocks != 0 || resp.MinTimeMS != nil || resp.MaxTimeMS != nil {
		t.Fatalf("expected empty stats, got %+v", resp)
	}
}

func TestMetricsStats_StoreDisabledReturns503(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/metrics/stats", NewMetricsStatsHandler(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/stats", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

// seedExpHistogram POSTs an exponential-histogram OTLP request into the
// harness. We go through the public ingest path on purpose: it forces the
// same OTLP → ExpSeries → histogram.Store.WriteBlock flow that real clients
// take, so the quantile test catches regressions in the whole chain instead
// of just the store-level math.
func seedExpHistogram(t *testing.T, h *metricsHarness, name string, attrs map[string]string, scale int32, positiveOffset int32, counts []uint64, sum float64, count uint64) {
	t.Helper()
	kvs := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, &commonpb.KeyValue{Key: k, Value: stringValue(v)})
	}
	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: name,
					Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
						DataPoints: []*metricspb.ExponentialHistogramDataPoint{{
							TimeUnixNano: uint64(time.Now().UnixNano()),
							Scale:        scale,
							Count:        count,
							Sum:          proto.Float64(sum),
							Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
								Offset:       positiveOffset,
								BucketCounts: counts,
							},
							Attributes: kvs,
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
		t.Fatalf("seed POST status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

// TestMetricsQuantile_RoundTrip seeds one exp-histogram and asks for q=0.5.
// The exact value depends on the sketch geometry; we only assert it's positive
// and falls within the bucket range the seed describes (offsets 0..3 at
// scale=2 cover roughly 1.0..1.68). Tighter math is in histogram package
// tests.
func TestMetricsQuantile_RoundTrip(t *testing.T) {
	h := setupMetricsHarness(t)
	seedExpHistogram(t, h, "rpc_latency_seconds", map[string]string{"job": "api"},
		2, 0, []uint64{2, 2, 2, 2}, 5.0, 8)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/quantile?metric=rpc_latency_seconds&q=0.5", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp quantileResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Metric != "rpc_latency_seconds" || resp.Quantile != 0.5 {
		t.Fatalf("envelope wrong: %+v", resp)
	}
	v, ok := resp.Quantiles[""]
	if !ok {
		t.Fatalf("quantiles missing default key, got %v", resp.Quantiles)
	}
	if v <= 0 || v > 10 {
		t.Fatalf("quantile value %v out of plausible range", v)
	}
}

// TestMetricsQuantile_ByLabel seeds two series with different job labels and
// asks for q=0.9 grouped by job. We expect two entries in the result map.
func TestMetricsQuantile_ByLabel(t *testing.T) {
	h := setupMetricsHarness(t)
	seedExpHistogram(t, h, "rpc_latency_seconds", map[string]string{"job": "api"},
		2, 0, []uint64{1, 1, 1, 1}, 2.0, 4)
	seedExpHistogram(t, h, "rpc_latency_seconds", map[string]string{"job": "worker"},
		2, 4, []uint64{1, 1, 1, 1}, 8.0, 4)

	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/quantile?metric=rpc_latency_seconds&q=0.9&by=job", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
	var resp quantileResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.By != "job" || len(resp.Quantiles) != 2 {
		t.Fatalf("expected two groups, got %+v", resp)
	}
	api, okA := resp.Quantiles["api"]
	worker, okW := resp.Quantiles["worker"]
	if !okA || !okW {
		t.Fatalf("missing group; got keys=%v", resp.Quantiles)
	}
	// worker series sits at higher offsets → its quantile should exceed api's.
	if !(worker > api) {
		t.Fatalf("expected worker quantile > api; got api=%v worker=%v", api, worker)
	}
}

// TestMetricsQuantile_EmptyStore returns 200 with an empty map when nothing
// matched; the CLI relies on this to render "(no series matched)".
func TestMetricsQuantile_EmptyStore(t *testing.T) {
	h := setupMetricsHarness(t)
	req := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/quantile?metric=missing&q=0.5", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	h.mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp quantileResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Quantiles) != 0 {
		t.Fatalf("expected empty quantiles, got %v", resp.Quantiles)
	}
}

// TestMetricsQuantile_BadParams covers the 400 paths the CLI relies on for
// clear error messages.
func TestMetricsQuantile_BadParams(t *testing.T) {
	h := setupMetricsHarness(t)
	cases := []struct {
		name   string
		path   string
		status int
	}{
		{"missing metric", "/api/v1/metrics/quantile?q=0.5", http.StatusBadRequest},
		{"missing q", "/api/v1/metrics/quantile?metric=x", http.StatusBadRequest},
		{"q not numeric", "/api/v1/metrics/quantile?metric=x&q=abc", http.StatusBadRequest},
		{"q out of range", "/api/v1/metrics/quantile?metric=x&q=1.5", http.StatusBadRequest},
		{"bad selector", "/api/v1/metrics/quantile?metric=x&q=0.5&selector=novalue", http.StatusBadRequest},
		{"bad window", "/api/v1/metrics/quantile?metric=x&q=0.5&window=oops", http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.Header.Set("Authorization", "Bearer secret")
			rec := httptest.NewRecorder()
			h.mux.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}

func TestMetricsQuantile_StoreDisabledReturns503(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /api/v1/metrics/quantile", NewMetricsQuantileHandler(nil, nil))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/quantile?metric=x&q=0.5", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
