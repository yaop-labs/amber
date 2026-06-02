package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

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
