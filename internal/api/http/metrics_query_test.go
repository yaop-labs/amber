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
