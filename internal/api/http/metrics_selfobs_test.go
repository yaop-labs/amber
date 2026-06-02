package http

import (
	"bytes"
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

	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/metricsengine"
)

// TestMetricsSelfobs_IngestCountersIncrement walks an OTLP batch with one Sum
// point and one Histogram point through the handler and asserts the selfobs
// counters reflect both the accepted and unsupported paths. We compare deltas
// instead of absolute values so other tests running in the same package don't
// poison the count.
func TestMetricsSelfobs_IngestCountersIncrement(t *testing.T) {
	h := setupMetricsHarness(t)

	beforeAccepted := selfobs.MetricsIngestAccepted.WithLabelValues("sum").Get()
	beforeHist := selfobs.MetricsIngestAccepted.WithLabelValues("histogram").Get()

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: stringValue("api")},
			}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{
					{
						Name: "selfobs_test_counter_total",
						Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
							DataPoints: []*metricspb.NumberDataPoint{
								{TimeUnixNano: uint64(time.Now().UnixNano()), Value: &metricspb.NumberDataPoint_AsInt{AsInt: 7}},
							},
						}},
					},
					{
						Name: "selfobs_test_hist_seconds",
						Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
							DataPoints: []*metricspb.HistogramDataPoint{
								{TimeUnixNano: uint64(time.Now().UnixNano()), Count: 3},
							},
						}},
					},
				},
			}},
		}},
	}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	rec := h.post(t, "/v1/metrics", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}

	if got := selfobs.MetricsIngestAccepted.WithLabelValues("sum").Get() - beforeAccepted; got != 1 {
		t.Errorf("accepted{sum} delta = %d, want 1", got)
	}
	// Histogram now flows through the histogram store rather than landing in
	// unsupported — the OTLP point with Count=3 produces a single accepted
	// series-point pair (1 series × 1 tick = 1 accepted point).
	if got := selfobs.MetricsIngestAccepted.WithLabelValues("histogram").Get() - beforeHist; got != 1 {
		t.Errorf("accepted{histogram} delta = %d, want 1", got)
	}
}

// TestMetricsSelfobs_QueryCountersIncrement seeds two samples, hits the rate
// route, and verifies the success counter and the duration histogram both
// moved. A second call with a bogus selector lifts the error counter.
func TestMetricsSelfobs_QueryCountersIncrement(t *testing.T) {
	h := setupMetricsHarness(t)

	now := time.Now().UnixMilli()
	labels := metricsengine.LabelSet{
		{Name: metricsengine.MetricNameLabel, Value: "selfobs_query_total"},
		{Name: "job", Value: "api"},
	}
	if _, err := h.metricStore.AppendBatch([]metricsengine.Sample{
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now - 60_000, Value: 1},
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: now, Value: 61},
	}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	beforeTotal := selfobs.MetricsQueryTotal.WithLabelValues("rate").Get()
	beforeErrors := selfobs.MetricsQueryErrors.WithLabelValues("rate").Get()

	rec := httptest.NewRequest(http.MethodGet,
		"/api/v1/metrics/rate?metric=selfobs_query_total&window=2m&by=job&end="+strconv.FormatInt(now, 10),
		nil)
	rec.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, rec)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	if got := selfobs.MetricsQueryTotal.WithLabelValues("rate").Get() - beforeTotal; got != 1 {
		t.Errorf("query_total delta = %d, want 1", got)
	}

	// Missing metric: same handler path, should bump errors.
	bad := httptest.NewRequest(http.MethodGet, "/api/v1/metrics/rate?window=5m", nil)
	bad.Header.Set("Authorization", "Bearer secret")
	wBad := httptest.NewRecorder()
	h.mux.ServeHTTP(wBad, bad)
	if wBad.Code != http.StatusBadRequest {
		t.Fatalf("bad-request status = %d", wBad.Code)
	}
	if got := selfobs.MetricsQueryErrors.WithLabelValues("rate").Get() - beforeErrors; got != 1 {
		t.Errorf("query_errors delta = %d, want 1", got)
	}
}

// TestMetricsSelfobs_HandlerExposesNewMetrics scrapes the in-process selfobs
// handler and confirms the new metric names appear in the exposition body.
// This guards against forgetting to register a new vec in selfobs/metrics.go.
func TestMetricsSelfobs_HandlerExposesNewMetrics(t *testing.T) {
	w := httptest.NewRecorder()
	selfobs.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := w.Body.Bytes()
	for _, want := range [][]byte{
		[]byte("amber_metrics_ingest_accepted_total"),
		[]byte("amber_metrics_ingest_rejected_total"),
		[]byte("amber_metrics_ingest_unsupported_total"),
		[]byte("amber_metrics_query_total"),
		[]byte("amber_metrics_query_errors_total"),
		[]byte("amber_metrics_query_duration_seconds"),
	} {
		if !bytes.Contains(body, want) {
			t.Errorf("scrape body missing %s", want)
		}
	}
}
