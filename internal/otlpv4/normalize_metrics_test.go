package otlpv4

import (
	"testing"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	memodel "github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestNormalizedMetricSampleNative(t *testing.T) {
	tests := []struct {
		name   string
		sample memodel.Sample
		check  func(*testing.T, *metricspb.Metric)
	}{
		{
			name: "scaled gauge",
			sample: memodel.Sample{
				Labels: memodel.LabelSet{
					{Name: "scope.library", Value: "otel"},
					{Name: memodel.MetricNameLabel, Value: "temperature"},
					{Name: "zone", Value: "a"},
					{Name: "resource.service.name", Value: "api"},
					{Name: scaleLabel, Value: "1000"},
				},
				Type: memodel.MetricTypeGauge, Timestamp: 1234, Value: 125,
			},
			check: func(t *testing.T, metric *metricspb.Metric) {
				point := metric.GetGauge().DataPoints[0]
				if point.GetAsDouble() != 0.125 || point.TimeUnixNano != 1_234_000_000 {
					t.Fatalf("normalized gauge point = %v", point)
				}
			},
		},
		{
			name: "integer counter",
			sample: memodel.Sample{
				Labels: memodel.LabelSet{{Name: memodel.MetricNameLabel, Value: "requests"}},
				Type:   memodel.MetricTypeCounter, Timestamp: 2, Value: 7,
			},
			check: func(t *testing.T, metric *metricspb.Metric) {
				sum := metric.GetSum()
				if !sum.IsMonotonic || sum.AggregationTemporality != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE ||
					sum.DataPoints[0].GetAsInt() != 7 {
					t.Fatalf("normalized sum = %v", sum)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := NormalizedMetricSampleNative(test.sample)
			if err != nil {
				t.Fatal(err)
			}
			message, err := envelope.Request()
			if err != nil {
				t.Fatal(err)
			}
			request := message.(*collectormetrics.ExportMetricsServiceRequest)
			resourceMetrics := request.ResourceMetrics[0]
			metric := resourceMetrics.ScopeMetrics[0].Metrics[0]
			wantName, _ := test.sample.Labels.Get(memodel.MetricNameLabel)
			if metric.Name != wantName {
				t.Fatalf("metric name = %q", metric.Name)
			}
			test.check(t, metric)
		})
	}
}

func TestNormalizedMetricSketchNative(t *testing.T) {
	labels := memodel.LabelSet{{Name: memodel.MetricNameLabel, Value: "latency"}, {Name: "route", Value: "/"}}
	tests := []struct {
		name   string
		sample engine.SketchSample
		check  func(*testing.T, *metricspb.Metric)
	}{
		{
			name: "explicit",
			sample: engine.SketchSample{Labels: labels, Timestamp: 5, Explicit: &histogram.ExplicitBucketHistogram{
				Bounds: []float64{0.1, 1}, Counts: []uint64{1, 2, 3}, Sum: 4.5, Count: 6, Min: 0.01, Max: 2, HasMinMax: true,
			}},
			check: func(t *testing.T, metric *metricspb.Metric) {
				point := metric.GetHistogram().DataPoints[0]
				if point.Count != 6 || len(point.BucketCounts) != 3 || point.GetMin() != 0.01 || point.TimeUnixNano != 5_000_000 {
					t.Fatalf("normalized explicit histogram = %v", point)
				}
			},
		},
		{
			name: "exponential",
			sample: engine.SketchSample{Labels: labels, Timestamp: 6, Exp: &histogram.ExponentialHistogram{
				Scale: 4, ZeroThreshold: 0.001, ZeroCount: 1,
				Positive: histogram.Buckets{Offset: -2, Counts: []uint64{2, 3}},
				Negative: histogram.Buckets{Offset: 1, Counts: []uint64{4}},
				Sum:      5, Count: 10,
			}},
			check: func(t *testing.T, metric *metricspb.Metric) {
				point := metric.GetExponentialHistogram().DataPoints[0]
				if point.Scale != 4 || point.Positive.Offset != -2 || len(point.Negative.BucketCounts) != 1 || point.Count != 10 {
					t.Fatalf("normalized exponential histogram = %v", point)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := NormalizedMetricSketchNative(test.sample)
			if err != nil {
				t.Fatal(err)
			}
			message, err := envelope.Request()
			if err != nil {
				t.Fatal(err)
			}
			request := message.(*collectormetrics.ExportMetricsServiceRequest)
			test.check(t, request.ResourceMetrics[0].ScopeMetrics[0].Metrics[0])
		})
	}
}

func TestNormalizedMetricNativeRejectsAmbiguousMetadata(t *testing.T) {
	base := memodel.Sample{Type: memodel.MetricTypeGauge}
	for _, labels := range []memodel.LabelSet{
		{{Name: memodel.MetricNameLabel, Value: ""}},
		{{Name: memodel.MetricNameLabel, Value: "a"}, {Name: memodel.MetricNameLabel, Value: "b"}},
		{{Name: memodel.MetricNameLabel, Value: "a"}, {Name: scaleLabel, Value: "0"}},
	} {
		base.Labels = labels
		if _, err := NormalizedMetricSampleNative(base); err == nil {
			t.Fatalf("NormalizedMetricSampleNative() error = nil for labels %v", labels)
		}
	}
}
