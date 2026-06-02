package otlp

import (
	"reflect"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestOTLPExpRoundTrip is the OTLP round-trip gate: an OTLP ExponentialHistogram
// data point, written to a store block and read back, must reproduce the exact
// same data point (scale, zero, offsets, counts, sum, count, min, max).
func TestOTLPExpRoundTrip(t *testing.T) {
	point := ExponentialHistogramPoint{
		Name:           "http.server.duration",
		Timestamp:      1717200000000,
		Attributes:     map[string]string{"route": "/api", "method": "GET"},
		Scale:          4,
		ZeroCount:      3,
		ZeroThreshold:  1e-6,
		PositiveOffset: -5,
		PositiveCounts: []uint64{1, 0, 2, 4, 8, 4, 2, 1},
		NegativeOffset: 2,
		NegativeCounts: []uint64{1, 1},
		Sum:            1234.5,
		Count:          26,
		Min:            0.001,
		Max:            512.0,
		HasMinMax:      true,
	}

	batch := Batch{
		ResourceAttributes: map[string]string{"service.name": "checkout"},
	}
	series := ExponentialSeries(batch, []ExponentialHistogramPoint{point})
	if len(series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(series))
	}

	dir := t.TempDir() + "/hblock.mhb"
	if err := histogram.WriteBlock(dir, series, nil); err != nil {
		t.Fatal(err)
	}
	exps, _, err := histogram.ReadBlock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(exps) != 1 || len(exps[0].Sketches) != 1 {
		t.Fatalf("unexpected read back: %+v", exps)
	}

	got := ExponentialPointFromSketch(point.Name, point.Timestamp, point.Attributes, exps[0].Sketches[0])

	// Compare field-by-field; counts must match exactly (the point had no leading
	// or trailing zero edges to be trimmed).
	if got.Scale != point.Scale {
		t.Errorf("scale %d != %d", got.Scale, point.Scale)
	}
	if got.ZeroCount != point.ZeroCount || got.ZeroThreshold != point.ZeroThreshold {
		t.Errorf("zero (%d,%v) != (%d,%v)", got.ZeroCount, got.ZeroThreshold, point.ZeroCount, point.ZeroThreshold)
	}
	if got.PositiveOffset != point.PositiveOffset || !reflect.DeepEqual(got.PositiveCounts, point.PositiveCounts) {
		t.Errorf("positive (%d,%v) != (%d,%v)", got.PositiveOffset, got.PositiveCounts, point.PositiveOffset, point.PositiveCounts)
	}
	if got.NegativeOffset != point.NegativeOffset || !reflect.DeepEqual(got.NegativeCounts, point.NegativeCounts) {
		t.Errorf("negative (%d,%v) != (%d,%v)", got.NegativeOffset, got.NegativeCounts, point.NegativeOffset, point.NegativeCounts)
	}
	if got.Sum != point.Sum || got.Count != point.Count {
		t.Errorf("sum/count (%v,%d) != (%v,%d)", got.Sum, got.Count, point.Sum, point.Count)
	}
	if got.Min != point.Min || got.Max != point.Max || got.HasMinMax != point.HasMinMax {
		t.Errorf("minmax (%v,%v,%v) != (%v,%v,%v)", got.HasMinMax, got.Min, got.Max, point.HasMinMax, point.Min, point.Max)
	}

	// The series label set must carry the metric name and merged attributes.
	labels := series[0].Labels
	if v, _ := labels.Get("__name__"); v != point.Name {
		t.Errorf("__name__ = %q", v)
	}
	if v, _ := labels.Get("route"); v != "/api" {
		t.Errorf("route = %q", v)
	}
	if v, _ := labels.Get("resource.service.name"); v != "checkout" {
		t.Errorf("resource.service.name = %q", v)
	}
}

func TestSummaryLowersToGaugeSeries(t *testing.T) {
	point := SummaryPoint{
		Name:       "rpc.latency",
		Timestamp:  500,
		Attributes: map[string]string{"service": "pay"},
		Quantiles: []SummaryQuantile{
			{Quantile: 0.5, Value: 12.5},
			{Quantile: 0.99, Value: 88.0},
		},
		Scale: 1000,
	}
	samples, err := SummarySamples(Batch{}, []SummaryPoint{point})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 2 {
		t.Fatalf("expected 2 gauge samples, got %d", len(samples))
	}
	for _, s := range samples {
		if s.Type != model.MetricTypeGauge {
			t.Errorf("summary quantile should be a gauge, got %v", s.Type)
		}
		if _, ok := s.Labels.Get("quantile"); !ok {
			t.Errorf("missing quantile label on %v", s.Labels)
		}
	}
}

func TestOTLPExplicitConversion(t *testing.T) {
	point := ExplicitHistogramPoint{
		Name:           "request.size",
		Timestamp:      100,
		ExplicitBounds: []float64{10, 50, 100},
		BucketCounts:   []uint64{2, 5, 3, 1},
		Sum:            420,
		Count:          11,
		Min:            1,
		Max:            150,
		HasMinMax:      true,
	}
	series := ExplicitSeries(Batch{}, []ExplicitHistogramPoint{point})
	if len(series) != 1 || len(series[0].Buckets) != 1 {
		t.Fatalf("unexpected series: %+v", series)
	}
	h := series[0].Buckets[0]
	if h.Count != 11 || h.Sum != 420 {
		t.Errorf("count/sum = %d/%v", h.Count, h.Sum)
	}
	if !reflect.DeepEqual(h.Counts, point.BucketCounts) {
		t.Errorf("counts %v != %v", h.Counts, point.BucketCounts)
	}
}
