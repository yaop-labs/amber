package otlp

import (
	"math"
	"testing"
)

func TestSamples(t *testing.T) {
	samples, err := Samples(Batch{
		ResourceAttributes: map[string]string{"service.name": "checkout"},
		Points: []Point{{
			Name:       "cpu_usage",
			Kind:       MetricGauge,
			Timestamp:  1000,
			Attributes: map[string]string{"host": "a"},
			FloatValue: 12.345,
			NumberKind: NumberFloat,
			Scale:      100,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(samples) != 1 {
		t.Fatalf("len(samples) = %d, want 1", len(samples))
	}
	if samples[0].Value != 1235 {
		t.Fatalf("value = %d, want 1235", samples[0].Value)
	}
	if got, ok := samples[0].Labels.Get("resource.service.name"); !ok || got != "checkout" {
		t.Fatalf("resource.service.name = %q/%v, want checkout/true", got, ok)
	}
	if got, ok := samples[0].Labels.Get("__name__"); !ok || got != "cpu_usage" {
		t.Fatalf("__name__ = %q/%v, want cpu_usage/true", got, ok)
	}
}

func TestSamplesRejectsInvalidPoint(t *testing.T) {
	if _, err := Samples(Batch{Points: []Point{{Name: "", NumberKind: NumberInt}}}); err == nil {
		t.Fatal("expected missing name error")
	}
	if _, err := Samples(Batch{Points: []Point{{Name: "cpu", NumberKind: NumberFloat, Scale: -1}}}); err == nil {
		t.Fatal("expected invalid scale error")
	}
}

func TestSamplesSkippedDropsUnencodableFloats(t *testing.T) {
	batch := Batch{Points: []Point{
		{Name: "m", Kind: MetricGauge, NumberKind: NumberFloat, FloatValue: 1.5, Timestamp: 1},
		{Name: "m", Kind: MetricGauge, NumberKind: NumberFloat, FloatValue: math.NaN(), Timestamp: 2},
		{Name: "m", Kind: MetricGauge, NumberKind: NumberFloat, FloatValue: math.Inf(1), Timestamp: 3},
		{Name: "m", Kind: MetricGauge, NumberKind: NumberFloat, FloatValue: 1e18, Timestamp: 4},
		{Name: "m", Kind: MetricGauge, NumberKind: NumberInt, IntValue: 7, Timestamp: 5},
	}}
	samples, skipped, err := SamplesSkipped(batch)
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 3 {
		t.Errorf("skipped = %d, want 3 (NaN, +Inf, overflow)", skipped)
	}
	if len(samples) != 2 {
		t.Fatalf("len(samples) = %d, want 2", len(samples))
	}
	if samples[0].Value != 1500 {
		t.Errorf("scaled value = %d, want 1500", samples[0].Value)
	}
	if samples[1].Value != 7 {
		t.Errorf("int value = %d, want 7", samples[1].Value)
	}
}
