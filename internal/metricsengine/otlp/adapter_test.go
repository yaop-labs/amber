package otlp

import "testing"

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
