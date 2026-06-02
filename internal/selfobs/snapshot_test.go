package selfobs

import (
	"testing"
)

// TestSnapshot_CountersAndGauges seeds a unique counter+func gauge into the
// global registry and checks both come back through Snapshot with their
// labels intact. We use a unique metric name per test so other tests can't
// poison the result.
func TestSnapshot_CountersAndGauges(t *testing.T) {
	cv := NewCounterVec("snapshot_test_total", "test", "kind")
	RegisterCounterVec(cv)
	cv.WithLabelValues("a").Add(7)
	cv.WithLabelValues("b").Add(3)

	RegisterGaugeFunc("snapshot_test_gauge", "test", func() float64 { return 42 })

	snap := Snapshot()
	var sawA, sawB, sawGauge bool
	for _, s := range snap {
		switch s.Name {
		case "snapshot_test_total":
			if len(s.Labels) != 1 || s.Labels[0].Name != "kind" {
				t.Errorf("counter labels wrong: %+v", s.Labels)
			}
			switch s.Labels[0].Value {
			case "a":
				sawA = s.Value == 7 && s.Type == "counter"
			case "b":
				sawB = s.Value == 3 && s.Type == "counter"
			}
		case "snapshot_test_gauge":
			sawGauge = s.Value == 42 && s.Type == "gauge"
		}
	}
	if !sawA || !sawB || !sawGauge {
		t.Fatalf("missing samples: a=%v b=%v gauge=%v", sawA, sawB, sawGauge)
	}
}

// TestSnapshot_HistogramFlattensToCountAndSum checks the v0 representation:
// no per-bucket series, only "<name>_count" (counter) and "<name>_sum"
// (gauge). This matches what the metricsengine read path can actually query.
func TestSnapshot_HistogramFlattensToCountAndSum(t *testing.T) {
	hv := NewHistogramVec("snapshot_test_hist_seconds", "test", []float64{0.1, 1, 10}, "op")
	RegisterHistogramVec(hv)
	hv.WithLabelValues("read").Observe(0.5)
	hv.WithLabelValues("read").Observe(2.0)

	snap := Snapshot()
	var count, sum *Sample
	for i, s := range snap {
		switch s.Name {
		case "snapshot_test_hist_seconds_count":
			count = &snap[i]
		case "snapshot_test_hist_seconds_sum":
			sum = &snap[i]
		case "snapshot_test_hist_seconds_bucket":
			t.Errorf("unexpected _bucket series in snapshot")
		}
	}
	if count == nil || count.Value != 2 || count.Type != "counter" {
		t.Fatalf("_count wrong: %+v", count)
	}
	if sum == nil || sum.Value != 2.5 || sum.Type != "gauge" {
		t.Fatalf("_sum wrong: %+v", sum)
	}
}
