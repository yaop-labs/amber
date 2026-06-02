package histogram

import (
	"math"
	"testing"
)

func TestExplicitQuantileInterpolation(t *testing.T) {
	bounds := []float64{1, 2, 4, 8, 16, 32, 64}
	// 100 values spread uniformly in (0,64]; check median is roughly in range.
	var data []float64
	for i := 1; i <= 1000; i++ {
		data = append(data, float64(i)*64.0/1000.0)
	}
	h := ExplicitFromValues(bounds, data)
	if h.Count != 1000 {
		t.Fatalf("count = %d, want 1000", h.Count)
	}
	for _, q := range []float64{0.1, 0.5, 0.9} {
		est := h.Quantile(q)
		truth := trueQuantile(data, q)
		// Explicit interpolation is coarse; allow generous wide-bucket slack.
		if est < truth*0.5 || est > truth*1.6 {
			t.Errorf("q=%.2f: est=%.3f truth=%.3f out of interpolation range", q, est, truth)
		}
	}
}

func TestMergeExplicitSharedBounds(t *testing.T) {
	bounds := []float64{10, 20, 30}
	a := ExplicitFromValues(bounds, []float64{5, 15, 25})
	b := ExplicitFromValues(bounds, []float64{15, 35, 45})
	merged := MergeExplicitAll([]*ExplicitBucketHistogram{a, b})
	if merged.Count != 6 {
		t.Fatalf("merged count = %d, want 6", merged.Count)
	}
	// bucket layout: [<=10][<=20][<=30][+Inf]
	want := []uint64{1, 2, 1, 2}
	for i, w := range want {
		if merged.Counts[i] != w {
			t.Errorf("bucket %d = %d, want %d", i, merged.Counts[i], w)
		}
	}
	if merged.Sum != 5+15+25+15+35+45 {
		t.Errorf("merged sum = %f", merged.Sum)
	}
}

func TestMergeExplicitBoundsMismatch(t *testing.T) {
	a := ExplicitFromValues([]float64{1, 2}, []float64{1.5})
	b := ExplicitFromValues([]float64{1, 3}, []float64{2})
	if MergeExplicit(a, b) {
		t.Fatal("expected merge to fail on mismatched bounds")
	}
}

func TestExplicitQuantileEmpty(t *testing.T) {
	h := NewExplicit([]float64{1, 2})
	if !math.IsNaN(h.Quantile(0.5)) {
		t.Fatal("empty histogram quantile should be NaN")
	}
}
