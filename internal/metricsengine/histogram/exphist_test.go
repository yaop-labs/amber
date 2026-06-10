package histogram

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

// trueQuantile computes the exact quantile of raw data using the same rank
// convention as ExponentialHistogram.Quantile (rank = q*N, first sample whose
// cumulative position reaches the rank).
func trueQuantile(values []float64, q float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	rank := q * float64(len(sorted))
	idx := int(math.Ceil(rank)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func relErr(est, truth float64) float64 {
	if truth == 0 {
		return math.Abs(est)
	}
	return math.Abs(est-truth) / math.Abs(truth)
}

// TestCrossScaleMergeGuarantee verifies that mixed-scale merge preserves the
// coarser scale's relative-error guarantee.
func TestCrossScaleMergeGuarantee(t *testing.T) {
	cases := []struct {
		fine, coarse int32
	}{
		{5, 3}, // validated: merge(scale5,scale3) within 4.3% of scale-3
		{6, 2}, // validated: merge(scale6,scale2) within 8.6% of scale-2
	}
	quantiles := []float64{0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99}

	for _, tc := range cases {
		rng := rand.New(rand.NewSource(int64(tc.fine*100 + tc.coarse)))
		// Two disjoint datasets drawn from positive lognormal-ish ranges.
		var dataFine, dataCoarse, all []float64
		for i := 0; i < 5000; i++ {
			v := math.Exp(rng.NormFloat64()*1.3 + 3) // positive, wide spread
			dataFine = append(dataFine, v)
			all = append(all, v)
		}
		for i := 0; i < 5000; i++ {
			v := math.Exp(rng.NormFloat64()*1.1 + 4)
			dataCoarse = append(dataCoarse, v)
			all = append(all, v)
		}

		hFine := FromValues(tc.fine, dataFine)
		hCoarse := FromValues(tc.coarse, dataCoarse)
		merged := Merge(hFine, hCoarse)

		if merged.Scale != tc.coarse {
			t.Fatalf("merge(scale%d,scale%d): target scale = %d, want %d (coarser)",
				tc.fine, tc.coarse, merged.Scale, tc.coarse)
		}
		if merged.Count != uint64(len(all)) {
			t.Fatalf("merged count = %d, want %d", merged.Count, len(all))
		}

		gamma := Gamma(tc.coarse)
		tolerance := 1.5 * gamma
		for _, q := range quantiles {
			est := merged.Quantile(q)
			truth := trueQuantile(all, q)
			if e := relErr(est, truth); e > tolerance {
				t.Errorf("merge(scale%d,scale%d) q=%.2f: est=%.4f truth=%.4f relErr=%.4f > %.4f (1.5*gamma, gamma=%.4f)",
					tc.fine, tc.coarse, q, est, truth, e, tolerance, gamma)
			}
		}
	}
}

// TestQuantileAccuracyVsRaw asserts that a single-scale sketch answers quantiles
// within the scale's gamma guarantee of the raw data.
func TestQuantileAccuracyVsRaw(t *testing.T) {
	for _, scale := range []int32{2, 4, 6} {
		rng := rand.New(rand.NewSource(int64(scale)))
		var data []float64
		for i := 0; i < 20000; i++ {
			data = append(data, math.Exp(rng.NormFloat64()*1.2+2))
		}
		h := FromValues(scale, data)
		gamma := Gamma(scale)
		tolerance := 1.5 * gamma
		for _, q := range []float64{0.05, 0.25, 0.5, 0.75, 0.9, 0.99} {
			est := h.Quantile(q)
			truth := trueQuantile(data, q)
			if e := relErr(est, truth); e > tolerance {
				t.Errorf("scale=%d q=%.2f: est=%.4f truth=%.4f relErr=%.4f > %.4f",
					scale, q, est, truth, e, tolerance)
			}
		}
	}
}

// TestDownscaleExactNesting verifies that down-scaling a fine sketch produces the
// exact same buckets as binning the same data directly at the coarse scale —
// because coarse boundaries are a strict subset of fine boundaries.
func TestDownscaleExactNesting(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	var data []float64
	for i := 0; i < 3000; i++ {
		data = append(data, math.Exp(rng.NormFloat64()+2))
	}
	fine := FromValues(7, data)
	direct := FromValues(3, data)

	downscaled := fine.Positive.downscale(7 - 3)
	if downscaled.total() != direct.Positive.total() {
		t.Fatalf("downscaled total %d != direct total %d", downscaled.total(), direct.Positive.total())
	}
	// Compare bucket-by-bucket on the union of absolute indices.
	get := func(b Buckets, idx int32) uint64 {
		p := int(idx - b.Offset)
		if p < 0 || p >= len(b.Counts) {
			return 0
		}
		return b.Counts[p]
	}
	lo := downscaled.Offset
	if direct.Positive.Offset < lo {
		lo = direct.Positive.Offset
	}
	hi := downscaled.Offset + int32(len(downscaled.Counts))
	if e := direct.Positive.Offset + int32(len(direct.Positive.Counts)); e > hi {
		hi = e
	}
	for idx := lo; idx < hi; idx++ {
		if a, b := get(downscaled, idx), get(direct.Positive, idx); a != b {
			t.Errorf("bucket %d: downscaled=%d direct=%d (nesting not exact)", idx, a, b)
		}
	}
}
