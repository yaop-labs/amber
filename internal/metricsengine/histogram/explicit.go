package histogram

import (
	"math"
	"sort"
)

// ExplicitBucketHistogram is an OTLP explicit-bucket histogram. Bounds are the
// sorted "le" boundaries; Counts has len(Bounds)+1 entries, the last being the
// +Inf overflow bucket. Boundaries are common to a storage block and stored once
// (see block.go); only the per-tick integer counts vary.
type ExplicitBucketHistogram struct {
	Bounds    []float64 `json:"bounds"`
	Counts    []uint64  `json:"counts"`
	Sum       float64   `json:"sum"`
	Count     uint64    `json:"count"`
	Min       float64   `json:"min"`
	Max       float64   `json:"max"`
	HasMinMax bool      `json:"has_min_max"`
}

// NewExplicit returns an empty explicit-bucket histogram for the given bounds.
func NewExplicit(bounds []float64) *ExplicitBucketHistogram {
	return &ExplicitBucketHistogram{
		Bounds: append([]float64(nil), bounds...),
		Counts: make([]uint64, len(bounds)+1),
	}
}

// ExplicitFromValues bins raw values into the given explicit bounds.
func ExplicitFromValues(bounds []float64, values []float64) *ExplicitBucketHistogram {
	h := NewExplicit(bounds)
	for _, v := range values {
		h.Observe(v)
	}
	return h
}

// Observe records a single value into its bucket and updates the synopsis.
func (h *ExplicitBucketHistogram) Observe(v float64) {
	if math.IsNaN(v) {
		return
	}
	h.Count++
	h.Sum += v
	if !h.HasMinMax {
		h.Min, h.Max, h.HasMinMax = v, v, true
	} else {
		if v < h.Min {
			h.Min = v
		}
		if v > h.Max {
			h.Max = v
		}
	}
	// First bucket whose upper bound (le) >= v; overflow bucket otherwise.
	i := sort.SearchFloat64s(h.Bounds, v)
	h.Counts[i]++
}

// boundsEqual reports whether two boundary slices are identical.
func boundsEqual(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// MergeExplicit adds the counts/synopsis of src into dst. Both must share
// identical bounds (explicit-bucket histograms are only re-aggregatable when
// their boundaries match). Returns false if bounds differ.
func MergeExplicit(dst, src *ExplicitBucketHistogram) bool {
	if dst == nil || src == nil || !boundsEqual(dst.Bounds, src.Bounds) {
		return false
	}
	for i, c := range src.Counts {
		dst.Counts[i] += c
	}
	dst.Sum += src.Sum
	dst.Count += src.Count
	if src.HasMinMax {
		if !dst.HasMinMax {
			dst.Min, dst.Max, dst.HasMinMax = src.Min, src.Max, true
		} else {
			if src.Min < dst.Min {
				dst.Min = src.Min
			}
			if src.Max > dst.Max {
				dst.Max = src.Max
			}
		}
	}
	return true
}

// MergeExplicitAll merges histograms sharing identical bounds into one. Returns
// nil if there is nothing to merge.
func MergeExplicitAll(hists []*ExplicitBucketHistogram) *ExplicitBucketHistogram {
	var out *ExplicitBucketHistogram
	for _, h := range hists {
		if h == nil {
			continue
		}
		if out == nil {
			out = NewExplicit(h.Bounds)
		}
		if !MergeExplicit(out, h) {
			return out // bounds mismatch: stop, return what we have
		}
	}
	return out
}

// Quantile estimates the q-th quantile via classic linear interpolation within
// the bucket containing the target rank (Prometheus histogram_quantile model).
func (h *ExplicitBucketHistogram) Quantile(q float64) float64 {
	if h.Count == 0 {
		return math.NaN()
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	rank := q * float64(h.Count)

	var cum uint64
	for i, c := range h.Counts {
		cum += c
		if float64(cum) < rank {
			continue
		}
		// Bucket i contains the rank. Bucket i spans (lower, upper].
		var lower, upper float64
		if i == 0 {
			lower = math.Inf(-1)
		} else {
			lower = h.Bounds[i-1]
		}
		if i == len(h.Bounds) {
			upper = math.Inf(1)
		} else {
			upper = h.Bounds[i]
		}
		// Clamp open-ended edge buckets to the observed min/max so estimates stay
		// finite, matching Prometheus' handling of the first/last bucket.
		if math.IsInf(lower, -1) {
			if h.HasMinMax {
				lower = h.Min
			} else {
				lower = upper
			}
		}
		if math.IsInf(upper, 1) {
			if h.HasMinMax {
				upper = h.Max
			} else {
				upper = lower
			}
		}
		// Linear interpolation within the bucket by where rank falls in [start,end).
		start := float64(cum - c)
		frac := 0.0
		if c > 0 {
			frac = (rank - start) / float64(c)
		}
		return lower + (upper-lower)*frac
	}
	return h.Max
}
