// Package histogram implements OTLP histogram storage and query.
// Exponential histograms are stored as mergeable sketches in their native
// scale, offset, and count layout.
package histogram

import "math"

// Buckets is the OTLP positive/negative bucket layout: a dense slice of counts
// starting at an absolute bucket index Offset. The bucket at absolute index
// (Offset+i) covers the value range (base^(Offset+i), base^(Offset+i+1)] where
// base = 2^(2^-scale).
type Buckets struct {
	Offset int32    `json:"offset"`
	Counts []uint64 `json:"counts"`
}

// ExponentialHistogram is semantically identical to an OTLP ExponentialHistogram
// data point. It round-trips losslessly.
type ExponentialHistogram struct {
	Scale         int32   `json:"scale"`
	ZeroThreshold float64 `json:"zero_threshold"`
	ZeroCount     uint64  `json:"zero_count"`
	Positive      Buckets `json:"positive"`
	Negative      Buckets `json:"negative"`
	Sum           float64 `json:"sum"`
	Count         uint64  `json:"count"`
	Min           float64 `json:"min"`
	Max           float64 `json:"max"`
	HasMinMax     bool    `json:"has_min_max"`
}

// Base returns the bucket base 2^(2^-scale) for the given scale.
func Base(scale int32) float64 { return math.Exp2(math.Exp2(-float64(scale))) }

// Gamma returns the relative-error guarantee (base-1)/(base+1) at the given scale.
func Gamma(scale int32) float64 {
	b := Base(scale)
	return (b - 1) / (b + 1)
}

// indexForValue maps a strictly-positive magnitude to its absolute bucket index
// at the given scale, matching OTLP: bucket i covers (base^i, base^(i+1)], so
// index = ceil(log_base(v)) - 1 = ceil(log2(v) * 2^scale) - 1.
func indexForValue(scale int32, v float64) int32 {
	scaled := math.Log2(v) * math.Exp2(float64(scale))
	return int32(math.Ceil(scaled)) - 1
}

// midpoint returns the geometric midpoint sqrt(lo*hi) = base^(index+0.5) of a
// bucket; this is the quantile estimate for a value falling in that bucket.
func midpoint(scale int32, index int32) float64 {
	return math.Exp2((float64(index) + 0.5) * math.Exp2(-float64(scale)))
}

// NewExponential returns an empty histogram at the given scale.
func NewExponential(scale int32) *ExponentialHistogram {
	return &ExponentialHistogram{Scale: scale}
}

// FromValues builds an exp-histogram at the given scale by observing each value.
// Used to construct sketches from known raw data (tests, ingest of raw points).
func FromValues(scale int32, values []float64) *ExponentialHistogram {
	h := NewExponential(scale)
	for _, v := range values {
		h.Observe(v)
	}
	return h
}

// Observe records a single value, updating buckets and synopsis fields.
func (h *ExponentialHistogram) Observe(v float64) {
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
	if math.Abs(v) <= h.ZeroThreshold {
		h.ZeroCount++
		return
	}
	idx := indexForValue(h.Scale, math.Abs(v))
	if v > 0 {
		h.Positive.add(idx, 1)
	} else {
		h.Negative.add(idx, 1)
	}
}

// add increments the bucket at the given absolute index by count, growing the
// dense slice on either side as needed.
func (b *Buckets) add(index int32, count uint64) {
	if count == 0 {
		return
	}
	if len(b.Counts) == 0 {
		b.Offset = index
		b.Counts = []uint64{count}
		return
	}
	if index < b.Offset {
		grow := int(b.Offset - index)
		b.Counts = append(make([]uint64, grow), b.Counts...)
		b.Offset = index
	}
	pos := int(index - b.Offset)
	if pos >= len(b.Counts) {
		b.Counts = append(b.Counts, make([]uint64, pos-len(b.Counts)+1)...)
	}
	b.Counts[pos] += count
}

// clone returns a deep copy of the bucket list.
func (b Buckets) clone() Buckets {
	if len(b.Counts) == 0 {
		return Buckets{Offset: b.Offset}
	}
	return Buckets{Offset: b.Offset, Counts: append([]uint64(nil), b.Counts...)}
}

// total returns the sum of all bucket counts.
func (b Buckets) total() uint64 {
	var n uint64
	for _, c := range b.Counts {
		n += c
	}
	return n
}

// downscale collapses buckets to a coarser scale.
// OTLP exponential bucket boundaries nest by powers of two, so each absolute
// bucket index maps to index>>by at the coarser scale.
func (b Buckets) downscale(by int32) Buckets {
	if by <= 0 || len(b.Counts) == 0 {
		return b.clone()
	}
	collapsed := make(map[int32]uint64, len(b.Counts))
	var minIdx, maxIdx int32
	first := true
	for i, c := range b.Counts {
		if c == 0 {
			continue
		}
		idx := (b.Offset + int32(i)) >> by
		collapsed[idx] += c
		if first {
			minIdx, maxIdx, first = idx, idx, false
		} else {
			if idx < minIdx {
				minIdx = idx
			}
			if idx > maxIdx {
				maxIdx = idx
			}
		}
	}
	if first {
		return Buckets{}
	}
	out := Buckets{Offset: minIdx, Counts: make([]uint64, maxIdx-minIdx+1)}
	for idx, c := range collapsed {
		out.Counts[idx-minIdx] = c
	}
	return out
}

// mergeInto adds every nonzero bucket of src into b (indices must be at the same
// scale).
func (b *Buckets) mergeInto(src Buckets) {
	for i, c := range src.Counts {
		if c != 0 {
			b.add(src.Offset+int32(i), c)
		}
	}
}

// Clone returns a deep copy of the histogram.
func (h *ExponentialHistogram) Clone() *ExponentialHistogram {
	if h == nil {
		return nil
	}
	out := *h
	out.Positive = h.Positive.clone()
	out.Negative = h.Negative.clone()
	return &out
}

// Merge combines two histograms at the coarsest input scale.
// The finer histogram is downscaled before bucket counts are added.
func Merge(a, b *ExponentialHistogram) *ExponentialHistogram {
	return MergeAll([]*ExponentialHistogram{a, b})
}

// MergeAll merges histograms into one sketch at the coarsest input scale.
// Nil inputs are skipped.
func MergeAll(hists []*ExponentialHistogram) *ExponentialHistogram {
	target := int32(math.MaxInt32)
	any := false
	for _, h := range hists {
		if h == nil {
			continue
		}
		if h.Scale < target {
			target = h.Scale
		}
		any = true
	}
	if !any {
		return nil
	}
	out := &ExponentialHistogram{Scale: target}
	for _, h := range hists {
		if h == nil {
			continue
		}
		by := h.Scale - target
		out.Positive.mergeInto(h.Positive.downscale(by))
		out.Negative.mergeInto(h.Negative.downscale(by))
		out.ZeroCount += h.ZeroCount
		out.Count += h.Count
		out.Sum += h.Sum
		if h.ZeroThreshold > out.ZeroThreshold {
			out.ZeroThreshold = h.ZeroThreshold
		}
		if h.HasMinMax {
			if !out.HasMinMax {
				out.Min, out.Max, out.HasMinMax = h.Min, h.Max, true
			} else {
				if h.Min < out.Min {
					out.Min = h.Min
				}
				if h.Max > out.Max {
					out.Max = h.Max
				}
			}
		}
	}
	return out
}

// Quantile estimates the q-th quantile (0..1) of the distribution by walking the
// merged buckets in value order (most-negative -> zero -> most-positive),
// accumulating counts, and returning the geometric midpoint of the bucket that
// contains the target rank.
func (h *ExponentialHistogram) Quantile(q float64) float64 {
	total := h.Count
	if total == 0 {
		return math.NaN()
	}
	if q < 0 {
		q = 0
	}
	if q > 1 {
		q = 1
	}
	rank := q * float64(total)

	var cum float64
	lastValue := math.NaN()
	have := false

	// Negative buckets hold negative values; the most-negative value lives in the
	// highest absolute index, so walk them from high index down.
	for i := len(h.Negative.Counts) - 1; i >= 0; i-- {
		c := h.Negative.Counts[i]
		if c == 0 {
			continue
		}
		idx := h.Negative.Offset + int32(i)
		val := -midpoint(h.Scale, idx)
		lastValue, have = val, true
		cum += float64(c)
		if cum >= rank {
			return val
		}
	}
	if h.ZeroCount > 0 {
		lastValue, have = 0, true
		cum += float64(h.ZeroCount)
		if cum >= rank {
			return 0
		}
	}
	for i := 0; i < len(h.Positive.Counts); i++ {
		c := h.Positive.Counts[i]
		if c == 0 {
			continue
		}
		idx := h.Positive.Offset + int32(i)
		val := midpoint(h.Scale, idx)
		lastValue, have = val, true
		cum += float64(c)
		if cum >= rank {
			return val
		}
	}
	if have {
		return lastValue
	}
	return math.NaN()
}
