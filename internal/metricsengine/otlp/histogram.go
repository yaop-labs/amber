package otlp

import (
	"maps"
	"sort"
	"strconv"

	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// ExponentialHistogramPoint mirrors an OTLP ExponentialHistogram data point. It
// converts to/from histogram.ExponentialHistogram losslessly so a data point
// round-trips through storage byte-identical.
type ExponentialHistogramPoint struct {
	Name           string
	Timestamp      int64
	Attributes     map[string]string
	Scale          int32
	ZeroCount      uint64
	ZeroThreshold  float64
	PositiveOffset int32
	PositiveCounts []uint64
	NegativeOffset int32
	NegativeCounts []uint64
	Sum            float64
	Count          uint64
	Min            float64
	Max            float64
	HasMinMax      bool
}

// ExplicitHistogramPoint mirrors an OTLP (explicit-bucket) Histogram data point.
type ExplicitHistogramPoint struct {
	Name           string
	Timestamp      int64
	Attributes     map[string]string
	ExplicitBounds []float64
	BucketCounts   []uint64 // len == len(ExplicitBounds)+1
	Sum            float64
	Count          uint64
	Min            float64
	Max            float64
	HasMinMax      bool
}

// Sketch converts the data point into the OTLP-native mergeable sketch.
func (p ExponentialHistogramPoint) Sketch() *histogram.ExponentialHistogram {
	return &histogram.ExponentialHistogram{
		Scale:         p.Scale,
		ZeroThreshold: p.ZeroThreshold,
		ZeroCount:     p.ZeroCount,
		Positive:      histogram.Buckets{Offset: p.PositiveOffset, Counts: cloneCounts(p.PositiveCounts)},
		Negative:      histogram.Buckets{Offset: p.NegativeOffset, Counts: cloneCounts(p.NegativeCounts)},
		Sum:           p.Sum,
		Count:         p.Count,
		Min:           p.Min,
		Max:           p.Max,
		HasMinMax:     p.HasMinMax,
	}
}

// ExponentialPointFromSketch is the inverse of Sketch; used to verify round-trip
// fidelity and to re-emit OTLP from stored data.
func ExponentialPointFromSketch(name string, timestamp int64, attrs map[string]string, h *histogram.ExponentialHistogram) ExponentialHistogramPoint {
	return ExponentialHistogramPoint{
		Name:           name,
		Timestamp:      timestamp,
		Attributes:     attrs,
		Scale:          h.Scale,
		ZeroCount:      h.ZeroCount,
		ZeroThreshold:  h.ZeroThreshold,
		PositiveOffset: h.Positive.Offset,
		PositiveCounts: cloneCounts(h.Positive.Counts),
		NegativeOffset: h.Negative.Offset,
		NegativeCounts: cloneCounts(h.Negative.Counts),
		Sum:            h.Sum,
		Count:          h.Count,
		Min:            h.Min,
		Max:            h.Max,
		HasMinMax:      h.HasMinMax,
	}
}

// SummaryPoint mirrors an OTLP Summary data point. Summary quantiles are NOT
// re-aggregatable, so they get no sketch: each quantile is stored as a plain
// gauge series (deliverable #3, "no special logic"). The quantile level rides on
// a "quantile" label, matching the Prometheus summary convention.
type SummaryPoint struct {
	Name       string
	Timestamp  int64
	Attributes map[string]string
	Quantiles  []SummaryQuantile
	Scale      int64 // float->int scale for the gauge codec; defaults to 1000
}

// SummaryQuantile is one (quantile, value) pair from a Summary data point.
type SummaryQuantile struct {
	Quantile float64
	Value    float64
}

// SummarySamples lowers a set of Summary points into plain gauge samples — one
// per quantile — reusing the existing gauge codec via Samples. There is no
// histogram-specific storage for summaries.
func SummarySamples(batch Batch, points []SummaryPoint) ([]model.Sample, error) {
	gaugePoints := make([]Point, 0, len(points))
	for _, p := range points {
		for _, q := range p.Quantiles {
			attrs := make(map[string]string, len(p.Attributes)+1)
			maps.Copy(attrs, p.Attributes)
			attrs["quantile"] = strconv.FormatFloat(q.Quantile, 'g', -1, 64)
			gaugePoints = append(gaugePoints, Point{
				Name:       p.Name,
				Kind:       MetricGauge,
				Timestamp:  p.Timestamp,
				Attributes: attrs,
				FloatValue: q.Value,
				NumberKind: NumberFloat,
				Scale:      p.Scale,
			})
		}
	}
	return Samples(Batch{
		ResourceAttributes: batch.ResourceAttributes,
		ScopeAttributes:    batch.ScopeAttributes,
		Points:             gaugePoints,
	})
}

// Histogram converts the explicit-bucket data point into a histogram value.
func (p ExplicitHistogramPoint) Histogram() *histogram.ExplicitBucketHistogram {
	return &histogram.ExplicitBucketHistogram{
		Bounds:    append([]float64(nil), p.ExplicitBounds...),
		Counts:    cloneCounts(p.BucketCounts),
		Sum:       p.Sum,
		Count:     p.Count,
		Min:       p.Min,
		Max:       p.Max,
		HasMinMax: p.HasMinMax,
	}
}

// ExponentialSeries groups exp-histogram points by their full label set into
// per-series sketch lists ready to hand to histogram.Store.WriteBlock. Points of
// the same series are ordered by timestamp.
func ExponentialSeries(batch Batch, points []ExponentialHistogramPoint) []histogram.ExpSeries {
	type acc struct {
		labels model.LabelSet
		ts     []int64
		sk     []*histogram.ExponentialHistogram
	}
	groups := make(map[uint64]*acc)
	order := make([]uint64, 0)
	for _, p := range points {
		labels := histogramLabels(batch, p.Name, p.Attributes)
		fp := labels.Fingerprint()
		g := groups[fp]
		if g == nil {
			g = &acc{labels: labels}
			groups[fp] = g
			order = append(order, fp)
		}
		g.ts = append(g.ts, p.Timestamp)
		g.sk = append(g.sk, p.Sketch())
	}
	out := make([]histogram.ExpSeries, 0, len(order))
	for i, fp := range order {
		g := groups[fp]
		sortByTimestamp(g.ts, g.sk)
		out = append(out, histogram.ExpSeries{
			ID:         uint64(i + 1),
			Labels:     g.labels,
			Timestamps: g.ts,
			Sketches:   g.sk,
		})
	}
	return out
}

// ExplicitSeries groups explicit-bucket points by label set into per-series
// vectors ready for histogram.Store.WriteBlock.
func ExplicitSeries(batch Batch, points []ExplicitHistogramPoint) []histogram.ExplicitSeries {
	type acc struct {
		labels model.LabelSet
		ts     []int64
		bk     []*histogram.ExplicitBucketHistogram
	}
	groups := make(map[uint64]*acc)
	order := make([]uint64, 0)
	for _, p := range points {
		labels := histogramLabels(batch, p.Name, p.Attributes)
		fp := labels.Fingerprint()
		g := groups[fp]
		if g == nil {
			g = &acc{labels: labels}
			groups[fp] = g
			order = append(order, fp)
		}
		g.ts = append(g.ts, p.Timestamp)
		g.bk = append(g.bk, p.Histogram())
	}
	out := make([]histogram.ExplicitSeries, 0, len(order))
	for i, fp := range order {
		g := groups[fp]
		sortExplicitByTimestamp(g.ts, g.bk)
		out = append(out, histogram.ExplicitSeries{
			ID:         uint64(i + 1),
			Labels:     g.labels,
			Timestamps: g.ts,
			Buckets:    g.bk,
		})
	}
	return out
}

func histogramLabels(batch Batch, name string, attrs map[string]string) model.LabelSet {
	labels := make(model.LabelSet, 0, len(batch.ResourceAttributes)+len(batch.ScopeAttributes)+len(attrs)+1)
	labels = appendAttrs(labels, "resource.", batch.ResourceAttributes)
	labels = appendAttrs(labels, "scope.", batch.ScopeAttributes)
	labels = appendAttrs(labels, "", attrs)
	labels = append(labels, model.Label{Name: model.MetricNameLabel, Value: name})
	return labels.Canonical()
}

func cloneCounts(c []uint64) []uint64 {
	if len(c) == 0 {
		return nil
	}
	return append([]uint64(nil), c...)
}

func sortByTimestamp(ts []int64, sk []*histogram.ExponentialHistogram) {
	idx := sortedIndex(ts)
	newTs := make([]int64, len(ts))
	newSk := make([]*histogram.ExponentialHistogram, len(sk))
	for i, j := range idx {
		newTs[i], newSk[i] = ts[j], sk[j]
	}
	copy(ts, newTs)
	copy(sk, newSk)
}

func sortExplicitByTimestamp(ts []int64, bk []*histogram.ExplicitBucketHistogram) {
	idx := sortedIndex(ts)
	newTs := make([]int64, len(ts))
	newBk := make([]*histogram.ExplicitBucketHistogram, len(bk))
	for i, j := range idx {
		newTs[i], newBk[i] = ts[j], bk[j]
	}
	copy(ts, newTs)
	copy(bk, newBk)
}

// sortedIndex returns the indices of ts in ascending timestamp order (stable).
func sortedIndex(ts []int64) []int {
	idx := make([]int, len(ts))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(i, j int) bool { return ts[idx[i]] < ts[idx[j]] })
	return idx
}
