package histogram

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

// buildDataset returns `ticks` exp-histograms at the given scale, each from
// `perTick` lognormal samples, plus the raw values (for the decode-all baseline)
// and the count of active buckets.
func buildDataset(scale int32, ticks, perTick int, seed int64) (sketches []*ExponentialHistogram, raw []float64) {
	rng := rand.New(rand.NewSource(seed))
	for tk := 0; tk < ticks; tk++ {
		h := NewExponential(scale)
		for i := 0; i < perTick; i++ {
			v := math.Exp(rng.NormFloat64()*1.2 + 3)
			h.Observe(v)
			raw = append(raw, v)
		}
		sketches = append(sketches, h)
	}
	return sketches, raw
}

// expandToPoints reconstructs raw-ish points from sketches by emitting each
// bucket's midpoint `count` times — the work a naive decode-all-points query
// would do before sorting.
func expandToPoints(sketches []*ExponentialHistogram) []float64 {
	var pts []float64
	for _, h := range sketches {
		for i, c := range h.Positive.Counts {
			if c == 0 {
				continue
			}
			mid := midpoint(h.Scale, h.Positive.Offset+int32(i))
			for k := uint64(0); k < c; k++ {
				pts = append(pts, mid)
			}
		}
		for k := uint64(0); k < h.ZeroCount; k++ {
			pts = append(pts, 0)
		}
		for i, c := range h.Negative.Counts {
			if c == 0 {
				continue
			}
			mid := -midpoint(h.Scale, h.Negative.Offset+int32(i))
			for k := uint64(0); k < c; k++ {
				pts = append(pts, mid)
			}
		}
	}
	return pts
}

// TestBenchByteComparison reports sketch bytes vs the bucket-series equivalent
// (the Prometheus-style per-le counter series). Run with -v to see the numbers.
func TestBenchByteComparison(t *testing.T) {
	const ticks, perTick = 60, 2000
	sketches, _ := buildDataset(4, ticks, perTick, 42)

	// Sketch storage: the actual block payload bytes.
	series := ExpSeries{ID: 1, Labels: lbls("__name__", "lat"), Sketches: sketches}
	for i := range sketches {
		series.Timestamps = append(series.Timestamps, int64(i*60))
	}
	sketchBytes := len(encodeExpPayload(series))

	// Bucket-series equivalent: each active bucket becomes its own (ts,count)
	// counter sample per tick. 16 bytes/sample (8 ts + 8 count) is generous to
	// the baseline (real Prometheus also pays per-series label overhead).
	bucketSeriesBytes := 0
	for _, h := range sketches {
		active := 0
		for _, c := range h.Positive.Counts {
			if c != 0 {
				active++
			}
		}
		for _, c := range h.Negative.Counts {
			if c != 0 {
				active++
			}
		}
		bucketSeriesBytes += active * 16
	}

	ratio := float64(bucketSeriesBytes) / float64(sketchBytes)
	t.Logf("sketch payload: %d bytes", sketchBytes)
	t.Logf("bucket-series equivalent: %d bytes", bucketSeriesBytes)
	t.Logf("compression vs bucket-series: %.2fx", ratio)
	if sketchBytes == 0 {
		t.Fatal("sketch payload is empty")
	}
}

func BenchmarkHistogramQuantileMergePath(b *testing.B) {
	dir := b.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		b.Fatal(err)
	}
	sketches, _ := buildDataset(5, 120, 2000, 11)
	series := ExpSeries{ID: 1, Labels: lbls("__name__", "lat")}
	for i, sk := range sketches {
		series.Timestamps = append(series.Timestamps, int64(i*60))
		series.Sketches = append(series.Sketches, sk)
	}
	if _, err := s.WriteBlock([]ExpSeries{series}, nil); err != nil {
		b.Fatal(err)
	}
	sel := index.NewSelector(index.MetricName("lat"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.HistogramQuantile(sel, 0.99, fullRange()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHistogramQuantileDecodeAllBaseline(b *testing.B) {
	sketches, _ := buildDataset(5, 120, 2000, 11)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Naive baseline: materialize all raw points and sort to find the quantile.
		pts := expandToPoints(sketches)
		sort.Float64s(pts)
		idx := int(math.Ceil(0.99*float64(len(pts)))) - 1
		if idx < 0 {
			idx = 0
		}
		_ = pts[idx]
	}
}
