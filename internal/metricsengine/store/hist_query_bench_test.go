package store

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// hist_query_bench_test.go reproduces the metrics-campaign qm3 quantile path at
// scale: ~10k exp-histogram series over ~180 ticks sealed into ~30 blocks. The
// campaign saw qm3-quantile p50 ~1.27s - slower than VM/prom/mimir and untouched
// by the scalar range-step work. The single-block histogram benches in the
// histogram package don't exercise the multi-block CollectExp path this hits.
//
// Scale reuses the scalar knobs where sensible:
//
//	AMBER_HIST_SERIES (default 10000)
//	AMBER_BENCH_TICKS (default 180)   -- shared with the scalar fixture
//	AMBER_BENCH_FLUSH (default 6)      -- ticks/flush -> ~30 sealed blocks

type histBenchFixture struct {
	st       *Store
	seriesN  int
	ticks    int
	baseTS   int64
	lastTS   int64
	blocks   int
	tickSpan int64
}

var (
	histFixtureOnce sync.Once
	histFixture     *histBenchFixture
	histFixtureDir  string
)

func histBenchLabelSets(n int) []model.LabelSet {
	out := make([]model.LabelSet, n)
	for i := range out {
		out[i] = model.LabelSet{
			{Name: model.MetricNameLabel, Value: "request_latency_seconds"},
			{Name: "route", Value: fmt.Sprintf("route-%04d", i%1000)},
			{Name: "service", Value: fmt.Sprintf("svc-%02d", i%10)},
		}
	}
	return out
}

// histSketchPool builds a reusable pool of distinct exp-histograms so the
// fixture's 10kx180 ticks don't each pay an observe loop, while keeping the
// encoded bucket counts (and thus decode/merge cost) realistic.
func histSketchPool(size int) []*histogram.ExponentialHistogram {
	rng := rand.New(rand.NewSource(7))
	pool := make([]*histogram.ExponentialHistogram, size)
	for i := range pool {
		h := histogram.NewExponential(5)
		for k := 0; k < 64; k++ {
			h.Observe(math.Exp(rng.NormFloat64()*1.2 + 3))
		}
		pool[i] = h
	}
	return pool
}

func buildHistFixture(tb testing.TB) *histBenchFixture {
	histFixtureOnce.Do(func() {
		seriesN := benchEnvInt("AMBER_HIST_SERIES", 10_000)
		ticks := benchEnvInt("AMBER_BENCH_TICKS", 180)
		flushEvery := benchEnvInt("AMBER_BENCH_FLUSH", 6)

		tmpBase := os.Getenv("TMPDIR")
		if tmpBase == "" {
			tmpBase = "/var/tmp"
		}
		dir, err := os.MkdirTemp(tmpBase, "amber-hist-bench-")
		if err != nil {
			tb.Fatal(err)
		}
		histFixtureDir = dir
		st, err := OpenWithOptions(dir, Options{})
		if err != nil {
			tb.Fatal(err)
		}
		labels := histBenchLabelSets(seriesN)
		pool := histSketchPool(257)
		base := int64(1_700_000_000_000)
		for tick := 0; tick < ticks; tick++ {
			ts := base + int64(tick)*tickIntervalMS
			for lo := 0; lo < seriesN; lo += 2000 {
				batch := make([]engine.SketchSample, 0, 2000)
				for j := lo; j < lo+2000 && j < seriesN; j++ {
					batch = append(batch, engine.SketchSample{
						Labels:    labels[j],
						Timestamp: ts,
						Exp:       pool[(j+tick)%len(pool)],
					})
				}
				if _, err := st.AppendSketches(batch); err != nil {
					tb.Fatal(err)
				}
			}
			if (tick+1)%flushEvery == 0 {
				if _, err := st.Flush(); err != nil {
					tb.Fatal(err)
				}
			}
		}
		if st.engine.BufferedSketchSamples() > 0 {
			if _, err := st.Flush(); err != nil {
				tb.Fatal(err)
			}
		}
		hs, err := st.HistStats()
		if err != nil {
			tb.Fatal(err)
		}
		histFixture = &histBenchFixture{
			st:       st,
			seriesN:  seriesN,
			ticks:    ticks,
			baseTS:   base,
			lastTS:   base + int64(ticks-1)*tickIntervalMS,
			blocks:   hs.Blocks,
			tickSpan: int64(ticks-1) * tickIntervalMS,
		}
		tb.Logf("hist fixture built: series=%d ticks=%d blocks=%d", seriesN, ticks, hs.Blocks)
	})
	return histFixture
}

// BenchmarkHistogramQuantile_qm3 is the campaign qm3-quantile: p99 of
// request_latency over a 5-minute window ending at the last tick (no grouping).
func BenchmarkHistogramQuantile_qm3(b *testing.B) {
	f := buildHistFixture(b)
	sel := index.NewSelector(index.MetricName("request_latency_seconds"))
	tr := histogram.TimeRange{Start: f.lastTS - 5*60_000, End: f.lastTS}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.st.HistogramQuantile(sel, 0.99, tr); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHistogramQuantileBy_qm3 mirrors the obsbench qm3-quantile exactly:
// p99 grouped by service (10 groups) over a 5-minute window (metricsWindow) at
// an instant eval point - here the last tick. This is the multi-block CollectExp
// path the campaign drove (p50 ~1.27s).
func BenchmarkHistogramQuantileBy_qm3(b *testing.B) {
	f := buildHistFixture(b)
	sel := index.NewSelector(index.MetricName("request_latency_seconds"))
	tr := histogram.TimeRange{Start: f.lastTS - 5*60_000, End: f.lastTS}
	by := []string{"service"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := f.st.HistogramQuantileBy(sel, 0.99, tr, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty result")
		}
	}
}
