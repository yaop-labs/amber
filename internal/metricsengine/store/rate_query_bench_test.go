package store

import (
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

// rate_query_bench_test.go reproduces the metrics-campaign query wall:
// ~100k series, ~18M scalar samples, ~30 sealed blocks. The campaign saw
// qm1-rate 19.9s, qm2-range-rate 71-101s, qm4-groupby 17.3s versus 130-190ms
// on the 6-tick smoke. The 6-tick smoke produced 1-4 blocks; the campaign ~30.
//
// Scale is env-tunable so the same fixture can be shrunk for fast iteration:
//
//	AMBER_BENCH_SERIES (default 100000)
//	AMBER_BENCH_TICKS  (default 180)   -- 10s apart -> 30m span
//	AMBER_BENCH_FLUSH  (default 6)      -- flush cadence -> ticks/flush blocks
//
// Build the full fixture once and share it across every Benchmark in the
// package (18M AppendBatch samples is ~2min); each benchmark only re-runs the
// read path.

// TestMain removes the shared on-disk bench fixture after the whole package
// run, so /var/tmp/amber-rate-bench-* dirs don't accumulate across runs.
func TestMain(m *testing.M) {
	code := m.Run()
	if fixture != nil {
		fixture.st.Close()
	}
	if fixtureDir != "" {
		os.RemoveAll(fixtureDir)
	}
	if histFixture != nil {
		histFixture.st.Close()
	}
	if histFixtureDir != "" {
		os.RemoveAll(histFixtureDir)
	}
	os.Exit(code)
}

const tickIntervalMS = 10_000

type benchFixture struct {
	st      *Store
	seriesN int
	ticks   int
	baseTS  int64
	lastTS  int64
	blocks  int
}

var (
	fixtureOnce sync.Once
	fixture     *benchFixture
	fixtureDir  string
)

func benchEnvInt(name string, def int) int {
	if raw := os.Getenv(name); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// buildFixture ingests seriesN counter series over `ticks` 10s-spaced ticks,
// flushing every `flushEvery` ticks so the store ends with ~ticks/flushEvery
// sealed blocks, each holding the full series cardinality. Mirrors the I1
// workload shape (single metric, 10 services, 1000 routes) used by obsbench.
func buildFixture(tb testing.TB) *benchFixture {
	fixtureOnce.Do(func() {
		seriesN := benchEnvInt("AMBER_BENCH_SERIES", 100_000)
		ticks := benchEnvInt("AMBER_BENCH_TICKS", 180)
		flushEvery := benchEnvInt("AMBER_BENCH_FLUSH", 6)

		// /tmp is tmpfs (RAM) on Fedora - a campaign-scale fixture is ~2GB and
		// several leaked runs once exhausted RAM and froze the desktop. Write
		// to /var/tmp (real disk) and clean up in TestMain. Honor TMPDIR if set.
		tmpBase := os.Getenv("TMPDIR")
		if tmpBase == "" {
			tmpBase = "/var/tmp"
		}
		dir, err := os.MkdirTemp(tmpBase, "amber-rate-bench-")
		if err != nil {
			tb.Fatal(err)
		}
		fixtureDir = dir
		st, err := OpenWithOptions(dir, Options{})
		if err != nil {
			tb.Fatal(err)
		}
		labels := benchLabelSets(seriesN)
		base := time.Now().UnixMilli()
		start := time.Now()
		for tick := 0; tick < ticks; tick++ {
			ts := base + int64(tick)*tickIntervalMS
			for lo := 0; lo < seriesN; lo += 2000 {
				batch := make([]model.Sample, 0, 2000)
				for j := lo; j < lo+2000 && j < seriesN; j++ {
					batch = append(batch, model.Sample{
						Labels: labels[j], Type: model.MetricTypeCounter,
						Timestamp: ts, Value: int64(tick * 10),
					})
				}
				if _, err := st.AppendBatch(batch); err != nil {
					tb.Fatal(err)
				}
			}
			if (tick+1)%flushEvery == 0 {
				if _, err := st.Flush(); err != nil {
					tb.Fatal(err)
				}
			}
		}
		if st.BufferedSamples() > 0 {
			if _, err := st.Flush(); err != nil {
				tb.Fatal(err)
			}
		}
		fixture = &benchFixture{
			st:      st,
			seriesN: seriesN,
			ticks:   ticks,
			baseTS:  base,
			lastTS:  base + int64(ticks-1)*tickIntervalMS,
			blocks:  st.BlockCount(),
		}
		tb.Logf("fixture built: series=%d ticks=%d blocks=%d samples=%d in %s",
			seriesN, ticks, fixture.blocks, seriesN*ticks, time.Since(start).Round(time.Millisecond))
	})
	return fixture
}

func benchRangeSelector(by string, window time.Duration) (query.RangeSelector, string) {
	sel := index.NewSelector(index.MetricName("http_requests_total"))
	return query.RangeSelector{Selector: sel, Window: window}, by
}

// BenchmarkRateByLabelRange_qm1 is the campaign qm1-rate: instant rate of
// http_requests_total by service (10 groups), 1m window at the last tick.
func BenchmarkRateByLabelRange_qm1(b *testing.B) {
	f := buildFixture(b)
	rs, by := benchRangeSelector("service", time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := f.st.RateByLabelRange(rs, f.lastTS, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty result")
		}
	}
}

// BenchmarkRateByLabelRange_qm4 is the campaign qm4-groupby: instant rate by
// route (1000 groups), 1m window. Same store method as qm1, wider fan-out key.
func BenchmarkRateByLabelRange_qm4(b *testing.B) {
	f := buildFixture(b)
	rs, by := benchRangeSelector("route", time.Minute)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := f.st.RateByLabelRange(rs, f.lastTS, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(out) == 0 {
			b.Fatal("empty result")
		}
	}
}

// BenchmarkRateByLabelRangeSteps_qm2 is the campaign qm2-range-rate: rate by
// service over a range, 1m window, 45s step. This is the worst campaign case
// (71-101s server-side -> client EOF).
func BenchmarkRateByLabelRangeSteps_qm2(b *testing.B) {
	f := buildFixture(b)
	rs, by := benchRangeSelector("service", time.Minute)
	// 5-minute query range ending at the last tick, 45s step.
	from := f.lastTS - 5*60_000
	step := 45 * time.Second
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := f.st.RateByLabelRangeSteps(rs, from, f.lastTS, step, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("empty result")
		}
	}
}

// BenchmarkRateByLabelRangeSteps_qm2_campaign mirrors the obsbench qm2-range-rate
// query exactly: range = half the ingest span, step = (span/2)/20, 1m window
// (see benchmarks/obsbench/metrics_queries.go ScenarioQM2Range). For the 180-tick
// fixture that is a ~15m range over ~15 in-window blocks - the query that EOF'd
// the campaign at 71-101s.
func BenchmarkRateByLabelRangeSteps_qm2_campaign(b *testing.B) {
	f := buildFixture(b)
	rs, by := benchRangeSelector("service", time.Minute)
	span := f.lastTS - f.baseTS
	from := f.baseTS
	to := from + span/2
	step := (time.Duration(span/2/20) * time.Millisecond).Round(time.Second)
	if step < time.Second {
		step = time.Second
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := f.st.RateByLabelRangeSteps(rs, from, to, step, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("empty result")
		}
	}
}

// BenchmarkRateByLabelRangeSteps_qm2_wide drives the full ingest span (every
// block in-window) as a worst-case upper bound - wider than the campaign query.
func BenchmarkRateByLabelRangeSteps_qm2_wide(b *testing.B) {
	f := buildFixture(b)
	rs, by := benchRangeSelector("service", time.Minute)
	from := f.baseTS
	step := 45 * time.Second
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := f.st.RateByLabelRangeSteps(rs, from, f.lastTS, step, by)
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("empty result")
		}
	}
}
