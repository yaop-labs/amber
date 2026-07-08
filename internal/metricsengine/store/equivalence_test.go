package store

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

// equivalence_test.go is the query-path equivalence property test from the
// pre-alpha review backlog: the same dataset, laid out differently on disk,
// must answer every query identically. Layouts cover the distinct executor
// routes without re-implementing any query semantics in the test:
//
//   - head-only (no flush)            -> in-memory series scans
//   - single block                    -> the *InBlockWithDirectory fast paths
//   - multi-block, continuous ticks   -> the streamed exact path (windows straddle)
//   - multi-block, gapped groups      -> the range-step summary path
//   - multi-block + huge MaxSampleGap -> the exact path, forced
//
// A divergence here is precisely the "silent fast-path drift" failure mode the
// equality gate catches across systems, but in-process.

type equivDataset struct {
	name       string
	seriesN    int
	groups     int   // tick groups; >1 with gapMS creates non-straddling blocks
	ticksPer   int   // ticks per group
	gapMS      int64 // extra gap between groups (0 = continuous)
	tickMS     int64
	baseTS     int64
	samplesFor func() [][]model.Sample // [tick][]samples, built lazily
	lastTS     int64
	firstTS    int64
}

func makeEquivDataset(name string, seriesN, groups, ticksPer int, gapMS int64, seed int64) *equivDataset {
	d := &equivDataset{
		name: name, seriesN: seriesN, groups: groups, ticksPer: ticksPer,
		gapMS: gapMS, tickMS: 10_000, baseTS: 1_700_000_000_000,
	}
	rng := rand.New(rand.NewSource(seed))
	labels := make([]model.LabelSet, seriesN)
	jitter := make([]int64, seriesN)
	for i := range labels {
		labels[i] = model.LabelSet{
			{Name: model.MetricNameLabel, Value: "http_requests_total"},
			{Name: "route", Value: fmt.Sprintf("route-%03d", i%40)},
			{Name: "service", Value: fmt.Sprintf("svc-%02d", i%6)},
		}
		jitter[i] = rng.Int63n(2000) // per-series phase, keeps spacing regular
	}
	values := make([]int64, seriesN)
	increments := make([][]int64, groups*ticksPer)
	for t := range increments {
		increments[t] = make([]int64, seriesN)
		for i := range increments[t] {
			increments[t][i] = rng.Int63n(100)
		}
	}
	d.samplesFor = func() [][]model.Sample {
		out := make([][]model.Sample, 0, groups*ticksPer)
		clear(values)
		tickIdx := 0
		for g := range groups {
			groupBase := d.baseTS + int64(g)*(int64(ticksPer)*d.tickMS+gapMS)
			for k := range ticksPer {
				ts := groupBase + int64(k)*d.tickMS
				batch := make([]model.Sample, seriesN)
				for i := range seriesN {
					values[i] += increments[tickIdx][i]
					batch[i] = model.Sample{
						Labels: labels[i], Type: model.MetricTypeCounter,
						Timestamp: ts + jitter[i], Value: values[i],
					}
				}
				out = append(out, batch)
				d.lastTS = ts
				tickIdx++
			}
		}
		d.firstTS = d.baseTS
		return out
	}
	return d
}

// buildEquivStore lays ticks out with the given flush policy:
// flushEvery == 0 -> never flush (head-only); N -> flush every N ticks and at the end.
func buildEquivStore(t *testing.T, ticks [][]model.Sample, flushEvery int) *Store {
	t.Helper()
	st, err := OpenWithOptions(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for i, batch := range ticks {
		if _, err := st.AppendBatch(batch); err != nil {
			t.Fatal(err)
		}
		if flushEvery > 0 && (i+1)%flushEvery == 0 {
			if _, err := st.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if flushEvery > 0 && st.BufferedSamples() > 0 {
		if _, err := st.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// equivResults captures every operation under test for one store layout.
type equivResults struct {
	rateInstant     map[string]float64
	rateSteps       []query.FloatStep
	increaseInstant map[string]int64
	increaseSteps   []query.IntStep
	sum             map[string]int64
	aggregate       map[string]query.Aggregate
	aggregateSteps  []query.AggregateStep
}

func runEquivQueries(t *testing.T, st *Store, d *equivDataset, maxGap time.Duration) equivResults {
	t.Helper()
	sel := index.NewSelector(index.MetricName("http_requests_total"))
	rs := query.RangeSelector{Selector: sel, Window: 30 * time.Second, MaxSampleGap: maxGap}
	from := d.firstTS + 60_000
	to := d.lastTS
	step := 45 * time.Second

	var (
		res equivResults
		err error
	)
	if res.rateInstant, err = st.RateByLabelRange(rs, d.lastTS, "service"); err != nil {
		t.Fatal(err)
	}
	if res.rateSteps, err = st.RateByLabelRangeSteps(rs, from, to, step, "service"); err != nil {
		t.Fatal(err)
	}
	if res.increaseInstant, err = st.IncreaseByLabel(sel, rs.Options(d.lastTS), "service"); err != nil {
		t.Fatal(err)
	}
	if res.increaseSteps, err = st.IncreaseByLabelRangeSteps(rs, from, to, step, "service"); err != nil {
		t.Fatal(err)
	}
	opts := query.TimeRange(d.firstTS, d.lastTS)
	if res.sum, err = st.SumByLabel(sel, opts, "service"); err != nil {
		t.Fatal(err)
	}
	if res.aggregate, err = st.AggregateByLabel(sel, opts, "service"); err != nil {
		t.Fatal(err)
	}
	if res.aggregateSteps, err = st.AggregateByLabelRangeSteps(rs, from, to, step, "service"); err != nil {
		t.Fatal(err)
	}
	return res
}

func TestQueryPathEquivalence(t *testing.T) {
	datasets := []*equivDataset{
		makeEquivDataset("continuous", 240, 1, 48, 0, 1),
		makeEquivDataset("continuous-seed2", 240, 1, 48, 0, 2),
		// 10-minute gaps between groups: sealed blocks cannot straddle a 30s
		// window, so the multi-block layout takes the summary path.
		makeEquivDataset("gapped", 180, 4, 12, 600_000, 3),
	}
	for _, d := range datasets {
		t.Run(d.name, func(t *testing.T) {
			ticks := d.samplesFor()
			flushMulti := d.ticksPer
			if d.groups == 1 {
				flushMulti = 12 // continuous: 4 blocks of 12 ticks
			}

			headOnly := runEquivQueries(t, buildEquivStore(t, ticks, 0), d, 0)
			singleBlock := runEquivQueries(t, buildEquivStore(t, ticks, len(ticks)), d, 0)
			multiStore := buildEquivStore(t, ticks, flushMulti)
			multiBlock := runEquivQueries(t, multiStore, d, 0)
			multiExact := runEquivQueries(t, multiStore, d, time.Hour)

			if len(headOnly.rateInstant) == 0 || len(headOnly.rateSteps) == 0 || len(headOnly.sum) == 0 {
				t.Fatal("degenerate dataset: empty reference results")
			}
			compareEquiv(t, "single-block vs head", headOnly, singleBlock)
			compareEquiv(t, "multi-block vs head", headOnly, multiBlock)
			compareEquiv(t, "multi-exact vs head", headOnly, multiExact)
		})
	}
}

func compareEquiv(t *testing.T, ctx string, want, got equivResults) {
	t.Helper()
	compareFloatMaps(t, ctx+"/rateInstant", want.rateInstant, got.rateInstant)
	compareIntMaps(t, ctx+"/increaseInstant", want.increaseInstant, got.increaseInstant)
	compareIntMaps(t, ctx+"/sum", want.sum, got.sum)
	compareAggMaps(t, ctx+"/aggregate", want.aggregate, got.aggregate)

	if len(want.rateSteps) != len(got.rateSteps) {
		t.Fatalf("%s/rateSteps: len %d != %d", ctx, len(want.rateSteps), len(got.rateSteps))
	}
	for i := range want.rateSteps {
		if want.rateSteps[i].TimestampMillis != got.rateSteps[i].TimestampMillis {
			t.Fatalf("%s/rateSteps[%d]: ts %d != %d", ctx, i, want.rateSteps[i].TimestampMillis, got.rateSteps[i].TimestampMillis)
		}
		compareFloatMaps(t, fmt.Sprintf("%s/rateSteps[%d]", ctx, i), want.rateSteps[i].Values, got.rateSteps[i].Values)
	}
	if len(want.increaseSteps) != len(got.increaseSteps) {
		t.Fatalf("%s/increaseSteps: len %d != %d", ctx, len(want.increaseSteps), len(got.increaseSteps))
	}
	for i := range want.increaseSteps {
		compareIntMaps(t, fmt.Sprintf("%s/increaseSteps[%d]", ctx, i), want.increaseSteps[i].Values, got.increaseSteps[i].Values)
	}
	if len(want.aggregateSteps) != len(got.aggregateSteps) {
		t.Fatalf("%s/aggregateSteps: len %d != %d", ctx, len(want.aggregateSteps), len(got.aggregateSteps))
	}
	for i := range want.aggregateSteps {
		compareAggMaps(t, fmt.Sprintf("%s/aggregateSteps[%d]", ctx, i), want.aggregateSteps[i].Values, got.aggregateSteps[i].Values)
	}
}

func compareFloatMaps(t *testing.T, ctx string, want, got map[string]float64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: key count %d != %d (want %v, got %v)", ctx, len(want), len(got), want, got)
	}
	for k, w := range want {
		g, ok := got[k]
		if !ok {
			t.Fatalf("%s: missing key %q", ctx, k)
		}
		if diff := math.Abs(w - g); diff > 1e-9*math.Max(1, math.Max(math.Abs(w), math.Abs(g))) {
			t.Fatalf("%s[%s]: %v != %v (diff %v)", ctx, k, w, g, diff)
		}
	}
}

func compareIntMaps(t *testing.T, ctx string, want, got map[string]int64) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: key count %d != %d (want %v, got %v)", ctx, len(want), len(got), want, got)
	}
	for k, w := range want {
		if g := got[k]; g != w {
			t.Fatalf("%s[%s]: %d != %d", ctx, k, w, g)
		}
	}
}

func compareAggMaps(t *testing.T, ctx string, want, got map[string]query.Aggregate) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: key count %d != %d", ctx, len(want), len(got))
	}
	for k, w := range want {
		if g := got[k]; g != w {
			t.Fatalf("%s[%s]: %+v != %+v", ctx, k, w, g)
		}
	}
}
