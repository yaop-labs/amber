package histogram

import (
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func lbls(pairs ...string) model.LabelSet {
	ls := make(model.LabelSet, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		ls = append(ls, model.Label{Name: pairs[i], Value: pairs[i+1]})
	}
	return ls.Canonical()
}

func TestStoreResumesSequencePastHoles(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	series := ExpSeries{
		ID:         1,
		Labels:     lbls("__name__", "lat", "job", "api"),
		Timestamps: []int64{1000},
		Sketches:   []*ExponentialHistogram{FromValues(4, []float64{1, 2, 3})},
	}
	if _, err := s.WriteBlock([]ExpSeries{series}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBlock([]ExpSeries{series}, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "hblock-000000.mhb")); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	path, err := reopened.WriteBlock([]ExpSeries{series}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(path); got != "hblock-000002.mhb" {
		t.Fatalf("next block = %s, want hblock-000002.mhb", got)
	}
}

func TestStoreRetentionRemovesExpiredBlocks(t *testing.T) {
	dir := t.TempDir()
	now := time.UnixMilli(10_000_000)
	s, err := OpenStoreWithOptions(dir, Options{
		Retention: time.Hour,
		Clock:     func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}

	old := now.Add(-2 * time.Hour).UnixMilli()
	newer := now.Add(-10 * time.Minute).UnixMilli()
	mkSeries := func(ts int64) ExpSeries {
		return ExpSeries{
			ID:         1,
			Labels:     lbls("__name__", "lat", "ts", strconvFormatInt(ts)),
			Timestamps: []int64{ts},
			Sketches:   []*ExponentialHistogram{FromValues(4, []float64{1, 2, 3})},
		}
	}
	if _, err := s.WriteBlock([]ExpSeries{mkSeries(old)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBlock([]ExpSeries{mkSeries(newer)}, nil); err != nil {
		t.Fatal(err)
	}

	paths, err := s.blockPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("block count = %d, want 1; paths=%v", len(paths), paths)
	}
	dirMeta, err := ReadDirectory(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, maxTS, ok := dirMeta.TimeRange(); !ok || maxTS != newer {
		t.Fatalf("remaining block max ts = %d ok=%v, want %d", maxTS, ok, newer)
	}
}

func TestStoreRejectsHistogramCardinalityLimit(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStoreWithOptions(dir, Options{MaxActiveSeries: 1})
	if err != nil {
		t.Fatal(err)
	}
	mkSeries := func(job string) ExpSeries {
		return ExpSeries{
			ID:         1,
			Labels:     lbls("__name__", "lat", "job", job),
			Timestamps: []int64{1000},
			Sketches:   []*ExponentialHistogram{FromValues(4, []float64{1, 2, 3})},
		}
	}
	if _, err := s.WriteBlock([]ExpSeries{mkSeries("api")}, nil); err != nil {
		t.Fatal(err)
	}
	_, err = s.WriteBlock([]ExpSeries{mkSeries("worker")}, nil)
	if err == nil || !strings.Contains(err.Error(), "active series limit exceeded") {
		t.Fatalf("err = %v, want active series limit", err)
	}
}

func TestStoreRejectsHistogramLabelLimit(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStoreWithOptions(dir, Options{MaxLabelsPerSeries: 1})
	if err != nil {
		t.Fatal(err)
	}
	series := ExpSeries{
		ID:         1,
		Labels:     lbls("__name__", "lat", "job", "api"),
		Timestamps: []int64{1000},
		Sketches:   []*ExponentialHistogram{FromValues(4, []float64{1, 2, 3})},
	}
	_, err = s.WriteBlock([]ExpSeries{series}, nil)
	if err == nil || !strings.Contains(err.Error(), "label limit exceeded") {
		t.Fatalf("err = %v, want label limit", err)
	}
}

func strconvFormatInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// expSemEqual compares two exp-histograms ignoring representation (leading/
// trailing zero buckets), as used by the lossless round-trip checks.
func expSemEqual(t *testing.T, a, b *ExponentialHistogram) {
	t.Helper()
	if a.Scale != b.Scale {
		t.Errorf("scale: %d != %d", a.Scale, b.Scale)
	}
	if a.ZeroCount != b.ZeroCount {
		t.Errorf("zero_count: %d != %d", a.ZeroCount, b.ZeroCount)
	}
	if a.ZeroThreshold != b.ZeroThreshold {
		t.Errorf("zero_threshold: %v != %v", a.ZeroThreshold, b.ZeroThreshold)
	}
	if a.Count != b.Count {
		t.Errorf("count: %d != %d", a.Count, b.Count)
	}
	if a.Sum != b.Sum {
		t.Errorf("sum: %v != %v", a.Sum, b.Sum)
	}
	if a.HasMinMax != b.HasMinMax || a.Min != b.Min || a.Max != b.Max {
		t.Errorf("minmax: (%v,%v,%v) != (%v,%v,%v)", a.HasMinMax, a.Min, a.Max, b.HasMinMax, b.Min, b.Max)
	}
	bucketsSemEqual(t, "positive", a.Positive, b.Positive)
	bucketsSemEqual(t, "negative", a.Negative, b.Negative)
}

func bucketsSemEqual(t *testing.T, which string, a, b Buckets) {
	t.Helper()
	get := func(bk Buckets, idx int32) uint64 {
		p := int(idx - bk.Offset)
		if p < 0 || p >= len(bk.Counts) {
			return 0
		}
		return bk.Counts[p]
	}
	lo, hi := a.Offset, a.Offset+int32(len(a.Counts))
	if b.Offset < lo {
		lo = b.Offset
	}
	if e := b.Offset + int32(len(b.Counts)); e > hi {
		hi = e
	}
	for idx := lo; idx < hi; idx++ {
		if get(a, idx) != get(b, idx) {
			t.Errorf("%s bucket %d: %d != %d", which, idx, get(a, idx), get(b, idx))
		}
	}
}

// TestStoreExpRoundTrip writes a multi-tick exp series (exercising the temporal
// delta layer) and reads it back, asserting every tick is reconstructed exactly.
func TestStoreExpRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(1))
	var sketches []*ExponentialHistogram
	var timestamps []int64
	for tick := 0; tick < 8; tick++ {
		var data []float64
		for i := 0; i < 500; i++ {
			data = append(data, math.Exp(rng.NormFloat64()+2))
		}
		sketches = append(sketches, FromValues(4, data))
		timestamps = append(timestamps, int64(1000+tick*60))
	}
	series := ExpSeries{ID: 1, Labels: lbls("__name__", "lat"), Timestamps: timestamps, Sketches: sketches}
	if _, err := s.WriteBlock([]ExpSeries{series}, nil); err != nil {
		t.Fatal(err)
	}

	paths, _ := s.blockPaths()
	exps, _, err := ReadBlock(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(exps) != 1 || len(exps[0].Sketches) != len(sketches) {
		t.Fatalf("unexpected read: series=%d ticks=%d", len(exps), len(exps[0].Sketches))
	}
	for i := range sketches {
		expSemEqual(t, sketches[i], exps[0].Sketches[i])
		if exps[0].Timestamps[i] != timestamps[i] {
			t.Errorf("tick %d ts %d != %d", i, exps[0].Timestamps[i], timestamps[i])
		}
	}
}

// TestSumByGroupingMerge is the sum-by-grouping merge test: histograms across
// multiple blocks and label values must be merged per group, and the per-group
// quantile must match a direct merge of that group's raw sketches.
func TestSumByGroupingMerge(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))

	// Two groups (region=eu, region=us), several series each, spread over 3 blocks.
	want := map[string][]*ExponentialHistogram{}
	for blk := 0; blk < 3; blk++ {
		var series []ExpSeries
		id := uint64(1)
		for _, region := range []string{"eu", "us"} {
			for inst := 0; inst < 3; inst++ {
				var data []float64
				n := 200 + rng.Intn(200)
				center := 2.0
				if region == "us" {
					center = 3.0
				}
				for i := 0; i < n; i++ {
					data = append(data, math.Exp(rng.NormFloat64()+center))
				}
				sk := FromValues(5, data)
				series = append(series, ExpSeries{
					ID:         id,
					Labels:     lbls("__name__", "lat", "region", region, "inst", string(rune('a'+inst))),
					Timestamps: []int64{int64(1000 + blk*100)},
					Sketches:   []*ExponentialHistogram{sk},
				})
				id++
				key := groupKey(lbls("region", region), []string{"region"})
				want[key] = append(want[key], sk)
			}
		}
		if _, err := s.WriteBlock(series, nil); err != nil {
			t.Fatal(err)
		}
	}

	sel := index.NewSelector(index.MetricName("lat"))
	for _, q := range []float64{0.5, 0.9, 0.99} {
		got, err := s.HistogramQuantileBy(sel, q, fullRange(), []string{"region"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(got))
		}
		for key, hists := range want {
			expected := MergeAll(hists).Quantile(q)
			if rel := relErr(got[key], expected); rel > 1e-9 {
				t.Errorf("q=%.2f group=%s: store=%v direct-merge=%v", q, key, got[key], expected)
			}
		}
	}
}

func TestStoreSummaryFromSynopsis(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir)
	h1 := FromValues(4, []float64{1, 2, 3, 4, 5})
	h2 := FromValues(4, []float64{10, 20, 30})
	series := []ExpSeries{
		{ID: 1, Labels: lbls("__name__", "x"), Timestamps: []int64{100}, Sketches: []*ExponentialHistogram{h1}},
		{ID: 2, Labels: lbls("__name__", "x"), Timestamps: []int64{100}, Sketches: []*ExponentialHistogram{h2}},
	}
	if _, err := s.WriteBlock(series, nil); err != nil {
		t.Fatal(err)
	}
	syn, err := s.Summary(index.NewSelector(index.MetricName("x")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if syn.Count != 8 {
		t.Errorf("count = %d, want 8", syn.Count)
	}
	if syn.Sum != 15+60 {
		t.Errorf("sum = %v, want 75", syn.Sum)
	}
	if syn.Min != 1 || syn.Max != 30 {
		t.Errorf("min/max = %v/%v, want 1/30", syn.Min, syn.Max)
	}
}

func TestStoreExplicitQuantile(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir)
	bounds := []float64{1, 2, 4, 8, 16, 32}
	var allData []float64
	var series []ExplicitSeries
	for blk := 0; blk < 2; blk++ {
		var data []float64
		for i := 1; i <= 500; i++ {
			v := float64(i) * 32.0 / 500.0
			data = append(data, v)
			allData = append(allData, v)
		}
		series = append(series, ExplicitSeries{
			ID:         uint64(blk + 1),
			Labels:     lbls("__name__", "req"),
			Timestamps: []int64{int64(100 + blk)},
			Buckets:    []*ExplicitBucketHistogram{ExplicitFromValues(bounds, data)},
		})
	}
	if _, err := s.WriteBlock(nil, series); err != nil {
		t.Fatal(err)
	}
	est, err := s.ExplicitQuantile(index.NewSelector(index.MetricName("req")), 0.5, fullRange())
	if err != nil {
		t.Fatal(err)
	}
	truth := trueQuantile(allData, 0.5)
	if est < truth*0.5 || est > truth*1.6 {
		t.Errorf("explicit median est=%v truth=%v out of range", est, truth)
	}
}

func TestStoreTimeRangeFilter(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir)
	early := FromValues(4, []float64{1, 1, 1})
	late := FromValues(4, []float64{1000, 1000, 1000})
	series := ExpSeries{
		ID:         1,
		Labels:     lbls("__name__", "x"),
		Timestamps: []int64{100, 200},
		Sketches:   []*ExponentialHistogram{early, late},
	}
	if _, err := s.WriteBlock([]ExpSeries{series}, nil); err != nil {
		t.Fatal(err)
	}
	// Only the early tick is in range; quantile should reflect ~1, not ~1000.
	merged, err := s.MergeExp(index.NewSelector(index.MetricName("x")), TimeRange{Start: 0, End: 150})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Count != 3 {
		t.Fatalf("expected only early tick (count 3), got %d", merged.Count)
	}
}

// TestStoreMetricNames seeds two series with distinct __name__ values across
// two blocks and asserts MetricNames deduplicates and sorts.
func TestStoreMetricNames(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sk := FromValues(4, []float64{1, 2, 3})
	if _, err := s.WriteBlock([]ExpSeries{{
		ID: 1, Labels: lbls("__name__", "zeta"), Timestamps: []int64{100}, Sketches: []*ExponentialHistogram{sk},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBlock([]ExpSeries{
		{ID: 1, Labels: lbls("__name__", "alpha"), Timestamps: []int64{200}, Sketches: []*ExponentialHistogram{sk}},
		// Duplicate "alpha" in a different label set must collapse to one entry.
		{ID: 2, Labels: lbls("__name__", "alpha", "host", "h1"), Timestamps: []int64{200}, Sketches: []*ExponentialHistogram{sk}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	names, err := s.MetricNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "alpha" || names[1] != "zeta" {
		t.Fatalf("MetricNames = %v, want [alpha zeta]", names)
	}
}

// TestStoreStats checks block-count, series-count, and time-range accumulation
// across two blocks. Bytes is asserted as >0 rather than exact to stay
// resilient to codec changes.
func TestStoreStats(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sk := FromValues(4, []float64{1, 2, 3})
	if _, err := s.WriteBlock([]ExpSeries{{
		ID: 1, Labels: lbls("__name__", "m"), Timestamps: []int64{50}, Sketches: []*ExponentialHistogram{sk},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.WriteBlock([]ExpSeries{
		{ID: 1, Labels: lbls("__name__", "m", "host", "a"), Timestamps: []int64{100}, Sketches: []*ExponentialHistogram{sk}},
		{ID: 2, Labels: lbls("__name__", "m", "host", "b"), Timestamps: []int64{200}, Sketches: []*ExponentialHistogram{sk}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Blocks != 2 || st.Series != 3 || st.Bytes <= 0 {
		t.Fatalf("stats = %+v, want blocks=2 series=3 bytes>0", st)
	}
	if !st.HasTime || st.MinTime != 50 || st.MaxTime != 200 {
		t.Fatalf("time range = %v..%v, want 50..200", st.MinTime, st.MaxTime)
	}
}

func TestStoreStatsEmpty(t *testing.T) {
	s, err := OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Blocks != 0 || st.Series != 0 || st.Bytes != 0 || st.HasTime {
		t.Fatalf("empty stats = %+v", st)
	}
}
