package query

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestSelectBlockSumAndRate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	err := block.WriteFile(path, []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeCounter,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}, {Name: "instance", Value: "a"}},
			Timestamps: []int64{0, 1000, 2000},
			Values:     []int64{10, 20, 30},
		},
		{
			ID:         2,
			Type:       model.MetricTypeCounter,
			Labels:     model.LabelSet{{Name: "job", Value: "worker"}, {Name: "instance", Value: "b"}},
			Timestamps: []int64{0, 1000, 2000},
			Values:     []int64{100, 200, 300},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	series, err := SelectBlock(path, index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchEqual, Value: "api",
	}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}

	rates, err := Rate(series)
	if err != nil {
		t.Fatal(err)
	}
	if len(rates) != 1 || rates[0].Rate != 10 {
		t.Fatalf("rates = %+v, want one rate of 10/sec", rates)
	}
}

func TestSelectBlockRegexMatcher(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{0},
		Values:     []int64{1},
	}}); err != nil {
		t.Fatal(err)
	}
	series, err := SelectBlock(path, index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchRegexp, Value: "a.*",
	}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
}

func TestSelectBlockRejectsInvalidSelector(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{0},
		Values:     []int64{1},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err := SelectBlock(path, index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchRegexp, Value: "[",
	}}}, Options{})
	if err == nil {
		t.Fatal("expected invalid selector error")
	}
}

func TestSelectSeries(t *testing.T) {
	start := int64(2000)
	series, err := SelectSeries([]block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{1000, 2000, 3000},
		Values:     []int64{1, 2, 3},
	}}, index.NewSelector(index.LabelEqual("job", "api")), Options{StartMillis: &start})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 || series[0].Values[0] != 2 {
		t.Fatalf("series = %+v, want trimmed in-memory series", series)
	}
}

func TestSelectBlockNegativeMatchers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{0},
			Values:     []int64{1},
		},
		{
			ID:         2,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "instance", Value: "a"}},
			Timestamps: []int64{0},
			Values:     []int64{2},
		},
	}); err != nil {
		t.Fatal(err)
	}
	series, err := SelectBlock(path, index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchNotEqual, Value: "api",
	}}}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || series[0].Entry.SeriesID != 2 {
		t.Fatalf("series = %+v, want only missing-job series", series)
	}
}

func TestRateHandlesCounterReset(t *testing.T) {
	rates, err := Rate([]block.DecodedSeries{{
		Entry:      block.DirectoryEntry{SeriesID: 1},
		Timestamps: []int64{0, 1000, 2000, 3000},
		Values:     []int64{100, 150, 10, 30},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rates) != 1 {
		t.Fatalf("len(rates) = %d, want 1", len(rates))
	}
	if rates[0].Rate != float64(70)/3 {
		t.Fatalf("rate = %f, want %f", rates[0].Rate, float64(70)/3)
	}
	summary, ok, err := RateSummaryForSamples(1, []int64{0, 1000, 2000, 3000}, []int64{100, 120, 10, 80}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || summary.Increase != 90 || summary.ResetCount != 1 {
		t.Fatalf("summary = %+v, want increase=90 reset_count=1", summary)
	}
}

func TestRateSummaryHonorsMaxSampleGap(t *testing.T) {
	opts := Options{}.WithMaxSampleGap(2 * time.Second)
	_, ok, err := RateSummaryForSamples(1, []int64{0, 1000, 5000}, []int64{1, 2, 3}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected stale gap to suppress rate summary")
	}
	summary, ok, err := RateSummaryForSamples(1, []int64{0, 1000, 2000}, []int64{1, 2, 3}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || summary.Increase != 2 {
		t.Fatalf("summary = %+v, want fresh increase", summary)
	}
}

func TestAggregateByLabelStepsInDirectoryBuckets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "steps.meb")
	timestamps := make([]int64, 128)
	values := make([]int64, 128)
	for i := range values {
		timestamps[i] = int64(i) * 1000
		values[i] = int64(i + 1)
	}
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: timestamps,
		Values:     values,
	}}); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := AggregateByLabelStepsInBlockWithDirectory(path, dir, index.NewSelector(index.LabelEqual("job", "api")), "job", 63_000, 127_000, 64*time.Second, 63*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("len(steps) = %d, want 2", len(steps))
	}
	if got := steps[0].Values["api"]; got.Sum != 2080 || got.Count != 64 || got.Min != 1 || got.Max != 64 {
		t.Fatalf("steps[0].api = %+v, want first bucket", got)
	}
	if got := steps[1].Values["api"]; got.Sum != 6176 || got.Count != 64 || got.Min != 65 || got.Max != 128 {
		t.Fatalf("steps[1].api = %+v, want second bucket", got)
	}
	if _, ok := AggregateByLabelStepsInDirectoryBuckets(dir, index.Selector{}, "job", []int64{63_000}, 62*time.Second); ok {
		t.Fatal("expected non-aligned window to require payload scan")
	}
	legacyDir := dir
	legacyDir.Series[0].AggregateBuckets = nil
	if _, ok := AggregateByLabelStepsInDirectoryBuckets(legacyDir, index.Selector{}, "job", []int64{63_000}, 63*time.Second); ok {
		t.Fatal("expected missing buckets to require payload scan")
	}
}

func TestAggregateByLabelStepsInBlockUsesBucketsAndDecodesPartialSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "steps.meb")
	alignedTimestamps := make([]int64, 64)
	alignedValues := make([]int64, 64)
	partialTimestamps := make([]int64, 64)
	partialValues := make([]int64, 64)
	for i := range alignedValues {
		alignedTimestamps[i] = int64(i+1) * 1000
		alignedValues[i] = 1
		partialTimestamps[i] = int64(i) * 1000
		partialValues[i] = 2
	}
	if err := block.WriteFile(path, []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: alignedTimestamps,
			Values:     alignedValues,
		},
		{
			ID:         2,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: partialTimestamps,
			Values:     partialValues,
		},
	}); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := AggregateByLabelStepsInBlockWithDirectory(path, dir, index.Selector{}, "job", 64_000, 64_000, time.Second, 63*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("len(steps) = %d, want 1", len(steps))
	}
	if got := steps[0].Values["api"]; got.Sum != 190 || got.Count != 127 || got.Min != 1 || got.Max != 2 {
		t.Fatalf("steps[0].api = %+v, want bucket-covered first series plus decoded partial second series", got)
	}
}

func TestRateByLabelStepsInBlockWithDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "steps.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeCounter,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{0, 1000, 2000, 3000},
		Values:     []int64{1, 2, 4, 5},
	}}); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := RateByLabelStepsInBlockWithDirectory(path, dir, index.NewSelector(index.LabelEqual("job", "api")), "job", 2000, 3000, time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0].Values["api"] != 1.5 || steps[1].Values["api"] != 1.5 {
		t.Fatalf("steps = %+v, want api=1.5 at both steps", steps)
	}
	increaseSteps, err := IncreaseByLabelStepsInBlockWithDirectory(path, dir, index.NewSelector(index.LabelEqual("job", "api")), "job", 2000, 3000, time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(increaseSteps) != 2 || increaseSteps[0].Values["api"] != 3 || increaseSteps[1].Values["api"] != 3 {
		t.Fatalf("increaseSteps = %+v, want api=3 at both steps", increaseSteps)
	}
}

func TestSelectBlockZoneMapPrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{0, 1000},
		Values:     []int64{1, 2},
	}}); err != nil {
		t.Fatal(err)
	}
	min := int64(10)
	series, err := SelectBlock(path, index.Selector{}, Options{MinValue: &min})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 0 {
		t.Fatalf("len(series) = %d, want 0 after zone-map prune", len(series))
	}
}

func TestSelectBlockTrimsToValueRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{1000, 2000, 3000},
		Values:     []int64{1, 2, 3},
	}}); err != nil {
		t.Fatal(err)
	}
	min, max := int64(2), int64(2)
	series, err := SelectBlock(path, index.Selector{}, Options{MinValue: &min, MaxValue: &max})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 1 || series[0].Values[0] != 2 {
		t.Fatalf("series = %+v, want only value 2", series)
	}
}

func TestSelectBlockTimePrune(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{1000, 2000},
		Values:     []int64{1, 2},
	}}); err != nil {
		t.Fatal(err)
	}
	start := int64(3000)
	series, err := SelectBlock(path, index.Selector{}, Options{StartMillis: &start})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 0 {
		t.Fatalf("len(series) = %d, want 0 after time prune", len(series))
	}
}

func TestSelectBlockTrimsToTimeRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{1000, 2000, 3000},
		Values:     []int64{1, 2, 3},
	}}); err != nil {
		t.Fatal(err)
	}
	start, end := int64(2000), int64(2000)
	series, err := SelectBlock(path, index.Selector{}, Options{StartMillis: &start, EndMillis: &end})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 1 || series[0].Values[0] != 2 {
		t.Fatalf("series = %+v, want only timestamp 2000/value 2", series)
	}
}

func TestAggregateByLabelInBlockUsesDirectorySummary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	if err := block.WriteFile(path, []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{0, 1000},
			Values:     []int64{1, 2},
		},
		{
			ID:         2,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{0, 1000},
			Values:     []int64{3, 4},
		},
	}); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	agg, err := AggregateByLabelInBlockWithDirectory(path, dir, index.Selector{}, Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if agg["api"].Sum != 10 {
		t.Fatalf("agg[api].Sum = %d, want 10", agg["api"].Sum)
	}
	if agg["api"].Count != 4 || agg["api"].Min != 1 || agg["api"].Max != 4 || agg["api"].Avg() != 2.5 {
		t.Fatalf("agg[api] = %+v, want count=4 min=1 max=4 avg=2.5", agg["api"])
	}
}

func TestAggregateByLabelInBlockUsesBucketsAndDecodesPartialSeries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query.meb")
	alignedTimestamps := make([]int64, 64)
	alignedValues := make([]int64, 64)
	partialTimestamps := make([]int64, 64)
	partialValues := make([]int64, 64)
	for i := range alignedValues {
		alignedTimestamps[i] = int64(i+1) * 1000
		alignedValues[i] = 1
		partialTimestamps[i] = int64(i) * 1000
		partialValues[i] = 2
	}
	if err := block.WriteFile(path, []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: alignedTimestamps,
			Values:     alignedValues,
		},
		{
			ID:         2,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: partialTimestamps,
			Values:     partialValues,
		},
	}); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	start, end := int64(1000), int64(64_000)
	agg, err := AggregateByLabelInBlockWithDirectory(path, dir, index.Selector{}, Options{StartMillis: &start, EndMillis: &end}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if got := agg["api"]; got.Sum != 190 || got.Count != 127 || got.Min != 1 || got.Max != 2 {
		t.Fatalf("agg[api] = %+v, want bucket-covered first series plus decoded partial second series", got)
	}
}
