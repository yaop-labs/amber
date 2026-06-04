package store

import (
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

// measureQueryAllocs runs fn b.N times and reports the per-iteration
// allocations as custom benchmark metrics (`query_B/op` and `query_allocs/op`).
// Unlike b.ReportAllocs() — which includes setup that ran before
// b.ResetTimer because runtime/pprof samples allocations across the whole
// process — this helper takes a fresh MemStats snapshot AFTER setup finished
// and a forced GC, so the delta is purely the work done inside fn.
//
// Use this helper for any "what does the query path itself allocate" question.
// Use b.ReportAllocs() for total program allocs including setup. The two will
// disagree by a lot for benchmarks where setup is much heavier than the
// measured work (true for every query bench against a pre-built store).
func measureQueryAllocs(b *testing.B, fn func()) {
	b.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fn()
	}
	b.StopTimer()

	runtime.ReadMemStats(&after)
	if b.N > 0 {
		bytesPerOp := float64(after.TotalAlloc-before.TotalAlloc) / float64(b.N)
		allocsPerOp := float64(after.Mallocs-before.Mallocs) / float64(b.N)
		b.ReportMetric(bytesPerOp, "query_B/op")
		b.ReportMetric(allocsPerOp, "query_allocs/op")
	}
}

func BenchmarkStoreAppendBatch(b *testing.B) {
	st, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()

	b.ReportAllocs()
	b.ResetTimer()

	const batchSize = 1000
	for written := 0; written < b.N; {
		n := batchSize
		if remaining := b.N - written; remaining < n {
			n = remaining
		}
		samples := make([]model.Sample, 0, n)
		for i := 0; i < n; i++ {
			series := (written + i) % 1000
			samples = append(samples, model.Sample{
				Labels: model.LabelSet{
					{Name: model.MetricNameLabel, Value: "bench_counter_total"},
					{Name: "job", Value: "bench"},
					{Name: "instance", Value: strconv.Itoa(series)},
				},
				Type:      model.MetricTypeCounter,
				Timestamp: int64(written+i) * 1000,
				Value:     int64(written + i),
			})
		}
		if _, err := st.AppendBatch(samples); err != nil {
			b.Fatal(err)
		}
		written += n
	}
}

func BenchmarkStoreSelectHighCardinality(b *testing.B) {
	st := buildBenchStore(b, 10_000, 4)
	selector := index.NewSelector(index.LabelEqual("job", "target"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		series, err := st.Select(selector, query.Options{})
		if err != nil {
			b.Fatal(err)
		}
		if len(series) == 0 {
			b.Fatal("expected matches")
		}
	}
}

func BenchmarkStoreSumByLabelHighCardinality(b *testing.B) {
	st := buildBenchStore(b, 10_000, 4)
	selector := index.NewSelector(index.MetricName("bench_gauge"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sum, err := st.SumByLabel(selector, query.Options{}, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(sum) == 0 {
			b.Fatal("expected sums")
		}
	}
}

func BenchmarkStoreRateByLabelHighCardinality(b *testing.B) {
	st := buildBenchStore(b, 10_000, 4)
	selector := index.NewSelector(index.MetricName("bench_gauge"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rates, err := st.RateByLabel(selector, query.Options{}, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(rates) == 0 {
			b.Fatal("expected rates")
		}
	}
}

func BenchmarkStoreRateByLabelRangeStepsHighCardinality(b *testing.B) {
	st := buildBenchStore(b, 10_000, 10)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	}
}

func BenchmarkStoreRateByLabelRangeStepsMultiBlockHighCardinality(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	}
}

// BenchmarkRateRangeStepsMultiBlockCleanAllocs is the same workload as
// BenchmarkStoreRateByLabelRangeStepsMultiBlockHighCardinality but uses
// measureQueryAllocs to report ONLY the per-query allocations. The standard
// b.ReportAllocs metric for the original benchmark is inflated by store
// setup (10k series × 10 samples ingested + flushed twice); this clean
// variant is what we use to track sync.Pool / arena perf PRs.
func BenchmarkRateRangeStepsMultiBlockCleanAllocs(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}
	measureQueryAllocs(b, func() {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	})
}

// BenchmarkRateRangeStepsSmallSelectorCleanAllocs measures the dashboard
// shape: tight selector (job=target matches ~100 of 10000 series), realistic
// step count, multi-block store. The hot path is selector evaluation +
// per-step buffer building, NOT label canonicalisation.
func BenchmarkRateRangeStepsSmallSelectorCleanAllocs(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(
			index.MetricName("bench_gauge"),
			index.LabelEqual("job", "target"),
		),
		Window: 4 * time.Second,
	}
	measureQueryAllocs(b, func() {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	})
}

// BenchmarkRateRangeStepsManyStepsCleanAllocs stresses per-step buffer
// allocations: 60 steps over the full range instead of the default 5. If
// pooling rateSample slices pays off, the win will show up here first.
func BenchmarkRateRangeStepsManyStepsCleanAllocs(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 60, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}
	measureQueryAllocs(b, func() {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 59_000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	})
}

func BenchmarkStoreRateByLabelRangeStepsMultiBlockMaxGapHighCardinality(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector:     index.NewSelector(index.MetricName("bench_gauge")),
		Window:       4 * time.Second,
		MaxSampleGap: 2 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.RateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected rate steps")
		}
	}
}

func BenchmarkStoreAggregateByLabelRangeStepsHighCardinality(b *testing.B) {
	st := buildBenchStore(b, 10_000, 10)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.AggregateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected aggregate steps")
		}
	}
}

func BenchmarkStoreAggregateByLabelRangeStepsMultiBlockHighCardinality(b *testing.B) {
	st := buildBenchStoreBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.AggregateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected aggregate steps")
		}
	}
}

func BenchmarkStoreAggregateByLabelRangeStepsSequentialBlocksHighCardinality(b *testing.B) {
	st := buildBenchStoreSequentialBlocks(b, 10_000, 10, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   4 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.AggregateByLabelRangeSteps(rangeSelector, 4000, 9000, time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) == 0 {
			b.Fatal("expected aggregate steps")
		}
	}
}

func BenchmarkStoreAggregateByLabelRangeStepsBucketBlocks(b *testing.B) {
	st := buildBenchStoreSequentialBlocks(b, 1000, 128, 2)
	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("bench_gauge")),
		Window:   63 * time.Second,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := st.AggregateByLabelRangeSteps(rangeSelector, 63_000, 127_000, 64*time.Second, "job")
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) != 2 {
			b.Fatal("expected aggregate steps")
		}
	}
}

func buildBenchStore(b *testing.B, seriesCount int, samplesPerSeries int) *Store {
	b.Helper()

	st, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = st.Close()
	})

	samples := make([]model.Sample, 0, seriesCount*samplesPerSeries)
	for series := 0; series < seriesCount; series++ {
		job := "other"
		if series%100 == 0 {
			job = "target"
		}
		labels := model.LabelSet{
			{Name: model.MetricNameLabel, Value: "bench_gauge"},
			{Name: "job", Value: job},
			{Name: "instance", Value: strconv.Itoa(series)},
		}
		for sample := 0; sample < samplesPerSeries; sample++ {
			samples = append(samples, model.Sample{
				Labels:    labels,
				Type:      model.MetricTypeGauge,
				Timestamp: int64(sample) * 1000,
				Value:     int64(series + sample),
			})
		}
	}
	if _, err := st.AppendBatch(samples); err != nil {
		b.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		b.Fatal(err)
	}
	return st
}

func buildBenchStoreSequentialBlocks(b *testing.B, seriesCount int, samplesPerSeries int, blockCount int) *Store {
	b.Helper()

	st, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = st.Close()
	})

	samplesPerBlock := samplesPerSeries / blockCount
	for blockIndex := 0; blockIndex < blockCount; blockIndex++ {
		startSample := blockIndex * samplesPerBlock
		endSample := startSample + samplesPerBlock
		if blockIndex == blockCount-1 {
			endSample = samplesPerSeries
		}
		samples := make([]model.Sample, 0, seriesCount*(endSample-startSample))
		for series := 0; series < seriesCount; series++ {
			job := "other"
			if series%100 == 0 {
				job = "target"
			}
			labels := model.LabelSet{
				{Name: model.MetricNameLabel, Value: "bench_gauge"},
				{Name: "job", Value: job},
				{Name: "instance", Value: strconv.Itoa(series)},
			}
			for sample := startSample; sample < endSample; sample++ {
				samples = append(samples, model.Sample{
					Labels:    labels,
					Type:      model.MetricTypeGauge,
					Timestamp: int64(sample) * 1000,
					Value:     int64(series + sample),
				})
			}
		}
		if _, err := st.AppendBatch(samples); err != nil {
			b.Fatal(err)
		}
		if _, err := st.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	return st
}

func buildBenchStoreBlocks(b *testing.B, seriesCount int, samplesPerSeries int, blockCount int) *Store {
	b.Helper()

	st, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		_ = st.Close()
	})

	for blockIndex := 0; blockIndex < blockCount; blockIndex++ {
		samples := make([]model.Sample, 0, seriesCount*samplesPerSeries/blockCount)
		for series := 0; series < seriesCount; series++ {
			job := "other"
			if series%100 == 0 {
				job = "target"
			}
			labels := model.LabelSet{
				{Name: model.MetricNameLabel, Value: "bench_gauge"},
				{Name: "job", Value: job},
				{Name: "instance", Value: strconv.Itoa(series)},
			}
			for sample := blockIndex; sample < samplesPerSeries; sample += blockCount {
				samples = append(samples, model.Sample{
					Labels:    labels,
					Type:      model.MetricTypeGauge,
					Timestamp: int64(sample) * 1000,
					Value:     int64(series + sample),
				})
			}
		}
		if _, err := st.AppendBatch(samples); err != nil {
			b.Fatal(err)
		}
		if _, err := st.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	return st
}
