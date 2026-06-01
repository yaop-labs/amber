package query

import (
	"errors"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

type Options struct {
	StartMillis        *int64
	EndMillis          *int64
	MinValue           *int64
	MaxValue           *int64
	MaxSampleGapMillis *int64
}

type RateResult struct {
	SeriesID uint64
	Rate     float64
}

type IncreaseResult struct {
	SeriesID uint64
	Increase int64
}

type RateSummary struct {
	SeriesID    uint64
	Labels      model.LabelSet
	FirstMillis int64
	LastMillis  int64
	FirstValue  int64
	LastValue   int64
	Increase    int64
	ResetCount  int
	Count       int
}

type Aggregate struct {
	Sum   int64
	Count int
	Min   int64
	Max   int64
}

func (a Aggregate) Avg() float64 {
	if a.Count == 0 {
		return 0
	}
	return float64(a.Sum) / float64(a.Count)
}

func SelectBlock(path string, selector index.Selector, opts Options) ([]block.DecodedSeries, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	series, err := block.ReadFileFiltered(path, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	})
	if err != nil {
		return nil, err
	}
	return trimSeries(series, opts), nil
}

func SelectBlockWithDirectory(path string, dir block.Directory, selector index.Selector, opts Options) ([]block.DecodedSeries, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	series, err := block.ReadFileFilteredWithDirectory(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	})
	if err != nil {
		return nil, err
	}
	return trimSeries(series, opts), nil
}

func SelectSeries(input []block.Series, selector index.Selector, opts Options) ([]block.DecodedSeries, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make([]block.DecodedSeries, 0, len(input))
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return nil, errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		out = append(out, block.DecodedSeries{
			Entry:      entry,
			Timestamps: append([]int64(nil), series.Timestamps...),
			Values:     append([]int64(nil), series.Values...),
		})
	}
	return trimSeries(out, opts), nil
}

func ScanBlockWithDirectoryShared(path string, dir block.Directory, selector index.Selector, opts Options, fn block.SeriesFunc) error {
	if err := selector.Validate(); err != nil {
		return err
	}
	return block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, fn)
}

func ScanSeries(input []block.Series, selector index.Selector, opts Options, fn block.SeriesFunc) error {
	if err := selector.Validate(); err != nil {
		return err
	}
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		if err := fn(block.DecodedSeries{
			Entry:      entry,
			Timestamps: series.Timestamps,
			Values:     series.Values,
		}); err != nil {
			return err
		}
	}
	return nil
}

func SumByLabel(series []block.DecodedSeries, label string) map[string]int64 {
	out := make(map[string]int64)
	for _, s := range series {
		key, ok := s.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		for _, value := range s.Values {
			out[key] += value
		}
	}
	return out
}

func AggregateByLabel(series []block.DecodedSeries, label string) map[string]Aggregate {
	out := make(map[string]Aggregate)
	for _, s := range series {
		key, ok := s.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		for _, value := range s.Values {
			agg := out[key]
			agg = addValue(agg, value)
			out[key] = agg
		}
	}
	return out
}

func AggregateByLabelSteps(series []block.DecodedSeries, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]AggregateStep, error) {
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := makeAggregateSteps(steps)
	for _, s := range series {
		aggregates, err := aggregateSummariesForSteps(s.Timestamps, s.Values, steps, window)
		if err != nil {
			return nil, err
		}
		key, ok := s.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		for i, agg := range aggregates {
			if agg.Count == 0 {
				continue
			}
			current := out[i].Values[key]
			out[i].Values[key] = mergeAggregate(current, agg)
		}
	}
	return out, nil
}

func AggregateByLabelStepsInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]AggregateStep, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	if out, ok := AggregateByLabelStepsInDirectoryBuckets(dir, selector, label, steps, window); ok {
		return out, nil
	}
	return aggregateByLabelStepsInBlockBucketsHybrid(path, dir, selector, label, steps, window)
}

func aggregateByLabelStepsInBlockBucketsHybrid(path string, dir block.Directory, selector index.Selector, label string, steps []int64, window time.Duration) ([]AggregateStep, error) {
	out := makeAggregateSteps(steps)
	if len(steps) == 0 {
		return out, nil
	}
	windowMillis := window.Milliseconds()
	opts := TimeRange(steps[0]-windowMillis, steps[len(steps)-1])
	decodeStepsBySeries := make(map[uint64][]bool)
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) {
			continue
		}
		if len(entry.AggregateBuckets) == 0 {
			decodeStepsBySeries[entry.SeriesID] = allSteps(len(steps))
			continue
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		var decodeSteps []bool
		for stepIndex, stepMillis := range steps {
			stepOpts := TimeWindow(stepMillis, window)
			if !matchTimeRange(entry, stepOpts) {
				continue
			}
			agg, covered := aggregateEntryBuckets(entry, stepOpts)
			if !covered {
				if seriesMayContainSamples(entry, stepOpts) {
					if decodeSteps == nil {
						decodeSteps = make([]bool, len(steps))
					}
					decodeSteps[stepIndex] = true
				}
				continue
			}
			if agg.Count > 0 {
				out[stepIndex].Values[key] = mergeAggregate(out[stepIndex].Values[key], agg)
			}
		}
		if decodeSteps != nil {
			decodeStepsBySeries[entry.SeriesID] = decodeSteps
		}
	}
	if len(decodeStepsBySeries) == 0 {
		return out, nil
	}
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		if _, ok := decodeStepsBySeries[entry.SeriesID]; !ok {
			return false
		}
		return matchLabels(entry, selector) && matchTimeRange(entry, opts)
	}, func(series block.DecodedSeries) error {
		return accumulateAggregateByLabelSelectedSteps(out, steps, series, label, window, decodeStepsBySeries[series.Entry.SeriesID])
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func AggregateByLabelStepsInDirectoryBuckets(dir block.Directory, selector index.Selector, label string, steps []int64, window time.Duration) ([]AggregateStep, bool) {
	if err := selector.Validate(); err != nil {
		return nil, false
	}
	if window.Milliseconds() <= 0 {
		return nil, false
	}
	out := makeAggregateSteps(steps)
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) {
			continue
		}
		if len(entry.AggregateBuckets) == 0 {
			return nil, false
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		for stepIndex, stepMillis := range steps {
			opts := TimeWindow(stepMillis, window)
			if !matchTimeRange(entry, opts) {
				continue
			}
			agg, covered := aggregateEntryBuckets(entry, opts)
			if !covered {
				return nil, false
			}
			if agg.Count > 0 {
				out[stepIndex].Values[key] = mergeAggregate(out[stepIndex].Values[key], agg)
			}
		}
	}
	return out, true
}

func SumByLabelSteps(series []block.DecodedSeries, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]IntStep, error) {
	aggregateSteps, err := AggregateByLabelSteps(series, label, startMillis, endMillis, step, window)
	if err != nil {
		return nil, err
	}
	out := make([]IntStep, len(aggregateSteps))
	for i, aggregateStep := range aggregateSteps {
		values := make(map[string]int64, len(aggregateStep.Values))
		for key, agg := range aggregateStep.Values {
			values[key] = agg.Sum
		}
		out[i] = IntStep{TimestampMillis: aggregateStep.TimestampMillis, Values: values}
	}
	return out, nil
}

func SumByLabelInBlock(path string, selector index.Selector, opts Options, label string) (map[string]int64, error) {
	aggs, err := AggregateByLabelInBlock(path, selector, opts, label)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(aggs))
	for key, agg := range aggs {
		out[key] = agg.Sum
	}
	return out, nil
}

func AggregateByLabelInBlock(path string, selector index.Selector, opts Options, label string) (map[string]Aggregate, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		return nil, err
	}
	return AggregateByLabelInBlockWithDirectory(path, dir, selector, opts, label)
}

func AggregateByLabelInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, opts Options, label string) (map[string]Aggregate, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	if hasPointFilters(opts) {
		if !hasValueFilters(opts) {
			return aggregateByLabelInBlockBucketsHybrid(path, dir, selector, opts, label)
		}
		return aggregateByLabelInBlockPayload(path, dir, selector, opts, label)
	}
	return AggregateByLabelInDirectory(dir, selector, opts, label), nil
}

func aggregateByLabelInBlockBucketsHybrid(path string, dir block.Directory, selector index.Selector, opts Options, label string) (map[string]Aggregate, error) {
	out := make(map[string]Aggregate)
	decodeSeries := make(map[uint64]struct{})
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		if len(entry.AggregateBuckets) == 0 {
			decodeSeries[entry.SeriesID] = struct{}{}
			continue
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		entryAgg := Aggregate{}
		found := false
		needsDecode := false
		for _, bucket := range entry.AggregateBuckets {
			if !bucketMatchesTime(bucket, opts) {
				continue
			}
			if !bucketFullyContained(bucket, opts) {
				needsDecode = true
				break
			}
			found = true
			entryAgg = mergeAggregate(entryAgg, aggregateFromBucket(bucket))
		}
		if needsDecode || (!found && seriesMayContainSamples(entry, opts)) {
			decodeSeries[entry.SeriesID] = struct{}{}
			continue
		}
		if found {
			out[key] = mergeAggregate(out[key], entryAgg)
		}
	}
	if len(decodeSeries) == 0 {
		return out, nil
	}
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		if _, ok := decodeSeries[entry.SeriesID]; !ok {
			return false
		}
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, func(series block.DecodedSeries) error {
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		agg := out[key]
		for i, timestamp := range series.Timestamps {
			if !sampleMatches(timestamp, series.Values[i], opts) {
				continue
			}
			agg = addValue(agg, series.Values[i])
		}
		if agg.Count > 0 {
			out[key] = agg
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func aggregateByLabelInBlockPayload(path string, dir block.Directory, selector index.Selector, opts Options, label string) (map[string]Aggregate, error) {
	out := make(map[string]Aggregate)
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, func(series block.DecodedSeries) error {
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		agg := out[key]
		for i, timestamp := range series.Timestamps {
			if !sampleMatches(timestamp, series.Values[i], opts) {
				continue
			}
			agg = addValue(agg, series.Values[i])
		}
		if agg.Count > 0 {
			out[key] = agg
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func AggregateByLabelInDirectory(dir block.Directory, selector index.Selector, opts Options, label string) map[string]Aggregate {
	out := make(map[string]Aggregate)
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		agg := out[key]
		agg = addZoneMap(agg, entry.ZoneMap)
		out[key] = agg
	}
	return out
}

func AggregateByLabelInDirectoryBuckets(dir block.Directory, selector index.Selector, opts Options, label string) (map[string]Aggregate, bool) {
	out := make(map[string]Aggregate)
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		if len(entry.AggregateBuckets) == 0 {
			return nil, false
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		agg, covered := aggregateEntryBuckets(entry, opts)
		if !covered {
			return nil, false
		}
		out[key] = mergeAggregate(out[key], agg)
	}
	return out, true
}

func Rate(series []block.DecodedSeries) ([]RateResult, error) {
	out := make([]RateResult, 0, len(series))
	for _, s := range series {
		rate, ok, err := rateOne(s)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, RateResult{SeriesID: s.Entry.SeriesID, Rate: rate})
	}
	return out, nil
}

func Increase(series []block.DecodedSeries) ([]IncreaseResult, error) {
	out := make([]IncreaseResult, 0, len(series))
	for _, s := range series {
		summary, ok, err := RateSummaryForSamples(s.Entry.SeriesID, s.Timestamps, s.Values, Options{})
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		out = append(out, IncreaseResult{SeriesID: s.Entry.SeriesID, Increase: summary.Increase})
	}
	return out, nil
}

func RateByLabel(series []block.DecodedSeries, label string) (map[string]float64, error) {
	out := make(map[string]float64)
	for _, s := range series {
		rate, ok, err := rateOneSamples(s.Timestamps, s.Values, Options{})
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key, ok := s.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += rate
	}
	return out, nil
}

func RateByLabelSteps(series []block.DecodedSeries, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]FloatStep, error) {
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := makeFloatSteps(steps)
	for _, s := range series {
		if err := addRateSteps(out, steps, s, label, window); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func RateByLabelStepsInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]FloatStep, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := makeFloatSteps(steps)
	opts := TimeRange(startMillis-window.Milliseconds(), endMillis)
	err = block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts)
	}, func(series block.DecodedSeries) error {
		return addRateSteps(out, steps, series, label, window)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func IncreaseByLabelSteps(series []block.DecodedSeries, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]IntStep, error) {
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := makeIntSteps(steps)
	for _, s := range series {
		if err := addIncreaseSteps(out, steps, s, label, window); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func IncreaseByLabelStepsInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, label string, startMillis int64, endMillis int64, step time.Duration, window time.Duration) ([]IntStep, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	steps, err := StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if window.Milliseconds() <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := makeIntSteps(steps)
	opts := TimeRange(startMillis-window.Milliseconds(), endMillis)
	err = block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts)
	}, func(series block.DecodedSeries) error {
		return addIncreaseSteps(out, steps, series, label, window)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func IncreaseByLabel(series []block.DecodedSeries, label string) (map[string]int64, error) {
	out := make(map[string]int64)
	for _, s := range series {
		summary, ok, err := RateSummaryForSamples(s.Entry.SeriesID, s.Timestamps, s.Values, Options{})
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key, ok := s.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += summary.Increase
	}
	return out, nil
}

func makeFloatSteps(steps []int64) []FloatStep {
	out := make([]FloatStep, len(steps))
	for i, ts := range steps {
		out[i] = FloatStep{TimestampMillis: ts, Values: make(map[string]float64)}
	}
	return out
}

func makeIntSteps(steps []int64) []IntStep {
	out := make([]IntStep, len(steps))
	for i, ts := range steps {
		out[i] = IntStep{TimestampMillis: ts, Values: make(map[string]int64)}
	}
	return out
}

func makeAggregateSteps(steps []int64) []AggregateStep {
	out := make([]AggregateStep, len(steps))
	for i, ts := range steps {
		out[i] = AggregateStep{TimestampMillis: ts, Values: make(map[string]Aggregate)}
	}
	return out
}

func addRateSteps(out []FloatStep, steps []int64, series block.DecodedSeries, label string, window time.Duration) error {
	return addRateStepsWithOptions(out, steps, series, label, window, Options{})
}

func addRateStepsWithOptions(out []FloatStep, steps []int64, series block.DecodedSeries, label string, window time.Duration, opts Options) error {
	summaries, err := rateSummariesForSteps(series.Entry.SeriesID, series.Timestamps, series.Values, steps, window, opts, 2)
	if err != nil {
		return err
	}
	key, ok := series.Entry.Labels.Get(label)
	if !ok {
		key = ""
	}
	for i, summary := range summaries {
		if summary.Count == 0 {
			continue
		}
		rate, ok, err := RateFromSummary(summary)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		out[i].Values[key] += rate
	}
	return nil
}

func addIncreaseSteps(out []IntStep, steps []int64, series block.DecodedSeries, label string, window time.Duration) error {
	return addIncreaseStepsWithOptions(out, steps, series, label, window, Options{})
}

func addIncreaseStepsWithOptions(out []IntStep, steps []int64, series block.DecodedSeries, label string, window time.Duration, opts Options) error {
	summaries, err := rateSummariesForSteps(series.Entry.SeriesID, series.Timestamps, series.Values, steps, window, opts, 2)
	if err != nil {
		return err
	}
	key, ok := series.Entry.Labels.Get(label)
	if !ok {
		key = ""
	}
	for i, summary := range summaries {
		if summary.Count == 0 {
			continue
		}
		out[i].Values[key] += summary.Increase
	}
	return nil
}

func AccumulateRateByLabelSteps(out []FloatStep, steps []int64, series block.DecodedSeries, label string, window time.Duration) error {
	if len(out) != len(steps) {
		return errors.New("query: step output length mismatch")
	}
	return addRateSteps(out, steps, series, label, window)
}

func AccumulateIncreaseByLabelSteps(out []IntStep, steps []int64, series block.DecodedSeries, label string, window time.Duration) error {
	if len(out) != len(steps) {
		return errors.New("query: step output length mismatch")
	}
	return addIncreaseSteps(out, steps, series, label, window)
}

func AccumulateAggregateByLabelSteps(out []AggregateStep, steps []int64, series block.DecodedSeries, label string, window time.Duration) error {
	if len(out) != len(steps) {
		return errors.New("query: step output length mismatch")
	}
	return accumulateAggregateByLabelSelectedSteps(out, steps, series, label, window, nil)
}

func accumulateAggregateByLabelSelectedSteps(out []AggregateStep, steps []int64, series block.DecodedSeries, label string, window time.Duration, selected []bool) error {
	if len(out) != len(steps) {
		return errors.New("query: step output length mismatch")
	}
	if selected != nil && len(selected) != len(steps) {
		return errors.New("query: selected step length mismatch")
	}
	aggregates, err := aggregateSummariesForSteps(series.Timestamps, series.Values, steps, window)
	if err != nil {
		return err
	}
	key, ok := series.Entry.Labels.Get(label)
	if !ok {
		key = ""
	}
	for i, agg := range aggregates {
		if selected != nil && !selected[i] {
			continue
		}
		if agg.Count == 0 {
			continue
		}
		out[i].Values[key] = mergeAggregate(out[i].Values[key], agg)
	}
	return nil
}

func RateByLabelInBlock(path string, selector index.Selector, opts Options, label string) (map[string]float64, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		return nil, err
	}
	return RateByLabelInBlockWithDirectory(path, dir, selector, opts, label)
}

func RateByLabelInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, opts Options, label string) (map[string]float64, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]float64)
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, func(series block.DecodedSeries) error {
		rate, ok, err := rateOneSamples(series.Timestamps, series.Values, opts)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += rate
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func IncreaseByLabelInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, opts Options, label string) (map[string]int64, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, func(series block.DecodedSeries) error {
		summary, ok, err := RateSummaryForSamples(series.Entry.SeriesID, series.Timestamps, series.Values, opts)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += summary.Increase
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func RateSummariesInBlockWithDirectory(path string, dir block.Directory, selector index.Selector, opts Options) ([]RateSummary, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	var out []RateSummary
	err := block.ScanFileFilteredWithDirectoryShared(path, dir, func(entry block.DirectoryEntry) bool {
		return matchLabels(entry, selector) && matchTimeRange(entry, opts) && matchZoneMap(entry.ZoneMap, opts)
	}, func(series block.DecodedSeries) error {
		summary, ok, err := RateSummaryForSamples(series.Entry.SeriesID, series.Timestamps, series.Values, opts)
		if err != nil {
			return err
		}
		if ok {
			summary.Labels = series.Entry.Labels
			out = append(out, summary)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func RateByLabelInSeries(input []block.Series, selector index.Selector, opts Options, label string) (map[string]float64, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]float64)
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return nil, errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		rate, ok, err := rateOneSamples(series.Timestamps, series.Values, opts)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += rate
	}
	return out, nil
}

func IncreaseByLabelInSeries(input []block.Series, selector index.Selector, opts Options, label string) (map[string]int64, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return nil, errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		summary, ok, err := RateSummaryForSamples(uint64(series.ID), series.Timestamps, series.Values, opts)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key, ok := entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += summary.Increase
	}
	return out, nil
}

func RateSummariesInSeries(input []block.Series, selector index.Selector, opts Options) ([]RateSummary, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make([]RateSummary, 0)
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return nil, errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		summary, ok, err := RateSummaryForSamples(uint64(series.ID), series.Timestamps, series.Values, opts)
		if err != nil {
			return nil, err
		}
		if ok {
			summary.Labels = entry.Labels
			out = append(out, summary)
		}
	}
	return out, nil
}

func rateOne(s block.DecodedSeries) (float64, bool, error) {
	return rateOneSamples(s.Timestamps, s.Values, Options{})
}

func rateOneSamples(timestamps []int64, values []int64, opts Options) (float64, bool, error) {
	summary, ok, err := RateSummaryForSamples(0, timestamps, values, opts)
	if err != nil || !ok {
		return 0, ok, err
	}
	return RateFromSummary(summary)
}

func RateSummaryForSamples(seriesID uint64, timestamps []int64, values []int64, opts Options) (RateSummary, bool, error) {
	if len(timestamps) != len(values) {
		return RateSummary{}, false, errors.New("query: timestamp/value length mismatch")
	}
	if err := validateMaxSampleGap(opts); err != nil {
		return RateSummary{}, false, err
	}
	timestamps, values = trimSamples(timestamps, values, opts)
	if len(values) < 2 {
		return RateSummary{}, false, nil
	}
	if hasStaleGap(timestamps, opts) {
		return RateSummary{}, false, nil
	}
	summary := RateSummary{
		SeriesID:    seriesID,
		FirstMillis: timestamps[0],
		LastMillis:  timestamps[len(timestamps)-1],
		FirstValue:  values[0],
		LastValue:   values[len(values)-1],
		Count:       len(values),
	}
	if summary.LastMillis <= summary.FirstMillis {
		return RateSummary{}, false, nil
	}
	for i := 1; i < len(values); i++ {
		delta := values[i] - values[i-1]
		if delta > 0 {
			summary.Increase += delta
		} else if delta < 0 {
			summary.ResetCount++
		}
	}
	return summary, true, nil
}

func RateWindowSummariesForSteps(seriesID uint64, timestamps []int64, values []int64, steps []int64, window time.Duration) ([]RateSummary, error) {
	return rateSummariesForSteps(seriesID, timestamps, values, steps, window, Options{}, 1)
}

func RateWindowSummariesForStepsWithOptions(seriesID uint64, timestamps []int64, values []int64, steps []int64, window time.Duration, opts Options) ([]RateSummary, error) {
	return rateSummariesForSteps(seriesID, timestamps, values, steps, window, opts, 1)
}

func rateSummariesForSteps(seriesID uint64, timestamps []int64, values []int64, steps []int64, window time.Duration, opts Options, minCount int) ([]RateSummary, error) {
	if len(timestamps) != len(values) {
		return nil, errors.New("query: timestamp/value length mismatch")
	}
	if err := validateMaxSampleGap(opts); err != nil {
		return nil, err
	}
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	if minCount < 1 {
		minCount = 1
	}
	out := make([]RateSummary, len(steps))
	if len(timestamps) < minCount || len(steps) == 0 {
		return out, nil
	}
	increasePrefix, resetPrefix := counterDeltaPrefixes(values)
	stalePrefix := staleGapPrefix(timestamps, opts)
	lo := 0
	hi := -1
	for stepIndex, stepMillis := range steps {
		windowStart := stepMillis - windowMillis
		for lo < len(timestamps) && timestamps[lo] < windowStart {
			lo++
		}
		if hi < lo-1 {
			hi = lo - 1
		}
		for hi+1 < len(timestamps) && timestamps[hi+1] <= stepMillis {
			hi++
		}
		count := hi - lo + 1
		if count < minCount {
			continue
		}
		if stalePrefix[hi]-stalePrefix[lo] > 0 {
			continue
		}
		out[stepIndex] = RateSummary{
			SeriesID:    seriesID,
			FirstMillis: timestamps[lo],
			LastMillis:  timestamps[hi],
			FirstValue:  values[lo],
			LastValue:   values[hi],
			Increase:    increasePrefix[hi] - increasePrefix[lo],
			ResetCount:  resetPrefix[hi] - resetPrefix[lo],
			Count:       count,
		}
	}
	return out, nil
}

func counterIncreasePrefix(values []int64) []int64 {
	increasePrefix, _ := counterDeltaPrefixes(values)
	return increasePrefix
}

func counterDeltaPrefixes(values []int64) ([]int64, []int) {
	prefix := make([]int64, len(values))
	resets := make([]int, len(values))
	for i := 1; i < len(values); i++ {
		prefix[i] = prefix[i-1]
		resets[i] = resets[i-1]
		if delta := values[i] - values[i-1]; delta > 0 {
			prefix[i] += delta
		} else if delta < 0 {
			resets[i]++
		}
	}
	return prefix, resets
}

func validateMaxSampleGap(opts Options) error {
	if opts.MaxSampleGapMillis != nil && *opts.MaxSampleGapMillis <= 0 {
		return errors.New("query: max sample gap must be at least 1ms")
	}
	return nil
}

func hasStaleGap(timestamps []int64, opts Options) bool {
	if opts.MaxSampleGapMillis == nil {
		return false
	}
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i]-timestamps[i-1] > *opts.MaxSampleGapMillis {
			return true
		}
	}
	return false
}

func staleGapPrefix(timestamps []int64, opts Options) []int {
	prefix := make([]int, len(timestamps))
	if opts.MaxSampleGapMillis == nil {
		return prefix
	}
	for i := 1; i < len(timestamps); i++ {
		prefix[i] = prefix[i-1]
		if timestamps[i]-timestamps[i-1] > *opts.MaxSampleGapMillis {
			prefix[i]++
		}
	}
	return prefix
}

func aggregateSummariesForSteps(timestamps []int64, values []int64, steps []int64, window time.Duration) ([]Aggregate, error) {
	if len(timestamps) != len(values) {
		return nil, errors.New("query: timestamp/value length mismatch")
	}
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return nil, errors.New("query: window must be at least 1ms")
	}
	out := make([]Aggregate, len(steps))
	if len(values) == 0 || len(steps) == 0 {
		return out, nil
	}
	prefix := make([]int64, len(values)+1)
	for i, value := range values {
		prefix[i+1] = prefix[i] + value
	}
	minDeque := make([]int, 0, len(values))
	maxDeque := make([]int, 0, len(values))
	lo := 0
	hi := -1
	for stepIndex, stepMillis := range steps {
		windowStart := stepMillis - windowMillis
		for lo < len(timestamps) && timestamps[lo] < windowStart {
			lo++
		}
		for len(minDeque) > 0 && minDeque[0] < lo {
			minDeque = minDeque[1:]
		}
		for len(maxDeque) > 0 && maxDeque[0] < lo {
			maxDeque = maxDeque[1:]
		}
		if hi < lo-1 {
			hi = lo - 1
		}
		for hi+1 < len(timestamps) && timestamps[hi+1] <= stepMillis {
			hi++
			for len(minDeque) > 0 && values[minDeque[len(minDeque)-1]] >= values[hi] {
				minDeque = minDeque[:len(minDeque)-1]
			}
			minDeque = append(minDeque, hi)
			for len(maxDeque) > 0 && values[maxDeque[len(maxDeque)-1]] <= values[hi] {
				maxDeque = maxDeque[:len(maxDeque)-1]
			}
			maxDeque = append(maxDeque, hi)
		}
		count := hi - lo + 1
		if count <= 0 {
			continue
		}
		out[stepIndex] = Aggregate{
			Sum:   prefix[hi+1] - prefix[lo],
			Count: count,
			Min:   values[minDeque[0]],
			Max:   values[maxDeque[0]],
		}
	}
	return out, nil
}

func mergeAggregate(current Aggregate, next Aggregate) Aggregate {
	if next.Count == 0 {
		return current
	}
	if current.Count == 0 {
		return next
	}
	if next.Min < current.Min {
		current.Min = next.Min
	}
	if next.Max > current.Max {
		current.Max = next.Max
	}
	current.Sum += next.Sum
	current.Count += next.Count
	return current
}

func aggregateFromBucket(bucket block.AggregateBucket) Aggregate {
	return Aggregate{
		Sum:   bucket.Sum,
		Count: bucket.Count,
		Min:   bucket.Min,
		Max:   bucket.Max,
	}
}

func aggregateEntryBuckets(entry block.DirectoryEntry, opts Options) (Aggregate, bool) {
	if len(entry.AggregateBuckets) == 0 {
		return Aggregate{}, false
	}
	out := Aggregate{}
	found := false
	for _, bucket := range entry.AggregateBuckets {
		if !bucketMatchesTime(bucket, opts) {
			continue
		}
		if !bucketFullyContained(bucket, opts) {
			return Aggregate{}, false
		}
		found = true
		out = mergeAggregate(out, aggregateFromBucket(bucket))
	}
	if !found && seriesMayContainSamples(entry, opts) {
		return Aggregate{}, false
	}
	return out, true
}

func allSteps(n int) []bool {
	out := make([]bool, n)
	for i := range out {
		out[i] = true
	}
	return out
}

func bucketMatchesTime(bucket block.AggregateBucket, opts Options) bool {
	if opts.StartMillis != nil && bucket.TimeMax < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && bucket.TimeMin > *opts.EndMillis {
		return false
	}
	return true
}

func bucketFullyContained(bucket block.AggregateBucket, opts Options) bool {
	if opts.StartMillis != nil && bucket.TimeMin < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && bucket.TimeMax > *opts.EndMillis {
		return false
	}
	return true
}

func seriesMayContainSamples(entry block.DirectoryEntry, opts Options) bool {
	if opts.StartMillis != nil && entry.TimeMax < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && entry.TimeMin > *opts.EndMillis {
		return false
	}
	return true
}

func RateFromSummary(summary RateSummary) (float64, bool, error) {
	if summary.Count < 2 {
		return 0, false, nil
	}
	dtMillis := summary.LastMillis - summary.FirstMillis
	if dtMillis <= 0 {
		return 0, false, nil
	}
	return float64(summary.Increase) / (float64(dtMillis) / 1000.0), true, nil
}

func matchLabels(entry block.DirectoryEntry, selector index.Selector) bool {
	for _, matcher := range selector.Matchers {
		if !supportedMatcher(matcher.Op) {
			return false
		}
		got, ok := entry.Labels.Get(matcher.Name)
		if !ok {
			if matcher.Op == index.MatchNotEqual || matcher.Op == index.MatchNotRegexp {
				continue
			}
			return false
		}
		if !matcher.Matches(got) {
			return false
		}
	}
	return true
}

func supportedMatcher(op index.MatchOp) bool {
	return op == index.MatchEqual || op == index.MatchRegexp || op == index.MatchNotEqual || op == index.MatchNotRegexp
}

func matchZoneMap(z block.ZoneMap, opts Options) bool {
	if opts.MinValue != nil && z.Max < *opts.MinValue {
		return false
	}
	if opts.MaxValue != nil && z.Min > *opts.MaxValue {
		return false
	}
	return true
}

func matchTimeRange(entry block.DirectoryEntry, opts Options) bool {
	if opts.StartMillis != nil && entry.TimeMax < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && entry.TimeMin > *opts.EndMillis {
		return false
	}
	return true
}

func trimSeries(series []block.DecodedSeries, opts Options) []block.DecodedSeries {
	if !hasPointFilters(opts) {
		return series
	}
	out := make([]block.DecodedSeries, 0, len(series))
	for _, s := range series {
		trimmed := s
		trimmed.Timestamps = make([]int64, 0, len(s.Timestamps))
		trimmed.Values = make([]int64, 0, len(s.Values))
		for i, ts := range s.Timestamps {
			if opts.StartMillis != nil && ts < *opts.StartMillis {
				continue
			}
			if opts.EndMillis != nil && ts > *opts.EndMillis {
				continue
			}
			value := s.Values[i]
			if opts.MinValue != nil && value < *opts.MinValue {
				continue
			}
			if opts.MaxValue != nil && value > *opts.MaxValue {
				continue
			}
			trimmed.Timestamps = append(trimmed.Timestamps, ts)
			trimmed.Values = append(trimmed.Values, value)
		}
		if len(trimmed.Timestamps) > 0 {
			out = append(out, trimmed)
		}
	}
	return out
}

func trimSamples(timestamps []int64, values []int64, opts Options) ([]int64, []int64) {
	if !hasPointFilters(opts) {
		return timestamps, values
	}
	trimmedTimestamps := make([]int64, 0, len(timestamps))
	trimmedValues := make([]int64, 0, len(values))
	for i, ts := range timestamps {
		if opts.StartMillis != nil && ts < *opts.StartMillis {
			continue
		}
		if opts.EndMillis != nil && ts > *opts.EndMillis {
			continue
		}
		value := values[i]
		if opts.MinValue != nil && value < *opts.MinValue {
			continue
		}
		if opts.MaxValue != nil && value > *opts.MaxValue {
			continue
		}
		trimmedTimestamps = append(trimmedTimestamps, ts)
		trimmedValues = append(trimmedValues, value)
	}
	return trimmedTimestamps, trimmedValues
}

func sampleMatches(timestamp int64, value int64, opts Options) bool {
	if opts.StartMillis != nil && timestamp < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && timestamp > *opts.EndMillis {
		return false
	}
	if opts.MinValue != nil && value < *opts.MinValue {
		return false
	}
	if opts.MaxValue != nil && value > *opts.MaxValue {
		return false
	}
	return true
}

func hasPointFilters(opts Options) bool {
	return opts.StartMillis != nil || opts.EndMillis != nil || opts.MinValue != nil || opts.MaxValue != nil
}

func hasValueFilters(opts Options) bool {
	return opts.MinValue != nil || opts.MaxValue != nil
}

func addValue(agg Aggregate, value int64) Aggregate {
	if agg.Count == 0 {
		agg.Min = value
		agg.Max = value
	} else {
		if value < agg.Min {
			agg.Min = value
		}
		if value > agg.Max {
			agg.Max = value
		}
	}
	agg.Sum += value
	agg.Count++
	return agg
}

func addZoneMap(agg Aggregate, z block.ZoneMap) Aggregate {
	if z.Count == 0 {
		return agg
	}
	if agg.Count == 0 {
		agg.Min = z.Min
		agg.Max = z.Max
	} else {
		if z.Min < agg.Min {
			agg.Min = z.Min
		}
		if z.Max > agg.Max {
			agg.Max = z.Max
		}
	}
	agg.Sum += z.Sum
	agg.Count += z.Count
	return agg
}

func directoryEntryFromSeries(series block.Series) block.DirectoryEntry {
	zoneMap := block.BuildZoneMap(series.Values)
	if series.Type != model.MetricTypeCounter {
		zoneMap.HasReset = false
	}
	return block.DirectoryEntry{
		SeriesID: uint64(series.ID),
		Type:     series.Type,
		Labels:   series.Labels.Canonical(),
		TimeMin:  minTimestamp(series.Timestamps),
		TimeMax:  maxTimestamp(series.Timestamps),
		ValueN:   len(series.Values),
		ZoneMap:  zoneMap,
	}
}

func minTimestamp(timestamps []int64) int64 {
	if len(timestamps) == 0 {
		return 0
	}
	min := timestamps[0]
	for _, timestamp := range timestamps[1:] {
		if timestamp < min {
			min = timestamp
		}
	}
	return min
}

func maxTimestamp(timestamps []int64) int64 {
	if len(timestamps) == 0 {
		return 0
	}
	max := timestamps[0]
	for _, timestamp := range timestamps[1:] {
		if timestamp > max {
			max = timestamp
		}
	}
	return max
}
