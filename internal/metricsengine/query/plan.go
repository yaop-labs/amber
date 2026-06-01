package query

import (
	"errors"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

type Operation int

const (
	OpSelect Operation = iota
	OpSumByLabel
	OpAggregateByLabel
	OpRateByLabel
	OpIncreaseByLabel
	OpRateByLabelRange
	OpIncreaseByLabelRange
	OpRateByLabelRangeSteps
	OpIncreaseByLabelRangeSteps
	OpSumByLabelRangeSteps
	OpAggregateByLabelRangeSteps
)

type Plan struct {
	Operation     Operation
	Selector      index.Selector
	Options       Options
	RangeSelector RangeSelector
	StartMillis   int64
	EndMillis     int64
	Step          time.Duration
	ByLabel       string
}

type Result struct {
	Series         []block.DecodedSeries
	Aggregates     map[string]Aggregate
	IntValues      map[string]int64
	FloatValues    map[string]float64
	IntSteps       []IntStep
	FloatSteps     []FloatStep
	AggregateSteps []AggregateStep
}

type ExecutionPath int

const (
	PathMaterializeSeries ExecutionPath = iota
	PathHeadOnly
	PathDirectoryAggregate
	PathBucketAggregate
	PathSingleBlockStreaming
	PathMultiBlockStreaming
	PathCoalescedSummaries
)

type CandidateStats struct {
	BlockCount          int
	BlockSeries         int
	BlockSamples        int
	HeadSeries          int
	HeadSamples         int
	StepCount           int
	HasPointFilters     bool
	BucketSeries        int
	BucketSamples       int
	PartialBucketSeries int
}

type PlanCost struct {
	EstimatedSeries         int
	EstimatedSamples        int
	EstimatedDecodedSamples int
	StepCount               int
	RequiresCoalesce        bool
	UsesDirectory           bool
	UsesStreaming           bool
	UsesBuckets             bool
}

type PlanCandidate struct {
	Path  ExecutionPath
	Cost  PlanCost
	Score int64
}

type ExecutionPlan struct {
	Plan       Plan
	Path       ExecutionPath
	Cost       PlanCost
	Stats      CandidateStats
	Candidates []PlanCandidate
}

func (p Plan) Validate() error {
	switch p.Operation {
	case OpSelect, OpSumByLabel, OpAggregateByLabel, OpRateByLabel, OpIncreaseByLabel:
		return p.Selector.Validate()
	case OpRateByLabelRange, OpIncreaseByLabelRange:
		return validateRangeSelector(p.RangeSelector)
	case OpRateByLabelRangeSteps, OpIncreaseByLabelRangeSteps, OpSumByLabelRangeSteps, OpAggregateByLabelRangeSteps:
		if err := validateRangeSelector(p.RangeSelector); err != nil {
			return err
		}
		_, err := StepMillis(p.StartMillis, p.EndMillis, p.Step)
		return err
	default:
		return errors.New("query: unsupported operation")
	}
}

func (p Plan) StorageSelectorOptions() (index.Selector, Options, error) {
	if err := p.Validate(); err != nil {
		return index.Selector{}, Options{}, err
	}
	switch p.Operation {
	case OpSelect, OpSumByLabel, OpAggregateByLabel, OpRateByLabel, OpIncreaseByLabel:
		return p.Selector.Optimized(), p.Options, nil
	case OpRateByLabelRange, OpIncreaseByLabelRange:
		return p.RangeSelector.Selector.Optimized(), p.RangeSelector.Options(p.EndMillis), nil
	case OpRateByLabelRangeSteps, OpIncreaseByLabelRangeSteps, OpSumByLabelRangeSteps, OpAggregateByLabelRangeSteps:
		readStart := p.StartMillis - p.RangeSelector.Window.Milliseconds()
		opts := TimeRange(readStart, p.EndMillis)
		if p.RangeSelector.MaxSampleGap > 0 {
			opts = opts.WithMaxSampleGap(p.RangeSelector.MaxSampleGap)
		}
		return p.RangeSelector.Selector.Optimized(), opts, nil
	default:
		return index.Selector{}, Options{}, errors.New("query: unsupported operation")
	}
}

func PlanExecution(plan Plan, stats CandidateStats) (ExecutionPlan, error) {
	if err := plan.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	baseCost := PlanCost{
		EstimatedSeries:  stats.BlockSeries + stats.HeadSeries,
		EstimatedSamples: stats.BlockSamples + stats.HeadSamples,
		StepCount:        stats.StepCount,
	}
	candidates, err := planCandidates(plan, stats, baseCost)
	if err != nil {
		return ExecutionPlan{}, err
	}
	best := chooseBestCandidate(candidates)
	return ExecutionPlan{
		Plan:       plan,
		Path:       best.Path,
		Cost:       best.Cost,
		Stats:      stats,
		Candidates: candidates,
	}, nil
}

func planCandidates(plan Plan, stats CandidateStats, baseCost PlanCost) ([]PlanCandidate, error) {
	switch plan.Operation {
	case OpSelect:
		cost := baseCost
		cost.EstimatedDecodedSamples = cost.EstimatedSamples
		return []PlanCandidate{newPlanCandidate(PathMaterializeSeries, cost)}, nil
	case OpSumByLabel, OpAggregateByLabel:
		candidates := make([]PlanCandidate, 0, 3)
		if stats.HeadSeries == 0 && !stats.HasPointFilters {
			cost := baseCost
			cost.UsesDirectory = true
			candidates = append(candidates, newPlanCandidate(PathDirectoryAggregate, cost))
		}
		if stats.BucketSamples > 0 && stats.HeadSeries == 0 {
			cost := baseCost
			cost.UsesDirectory = true
			cost.UsesStreaming = stats.PartialBucketSeries > 0
			cost.UsesBuckets = true
			cost.EstimatedDecodedSamples = maxInt(0, cost.EstimatedSamples-stats.BucketSamples)
			candidates = append(candidates, newPlanCandidate(PathBucketAggregate, cost))
		}
		cost := baseCost
		cost.UsesStreaming = true
		cost.EstimatedDecodedSamples = cost.EstimatedSamples
		candidates = append(candidates, newPlanCandidate(PathMultiBlockStreaming, cost))
		return candidates, nil
	case OpRateByLabel, OpIncreaseByLabel, OpRateByLabelRange, OpIncreaseByLabelRange:
		path := rateExecutionPath(stats)
		cost := baseCost
		cost.UsesStreaming = path == PathSingleBlockStreaming
		cost.RequiresCoalesce = path == PathCoalescedSummaries
		cost.EstimatedDecodedSamples = cost.EstimatedSamples
		return []PlanCandidate{newPlanCandidate(path, cost)}, nil
	case OpRateByLabelRangeSteps, OpIncreaseByLabelRangeSteps:
		cost := baseCost
		var path ExecutionPath
		if stats.BlockCount == 1 && stats.HeadSeries == 0 {
			path = PathSingleBlockStreaming
		} else if rateRangeStepSummaryViable(plan, stats) {
			path = PathCoalescedSummaries
			cost.RequiresCoalesce = true
			cost.EstimatedDecodedSamples = stats.BlockSeries * maxInt(1, stats.StepCount)
		} else {
			path = PathMultiBlockStreaming
			cost.RequiresCoalesce = stats.BlockCount > 1 || stats.HeadSeries > 0
		}
		cost.UsesStreaming = true
		if cost.EstimatedDecodedSamples == 0 {
			cost.EstimatedDecodedSamples = cost.EstimatedSamples
		}
		return []PlanCandidate{newPlanCandidate(path, cost)}, nil
	case OpSumByLabelRangeSteps, OpAggregateByLabelRangeSteps:
		candidates := make([]PlanCandidate, 0, 2)
		if stats.BucketSamples > 0 && stats.BucketSamples >= stats.BlockSamples && stats.HeadSeries == 0 {
			cost := baseCost
			cost.UsesDirectory = true
			cost.UsesStreaming = stats.PartialBucketSeries > 0
			cost.UsesBuckets = true
			cost.EstimatedDecodedSamples = maxInt(0, cost.EstimatedSamples-stats.BucketSamples)
			candidates = append(candidates, newPlanCandidate(PathBucketAggregate, cost))
		}
		cost := baseCost
		if stats.BlockCount == 1 && stats.HeadSeries == 0 {
			cost.UsesStreaming = true
			cost.EstimatedDecodedSamples = cost.EstimatedSamples
			candidates = append(candidates, newPlanCandidate(PathSingleBlockStreaming, cost))
		} else {
			cost.RequiresCoalesce = stats.BlockCount > 1 || stats.HeadSeries > 0
			cost.UsesStreaming = true
			cost.EstimatedDecodedSamples = cost.EstimatedSamples
			candidates = append(candidates, newPlanCandidate(PathMultiBlockStreaming, cost))
		}
		return candidates, nil
	default:
		return nil, errors.New("query: unsupported operation")
	}
}

func newPlanCandidate(path ExecutionPath, cost PlanCost) PlanCandidate {
	return PlanCandidate{Path: path, Cost: cost, Score: planCandidateScore(cost)}
}

func planCandidateScore(cost PlanCost) int64 {
	score := int64(cost.EstimatedDecodedSamples)*10 + int64(cost.EstimatedSamples) + int64(cost.EstimatedSeries)*5 + int64(cost.StepCount)
	if cost.RequiresCoalesce {
		score += int64(cost.EstimatedSeries) * 20
	}
	if cost.UsesStreaming {
		score += int64(cost.EstimatedSeries)
	}
	if cost.UsesDirectory {
		score -= int64(cost.EstimatedSeries) * 5
	}
	if cost.UsesBuckets {
		score -= int64(cost.EstimatedSeries) * 3
	}
	if score < 0 {
		return 0
	}
	return score
}

func chooseBestCandidate(candidates []PlanCandidate) PlanCandidate {
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if candidate.Score < best.Score {
			best = candidate
		}
	}
	return best
}

func rateExecutionPath(stats CandidateStats) ExecutionPath {
	if stats.BlockCount == 0 {
		return PathHeadOnly
	}
	if stats.BlockCount == 1 && stats.HeadSeries == 0 {
		return PathSingleBlockStreaming
	}
	return PathCoalescedSummaries
}

func rateRangeStepSummaryViable(plan Plan, stats CandidateStats) bool {
	return stats.StepCount <= 2 && stats.BlockCount > 1 && stats.HeadSeries == 0 && plan.RangeSelector.MaxSampleGap <= 0
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func validateRangeSelector(rangeSelector RangeSelector) error {
	if err := rangeSelector.Selector.Validate(); err != nil {
		return err
	}
	if rangeSelector.Window.Milliseconds() <= 0 {
		return errors.New("query: window must be at least 1ms")
	}
	if rangeSelector.MaxSampleGap < 0 {
		return errors.New("query: max sample gap must be non-negative")
	}
	return nil
}

func DirectoryStats(dir block.Directory, selector index.Selector, opts Options) (int, int, error) {
	if err := selector.Validate(); err != nil {
		return 0, 0, err
	}
	seriesCount := 0
	sampleCount := 0
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		seriesCount++
		sampleCount += entry.ValueN
	}
	return seriesCount, sampleCount, nil
}

func DirectoryBucketStats(dir block.Directory, selector index.Selector, opts Options) (int, int, int, error) {
	if err := selector.Validate(); err != nil {
		return 0, 0, 0, err
	}
	if hasValueFilters(opts) {
		return 0, 0, 0, nil
	}
	bucketSeries := 0
	bucketSamples := 0
	partialSeries := 0
	for _, entry := range dir.Series {
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		if len(entry.AggregateBuckets) == 0 {
			partialSeries++
			continue
		}
		hasBucket := false
		hasPartial := false
		for _, bucket := range entry.AggregateBuckets {
			if !bucketMatchesTime(bucket, opts) {
				continue
			}
			if !bucketFullyContained(bucket, opts) {
				hasPartial = true
				continue
			}
			hasBucket = true
			bucketSamples += bucket.Count
		}
		if hasBucket {
			bucketSeries++
		}
		if hasPartial || (!hasBucket && seriesMayContainSamples(entry, opts)) {
			partialSeries++
		}
	}
	return bucketSeries, bucketSamples, partialSeries, nil
}

func SeriesStats(input []block.Series, selector index.Selector, opts Options) (int, int, error) {
	if err := selector.Validate(); err != nil {
		return 0, 0, err
	}
	seriesCount := 0
	sampleCount := 0
	for _, series := range input {
		if len(series.Timestamps) != len(series.Values) {
			return 0, 0, errors.New("query: timestamp/value length mismatch")
		}
		if len(series.Timestamps) == 0 {
			continue
		}
		entry := directoryEntryFromSeries(series)
		if !matchLabels(entry, selector) || !matchTimeRange(entry, opts) || !matchZoneMap(entry.ZoneMap, opts) {
			continue
		}
		seriesCount++
		sampleCount += len(series.Values)
	}
	return seriesCount, sampleCount, nil
}
