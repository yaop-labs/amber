package query

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

func TestPlanStorageSelectorOptions(t *testing.T) {
	selector := index.NewSelector(index.MetricName("requests_total"))
	plan := Plan{
		Operation: OpRateByLabelRangeSteps,
		RangeSelector: RangeSelector{
			Selector: selector,
			Window:   2 * time.Second,
		},
		StartMillis: 2000,
		EndMillis:   3000,
		Step:        time.Second,
	}
	gotSelector, opts, err := plan.StorageSelectorOptions()
	if err != nil {
		t.Fatal(err)
	}
	if len(gotSelector.Matchers) != 1 || gotSelector.Matchers[0].Value != "requests_total" {
		t.Fatalf("selector = %+v, want metric selector", gotSelector)
	}
	if opts.StartMillis == nil || *opts.StartMillis != 0 || opts.EndMillis == nil || *opts.EndMillis != 3000 {
		t.Fatalf("opts = %+v, want read window [0,3000]", opts)
	}
}

func TestPlanExecutionChoosesPaths(t *testing.T) {
	plan := Plan{
		Operation: OpRateByLabelRangeSteps,
		RangeSelector: RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   time.Minute,
		},
		StartMillis: 0,
		EndMillis:   60_000,
		Step:        time.Second,
	}
	execPlan, err := PlanExecution(plan, CandidateStats{
		BlockCount:   1,
		BlockSeries:  10,
		BlockSamples: 100,
		StepCount:    61,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathSingleBlockStreaming || !execPlan.Cost.UsesStreaming || execPlan.Cost.RequiresCoalesce {
		t.Fatalf("execPlan = %+v, want one-block streaming without coalesce", execPlan)
	}
	execPlan, err = PlanExecution(plan, CandidateStats{
		BlockCount:   2,
		BlockSeries:  10,
		BlockSamples: 100,
		HeadSeries:   1,
		HeadSamples:  2,
		StepCount:    61,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathMultiBlockStreaming || !execPlan.Cost.RequiresCoalesce {
		t.Fatalf("execPlan = %+v, want multi-block streaming with coalesce", execPlan)
	}
	plan.EndMillis = 1000
	execPlan, err = PlanExecution(plan, CandidateStats{
		BlockCount:   2,
		BlockSeries:  10,
		BlockSamples: 100,
		StepCount:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathCoalescedSummaries || !execPlan.Cost.RequiresCoalesce || execPlan.Cost.EstimatedDecodedSamples != 20 {
		t.Fatalf("execPlan = %+v, want short multi-block summary path", execPlan)
	}
	plan.RangeSelector.MaxSampleGap = time.Second
	execPlan, err = PlanExecution(plan, CandidateStats{
		BlockCount:   2,
		BlockSeries:  10,
		BlockSamples: 100,
		StepCount:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathMultiBlockStreaming {
		t.Fatalf("execPlan = %+v, want max-gap query to avoid summary path", execPlan)
	}
}

func TestPlanExecutionChoosesDirectoryAggregate(t *testing.T) {
	plan := Plan{Operation: OpAggregateByLabel, Selector: index.NewSelector(index.MetricName("cpu_usage"))}
	execPlan, err := PlanExecution(plan, CandidateStats{BlockCount: 1, BlockSeries: 10, BlockSamples: 100})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathDirectoryAggregate || !execPlan.Cost.UsesDirectory || execPlan.Cost.EstimatedDecodedSamples != 0 {
		t.Fatalf("execPlan = %+v, want directory aggregate without decoded samples", execPlan)
	}
	start, end := int64(0), int64(63_000)
	plan = Plan{Operation: OpAggregateByLabel, Selector: index.NewSelector(index.MetricName("cpu_usage")), Options: Options{StartMillis: &start, EndMillis: &end}}
	execPlan, err = PlanExecution(plan, CandidateStats{
		BlockCount:          1,
		BlockSeries:         10,
		BlockSamples:        100,
		HasPointFilters:     true,
		BucketSeries:        8,
		BucketSamples:       80,
		PartialBucketSeries: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathBucketAggregate || !execPlan.Cost.UsesBuckets || execPlan.Cost.EstimatedDecodedSamples != 20 {
		t.Fatalf("execPlan = %+v, want bucket aggregate with 20 decoded samples", execPlan)
	}
	if len(execPlan.Candidates) < 2 || execPlan.Candidates[0].Path != PathBucketAggregate {
		t.Fatalf("candidates = %+v, want bucket candidate ranked first", execPlan.Candidates)
	}
}

func TestPlanExecutionChoosesAggregateRangeStepPaths(t *testing.T) {
	plan := Plan{
		Operation: OpAggregateByLabelRangeSteps,
		RangeSelector: RangeSelector{
			Selector: index.NewSelector(index.MetricName("cpu_usage")),
			Window:   time.Minute,
		},
		StartMillis: 0,
		EndMillis:   60_000,
		Step:        time.Second,
	}
	execPlan, err := PlanExecution(plan, CandidateStats{BlockCount: 1, BlockSeries: 10, BlockSamples: 100, StepCount: 61})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathSingleBlockStreaming || !execPlan.Cost.UsesStreaming || execPlan.Cost.RequiresCoalesce {
		t.Fatalf("execPlan = %+v, want one-block aggregate streaming", execPlan)
	}
	execPlan, err = PlanExecution(plan, CandidateStats{BlockCount: 2, BlockSeries: 10, BlockSamples: 100, HeadSeries: 1, HeadSamples: 2, StepCount: 61})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathMultiBlockStreaming || !execPlan.Cost.RequiresCoalesce {
		t.Fatalf("execPlan = %+v, want multi-block aggregate streaming with coalesce", execPlan)
	}
	execPlan, err = PlanExecution(plan, CandidateStats{
		BlockCount:    2,
		BlockSeries:   10,
		BlockSamples:  100,
		BucketSeries:  5,
		BucketSamples: 50,
		StepCount:     61,
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != PathMultiBlockStreaming || execPlan.Cost.UsesBuckets {
		t.Fatalf("execPlan = %+v, want multi-block aggregate streaming when bucket executor is not viable", execPlan)
	}
}
