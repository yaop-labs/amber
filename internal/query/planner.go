package query

import (
	"slices"

	"github.com/yaop-labs/amber/internal/index"
)

// ExecutionPlan is the planned log query: the candidate segments after time
// pruning and the ordered steps the executor will run.
type ExecutionPlan struct {
	Segments       []index.SegmentTimeRange
	Steps          []PlanStep
	TotalSegments  int
	PrunedSegments int
}

// PlanStep is one stage of log query execution.
type PlanStep uint8

const (
	StepSegmentPruning PlanStep = iota
	StepBitmapFilter
	StepFTSSearch
	StepFetchRecords
	StepPostFilter
	StepPaginate
)

func (s PlanStep) String() string {
	switch s {
	case StepSegmentPruning:
		return "SegmentPruning"
	case StepBitmapFilter:
		return "BitmapFilter"
	case StepFTSSearch:
		return "FTSSearch"
	case StepFetchRecords:
		return "FetchRecords"
	case StepPostFilter:
		return "PostFilter"
	case StepPaginate:
		return "Paginate"
	default:
		return "Unknown"
	}
}

// Planner builds an ExecutionPlan for a log query using the sparse index for
// time-based segment pruning.
type Planner struct {
	sparse *index.SparseIndex
}

// NewPlanner returns a planner backed by the given sparse index.
func NewPlanner(sparse *index.SparseIndex) *Planner {
	return &Planner{sparse: sparse}
}

// Plan prunes segments by time and assembles the steps (bitmap filter, FTS,
// fetch, post-filter, paginate) the query requires.
func (p *Planner) Plan(q *LogQuery) *ExecutionPlan {
	plan := &ExecutionPlan{
		TotalSegments: p.sparse.Size(),
	}

	var candidates []index.SegmentTimeRange
	if q.HasTimeRange() {
		candidates = p.sparse.Lookup(q.FromUnixNano(), q.ToUnixNano())
	} else {
		candidates = p.sparse.All()
	}

	plan.Segments = candidates
	plan.PrunedSegments = plan.TotalSegments - len(candidates)
	plan.Steps = append(plan.Steps, StepSegmentPruning)

	if len(candidates) == 0 {
		return plan
	}

	if q.HasFieldFilters() {
		plan.Steps = append(plan.Steps, StepBitmapFilter)
	}

	if q.HasFullText() {
		plan.Steps = append(plan.Steps, StepFTSSearch)
	}

	plan.Steps = append(plan.Steps, StepFetchRecords)

	if len(q.Attrs) > 0 || q.HasTimeRange() {
		plan.Steps = append(plan.Steps, StepPostFilter)
	}

	plan.Steps = append(plan.Steps, StepPaginate)

	return plan
}

// HasStep reports whether the plan includes the given step.
func (plan *ExecutionPlan) HasStep(step PlanStep) bool {
	return slices.Contains(plan.Steps, step)
}
