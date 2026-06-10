package query

import (
	"slices"

	"github.com/yaop-labs/amber/internal/index"
)

type ExecutionPlan struct {
	Segments       []index.SegmentTimeRange
	Steps          []PlanStep
	TotalSegments  int
	PrunedSegments int
}

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

type Planner struct {
	sparse *index.SparseIndex
}

func NewPlanner(sparse *index.SparseIndex) *Planner {
	return &Planner{sparse: sparse}
}

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

func (plan *ExecutionPlan) HasStep(step PlanStep) bool {
	return slices.Contains(plan.Steps, step)
}
