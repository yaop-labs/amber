package query

// Direct trace-summary aggregation over the .cidx covering projection (variant
// B). Instead of materializing the newest 100k matching spans, pushing them
// through a top-k heap, and regrouping, it folds covering rows straight into a
// per-trace accumulator and early-terminates once it has enough traces. Because
// a trace's spans are time-local (emitted within seconds), the newest traces
// live in the most recent segment or two, so this reads ~1-2 segments' tiny
// projections instead of ~10 - the path from ~0.5s to tens of ms. The row scan
// remains the fallback for segments without a .cidx (active/legacy).

import (
	"container/heap"
	"context"
	"sort"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
)

// TraceSummary is a query-layer trace rollup over the matching spans (the HTTP
// layer maps it to its JSON shape).
type TraceSummary struct {
	TraceID   model.TraceID
	Service   string
	Operation string
	Start     time.Time
	End       time.Time
	SpanCount int
	HasErrors bool
}

type traceAcc struct {
	start, end int64
	count      int
	hasErr     bool
	rootSvc    string
	rootOp     string
}

// foldSpan merges one span (visited newest-first) into the trace accumulator,
// matching buildTraceSummaries: start=min, end=max, count, hasErr=OR, and the
// root service/operation = the last root span seen (or the first/newest span if
// the trace has no root among the matched spans).
func foldSpan(acc map[model.TraceID]*traceAcc, tid model.TraceID, start, end int64, isErr bool, svc, op string, isRoot bool) {
	a := acc[tid]
	if a == nil {
		a = &traceAcc{start: start, end: end, rootSvc: svc, rootOp: op}
		acc[tid] = a
	} else {
		if start < a.start {
			a.start = start
		}
		if end > a.end {
			a.end = end
		}
	}
	a.count++
	if isErr {
		a.hasErr = true
	}
	if isRoot {
		a.rootSvc = svc
		a.rootOp = op
	}
}

// SpanTraceSummaries returns the newest trace summaries for a service-filtered
// search, sorted by start descending. It reads covering projections newest-first
// and stops once it has `needed` traces plus one more segment (to absorb any
// time-local trailing spans), capped at maxSpans as a hard safety bound. total
// is the number of traces seen; truncated reports the span cap was hit.
func (e *Executor) SpanTraceSummaries(ctx context.Context, q *SpanQuery, needed, maxSpans int) ([]TraceSummary, int, bool, error) {
	if err := q.Validate(); err != nil {
		return nil, 0, false, err
	}
	if maxSpans <= 0 {
		maxSpans = 100
	}

	plan := NewPlanner(e.spanSparse).Plan(&LogQuery{From: q.From, To: q.To})
	if len(plan.Segments) == 0 {
		return nil, 0, false, nil
	}
	segs := make([]index.SegmentTimeRange, len(plan.Segments))
	copy(segs, plan.Segments)
	segs = filterQueryableSegments(segs, e.spanManager)
	sort.Slice(segs, func(i, j int) bool { return segs[i].MaxTS > segs[j].MaxTS })

	canCover := coveringSummaryShape(q)
	acc := make(map[model.TraceID]*traceAcc)
	spanCount := 0
	truncated := false
	enoughAfter := -1

	for i, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, 0, false, err
		}
		used := false
		if canCover {
			if ci, ok := e.spanCover(seg.FileName); ok {
				if err := e.coverFold(ci, q, acc, &spanCount); err == nil {
					used = true
				}
			}
		}
		if !used {
			if err := e.scanFold(ctx, q, seg, acc, &spanCount, maxSpans); err != nil {
				return nil, 0, false, err
			}
		}
		if needed > 0 && enoughAfter < 0 && len(acc) >= needed {
			enoughAfter = i + 1 // drain one more segment for trailing spans
		}
		if spanCount >= maxSpans {
			truncated = true
			break
		}
		if enoughAfter >= 0 && i >= enoughAfter {
			break
		}
	}

	out := make([]TraceSummary, 0, len(acc))
	for id, a := range acc {
		out = append(out, TraceSummary{
			TraceID:   id,
			Service:   a.rootSvc,
			Operation: a.rootOp,
			Start:     time.Unix(0, a.start),
			End:       time.Unix(0, a.end),
			SpanCount: a.count,
			HasErrors: a.hasErr,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start.After(out[j].Start) })
	return out, len(out), truncated, nil
}

// coverFold folds a segment's covering rows (newest-first) into acc.
func (e *Executor) coverFold(ci *index.CoverIndex, q *SpanQuery, acc map[model.TraceID]*traceAcc, spanCount *int) error {
	minDur := int64(q.MinDuration)
	maxDur := int64(q.MaxDuration)
	hasFrom := !q.From.IsZero()
	hasTo := !q.To.IsZero()
	fromNano, toNano := q.From.UnixNano(), q.To.UnixNano()

	for _, svc := range q.Services {
		if !ci.HasService(svc) {
			continue
		}
		var allowedOps map[uint32]struct{}
		if len(q.Operations) > 0 {
			allowedOps = make(map[uint32]struct{}, len(q.Operations))
			for _, op := range q.Operations {
				if id, ok := ci.OpID(op); ok {
					allowedOps[id] = struct{}{}
				}
			}
			if len(allowedOps) == 0 {
				continue
			}
		}
		p, err := ci.ReadServiceProjection(svc)
		if err != nil {
			return err
		}
		// Rows are id-sorted (~ start asc); walk reverse for newest-first.
		var ords []uint32
		var rows []int
		for i := p.Len() - 1; i >= 0; i-- {
			if hasFrom && p.Start[i] < fromNano {
				continue
			}
			if hasTo && p.Start[i] > toNano {
				continue
			}
			if dur := p.End[i] - p.Start[i]; (minDur > 0 && dur < minDur) || (maxDur > 0 && dur > maxDur) {
				continue
			}
			if allowedOps != nil {
				if _, ok := allowedOps[p.OpID[i]]; !ok {
					continue
				}
			}
			ords = append(ords, p.TraceOrd[i])
			rows = append(rows, i)
		}
		if len(ords) == 0 {
			continue
		}
		traceIDs, err := ci.TraceIDs(ords)
		if err != nil {
			return err
		}
		for j, i := range rows {
			foldSpan(acc, model.TraceID(traceIDs[j]),
				p.Start[i], p.End[i], p.Status[i] == uint8(model.SpanStatusError),
				svc, ci.Operation(p.OpID[i]), p.IsRoot[i])
			*spanCount++
		}
	}
	return nil
}

// scanFold folds one segment's matching spans into acc via the row scan, for
// segments without a .cidx (active/legacy). Spans are folded newest-first.
func (e *Executor) scanFold(ctx context.Context, q *SpanQuery, seg index.SegmentTimeRange, acc map[model.TraceID]*traceAcc, spanCount *int, maxSpans int) error {
	hp := &spanMinHeap{}
	heap.Init(hp)
	if _, err := e.execSpanSegment(ctx, q, seg, Cursor{}, hp, maxSpans); err != nil {
		return err
	}
	spans := make([]model.SpanEntry, hp.Len())
	for i := len(spans) - 1; i >= 0; i-- { // pop ascending -> place newest first
		spans[i] = heap.Pop(hp).(model.SpanEntry)
	}
	for i := range spans {
		s := &spans[i]
		foldSpan(acc, s.TraceID, s.StartTime.UnixNano(), s.EndTime.UnixNano(),
			s.Status == model.SpanStatusError, s.Service, s.Operation, s.IsRoot())
		*spanCount++
	}
	return nil
}
