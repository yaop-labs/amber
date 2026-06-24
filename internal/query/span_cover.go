package query

// Covering-index trace-summary path. For a service-filtered trace search, the
// per-segment .cidx projection (coverindex.go) yields the matching spans'
// summary fields (trace id, start, end, status, operation, is-root) WITHOUT
// decompressing row blocks. Spans returned here carry only those fields and are
// for trace summaries only (buildTraceSummaries); the row scan remains the
// source of truth and the fallback for any segment lacking a .cidx.

import (
	"container/heap"
	"context"
	"path/filepath"
	"sort"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
)

// nonRootParent is any non-zero SpanID, so a synthesized span reports
// IsRoot()==false (the projection stores the real is-root bit).
var nonRootParent = model.SpanID{0xFF}

// spanCover returns the cached/loaded covering index for a span segment.
func (e *Executor) spanCover(name string) (*index.CoverIndex, bool) {
	if ci, ok := e.spanCoverCache.get(name); ok {
		return ci, true
	}
	if e.spanDir == "" {
		return nil, false
	}
	ci, err := index.OpenCoverIndex(filepath.Join(e.spanDir, name+".cidx"))
	if err != nil {
		return nil, false
	}
	e.spanCoverCache.put(name, ci)
	return ci, true
}

// coveringSummaryShape reports whether a query is a plain service-filtered trace
// search the covering path can serve (operation/duration are post-filters). A
// trace_id or status filter falls back to the scan.
func coveringSummaryShape(q *SpanQuery) bool {
	return len(q.Services) > 0 && model.IsZeroTraceID(q.TraceID) && len(q.Statuses) == 0
}

// SpanSummaryFetch returns up to maxSpans newest matching spans for a trace-
// summary query (newest-first), using each sealed segment's .cidx projection
// when present and falling back to the row scan for segments without one
// (active/legacy). Returned spans carry only summary fields - use them solely
// for buildTraceSummaries. The bool reports whether the maxSpans cap truncated.
func (e *Executor) SpanSummaryFetch(ctx context.Context, q *SpanQuery, maxSpans int) ([]model.SpanEntry, bool, error) {
	if err := q.Validate(); err != nil {
		return nil, false, err
	}
	if maxSpans <= 0 {
		maxSpans = 100
	}

	plan := NewPlanner(e.spanSparse).Plan(&LogQuery{From: q.From, To: q.To})
	if len(plan.Segments) == 0 {
		return nil, false, nil
	}
	segs := make([]index.SegmentTimeRange, len(plan.Segments))
	copy(segs, plan.Segments)
	segs = filterQueryableSegments(segs, e.spanManager)
	sort.Slice(segs, func(i, j int) bool { return segs[i].MaxTS > segs[j].MaxTS })

	canCover := coveringSummaryShape(q)
	hp := &spanMinHeap{}
	heap.Init(hp)
	totalHits := 0

	push := func(se model.SpanEntry) {
		if hp.Len() < maxSpans {
			heap.Push(hp, se)
		} else if spanEntryNewer(se, (*hp)[0]) {
			(*hp)[0] = se
			heap.Fix(hp, 0)
		}
	}

	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if hp.Len() >= maxSpans {
			thresholdTS := (*hp)[0].StartTime.UnixNano()
			if seg.MaxTS < thresholdTS {
				continue // older than the kept window; cannot contribute
			}
		}
		used := false
		if canCover {
			if ci, ok := e.spanCover(seg.FileName); ok {
				spans, err := e.coverSpans(ci, q)
				if err == nil {
					for i := range spans {
						push(spans[i])
					}
					totalHits += len(spans)
					used = true
				}
				// On a covering read error, fall through to the scan so the
				// segment still contributes (correctness over speed).
			}
		}
		if !used {
			matched, err := e.execSpanSegment(ctx, q, seg, Cursor{}, hp, maxSpans)
			if err != nil {
				return nil, false, err
			}
			totalHits += matched
		}
	}

	spans := make([]model.SpanEntry, hp.Len())
	for i := len(spans) - 1; i >= 0; i-- {
		spans[i] = heap.Pop(hp).(model.SpanEntry)
	}
	return spans, totalHits > maxSpans, nil
}

// coverSpans reads one segment's covering projection for the query's services
// and returns the matching spans (all of them; the caller applies the top-k cap)
// as summary-only SpanEntry. Returns an error only on IO/decode failure, letting
// the caller scan the segment instead.
func (e *Executor) coverSpans(ci *index.CoverIndex, q *SpanQuery) ([]model.SpanEntry, error) {
	minDur := int64(q.MinDuration)
	maxDur := int64(q.MaxDuration)
	hasFrom := !q.From.IsZero()
	hasTo := !q.To.IsZero()
	fromNano, toNano := q.From.UnixNano(), q.To.UnixNano()

	var out []model.SpanEntry
	for _, svc := range q.Services {
		if !ci.HasService(svc) {
			continue
		}
		// Resolve the operation filter to this segment's op ids; if none of the
		// requested operations exist here, no span in this service matches.
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
			return nil, err
		}

		var ords []uint32
		var rows []int
		for i := range p.Len() {
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
			return nil, err
		}
		for j, i := range rows {
			se := model.SpanEntry{
				TraceID:   model.TraceID(traceIDs[j]),
				Service:   svc,
				Operation: ci.Operation(p.OpID[i]),
				StartTime: time.Unix(0, p.Start[i]),
				EndTime:   time.Unix(0, p.End[i]),
				Status:    model.SpanStatus(p.Status[i]),
			}
			if !p.IsRoot[i] {
				se.ParentID = nonRootParent
			}
			out = append(out, se)
		}
	}
	return out, nil
}
