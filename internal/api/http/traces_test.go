package http

import (
	"encoding/hex"
	"strconv"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
)

func TestCollectTraceSummariesAggregatesAcrossSpanPages(t *testing.T) {
	start := time.Unix(1000, 0)
	traceA := mustTraceID("0000000000000000000000000000000a")
	traceB := mustTraceID("0000000000000000000000000000000b")
	traceC := mustTraceID("0000000000000000000000000000000c")

	all := []model.SpanEntry{
		makeSpan(traceA, "svc-a", "op-a", start.Add(5*time.Minute)),
		makeSpan(traceA, "svc-a", "op-a-child", start.Add(4*time.Minute)),
		makeSpan(traceB, "svc-b", "op-b", start.Add(3*time.Minute)),
		makeSpan(traceC, "svc-c", "op-c", start.Add(2*time.Minute)),
	}

	// The fetch mock uses cursor as an opaque "next index into all" so we
	// can test the pagination-loop without exercising the real codec.
	summaries, total, err := collectTraceSummaries(func(q *query.SpanQuery) (*query.SpanResult, error) {
		start := 0
		if q.Cursor != "" {
			n, perr := strconv.Atoi(q.Cursor)
			if perr != nil {
				return nil, perr
			}
			start = n
		}
		end := start + q.Limit
		if end > len(all) {
			end = len(all)
		}
		page := all[start:end]
		next := ""
		if end < len(all) {
			next = strconv.Itoa(end)
		}
		return &query.SpanResult{
			Spans:      page,
			TotalHits:  len(all),
			Truncated:  end < len(all),
			NextCursor: next,
		}, nil
	}, query.SpanQuery{})
	if err != nil {
		t.Fatalf("collectTraceSummaries() error = %v", err)
	}

	if total != 3 {
		t.Fatalf("total = %d, want 3 unique traces", total)
	}
	if len(summaries) != 3 {
		t.Fatalf("len(summaries) = %d, want 3", len(summaries))
	}
	if summaries[0].TraceID != hex.EncodeToString(traceA[:]) {
		t.Fatalf("first trace = %s, want %s", summaries[0].TraceID, hex.EncodeToString(traceA[:]))
	}
	if summaries[1].TraceID != hex.EncodeToString(traceB[:]) {
		t.Fatalf("second trace = %s, want %s", summaries[1].TraceID, hex.EncodeToString(traceB[:]))
	}
	if summaries[2].TraceID != hex.EncodeToString(traceC[:]) {
		t.Fatalf("third trace = %s, want %s", summaries[2].TraceID, hex.EncodeToString(traceC[:]))
	}
}

func makeSpan(traceID model.TraceID, service, op string, start time.Time) model.SpanEntry {
	spanID := model.SpanID{byte(start.Unix())}
	entry, err := model.NewSpanEntry(traceID, spanID, model.ZeroSpanID(), service, op)
	if err != nil {
		panic(err)
	}
	entry.StartTime = start
	entry.EndTime = start.Add(50 * time.Millisecond)
	return entry
}

func mustTraceID(raw string) model.TraceID {
	decoded, err := hex.DecodeString(raw)
	if err != nil {
		panic(err)
	}
	var id model.TraceID
	copy(id[:], decoded)
	return id
}
