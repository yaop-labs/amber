package http

import (
	"encoding/hex"
	"sort"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
)

func TestBuildTraceSummariesGroupsAndOrders(t *testing.T) {
	start := time.Unix(1000, 0)
	traceA := mustTraceID("0000000000000000000000000000000a")
	traceB := mustTraceID("0000000000000000000000000000000b")
	traceC := mustTraceID("0000000000000000000000000000000c")

	// Two spans of traceA (multi-span trace), one each of B and C, in the
	// newest-first order SpanSummaryFetch returns.
	all := []model.SpanEntry{
		makeSpan(traceA, "svc-a", "op-a", start.Add(5*time.Minute)),
		makeSpan(traceA, "svc-a", "op-a-child", start.Add(4*time.Minute)),
		makeSpan(traceB, "svc-b", "op-b", start.Add(3*time.Minute)),
		makeSpan(traceC, "svc-c", "op-c", start.Add(2*time.Minute)),
	}

	summaries := buildTraceSummaries(all)
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].StartTime.After(summaries[j].StartTime)
	})

	if len(summaries) != 3 {
		t.Fatalf("len(summaries) = %d, want 3 unique traces", len(summaries))
	}
	want := []model.TraceID{traceA, traceB, traceC}
	for i, w := range want {
		if summaries[i].TraceID != hex.EncodeToString(w[:]) {
			t.Fatalf("summary[%d] = %s, want %s", i, summaries[i].TraceID, hex.EncodeToString(w[:]))
		}
	}
	// traceA's two spans must collapse into one summary with SpanCount 2.
	if summaries[0].SpanCount != 2 {
		t.Fatalf("traceA span count = %d, want 2", summaries[0].SpanCount)
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
