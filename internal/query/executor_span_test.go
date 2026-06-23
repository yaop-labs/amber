package query

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// buildSpanDataset writes n spans across one hour into the span manager
// and returns a ready executor. The shape mirrors buildCursorDataset but
// targets the span side, which had no integration coverage before.
func buildSpanDataset(t *testing.T, n int) (*Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	logDir := dir + "/logs"
	spanDir := dir + "/spans"

	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	logMgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	services := []string{"api-gateway", "auth-service", "payment"}
	operations := []string{"GET /widgets", "POST /orders", "DELETE /sessions"}
	base := time.Now().Add(-time.Hour).UnixNano()
	step := int64(time.Hour) / int64(n)
	if step == 0 {
		step = 1
	}

	// Use a single trace id so trace-id queries find every span.
	var traceID model.TraceID
	copy(traceID[:], []byte("aaaaaaaaaaaaaaaa"))

	buf := &bytes.Buffer{}
	batch := make([]storage.BatchItem, 0, n)
	for i := 0; i < n; i++ {
		ts := base + int64(i)*step
		var spanID model.SpanID
		spanID[0] = byte(i)
		spanID[1] = byte(i >> 8)
		span := model.SpanEntry{
			ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(i)),
			TraceID:   traceID,
			SpanID:    spanID,
			Service:   services[i%len(services)],
			Operation: operations[i%len(operations)],
			StartTime: time.Unix(0, ts),
			EndTime:   time.Unix(0, ts+int64(10*time.Millisecond)),
			Status:    model.SpanStatusOK,
		}
		buf.Reset()
		span.WriteTo(buf)
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		batch = append(batch, storage.BatchItem{Data: data, TS: ts})
	}
	if err := spanMgr.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if active, ok := spanMgr.ActiveSegmentMeta(); ok {
		spanSparse.TouchRange(active.ID, active.FileName, base, base+int64(time.Hour)-1)
	}
	if err := spanMgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	exec := NewExecutor(logMgr, spanMgr, sparse, spanSparse)
	for _, seg := range spanMgr.Segments() {
		segPath := spanDir + "/" + seg.FileName
		if idx, err := index.BuildSpanBitmapIndex(segPath, nil); err == nil {
			exec.RegisterSpanBitmapIndex(seg.FileName, idx)
		}
	}

	cleanup := func() { logMgr.Close(); spanMgr.Close() }
	return exec, cleanup
}

func TestExecutor_ExecSpan_Empty(t *testing.T) {
	sm, sparse, _ := setupTestStore(t)
	exec := NewExecutor(sm, sm, sparse, index.NewSparseIndex())

	result, err := exec.ExecSpan(context.Background(), &SpanQuery{Limit: 10})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(result.Spans) != 0 {
		t.Errorf("expected 0 spans, got %d", len(result.Spans))
	}
}

// TestExecutor_ExecSpan_ServiceFilter exercises the most common shape:
// "give me spans for service X." Covers the span planner, scan, and
// per-record service filter at once.
func TestExecutor_ExecSpan_ServiceFilter(t *testing.T) {
	exec, cleanup := buildSpanDataset(t, 30)
	defer cleanup()

	result, err := exec.ExecSpan(context.Background(), &SpanQuery{
		Services: []string{"api-gateway"},
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(result.Spans) == 0 {
		t.Fatal("expected spans for api-gateway, got 0")
	}
	for _, s := range result.Spans {
		if s.Service != "api-gateway" {
			t.Errorf("filter leaked: got service %q", s.Service)
		}
	}
}

// TestExecutor_ExecSpan_ServiceAndOperation is the QT2 shape: service +
// operation, served by intersecting the span .bidx bitmaps. The dataset aligns
// service[i%3] with operation[i%3], so an aligned pair intersects to a non-empty
// set and a crossed pair intersects to nothing (exercising the bitmap early-out).
func TestExecutor_ExecSpan_ServiceAndOperation(t *testing.T) {
	exec, cleanup := buildSpanDataset(t, 30)
	defer cleanup()

	hit, err := exec.ExecSpan(context.Background(), &SpanQuery{
		Services:   []string{"api-gateway"},
		Operations: []string{"GET /widgets"},
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(hit.Spans) == 0 {
		t.Fatal("expected spans for api-gateway + GET /widgets, got 0")
	}
	for _, s := range hit.Spans {
		if s.Service != "api-gateway" || s.Operation != "GET /widgets" {
			t.Errorf("filter leaked: got %q / %q", s.Service, s.Operation)
		}
	}

	miss, err := exec.ExecSpan(context.Background(), &SpanQuery{
		Services:   []string{"api-gateway"},
		Operations: []string{"POST /orders"},
		Limit:      100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(miss.Spans) != 0 {
		t.Errorf("non-co-occurring service+operation: got %d spans, want 0", len(miss.Spans))
	}
}

// TestExecutor_ExecSpan_MinDuration is the QT3 shape: service-scoped duration
// search, pruned by the span .bidx dur_bucket field then exact-filtered by the
// scan. Every span in the dataset is 10 ms.
func TestExecutor_ExecSpan_MinDuration(t *testing.T) {
	exec, cleanup := buildSpanDataset(t, 30)
	defer cleanup()

	keep, err := exec.ExecSpan(context.Background(), &SpanQuery{
		MinDuration: 5 * time.Millisecond,
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(keep.Spans) != 30 {
		t.Fatalf("min_duration 5ms: got %d spans, want 30", len(keep.Spans))
	}

	drop, err := exec.ExecSpan(context.Background(), &SpanQuery{
		MinDuration: 20 * time.Millisecond,
		Limit:       100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(drop.Spans) != 0 {
		t.Fatalf("min_duration 20ms: got %d spans, want 0", len(drop.Spans))
	}
}

// TestExecutor_ExecSpan_TraceIDFilter is the span path's reason for
// existing: pulling a complete trace by id. Every span in the dataset
// shares one trace id, so we expect every record back regardless of
// service.
func TestExecutor_ExecSpan_TraceIDFilter(t *testing.T) {
	exec, cleanup := buildSpanDataset(t, 30)
	defer cleanup()

	var traceID model.TraceID
	copy(traceID[:], []byte("aaaaaaaaaaaaaaaa"))

	result, err := exec.ExecSpan(context.Background(), &SpanQuery{
		TraceID: traceID,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(result.Spans) != 30 {
		t.Errorf("trace_id filter: got %d spans, want 30", len(result.Spans))
	}
}

// TestExecutor_ExecSpan_TimeRangePruning checks that a tight time window
// excludes spans outside it. Anti-regression for the span sparse-index
// path, which was historically untested.
func TestExecutor_ExecSpan_TimeRangePruning(t *testing.T) {
	exec, cleanup := buildSpanDataset(t, 30)
	defer cleanup()

	future := time.Now().Add(time.Hour)
	result, err := exec.ExecSpan(context.Background(), &SpanQuery{
		From:  future,
		To:    future.Add(time.Hour),
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("ExecSpan: %v", err)
	}
	if len(result.Spans) != 0 {
		t.Errorf("future time window: got %d spans, want 0", len(result.Spans))
	}
}
