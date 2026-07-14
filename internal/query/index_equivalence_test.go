package query

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// TestIndexedAndScanPathsAreEquivalent pins the role of every sidecar used by
// ExecLog and ExecSpan: an index may remove work, but it may not change the
// returned IDs, ordering, truncation, or cursor.
func TestIndexedAndScanPathsAreEquivalent(t *testing.T) {
	dir := t.TempDir()
	logDir, spanDir := filepath.Join(dir, "logs"), filepath.Join(dir, "spans")
	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	logMgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		t.Fatalf("open log manager: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		_ = logMgr.Close()
		t.Fatalf("open span manager: %v", err)
	}
	t.Cleanup(func() {
		_ = logMgr.Close()
		_ = spanMgr.Close()
	})

	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	traceIDs := [4]model.TraceID{}
	for i := range traceIDs {
		traceIDs[i][0] = byte(i + 1)
		traceIDs[i][15] = byte(0xA0 + i)
	}
	levels := []model.Level{model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError}
	statuses := []model.SpanStatus{model.SpanStatusUnset, model.SpanStatusOK, model.SpanStatusError}
	bodies := []string{
		"payment timeout database",
		"payment accepted",
		"cache timeout recovered",
		"healthy request",
	}
	durations := []time.Duration{time.Millisecond, 7 * time.Millisecond, 50 * time.Millisecond, 500 * time.Millisecond}

	var buf bytes.Buffer
	for segment := 0; segment < 3; segment++ {
		logs := make([]storage.BatchItem, 0, 40)
		spans := make([]storage.BatchItem, 0, 40)
		for offset := 0; offset < 40; offset++ {
			i := segment*40 + offset
			ts := base.Add(time.Duration(i) * time.Second)
			entry := model.LogEntry{
				ID:        makeMonotonicID(uint64(ts.UnixMilli()), uint64(i+1)),
				Timestamp: ts,
				Level:     levels[i%len(levels)],
				Service:   fmt.Sprintf("svc-%d", i%3),
				Host:      fmt.Sprintf("host-%d", i%2),
				TraceID:   traceIDs[i%len(traceIDs)],
				Body:      bodies[i%len(bodies)],
				Attrs: []model.Attr{
					{Key: "env", Value: []string{"prod", "stage"}[i%2]},
					{Key: "shard", Value: fmt.Sprintf("%d", i%5)},
				},
			}
			buf.Reset()
			if _, err := entry.WriteTo(&buf); err != nil {
				t.Fatalf("encode log %d: %v", i, err)
			}
			logs = append(logs, storage.BatchItem{Data: bytes.Clone(buf.Bytes()), TS: ts.UnixNano()})

			spanID := model.SpanID{byte(i + 1)}
			span := model.SpanEntry{
				ID:        makeMonotonicID(uint64(ts.UnixMilli()), uint64(10_000+i)),
				TraceID:   traceIDs[i%len(traceIDs)],
				SpanID:    spanID,
				Service:   fmt.Sprintf("svc-%d", i%3),
				Operation: fmt.Sprintf("op-%d", i%4),
				StartTime: ts,
				EndTime:   ts.Add(durations[i%len(durations)]),
				Status:    statuses[i%len(statuses)],
			}
			if i%2 != 0 {
				span.ParentID = model.SpanID{0xFF}
			}
			buf.Reset()
			if _, err := span.WriteTo(&buf); err != nil {
				t.Fatalf("encode span %d: %v", i, err)
			}
			spans = append(spans, storage.BatchItem{Data: bytes.Clone(buf.Bytes()), TS: ts.UnixNano()})
		}
		if err := logMgr.WriteBatch(logs); err != nil {
			t.Fatalf("write log segment %d: %v", segment, err)
		}
		if err := spanMgr.WriteBatch(spans); err != nil {
			t.Fatalf("write span segment %d: %v", segment, err)
		}
		if err := logMgr.Rotate(); err != nil {
			t.Fatalf("rotate log segment %d: %v", segment, err)
		}
		if err := spanMgr.Rotate(); err != nil {
			t.Fatalf("rotate span segment %d: %v", segment, err)
		}
	}

	logSparse := sparseFromSegments(logMgr.Segments())
	spanSparse := sparseFromSegments(spanMgr.Segments())
	scan := NewExecutor(logMgr, spanMgr, logSparse, spanSparse)
	t.Cleanup(scan.Close)

	logQueries := []*LogQuery{
		{Limit: 7},
		{Services: []string{"svc-0", "svc-2"}, Limit: 19},
		{Hosts: []string{"host-1"}, Levels: []string{"WARN", "ERROR"}, Limit: 13},
		{Attrs: map[string]string{"env": "prod", "shard": "2"}, Limit: 11},
		{FullText: "timeout", Limit: 17},
		{FullText: "payment timeout", Limit: 9},
		{TraceID: traceIDs[2], Limit: 8},
		{From: base.Add(23 * time.Second), To: base.Add(87 * time.Second), Services: []string{"svc-1"}, FullText: "payment", Limit: 6},
	}
	spanQueries := []*SpanQuery{
		{Limit: 7},
		{Services: []string{"svc-0", "svc-2"}, Limit: 19},
		{Operations: []string{"op-1", "op-3"}, Statuses: []model.SpanStatus{model.SpanStatusOK, model.SpanStatusError}, Limit: 13},
		{MinDuration: 7 * time.Millisecond, MaxDuration: 50 * time.Millisecond, Limit: 11},
		{TraceID: traceIDs[1], Limit: 8},
		{From: base.Add(23 * time.Second), To: base.Add(87 * time.Second), Services: []string{"svc-1"}, Operations: []string{"op-0", "op-2"}, MinDuration: time.Millisecond, MaxDuration: 500 * time.Millisecond, Limit: 6},
	}

	logBaseline := executeLogQueries(t, scan, logQueries)
	spanBaseline := executeSpanQueries(t, scan, spanQueries)
	logQueries = append(logQueries, &LogQuery{Limit: 7, Cursor: logBaseline[0].cursor})
	spanQueries = append(spanQueries, &SpanQuery{Limit: 7, Cursor: spanBaseline[0].cursor})
	logBaseline = append(logBaseline, executeLogQueries(t, scan, logQueries[len(logQueries)-1:])...)
	spanBaseline = append(spanBaseline, executeSpanQueries(t, scan, spanQueries[len(spanQueries)-1:])...)

	indexed := NewExecutorWithCache(logMgr, spanMgr, logSparse, spanSparse, logDir, spanDir, 32)
	t.Cleanup(indexed.Close)
	for _, seg := range logMgr.Segments() {
		built, err := index.BuildLogSealIndexes(filepath.Join(logDir, seg.FileName), nil)
		if err != nil {
			t.Fatalf("build log sidecars %s: %v", seg.FileName, err)
		}
		indexed.RegisterBitmapIndex(seg.FileName, built.Bitmap)
		indexed.RegisterFTSIndex(seg.FileName, built.FTS)
		if built.Ribbon != nil {
			indexed.RegisterLogRibbon(seg.FileName, built.Ribbon)
		}
		if built.FTSRibbon != nil {
			indexed.RegisterLogFTSRibbon(seg.FileName, built.FTSRibbon)
		}
	}
	for _, seg := range spanMgr.Segments() {
		built, err := index.BuildSpanSealIndexes(filepath.Join(spanDir, seg.FileName), nil)
		if err != nil {
			t.Fatalf("build span sidecars %s: %v", seg.FileName, err)
		}
		if built.Ribbon != nil {
			indexed.RegisterSpanRibbon(seg.FileName, built.Ribbon)
		}
	}

	assertLogSnapshots(t, logBaseline, executeLogQueries(t, indexed, logQueries))
	assertSpanSnapshots(t, spanBaseline, executeSpanQueries(t, indexed, spanQueries))
}

func sparseFromSegments(segments []storage.SegmentMeta) *index.SparseIndex {
	sparse := index.NewSparseIndex()
	for _, seg := range segments {
		sparse.Add(index.SegmentTimeRange{SegmentID: seg.ID, FileName: seg.FileName, MinTS: seg.MinTS, MaxTS: seg.MaxTS})
	}
	return sparse
}

type resultSnapshot struct {
	ids       []model.EntryID
	truncated bool
	cursor    string
}

func executeLogQueries(t *testing.T, exec *Executor, queries []*LogQuery) []resultSnapshot {
	t.Helper()
	out := make([]resultSnapshot, len(queries))
	for i, q := range queries {
		result, err := exec.ExecLog(context.Background(), q)
		if err != nil {
			t.Fatalf("log query %d: %v", i, err)
		}
		out[i] = resultSnapshot{truncated: result.Truncated, cursor: result.NextCursor}
		for _, entry := range result.Entries {
			out[i].ids = append(out[i].ids, entry.ID)
		}
	}
	return out
}

func executeSpanQueries(t *testing.T, exec *Executor, queries []*SpanQuery) []resultSnapshot {
	t.Helper()
	out := make([]resultSnapshot, len(queries))
	for i, q := range queries {
		result, err := exec.ExecSpan(context.Background(), q)
		if err != nil {
			t.Fatalf("span query %d: %v", i, err)
		}
		out[i] = resultSnapshot{truncated: result.Truncated, cursor: result.NextCursor}
		for _, span := range result.Spans {
			out[i].ids = append(out[i].ids, span.ID)
		}
	}
	return out
}

func assertLogSnapshots(t *testing.T, want, got []resultSnapshot) {
	t.Helper()
	assertSnapshots(t, "log", want, got)
}

func assertSpanSnapshots(t *testing.T, want, got []resultSnapshot) {
	t.Helper()
	assertSnapshots(t, "span", want, got)
}

func assertSnapshots(t *testing.T, signal string, want, got []resultSnapshot) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s snapshot count = %d, want %d", signal, len(got), len(want))
	}
	for i := range want {
		if !slices.Equal(got[i].ids, want[i].ids) || got[i].truncated != want[i].truncated || got[i].cursor != want[i].cursor {
			t.Errorf("%s query %d differs\nscan=%+v\nindexed=%+v", signal, i, want[i], got[i])
		}
	}
}
