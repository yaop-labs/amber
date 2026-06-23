package query

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// buildCoverDataset writes n spans (several services, traces, durations) into a
// span manager, seals them WITH the .cidx covering index, and returns an
// executor plus the span dir so a test can delete .cidx to force the scan path.
func buildCoverDataset(t *testing.T, n int) (*Executor, string, func()) {
	t.Helper()
	dir := t.TempDir()
	logDir := dir + "/logs"
	spanDir := dir + "/spans"
	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	logMgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		t.Fatal(err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		t.Fatal(err)
	}
	spanSparse := index.NewSparseIndex()

	services := []string{"api-gateway", "auth-service", "payment"}
	operations := []string{"GET /widgets", "POST /orders", "DELETE /sessions"}
	base := time.Now().Add(-time.Hour).UnixNano()
	step := int64(time.Hour) / int64(n)
	if step == 0 {
		step = 1
	}

	buf := &bytes.Buffer{}
	batch := make([]storage.BatchItem, 0, n)
	for i := 0; i < n; i++ {
		ts := base + int64(i)*step
		var traceID model.TraceID
		traceID[0] = byte(i / 5) // ~5 spans per trace
		traceID[1] = byte((i / 5) >> 8)
		var parent model.SpanID
		isRoot := i%5 == 0
		if !isRoot {
			parent[0] = 7
		}
		span := model.SpanEntry{
			ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(i)),
			TraceID:   traceID,
			SpanID:    model.SpanID{byte(i), byte(i >> 8)},
			ParentID:  parent,
			Service:   services[i%len(services)],
			Operation: operations[i%len(operations)],
			StartTime: time.Unix(0, ts),
			EndTime:   time.Unix(0, ts+int64(i%7+1)*int64(time.Millisecond)),
			Status:    model.SpanStatus(i % 3),
		}
		buf.Reset()
		if _, err := span.WriteTo(buf); err != nil {
			t.Fatal(err)
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		batch = append(batch, storage.BatchItem{Data: data, TS: ts})
	}
	if err := spanMgr.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}
	if active, ok := spanMgr.ActiveSegmentMeta(); ok {
		spanSparse.TouchRange(active.ID, active.FileName, base, base+int64(time.Hour)-1)
	}
	if err := spanMgr.Rotate(); err != nil {
		t.Fatal(err)
	}

	exec := NewExecutor(logMgr, spanMgr, index.NewSparseIndex(), spanSparse)
	for _, seg := range spanMgr.Segments() {
		segPath := filepath.Join(spanDir, seg.FileName)
		if _, err := index.BuildSpanSealIndexes(segPath, nil); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}
	return exec, spanDir, func() { logMgr.Close(); spanMgr.Close() }
}

// summaryKey reduces a span to the fields buildTraceSummaries reads, so covering
// (partial spans) and scan (full spans) can be compared.
func summaryKey(s model.SpanEntry) string {
	return fmt.Sprintf("%x|%d|%d|%s|%d|%v",
		s.TraceID, s.StartTime.UnixNano(), s.EndTime.UnixNano(),
		s.Operation, s.Status, s.IsRoot())
}

func keysOf(spans []model.SpanEntry) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = summaryKey(s)
	}
	sort.Strings(out)
	return out
}

func TestSpanSummaryFetchCoveringMatchesScan(t *testing.T) {
	exec, spanDir, cleanup := buildCoverDataset(t, 600)
	defer cleanup()
	ctx := context.Background()

	queries := []SpanQuery{
		{Services: []string{"api-gateway"}},
		{Services: []string{"auth-service", "payment"}},
		{Services: []string{"api-gateway"}, Operations: []string{"GET /widgets"}},
		{Services: []string{"payment"}, MinDuration: 3 * time.Millisecond},
		{Services: []string{"api-gateway"}, MaxDuration: 2 * time.Millisecond},
	}

	for i, q := range queries {
		t.Run(fmt.Sprintf("q%d", i), func(t *testing.T) {
			cov, _, err := exec.SpanSummaryFetch(ctx, &q, 100000)
			if err != nil {
				t.Fatalf("covering: %v", err)
			}
			// Confirm the covering path actually engaged (a .cidx existed).
			if len(cov) == 0 {
				t.Fatal("covering returned no spans; dataset/filter issue")
			}
			covKeys := keysOf(cov)

			// Force the scan path by removing the .cidx files + dropping caches.
			cidx, _ := filepath.Glob(filepath.Join(spanDir, "*.cidx"))
			for _, f := range cidx {
				if err := os.Remove(f); err != nil {
					t.Fatal(err)
				}
			}
			for _, seg := range exec.spanManager.Segments() {
				exec.spanCoverCache.delete(seg.FileName)
			}

			scan, _, err := exec.SpanSummaryFetch(ctx, &q, 100000)
			if err != nil {
				t.Fatalf("scan: %v", err)
			}
			scanKeys := keysOf(scan)

			if len(covKeys) != len(scanKeys) {
				t.Fatalf("count: covering=%d scan=%d", len(covKeys), len(scanKeys))
			}
			for j := range covKeys {
				if covKeys[j] != scanKeys[j] {
					t.Fatalf("mismatch at %d:\n cov=%s\nscan=%s", j, covKeys[j], scanKeys[j])
				}
			}

			// Rebuild .cidx for the next subtest.
			for _, seg := range exec.spanManager.Segments() {
				if _, err := index.BuildSpanSealIndexes(filepath.Join(spanDir, seg.FileName), nil); err != nil {
					t.Fatal(err)
				}
				exec.spanCoverCache.delete(seg.FileName)
			}
		})
	}
}
