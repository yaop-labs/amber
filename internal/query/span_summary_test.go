package query

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// buildMultiSegCoverDataset writes n spans across several sealed segments (a new
// segment every segSize spans), each with a .cidx, so SpanTraceSummaries'
// early-termination across segments is exercised.
func buildMultiSegCoverDataset(t *testing.T, n, segSize int) (*Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	logMgr, err := storage.OpenSegmentManager(dir+"/logs", storage.RotationPolicy{MaxRecords: 1 << 30, MaxBytes: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	spanDir := dir + "/spans"
	spanMgr, err := storage.OpenSegmentManager(spanDir, storage.RotationPolicy{MaxRecords: 1 << 30, MaxBytes: 1 << 40})
	if err != nil {
		t.Fatal(err)
	}
	spanSparse := index.NewSparseIndex()

	services := []string{"api-gateway", "auth-service", "payment"}
	operations := []string{"GET /widgets", "POST /orders", "DELETE /sessions"}
	base := time.Now().Add(-2 * time.Hour).UnixNano()
	step := int64(time.Second)

	write := func(from, to int) {
		buf := &bytes.Buffer{}
		batch := make([]storage.BatchItem, 0, to-from)
		for i := from; i < to; i++ {
			ts := base + int64(i)*step
			var tid model.TraceID
			tid[0] = byte(i / 5)
			tid[1] = byte((i / 5) >> 8)
			var parent model.SpanID
			isRoot := i%5 == 0
			if !isRoot {
				parent[0] = 7
			}
			span := model.SpanEntry{
				ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(i)),
				TraceID:   tid,
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
			spanSparse.TouchRange(active.ID, active.FileName,
				base+int64(from)*step, base+int64(to-1)*step)
		}
		if err := spanMgr.Rotate(); err != nil {
			t.Fatal(err)
		}
	}
	for from := 0; from < n; from += segSize {
		to := from + segSize
		if to > n {
			to = n
		}
		write(from, to)
	}

	exec := NewExecutor(logMgr, spanMgr, index.NewSparseIndex(), spanSparse)
	for _, seg := range spanMgr.Segments() {
		if _, err := index.BuildSpanSealIndexes(filepath.Join(spanDir, seg.FileName), nil); err != nil {
			t.Fatalf("seal: %v", err)
		}
	}
	return exec, func() { logMgr.Close(); spanMgr.Close() }
}

// oracleSummaries groups spans exactly like buildTraceSummaries (newest-first
// iteration, last-root wins) and returns trace hex -> compact summary string.
func oracleSummaries(spans []model.SpanEntry) map[string]string {
	type acc struct {
		start, end      time.Time
		count           int
		hasErr          bool
		rootSvc, rootOp string
		rootInit        bool
	}
	g := map[model.TraceID]*acc{}
	for i := range spans {
		s := &spans[i]
		a := g[s.TraceID]
		if a == nil {
			a = &acc{start: s.StartTime, end: s.EndTime, rootSvc: s.Service, rootOp: s.Operation, rootInit: true}
			g[s.TraceID] = a
		} else {
			if s.StartTime.Before(a.start) {
				a.start = s.StartTime
			}
			if s.EndTime.After(a.end) {
				a.end = s.EndTime
			}
		}
		a.count++
		if s.Status == model.SpanStatusError {
			a.hasErr = true
		}
		if s.IsRoot() {
			a.rootSvc, a.rootOp = s.Service, s.Operation
		}
		_ = a.rootInit
	}
	out := map[string]string{}
	for id, a := range g {
		out[fmt.Sprintf("%x", id)] = fmt.Sprintf("%d|%d|%d|%v|%s|%s",
			a.start.UnixNano(), a.end.UnixNano(), a.count, a.hasErr, a.rootSvc, a.rootOp)
	}
	return out
}

func TestSpanTraceSummariesMatchesOracle(t *testing.T) {
	exec, cleanup := buildMultiSegCoverDataset(t, 1500, 150) // 10 segments
	defer cleanup()
	ctx := context.Background()

	queries := []SpanQuery{
		{Services: []string{"api-gateway"}},
		{Services: []string{"auth-service"}, Operations: []string{"POST /orders"}},
		{Services: []string{"payment"}, MinDuration: 3 * time.Millisecond},
	}

	for qi, q := range queries {
		t.Run(fmt.Sprintf("q%d", qi), func(t *testing.T) {
			// Oracle: A fetches all matching spans (no truncation at this size),
			// grouped like buildTraceSummaries.
			spans, _, err := exec.SpanSummaryFetch(ctx, &q, 1_000_000)
			if err != nil {
				t.Fatal(err)
			}
			oracle := oracleSummaries(spans)

			const limit = 20
			got, _, _, err := exec.SpanTraceSummaries(ctx, &q, limit, 1_000_000)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) < limit {
				t.Fatalf("got %d summaries, want >= %d", len(got), limit)
			}
			// The top-`limit` newest summaries must match the oracle exactly.
			for i := 0; i < limit; i++ {
				s := got[i]
				key := fmt.Sprintf("%x", s.TraceID)
				want, ok := oracle[key]
				if !ok {
					t.Fatalf("top[%d] trace %s not in oracle", i, key)
				}
				gotStr := fmt.Sprintf("%d|%d|%d|%v|%s|%s",
					s.Start.UnixNano(), s.End.UnixNano(), s.SpanCount, s.HasErrors, s.Service, s.Operation)
				if gotStr != want {
					t.Fatalf("top[%d] trace %s:\n got=%s\nwant=%s", i, key, gotStr, want)
				}
			}

			// Order: summaries must be start-descending.
			for i := 1; i < len(got); i++ {
				if got[i-1].Start.Before(got[i].Start) {
					t.Fatalf("not start-descending at %d", i)
				}
			}
		})
	}
}
