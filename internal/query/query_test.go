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

func TestLogQuery_Validate(t *testing.T) {
	q := &LogQuery{Limit: 10}
	if err := q.Validate(); err != nil {
		t.Errorf("valid query: %v", err)
	}
}

func TestLogQuery_Validate_DefaultLimit(t *testing.T) {
	q := &LogQuery{}
	q.Validate()
	if q.Limit != 100 {
		t.Errorf("expected default limit=100, got %d", q.Limit)
	}
}

func TestLogQuery_Validate_FromAfterTo(t *testing.T) {
	q := &LogQuery{
		From: time.Now(),
		To:   time.Now().Add(-time.Hour),
	}
	if err := q.Validate(); err == nil {
		t.Error("expected error for from > to")
	}
}

func TestLogQuery_HasTimeRange(t *testing.T) {
	q := &LogQuery{From: time.Now()}
	if !q.HasTimeRange() {
		t.Error("expected HasTimeRange=true")
	}
	q2 := &LogQuery{}
	if q2.HasTimeRange() {
		t.Error("expected HasTimeRange=false")
	}
}

func TestLogQuery_HasFieldFilters(t *testing.T) {
	q := &LogQuery{Levels: []string{"ERROR"}}
	if !q.HasFieldFilters() {
		t.Error("expected HasFieldFilters=true")
	}
}

func TestPlanner_EmptyIndex(t *testing.T) {
	sparse := index.NewSparseIndex()
	p := NewPlanner(sparse)

	plan := p.Plan(&LogQuery{FullText: "error"})
	if len(plan.Segments) != 0 {
		t.Errorf("expected 0 segments for empty index, got %d", len(plan.Segments))
	}
}

func TestPlanner_TimeRangePruning(t *testing.T) {
	sparse := index.NewSparseIndex()
	sparse.Add(index.SegmentTimeRange{SegmentID: 1, FileName: "seg_1.alog", MinTS: 100, MaxTS: 200})
	sparse.Add(index.SegmentTimeRange{SegmentID: 2, FileName: "seg_2.alog", MinTS: 300, MaxTS: 400})
	sparse.Add(index.SegmentTimeRange{SegmentID: 3, FileName: "seg_3.alog", MinTS: 500, MaxTS: 600})

	p := NewPlanner(sparse)

	q := &LogQuery{}
	q.From = time.Unix(0, 250)
	q.To = time.Unix(0, 450)

	plan := p.Plan(q)
	if len(plan.Segments) != 1 {
		t.Errorf("expected 1 segment after pruning, got %d", len(plan.Segments))
	}
	if plan.Segments[0].SegmentID != 2 {
		t.Errorf("expected seg 2, got %d", plan.Segments[0].SegmentID)
	}
	if plan.PrunedSegments != 2 {
		t.Errorf("expected 2 pruned, got %d", plan.PrunedSegments)
	}
}

func TestPlanner_Steps_FullQuery(t *testing.T) {
	sparse := index.NewSparseIndex()
	sparse.Add(index.SegmentTimeRange{SegmentID: 1, MinTS: 0, MaxTS: 1000})
	p := NewPlanner(sparse)

	q := &LogQuery{
		Levels:   []string{"ERROR"},
		FullText: "connection refused",
		Limit:    10,
	}
	q.From = time.Unix(0, 0)
	q.To = time.Unix(0, 1000)

	plan := p.Plan(q)

	for _, step := range []PlanStep{StepSegmentPruning, StepBitmapFilter, StepFTSSearch, StepFetchRecords, StepPaginate} {
		if !plan.HasStep(step) {
			t.Errorf("expected step %s in plan", step)
		}
	}
}

func TestPlanner_Steps_NoFilters(t *testing.T) {
	sparse := index.NewSparseIndex()
	sparse.Add(index.SegmentTimeRange{SegmentID: 1, MinTS: 0, MaxTS: 1000})
	p := NewPlanner(sparse)

	plan := p.Plan(&LogQuery{Limit: 10})

	if plan.HasStep(StepBitmapFilter) {
		t.Error("unexpected BitmapFilter step for query with no field filters")
	}
	if plan.HasStep(StepFTSSearch) {
		t.Error("unexpected FTSSearch step for query with no full text")
	}
	if !plan.HasStep(StepFetchRecords) {
		t.Error("expected FetchRecords step")
	}
}

func setupTestStore(t *testing.T) (*storage.SegmentManager, *index.SparseIndex, string) {
	t.Helper()
	dir := t.TempDir()
	sm, err := storage.OpenSegmentManager(dir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	t.Cleanup(func() { sm.Close() })
	return sm, index.NewSparseIndex(), dir
}

func TestExecutor_ExecLog_Empty(t *testing.T) {
	sm, sparse, _ := setupTestStore(t)
	exec := NewExecutor(sm, sm, sparse, index.NewSparseIndex())

	result, err := exec.ExecLog(context.Background(), &LogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("ExecLog: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries, got %d", len(result.Entries))
	}
}

func TestExecutor_ExecLog_NoSegments_AfterPruning(t *testing.T) {
	sm, sparse, _ := setupTestStore(t)

	sparse.Add(index.SegmentTimeRange{
		SegmentID: 1,
		FileName:  "seg_00000001.alog",
		MinTS:     100,
		MaxTS:     200,
	})

	exec := NewExecutor(sm, sm, sparse, index.NewSparseIndex())

	q := &LogQuery{Limit: 10}
	q.From = time.Unix(0, 300)
	q.To = time.Unix(0, 400)

	result, err := exec.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatalf("ExecLog: %v", err)
	}
	if len(result.Entries) != 0 {
		t.Errorf("expected 0 entries after pruning, got %d", len(result.Entries))
	}
}

// TestExecutor_ExecLog_CacheHit verifies that repeated identical queries
// return the cached result with CacheHit=true on the second call. This
// guards the hot dashboard / benchmark warm-repeat path — if this regresses,
// p50 latencies on R1-R4 explode because every query re-scans segments.
//
// Empty results are no longer cached (an ingest-then-query embedder would
// otherwise pin a stale empty for 5s), so this test uses a real dataset
// where the result is non-empty.
func TestExecutor_ExecLog_CacheHit(t *testing.T) {
	exec, cleanup := buildCursorDataset(t, 50)
	defer cleanup()

	q := &LogQuery{Services: []string{"api-gateway"}, Limit: 100}

	first, err := exec.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatalf("first ExecLog: %v", err)
	}
	if first.CacheHit {
		t.Error("first call should not be a cache hit")
	}
	if len(first.Entries) == 0 {
		t.Fatal("setup gave no entries; can't validate cache behavior")
	}

	second, err := exec.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatalf("second ExecLog: %v", err)
	}
	if !second.CacheHit {
		t.Error("second call must be a cache hit — repeated identical query within TTL")
	}
}

// TestExecutor_ExecLog_EmptyResultNotCached pins the F2 contract: empty
// results bypass the cache, so a query issued before ingest catches up
// won't poison subsequent calls.
func TestExecutor_ExecLog_EmptyResultNotCached(t *testing.T) {
	sm, sparse, _ := setupTestStore(t)
	exec := NewExecutor(sm, sm, sparse, index.NewSparseIndex())

	q := &LogQuery{Services: []string{"never"}, Limit: 10}

	first, err := exec.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if len(first.Entries) != 0 {
		t.Fatalf("expected empty first result, got %d entries", len(first.Entries))
	}
	if first.CacheHit {
		t.Error("first call shouldn't be a cache hit")
	}

	second, err := exec.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.CacheHit {
		t.Error("empty result must not be cached; second call wrongly read from cache")
	}
}

// TestExecutor_ExecLog_CacheKey_DistinctQueries verifies that different
// queries produce different cache keys (no false-positive hits).
func TestExecutor_ExecLog_CacheKey_DistinctQueries(t *testing.T) {
	sm, sparse, _ := setupTestStore(t)
	exec := NewExecutor(sm, sm, sparse, index.NewSparseIndex())

	q1 := &LogQuery{Services: []string{"api-gateway"}, Limit: 100}
	q2 := &LogQuery{Services: []string{"api-gateway"}, Limit: 50}

	if _, err := exec.ExecLog(context.Background(), q1); err != nil {
		t.Fatalf("q1: %v", err)
	}

	r2, err := exec.ExecLog(context.Background(), q2)
	if err != nil {
		t.Fatalf("q2: %v", err)
	}
	if r2.CacheHit {
		t.Error("different query (limit=50 vs limit=100) must not hit q1's cache entry")
	}
}

func TestLogQuery_ToUnixNano_ZeroReturnsMaxInt64(t *testing.T) {
	q := &LogQuery{}
	nano := q.ToUnixNano()
	if nano <= 0 {
		t.Errorf("expected large positive value for zero To, got %d", nano)
	}
}

func TestLogQuery_FromUnixNano_ZeroReturnsZero(t *testing.T) {
	q := &LogQuery{}
	if q.FromUnixNano() != 0 {
		t.Errorf("expected 0 for zero From, got %d", q.FromUnixNano())
	}
}

// buildBodyDataset writes logs with the given bodies into one segment and
// returns a ready executor that registers NO FTS index. The scan-time
// full-text fallback (needScanFTS) is gated on the absence of an FTS index,
// not on whether the segment is active — so this exercises the same code path
// the production active (unsealed) segment hits, while staying readable.
func buildBodyDataset(t *testing.T, bodies []string) (*Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	mgr, err := storage.OpenSegmentManager(dir+"/logs", policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(dir+"/spans", policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}

	sparse := index.NewSparseIndex()
	base := time.Now().Add(-time.Hour).UnixNano()
	buf := &bytes.Buffer{}
	batch := make([]storage.BatchItem, 0, len(bodies))
	for i, body := range bodies {
		ts := base + int64(i)*int64(time.Second)
		entry := model.LogEntry{
			ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(i)),
			Timestamp: time.Unix(0, ts),
			Level:     model.LevelInfo,
			Service:   "api",
			Body:      body,
		}
		buf.Reset()
		entry.WriteTo(buf)
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		batch = append(batch, storage.BatchItem{Data: data, TS: entry.Timestamp.UnixNano()})
	}
	if err := mgr.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if active, ok := mgr.ActiveSegmentMeta(); ok {
		sparse.TouchRange(active.ID, active.FileName, base, base+int64(len(bodies))*int64(time.Second))
	}
	if err := mgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	exec := NewExecutor(mgr, spanMgr, sparse, index.NewSparseIndex())
	return exec, func() { mgr.Close(); spanMgr.Close() }
}

// TestExecutor_ExecLog_FullText_ActiveScanFallback pins the fix for full-text
// search being silently ignored on the active (unsealed) segment, which has
// no FTS index. Before the fix, `q` was a no-op for recent data and every
// query returned the full set.
func TestExecutor_ExecLog_FullText_ActiveScanFallback(t *testing.T) {
	exec, cleanup := buildBodyDataset(t, []string{
		"connection refused to postgres:5432",
		"rate limit exceeded ip=10.0.0.1",
		"request completed ok",
		"connection refused to redis:6379",
		"cache warmed successfully",
	})
	defer cleanup()

	run := func(q string) []string {
		t.Helper()
		res, err := exec.ExecLog(context.Background(), &LogQuery{FullText: q, Limit: 100})
		if err != nil {
			t.Fatalf("ExecLog(%q): %v", q, err)
		}
		bodies := make([]string, len(res.Entries))
		for i, e := range res.Entries {
			bodies[i] = e.Body
		}
		return bodies
	}

	if got := run("refused"); len(got) != 2 {
		t.Errorf("q=refused: got %d entries %v, want 2 (the refused bodies)", len(got), got)
	}
	if got := run("rate"); len(got) != 1 {
		t.Errorf("q=rate: got %d entries %v, want 1", len(got), got)
	}
	if got := run("nonexistentxyz"); len(got) != 0 {
		t.Errorf("q=nonexistentxyz: got %d entries, want 0", len(got))
	}
	// Multi-token queries are AND: every token must be present in the body.
	if got := run("connection refused"); len(got) != 2 {
		t.Errorf("q='connection refused': got %d, want 2", len(got))
	}
	if got := run("refused rate"); len(got) != 0 {
		t.Errorf("q='refused rate': got %d, want 0 (no body has both tokens)", len(got))
	}
}

func TestPlanStep_String(t *testing.T) {
	steps := map[PlanStep]string{
		StepSegmentPruning: "SegmentPruning",
		StepBitmapFilter:   "BitmapFilter",
		StepFTSSearch:      "FTSSearch",
		StepFetchRecords:   "FetchRecords",
		StepPostFilter:     "PostFilter",
		StepPaginate:       "Paginate",
	}
	for step, want := range steps {
		if step.String() != want {
			t.Errorf("step %d: got %s, want %s", step, step.String(), want)
		}
	}
}
