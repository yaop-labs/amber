package amber_test

// Coverage tests for the public embedded surface. Same hard rule as
// amber_example_test.go: only the github.com/yaop-labs/amber import is
// allowed, no internal/... reach-around. If a test here cannot be written
// using exclusively the public surface, that surface has a gap.
//
// Covered:
//   - durability across a clean Close (sealed segment + meta survive a
//     reopen, queries find the data)
//   - IsReady reaches true when there is real bootstrap work to do (a
//     directory full of sealed segments) and queries see the data after
//   - Span ingest + QuerySpans round-trip with service and trace_id
//     filters

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/amber"
)

// defaultOpts returns options tuned for fast tests: short batch flush so
// queries don't have to sleep, generous queue so SendLog never blocks.
func defaultOpts() *amber.Options {
	return &amber.Options{
		SegmentMaxRecords: 1000,
		BatchSize:         50,
		BatchTimeout:      20 * time.Millisecond,
		QueueSize:         1000,
		Cardinality: amber.CardinalityLimits{
			MaxAttrsPerEntry:  32,
			MaxAttrValueBytes: 1024,
		},
	}
}

// eventually polls cond every step until it returns true or timeout fires.
// Safe to use against QueryLogs/QuerySpans now that empty results are not
// cached (a stale empty would otherwise lock in for the result-cache TTL).
func eventually(t *testing.T, timeout, step time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(step)
	}
	return cond()
}

// TestEmbedded_DurabilityAcrossClose writes through the public API, closes
// the DB cleanly (which seals the active segment and fsyncs the WAL), then
// reopens the same directory and asserts the query path returns the same
// records. This is the contract any embedder relies on for graceful
// shutdown.
func TestEmbedded_DurabilityAcrossClose(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := amber.Open(dir, defaultOpts())
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}

	const n = 25
	for i := 0; i < n; i++ {
		entry := amber.LogEntry{
			Timestamp: time.Now(),
			Level:     amber.LevelInfo,
			Service:   "durable-svc",
			Host:      "host-a",
			Body:      "row",
			Attrs:     []amber.Attr{{Key: "i", Value: time.Now().Format(time.RFC3339Nano)}},
		}
		if err := db.Log(ctx, entry); err != nil {
			t.Fatalf("log[%d]: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close #1: %v", err)
	}

	// Reopen the same data dir; pretend a new process started fresh.
	db2, err := amber.Open(dir, defaultOpts())
	if err != nil {
		t.Fatalf("open #2: %v", err)
	}
	defer func() {
		if err := db2.Close(); err != nil {
			t.Errorf("close #2: %v", err)
		}
	}()

	// Poll: empty results no longer pin a stale "no data" cache entry,
	// so a tight loop won't lock in a false negative.
	var lastCount int
	if !eventually(t, 3*time.Second, 25*time.Millisecond, func() bool {
		r, err := db2.QueryLogs(ctx, &amber.LogQuery{
			Services: []string{"durable-svc"},
			Limit:    100,
		})
		if err != nil {
			t.Fatalf("query after reopen: %v", err)
		}
		lastCount = len(r.Entries)
		return lastCount >= n
	}) {
		t.Fatalf("after reopen, expected >= %d durable-svc entries, got %d", n, lastCount)
	}
}

// A SIGKILL-style crash test cannot run in-process: the "crashed" DB
// keeps fds and goroutines alive in the same address space, so the
// reopen races against the original runtime instead of starting fresh
// against an unowned directory. Crash recovery is covered at the unit
// level by internal/storage/crash_recovery_test.go, which can poke
// segment files directly without the lifecycle hazard.

// TestEmbedded_IsReadyOnPopulatedDir creates a dir with real sealed
// segments, closes, reopens, and verifies IsReady eventually flips true
// and queries return data after that signal. The contract being pinned:
// "after IsReady is true, the embedder can serve traffic without
// partial-result risk." Sealed segments give bootstrap real work, so the
// false→true transition is meaningful (not a vacuous always-true).
func TestEmbedded_IsReadyOnPopulatedDir(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	// Phase 1: ingest enough to force a sealed segment on disk before close.
	opts := defaultOpts()
	opts.SegmentMaxRecords = 30 // small so the rotate fires quickly
	db, err := amber.Open(dir, opts)
	if err != nil {
		t.Fatalf("open #1: %v", err)
	}

	const n = 60 // forces at least one rotation under SegmentMaxRecords=30
	for i := 0; i < n; i++ {
		if err := db.Log(ctx, amber.LogEntry{
			Timestamp: time.Now(),
			Level:     amber.LevelInfo,
			Service:   "ready-svc",
			Host:      "host-c",
			Body:      "row",
		}); err != nil {
			t.Fatalf("log[%d]: %v", i, err)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close #1: %v", err)
	}

	// Phase 2: reopen against the populated dir. Bootstrap has actual
	// sealed segments to load (ribbon filters, bitmap caches), so the
	// false→true transition is observable rather than instantaneous.
	db2, err := amber.Open(dir, defaultOpts())
	if err != nil {
		t.Fatalf("open #2: %v", err)
	}
	defer func() {
		if err := db2.Close(); err != nil {
			t.Errorf("close #2: %v", err)
		}
	}()

	if !eventually(t, 5*time.Second, 25*time.Millisecond, func() bool {
		return db2.IsReady()
	}) {
		t.Fatalf("IsReady never returned true after reopening populated dir")
	}

	// Queries must find the sealed data once we're ready. If IsReady true
	// but queries are empty, the contract is broken.
	r, err := db2.QueryLogs(ctx, &amber.LogQuery{
		Services: []string{"ready-svc"},
		Limit:    100,
	})
	if err != nil {
		t.Fatalf("query after IsReady=true: %v", err)
	}
	if len(r.Entries) == 0 {
		t.Fatalf("IsReady=true but ready-svc has no entries")
	}
}

// TestEmbedded_SpanRoundTrip writes spans through DB.Span and reads them
// back through DB.QuerySpans, covering both the service-filter and
// trace_id-filter code paths. Span is half of the contract — the existing
// example test exercises only logs.
func TestEmbedded_SpanRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := amber.Open(dir, defaultOpts())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	// Hand-built trace and span ids. The public surface exposes the array
	// types but no constructors for them — an embedder passes values from
	// its own tracing layer (forager from OTLP). NewSpanEntry, however, is
	// exported and produces a valid ULID-backed entry ID; using it covers
	// the same code path real callers would.
	var traceID amber.TraceID
	copy(traceID[:], []byte("amber-trace-id00"))
	var spanID amber.SpanID
	copy(spanID[:], []byte("spanid01"))
	var parentID amber.SpanID

	span, err := amber.NewSpanEntry(traceID, spanID, parentID, "span-svc", "GET /api/v1/widgets")
	if err != nil {
		t.Fatalf("NewSpanEntry: %v", err)
	}
	now := time.Now()
	span.StartTime = now
	span.EndTime = now.Add(15 * time.Millisecond)
	span.Attrs = []amber.Attr{
		{Key: "http.method", Value: "GET"},
		{Key: "http.status_code", Value: "200"},
	}
	if err := db.Span(ctx, span); err != nil {
		t.Fatalf("span: %v", err)
	}

	// Poll for the span to surface — batch flush is async. Empty results
	// are no longer cached, so this loop will not get stuck on a stale
	// negative.
	if !eventually(t, 3*time.Second, 25*time.Millisecond, func() bool {
		r, err := db.QuerySpans(ctx, &amber.SpanQuery{
			Services: []string{"span-svc"},
			Limit:    10,
		})
		return err == nil && len(r.Spans) > 0
	}) {
		t.Fatalf("span never surfaced via service filter")
	}

	// Filter by trace_id: stricter; verifies the trace-lookup path that
	// distributed-tracing UIs depend on.
	r, err := db.QuerySpans(ctx, &amber.SpanQuery{
		TraceID: traceID,
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("query by trace_id: %v", err)
	}
	if len(r.Spans) == 0 {
		t.Fatalf("trace_id filter returned no spans")
	}

	// Light cross-check on the round-trip fidelity. We don't assert the
	// full struct because the in-memory and on-disk forms differ in
	// monotonic clock metadata after UnixNano serialization; what we care
	// about is that the identifying fields survive.
	got := r.Spans[0]
	if got.Service != "span-svc" {
		t.Errorf("Service: got %q, want %q", got.Service, "span-svc")
	}
	if got.Operation != span.Operation {
		t.Errorf("Operation: got %q, want %q", got.Operation, span.Operation)
	}
	if got.TraceID != traceID {
		t.Errorf("TraceID: got %x, want %x", got.TraceID, traceID)
	}
	if !strings.Contains(got.Attrs[0].Key+got.Attrs[1].Key, "http.method") {
		t.Errorf("attrs not preserved: %v", got.Attrs)
	}
}

// TestEmbedded_QueryTraceReturnsBothSides pins the wrapper contract: one
// call gives back the logs AND the spans for a trace id, so the consumer
// (UI/TUI/gateway) does not need two round trips. The wrapper does NOT
// correlate, build trees, or order across sides — that stays upstream.
func TestEmbedded_QueryTraceReturnsBothSides(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	db, err := amber.Open(dir, defaultOpts())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	var traceID amber.TraceID
	copy(traceID[:], []byte("trace-correlate0"))
	var spanID amber.SpanID
	copy(spanID[:], []byte("span0001"))

	// Write one log and one span, both tagged with the same trace id.
	logEntry := amber.LogEntry{
		Timestamp: time.Now(),
		Level:     amber.LevelInfo,
		Service:   "trace-svc",
		Host:      "host-d",
		Body:      "request handled",
		TraceID:   traceID,
		SpanID:    spanID,
	}
	if err := db.Log(ctx, logEntry); err != nil {
		t.Fatalf("log: %v", err)
	}

	span, err := amber.NewSpanEntry(traceID, spanID, amber.SpanID{}, "trace-svc", "GET /widgets")
	if err != nil {
		t.Fatalf("NewSpanEntry: %v", err)
	}
	span.StartTime = time.Now()
	span.EndTime = span.StartTime.Add(5 * time.Millisecond)
	if err := db.Span(ctx, span); err != nil {
		t.Fatalf("span: %v", err)
	}

	// Poll: both ingest paths are async, and the side that flushes later
	// dictates when the result is complete. Empty results are not cached,
	// so a tight loop won't lock in a partial answer.
	var got *amber.TraceResult
	if !eventually(t, 3*time.Second, 25*time.Millisecond, func() bool {
		r, err := db.QueryTrace(ctx, traceID, 100)
		if err != nil {
			return false
		}
		got = r
		return len(r.Logs) > 0 && len(r.Spans) > 0
	}) {
		var lc, sc int
		if got != nil {
			lc, sc = len(got.Logs), len(got.Spans)
		}
		t.Fatalf("QueryTrace never saw both sides: logs=%d spans=%d", lc, sc)
	}

	if got.Logs[0].TraceID != traceID {
		t.Errorf("log trace_id: got %x, want %x", got.Logs[0].TraceID, traceID)
	}
	if got.Spans[0].TraceID != traceID {
		t.Errorf("span trace_id: got %x, want %x", got.Spans[0].TraceID, traceID)
	}
}
