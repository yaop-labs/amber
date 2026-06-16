package obsbench

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestTracesGenerator_Deterministic: two generators with the same config must
// produce identical traces — IDs, services, operations, durations — so every
// system ingests byte-identical work and the equality gate is meaningful.
func TestTracesGenerator_Deterministic(t *testing.T) {
	cfg := TracesGenConfig{Seed: 42, SpansPerTrace: 10}
	a, b := NewTracesGenerator(cfg), NewTracesGenerator(cfg)
	for i := 0; i < 200; i++ {
		if a.TraceID(i) != b.TraceID(i) {
			t.Fatalf("trace %d: trace ID diverged", i)
		}
		if a.Service(i) != b.Service(i) || a.RootOperation(i) != b.RootOperation(i) {
			t.Fatalf("trace %d: service/op diverged", i)
		}
		sa, sb := a.Trace(i), b.Trace(i)
		if len(sa) != len(sb) {
			t.Fatalf("trace %d: span count diverged", i)
		}
		for s := range sa {
			if sa[s] != sb[s] || a.SpanID(i, s) != b.SpanID(i, s) {
				t.Fatalf("trace %d span %d diverged", i, s)
			}
		}
	}
}

// TestTracesGenerator_Invariants guards the two contracts the search gates rely
// on: one service per trace, a connected tree with a single root, disjoint
// root/child operation namespaces, and every child strictly shorter than the
// smallest QT3 threshold (so duration search matches only root spans).
func TestTracesGenerator_Invariants(t *testing.T) {
	g := NewTracesGenerator(TracesGenConfig{Seed: 7, SpansPerTrace: 10})
	for i := 0; i < 500; i++ {
		spans := g.Trace(i)
		roots := 0
		for _, sp := range spans {
			if sp.IsRoot() {
				roots++
				if sp.Operation != g.RootOperation(i) {
					t.Fatalf("trace %d: root op mismatch", i)
				}
				if strings.HasPrefix(sp.Operation, "internal.call.") {
					t.Fatalf("trace %d: root op in child namespace: %s", i, sp.Operation)
				}
				continue
			}
			if sp.ParentIndex < 0 || sp.ParentIndex >= sp.Index {
				t.Fatalf("trace %d span %d: parent %d not an earlier span", i, sp.Index, sp.ParentIndex)
			}
			if !strings.HasPrefix(sp.Operation, "internal.call.") {
				t.Fatalf("trace %d span %d: child op in root namespace: %s", i, sp.Index, sp.Operation)
			}
			if sp.Duration >= traceMinThreshold {
				t.Fatalf("trace %d span %d: child duration %s >= min threshold %s", i, sp.Index, sp.Duration, traceMinThreshold)
			}
		}
		if roots != 1 {
			t.Fatalf("trace %d: %d roots, want exactly 1", i, roots)
		}
	}
}

// TestTraceInstance_CrossSystemIdentity: the same (seed, iteration) over two
// different absolute ingest windows must produce the same paramsKey — that
// equality is what lets the gate match instances across systems.
func TestTraceInstance_CrossSystemIdentity(t *testing.T) {
	mk := func(from time.Time) TraceQueryRunOptions {
		return TraceQueryRunOptions{Seed: 9, GenSeed: 1, Traces: 10000, From: from, To: from.Add(30 * time.Minute)}
	}
	a := mk(time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC))
	b := mk(time.Date(2026, 6, 15, 22, 30, 0, 0, time.UTC))
	for _, scenario := range AllTraceScenarios {
		for iter := 0; iter < 10; iter++ {
			qa := traceInstance(scenario, iter, a)
			qb := traceInstance(scenario, iter, b)
			if qa.paramsKey() != qb.paramsKey() {
				t.Fatalf("%s iter %d: params diverged:\n%s\n%s", scenario, iter, qa.paramsKey(), qb.paramsKey())
			}
		}
	}
}

// TestE2E_AmberTracesIngestVerifyQuery drives the full trace phase against a
// real amber stack: OTLP ingest -> sampled completeness verify -> QT1–QT3
// natively. Payload-format or endpoint drift (including the operation /
// min_duration query params wired for QT2/QT3) fails here instead of producing
// a zero-result benchmark.
func TestE2E_AmberTracesIngestVerifyQuery(t *testing.T) {
	stack, srv := startAmber(t)
	target := TargetConfig{Kind: "amber", BaseURL: srv.URL}

	const traces = 2000
	const spt = 10
	// Rate-cap below amber's span drain rate so the bounded ingest queue
	// (default 10k) never overruns under -race; a lossless ingest is what the
	// QT1 span-count and verify gates below assert. Throughput is the
	// campaign's job, not this correctness test's.
	opts := TracesIngestOptions{
		Gen:         TracesGenConfig{Seed: 3, SpansPerTrace: spt},
		Traces:      traces,
		Rate:        10000,
		Workers:     2,
		BatchTraces: 50,
	}
	res, err := IngestTraces(context.Background(), "amber", target, opts, nil)
	if err != nil {
		t.Fatalf("IngestTraces: %v", err)
	}
	wantSpans := uint64(traces * spt)
	if res.SentSpans != wantSpans {
		t.Fatalf("sent spans=%d, want %d", res.SentSpans, wantSpans)
	}
	if res.AckedSpans != wantSpans || res.FailedBatches != 0 {
		t.Fatalf("acked=%d failed=%d errors=%v, want %d/0", res.AckedSpans, res.FailedBatches, res.Errors, wantSpans)
	}

	// Durability barrier so the search/lookup paths see every span.
	if err := stack.Batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	vr, err := VerifyTraces(context.Background(), target, res, 400)
	if err != nil {
		t.Fatalf("VerifyTraces: %v", err)
	}
	if !vr.OK() {
		t.Fatalf("verify: %d/%d complete, first miss %s had %d spans (silent ingest loss)",
			vr.Complete, vr.Sampled, vr.FirstMiss, vr.MissSpans)
	}

	outcomes, err := RunTraceQueries(context.Background(), target, TraceQueryRunOptions{
		Scenarios:  AllTraceScenarios,
		Iterations: 10,
		Seed:       5,
		GenSeed:    3,
		Traces:     traces,
		From:       res.StartedAt,
		To:         res.FinishedAt,
	})
	if err != nil {
		t.Fatalf("RunTraceQueries: %v", err)
	}
	sums := map[string]int{}
	for _, oc := range outcomes {
		if oc.Error != "" {
			t.Fatalf("%s iter %d: %s", oc.Scenario, oc.Iteration, oc.Error)
		}
		sums[oc.Scenario] += oc.Count
		// QT1 is an exact gate: every looked-up trace must return all its spans.
		if oc.Scenario == ScenarioQT1Lookup && oc.Count != spt {
			t.Fatalf("qt1 iter %d: span_count=%d, want %d", oc.Iteration, oc.Count, spt)
		}
	}
	// QT2/QT3 must find matches at this scale (2000 traces); a zero sum means
	// the operation / min_duration filter never reached the span data.
	if sums[ScenarioQT2ServiceOp] == 0 {
		t.Fatal("qt2 service+operation search returned no traces across 10 iterations")
	}
	if sums[ScenarioQT3Duration] == 0 {
		t.Fatal("qt3 duration search returned no traces across 10 iterations")
	}
}
