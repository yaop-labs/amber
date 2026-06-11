package obsbench

import (
	"context"
	"testing"
	"time"
)

// TestE2E_AmberQueryScenarios runs the full Q1–Q4 set against a real amber
// stack and pins the truths the harness depends on: deterministic instance
// parameters, and Q4's count matching the dataset's known rare-token total.
func TestE2E_AmberQueryScenarios(t *testing.T) {
	const n = 2000
	path := genDataset(t, n, 11) // RareTokenEvery=100 → 20 rare records
	stack, srv := startAmber(t)
	target := TargetConfig{Kind: "amber", BaseURL: srv.URL, Protocol: "native"}

	startedAt := time.Now()
	res, err := Ingest(context.Background(), "amber", target, path, IngestOptions{Workers: 4, BatchSize: 250})
	if err != nil || res.AckedRecords != n {
		t.Fatalf("Ingest: acked=%d err=%v", res.AckedRecords, err)
	}
	if err := stack.Batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if res.StartedAt.Before(startedAt.Add(-time.Second)) || res.FinishedAt.Before(res.StartedAt) {
		t.Fatalf("ingest window wrong: %v..%v", res.StartedAt, res.FinishedAt)
	}

	opts := QueryRunOptions{
		Scenarios:   AllScenarios,
		Iterations:  5,
		Seed:        77,
		DatasetSeed: 11,
		From:        res.StartedAt.Add(-time.Minute),
		To:          res.FinishedAt.Add(time.Minute),
		Limit:       100,
	}
	outcomes, err := RunQueries(context.Background(), target, opts)
	if err != nil {
		t.Fatalf("RunQueries: %v", err)
	}
	if len(outcomes) != len(AllScenarios)*opts.Iterations {
		t.Fatalf("got %d outcomes, want %d", len(outcomes), len(AllScenarios)*opts.Iterations)
	}

	for _, oc := range outcomes {
		if oc.Error != "" {
			t.Fatalf("%s iter=%d: %s", oc.Scenario, oc.Iteration, oc.Error)
		}
		if oc.Scenario == ScenarioQ4Rare {
			// Dataset truth: 2000 records / RareTokenEvery=100 = 20 matches,
			// under the limit, so both count and total must be exact.
			if oc.Count != 20 || oc.Total != 20 {
				t.Fatalf("q4 count=%d total=%d, want 20/20 (dataset truth)", oc.Count, oc.Total)
			}
		}
		if oc.Scenario == ScenarioQ3FTS && oc.Count == 0 {
			t.Fatalf("q3 matched nothing — token set diverged from genBodies")
		}
	}

	// Same seed → identical instances and counts: a re-run must agree with
	// itself, which is the equality gate's own sanity check.
	again, err := RunQueries(context.Background(), target, opts)
	if err != nil {
		t.Fatalf("RunQueries(2): %v", err)
	}
	mismatches := CompareCounts([]ResultFile{
		{Target: "run1", Outcomes: outcomes},
		{Target: "run2", Outcomes: again},
	})
	if len(mismatches) != 0 {
		t.Fatalf("self-comparison failed the gate: %+v", mismatches[0])
	}
}

func TestCompareCountsFlagsDivergence(t *testing.T) {
	a := ResultFile{Target: "sysA", Outcomes: []QueryOutcome{
		{Scenario: ScenarioQ1Point, Iteration: 0, Params: "p", Count: 10},
		{Scenario: ScenarioQ1Point, Iteration: 1, Params: "q", Count: 5},
	}}
	b := ResultFile{Target: "sysB", Outcomes: []QueryOutcome{
		{Scenario: ScenarioQ1Point, Iteration: 0, Params: "p", Count: 10},
		{Scenario: ScenarioQ1Point, Iteration: 1, Params: "q", Count: 4}, // silent drop
	}}
	mismatches := CompareCounts([]ResultFile{a, b})
	if len(mismatches) != 1 {
		t.Fatalf("got %d mismatches, want 1", len(mismatches))
	}
	m := mismatches[0]
	if m.Iteration != 1 || m.Counts["sysA"] != 5 || m.Counts["sysB"] != 4 {
		t.Fatalf("wrong mismatch: %+v", m)
	}
}

func TestQueryInstanceDeterminism(t *testing.T) {
	opts := QueryRunOptions{Seed: 5, From: time.UnixMilli(1000), To: time.UnixMilli(100000), Limit: 100}
	for _, scenario := range AllScenarios {
		a := instance(scenario, 3, opts)
		b := instance(scenario, 3, opts)
		if a != b {
			t.Fatalf("%s: instance not deterministic: %+v vs %+v", scenario, a, b)
		}
		c := instance(scenario, 4, opts)
		if scenario != ScenarioQ4Rare && a == c {
			t.Fatalf("%s: iterations 3 and 4 produced identical params", scenario)
		}
	}
}
