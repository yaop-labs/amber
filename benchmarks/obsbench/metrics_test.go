package obsbench

import (
	"context"
	"testing"
	"time"
)

// TestMetricsGenerator_Deterministic: two generators with the same config
// must produce identical series and values tick for tick — the cross-system
// reproducibility contract.
func TestMetricsGenerator_Deterministic(t *testing.T) {
	cfg := MetricsGenConfig{Seed: 42, ScalarSeries: 200, HistSeries: 20, Routes: 50, ChurnPercent: 20}
	a, b := NewMetricsGenerator(cfg), NewMetricsGenerator(cfg)
	for tick := 0; tick < 5; tick++ {
		a.BeginTick(tick)
		b.BeginTick(tick)
		for i := 0; i < a.ScalarCount(); i++ {
			an, al, av, ac := a.ScalarPoint(i, tick)
			bn, bl, bv, bc := b.ScalarPoint(i, tick)
			if an != bn || al != bl || av != bv || ac != bc {
				t.Fatalf("tick %d series %d diverged: (%s %+v %d) vs (%s %+v %d)", tick, i, an, al, av, bn, bl, bv)
			}
		}
		for i := 0; i < a.HistCount(); i++ {
			al, ah := a.HistPoint(i)
			bl, bh := b.HistPoint(i)
			if al != bl || ah.Count != bh.Count || ah.Sum != bh.Sum || ah.Offset != bh.Offset {
				t.Fatalf("tick %d hist %d diverged", tick, i)
			}
		}
	}
}

// TestMetricsGenerator_CountersMonotonic guards the cumulative-temporality
// contract: a non-monotonic counter would be silently dropped or misread by
// every Prometheus-lineage system.
func TestMetricsGenerator_CountersMonotonic(t *testing.T) {
	g := NewMetricsGenerator(MetricsGenConfig{Seed: 1, ScalarSeries: 100, HistSeries: 10})
	prev := make([]int64, g.CounterCount())
	prevHist := make([]uint64, g.HistCount())
	for tick := 0; tick < 10; tick++ {
		g.BeginTick(tick)
		for i := 0; i < g.CounterCount(); i++ {
			_, _, v, isCounter := g.ScalarPoint(i, tick)
			if !isCounter {
				t.Fatalf("series %d should be a counter", i)
			}
			if v <= prev[i] {
				t.Fatalf("counter %d not monotonic at tick %d: %d -> %d", i, tick, prev[i], v)
			}
			prev[i] = v
		}
		for i := 0; i < g.HistCount(); i++ {
			_, h := g.HistPoint(i)
			if h.Count < prevHist[i] {
				t.Fatalf("hist %d count shrank at tick %d", i, tick)
			}
			prevHist[i] = h.Count
		}
	}
}

// TestMetricsGenerator_Churn: an epoch change must rotate the gen label and
// reset cumulative state for exactly the churnable subset (I2 semantics:
// replaced series behave like restarted instances).
func TestMetricsGenerator_Churn(t *testing.T) {
	g := NewMetricsGenerator(MetricsGenConfig{Seed: 7, ScalarSeries: 200, HistSeries: 20, ChurnPercent: 20})
	g.BeginTick(0)
	g.BeginTick(1)

	churned, stable := -1, -1
	for i := 0; i < g.CounterCount(); i++ {
		if g.churns(i) && churned == -1 {
			churned = i
		}
		if !g.churns(i) && stable == -1 {
			stable = i
		}
	}
	if churned == -1 || stable == -1 {
		t.Fatal("expected both churnable and stable series")
	}
	_, lblBefore, _, _ := g.ScalarPoint(churned, 1)
	_, _, stableBefore, _ := g.ScalarPoint(stable, 1)

	g.SetEpoch(1)
	_, lblAfter, churnedVal, _ := g.ScalarPoint(churned, 1)
	_, stableLbl, stableAfter, _ := g.ScalarPoint(stable, 1)

	if lblBefore.Gen != 0 || lblAfter.Gen != 1 {
		t.Fatalf("gen label: before=%d after=%d, want 0/1", lblBefore.Gen, lblAfter.Gen)
	}
	if churnedVal != 0 {
		t.Fatalf("churned counter = %d, want reset to 0", churnedVal)
	}
	if stableLbl.Gen != -1 || stableAfter != stableBefore {
		t.Fatalf("stable series must be untouched (gen=%d, %d -> %d)", stableLbl.Gen, stableBefore, stableAfter)
	}

	// 20% of the index space churns.
	n := 0
	for i := 0; i < g.ScalarCount(); i++ {
		if g.churns(i) {
			n++
		}
	}
	if n != g.ScalarCount()/5 {
		t.Fatalf("churn subset = %d of %d, want 20%%", n, g.ScalarCount())
	}
}

// TestMetricsInstance_CrossSystemIdentity: the same (seed, iteration) over
// two different absolute ingest windows must produce the same paramsKey —
// that equality is what lets the gate match instances across systems.
func TestMetricsInstance_CrossSystemIdentity(t *testing.T) {
	mk := func(from time.Time) MetricsQueryRunOptions {
		return MetricsQueryRunOptions{Seed: 9, From: from, To: from.Add(30 * time.Minute)}
	}
	a := mk(time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC))
	b := mk(time.Date(2026, 6, 13, 22, 30, 0, 0, time.UTC))
	for _, scenario := range AllMetricsScenarios {
		for iter := 0; iter < 10; iter++ {
			qa := metricsInstance(scenario, iter, a)
			qb := metricsInstance(scenario, iter, b)
			if qa.paramsKey() != qb.paramsKey() {
				t.Fatalf("%s iter %d: params diverged:\n%s\n%s", scenario, iter, qa.paramsKey(), qb.paramsKey())
			}
			if qa.Eval.Equal(qb.Eval) && scenario != ScenarioQM2Range {
				t.Fatalf("%s iter %d: absolute eval times should differ across windows", scenario, iter)
			}
		}
	}
}

// TestE2E_AmberMetricsIngestVerifyQuery drives the full metrics phase against
// a real amber stack: OTLP ingest -> sample-count verify -> QM1–QM4 natively.
// Payload-format or endpoint drift fails here instead of producing a
// zero-sample benchmark.
func TestE2E_AmberMetricsIngestVerifyQuery(t *testing.T) {
	stack, srv := startAmber(t)
	_ = stack
	target := TargetConfig{Kind: "amber", BaseURL: srv.URL}

	const ticks = 10
	opts := MetricsIngestOptions{
		Gen:         MetricsGenConfig{Seed: 3, ScalarSeries: 200, HistSeries: 20, Routes: 50, ChurnPercent: 20},
		Interval:    20 * time.Millisecond,
		Ticks:       ticks,
		ChurnEvery:  time.Hour, // no churn within the test run
		Workers:     4,
		BatchSeries: 64,
	}
	res, err := IngestMetrics(context.Background(), "amber", target, opts, nil)
	if err != nil {
		t.Fatalf("IngestMetrics: %v", err)
	}
	wantScalar := uint64(ticks * 200)
	wantHist := uint64(ticks * 20)
	if res.SentScalar != wantScalar || res.SentHist != wantHist {
		t.Fatalf("sent scalar=%d hist=%d, want %d/%d", res.SentScalar, res.SentHist, wantScalar, wantHist)
	}
	if res.AckedSamples != wantScalar+wantHist || res.FailedBatches != 0 {
		t.Fatalf("acked=%d failed=%d errors=%v, want %d/0", res.AckedSamples, res.FailedBatches, res.Errors, wantScalar+wantHist)
	}

	got, err := CountQueryableMetricSamples(context.Background(), target, res.StartedAt, res.FinishedAt)
	if err != nil {
		t.Fatalf("CountQueryableMetricSamples: %v", err)
	}
	if got != wantScalar {
		t.Fatalf("queryable scalar = %d, want %d (silent ingest loss)", got, wantScalar)
	}

	outcomes, err := RunMetricsQueries(context.Background(), target, MetricsQueryRunOptions{
		Scenarios:  AllMetricsScenarios,
		Iterations: 3,
		Seed:       5,
		From:       res.StartedAt,
		To:         res.FinishedAt,
	})
	if err != nil {
		t.Fatalf("RunMetricsQueries: %v", err)
	}
	sawCount := map[string]int{}
	for _, oc := range outcomes {
		if oc.Error != "" {
			t.Fatalf("%s iter %d: %s", oc.Scenario, oc.Iteration, oc.Error)
		}
		if oc.Count > sawCount[oc.Scenario] {
			sawCount[oc.Scenario] = oc.Count
		}
	}
	// 200 scalar series spread over 10 services / 50 routes; 20 hist series
	// over 10 services. The late-eval iterations must see every group.
	if sawCount[ScenarioQM1Rate] != 10 {
		t.Fatalf("qm1 max group count = %d, want 10", sawCount[ScenarioQM1Rate])
	}
	if sawCount[ScenarioQM2Range] != 10 {
		t.Fatalf("qm2 max group count = %d, want 10", sawCount[ScenarioQM2Range])
	}
	if sawCount[ScenarioQM3Quantile] != 10 {
		t.Fatalf("qm3 max group count = %d, want 10", sawCount[ScenarioQM3Quantile])
	}
	if sawCount[ScenarioQM4GroupBy] != 50 {
		t.Fatalf("qm4 max group count = %d, want 50", sawCount[ScenarioQM4GroupBy])
	}
}
