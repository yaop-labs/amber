package obsbench

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"
)

// Trace ingest verification (METHODOLOGY.md): a completeness gate.
//
// Unlike logs (exact record count) and metrics (exact scalar-sample count),
// there is no system-independent global span count a trace store cheaply
// exposes - Tempo and the Jaeger API answer per-trace, not "how many spans did
// you store". So trace verification is a sampled per-trace check: fetch a
// deterministic spread of trace IDs and assert each returns exactly
// SpansPerTrace spans. A trace dropped at ingest, or a partially stored trace,
// fails here (span count != SpansPerTrace) instead of producing a benchmark
// over silently incomplete data.
//
// This is a proxy, not a full count: it cannot detect loss confined to traces
// outside the sample. The sample is large and evenly spread to make that
// unlikely; the gate is intentionally conservative (any miss fails the run).

// TraceVerifyResult reports the sampled completeness check.
type TraceVerifyResult struct {
	Sampled   int
	Complete  int
	FirstMiss string // hex trace ID of the first incomplete/missing trace, if any
	MissSpans int    // span count returned for FirstMiss
}

// OK reports whether every sampled trace was complete.
func (r TraceVerifyResult) OK() bool { return r.Sampled > 0 && r.Complete == r.Sampled }

// VerifyTraces samples `samples` trace IDs evenly across the ingested index
// space and checks each returns SpansPerTrace spans via the target's trace-ID
// lookup. samples <= 0 defaults to 1000 (capped at the trace count).
func VerifyTraces(ctx context.Context, target TargetConfig, ir TracesIngestResult, samples int) (TraceVerifyResult, error) {
	exec, err := traceExecutorFor(target)
	if err != nil {
		return TraceVerifyResult{}, err
	}
	if ir.Traces <= 0 {
		return TraceVerifyResult{}, fmt.Errorf("traces verify: ingest summary has no traces")
	}
	if samples <= 0 {
		samples = 1000
	}
	if samples > ir.Traces {
		samples = ir.Traces
	}
	gen := NewTracesGenerator(TracesGenConfig{Seed: ir.Seed, SpansPerTrace: ir.SpansPerTrace})
	client := &http.Client{Timeout: 60 * time.Second}

	var res TraceVerifyResult
	stride := ir.Traces / samples
	if stride < 1 {
		stride = 1
	}
	for s := 0; s < samples; s++ {
		idx := s * stride
		if idx >= ir.Traces {
			break
		}
		id := gen.TraceID(idx)
		qi := TraceQueryInstance{Scenario: ScenarioQT1Lookup, TraceID: hex.EncodeToString(id[:])}
		count, err := exec.Execute(ctx, client, target, qi)
		if err != nil {
			return res, fmt.Errorf("traces verify: lookup idx %d: %w", idx, err)
		}
		res.Sampled++
		if count == ir.SpansPerTrace {
			res.Complete++
		} else if res.FirstMiss == "" {
			res.FirstMiss = qi.TraceID
			res.MissSpans = count
		}
	}
	return res, nil
}
