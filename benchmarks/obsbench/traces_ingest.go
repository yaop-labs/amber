package obsbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Trace ingest runner: rate-capped continuous OTLP push (METHODOLOGY.md,
// T1). Every system under test accepts the same OTLP protobuf payload - the
// same-client fairness baseline; only the mount path differs per system.
//
// Unlike the metrics runner (tick-driven, one sample per series per tick), a
// trace workload is a stream: traces are emitted back to back up to a target
// count, rate-capped on spans/sec the way the log runner caps records/sec.
// Span start times are stamped at send so each system indexes the workload
// over its own wall clock; the query phase selects sub-windows by fraction.

// otlpTracesPath returns the system's OTLP traces mount.
func otlpTracesPath(t TargetConfig) string {
	if t.OTLPTracesPath != "" {
		return t.OTLPTracesPath
	}
	switch t.Kind {
	case "victoriatraces":
		return "/insert/opentelemetry/v1/traces"
	default: // amber, tempo
		return "/v1/traces"
	}
}

// TracesIngestOptions drives one trace ingest run.
type TracesIngestOptions struct {
	Gen TracesGenConfig
	// Traces is the total number of traces to emit (required, > 0).
	Traces int
	// Rate caps spans/sec across workers (0 = burst, no cap).
	Rate    int
	Workers int
	// BatchTraces is the number of whole traces per OTLP request (default 200).
	// Traces are never split across requests, so QT1 always finds a complete
	// trace regardless of where a batch boundary falls.
	BatchTraces int
}

// TracesIngestResult is the run summary; the JSON file is the publishable
// artifact and supplies the query phase's time window plus the QT1 trace-ID
// space (Seed + Traces) and the verify step's expected per-trace span count.
type TracesIngestResult struct {
	Target        string            `json:"target"`
	StartedAt     time.Time         `json:"started_at"`
	FinishedAt    time.Time         `json:"finished_at"`
	Seed          uint64            `json:"seed"`
	Traces        int               `json:"traces"`
	SpansPerTrace int               `json:"spans_per_trace"`
	SentSpans     uint64            `json:"sent_spans"`
	AckedSpans    uint64            `json:"acked_spans"`
	FailedBatches uint64            `json:"failed_batches"`
	Duration      time.Duration     `json:"duration_ns"`
	SpansPerSec   float64           `json:"spans_per_sec"`
	LatencyP50    time.Duration     `json:"latency_p50_ns"`
	LatencyP95    time.Duration     `json:"latency_p95_ns"`
	LatencyP99    time.Duration     `json:"latency_p99_ns"`
	Errors        map[string]uint64 `json:"errors,omitempty"`
}

type tracesBatch struct {
	body  []byte
	spans int
}

// IngestTraces streams Traces traces into the target.
func IngestTraces(ctx context.Context, targetName string, target TargetConfig, opts TracesIngestOptions, logf func(string, ...any)) (TracesIngestResult, error) {
	if opts.Traces <= 0 {
		return TracesIngestResult{}, fmt.Errorf("traces ingest: Traces must be > 0")
	}
	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	if opts.BatchTraces <= 0 {
		opts.BatchTraces = 200
	}
	gen := NewTracesGenerator(opts.Gen)
	spt := gen.SpansPerTrace()
	path := otlpTracesPath(target)
	// Trace ingest may target a different host:port than queries (Tempo's OTLP
	// receiver vs its query API); swap only the base URL, keep auth/org headers.
	ingestTarget := target
	if target.IngestBaseURL != "" {
		ingestTarget.BaseURL = target.IngestBaseURL
	}
	client := &http.Client{Timeout: 60 * time.Second}

	batches := make(chan tracesBatch, opts.Workers*2)
	var (
		acked, failedBatches atomic.Uint64
		mu                   sync.Mutex
		latencies            []time.Duration
		errCounts            = make(map[string]uint64)
	)
	var wg sync.WaitGroup
	for range opts.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				start := time.Now()
				status, respBody, err := post(ctx, client, ingestTarget, path, "application/x-protobuf", b.body)
				elapsed := time.Since(start)
				if err != nil {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, "transport: "+err.Error())
					continue
				}
				accepted, ok := countAcceptedSpans(target.Kind, status, respBody, b.spans)
				if !ok {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, fmt.Sprintf("status %d", status))
					continue
				}
				acked.Add(uint64(accepted))
				if accepted < b.spans {
					recordErr(&mu, errCounts, "partial reject")
				}
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}()
	}

	// Token bucket over batches: Rate is spans/sec, so a batch costs
	// BatchTraces*SpansPerTrace tokens.
	var limiter *time.Ticker
	if opts.Rate > 0 {
		batchSpans := opts.BatchTraces * spt
		interval := time.Duration(float64(batchSpans) / float64(opts.Rate) * float64(time.Second))
		if interval <= 0 {
			interval = time.Nanosecond
		}
		limiter = time.NewTicker(interval)
		defer limiter.Stop()
	}

	res := TracesIngestResult{
		Target:        targetName,
		Seed:          opts.Gen.Seed,
		Traces:        opts.Traces,
		SpansPerTrace: spt,
	}
	start := time.Now()
	res.StartedAt = start

sendLoop:
	for lo := 0; lo < opts.Traces; lo += opts.BatchTraces {
		hi := min(lo+opts.BatchTraces, opts.Traces)
		if limiter != nil {
			select {
			case <-limiter.C:
			case <-ctx.Done():
				break sendLoop
			}
		}
		body, err := buildTracesOTLP(gen, lo, hi, time.Now())
		if err != nil {
			return res, fmt.Errorf("traces ingest: build: %w", err)
		}
		spans := (hi - lo) * spt
		select {
		case batches <- tracesBatch{body: body, spans: spans}:
			res.SentSpans += uint64(spans)
		case <-ctx.Done():
			break sendLoop
		}
		if logf != nil && (hi/opts.BatchTraces)%200 == 0 {
			logf("sent %d/%d traces acked_spans=%d failed=%d", hi, opts.Traces, acked.Load(), failedBatches.Load())
		}
	}
	close(batches)
	wg.Wait()

	res.Duration = time.Since(start)
	res.FinishedAt = start.Add(res.Duration)
	res.AckedSpans = acked.Load()
	res.FailedBatches = failedBatches.Load()
	res.Errors = errCounts
	if res.Duration > 0 {
		res.SpansPerSec = float64(res.AckedSpans) / res.Duration.Seconds()
	}
	res.LatencyP50, res.LatencyP95, res.LatencyP99 = Percentiles(latencies)
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// countAcceptedSpans mirrors countAcceptedMetrics: amber answers
// {"accepted":N,"rejected":M} per span, everyone else acks the whole request
// by status code.
func countAcceptedSpans(kind string, status int, body []byte, spans int) (int, bool) {
	if kind == "amber" {
		var resp struct {
			Accepted *int `json:"accepted"`
		}
		if json.Unmarshal(body, &resp) == nil && resp.Accepted != nil {
			return *resp.Accepted, true
		}
	}
	if status >= 200 && status <= 299 {
		return spans, true
	}
	return 0, false
}

// buildTracesOTLP renders whole traces [lo, hi) into one OTLP request, grouped
// into one ResourceSpans per service the way a collector batches real traffic.
// sendTime is stamped on every span's start (end = start + generated
// duration), so the trace "happens" at ingest wall time and the query window
// derives from [StartedAt, FinishedAt].
func buildTracesOTLP(gen *TracesGenerator, lo, hi int, sendTime time.Time) ([]byte, error) {
	startNano := uint64(sendTime.UnixNano())
	byService := make(map[string][]*tracepb.Span)
	for i := lo; i < hi; i++ {
		traceID := gen.TraceID(i)
		service := gen.Service(i)
		for _, gs := range gen.Trace(i) {
			spanID := gen.SpanID(i, gs.Index)
			sp := &tracepb.Span{
				TraceId:           traceID[:],
				SpanId:            spanID[:],
				Name:              gs.Operation,
				StartTimeUnixNano: startNano,
				EndTimeUnixNano:   startNano + uint64(gs.Duration.Nanoseconds()),
			}
			if !gs.IsRoot() {
				parentID := gen.SpanID(i, gs.ParentIndex)
				sp.ParentSpanId = parentID[:]
			}
			if gs.Error {
				sp.Status = &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}
			}
			byService[service] = append(byService[service], sp)
		}
	}

	req := &collectortrace.ExportTraceServiceRequest{}
	for service, spans := range byService {
		req.ResourceSpans = append(req.ResourceSpans, &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: strVal(service)},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
		})
	}
	return proto.Marshal(req)
}
