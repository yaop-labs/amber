package obsbench

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Trace query scenarios per METHODOLOGY.md (QT1-QT3). As with metrics,
// every instance derives its parameters from (seed, iteration) and identifies
// itself by parameters that are system-independent (trace index, service,
// operation, duration threshold), never by absolute times - each system
// ingested over its own wall clock.
//
// The gate quantity differs by scenario:
//
//   - QT1 (trace-ID lookup) is an exact set-equality gate: every system must
//     return the same span count for the same trace (SpansPerTrace).
//   - QT2/QT3 (search) use a fixed result limit L over the full ingest window.
//     At campaign scale a service+operation or duration predicate matches far
//     more than L traces, and every trace search API caps its result page, so
//     the honest, comparable unit of work is "return a full page of L matching
//     traces". The gate is count==L on every system - the trace analogue of
//     the metrics group-cardinality gate (a documented coarse gate), not a
//     claim that the systems returned the identical L traces.
const (
	ScenarioQT1Lookup    = "qt1-trace-lookup"
	ScenarioQT2ServiceOp = "qt2-service-op"
	ScenarioQT3Duration  = "qt3-duration"
)

var AllTraceScenarios = []string{ScenarioQT1Lookup, ScenarioQT2ServiceOp, ScenarioQT3Duration}

// traceSearchLimit is L: the fixed result page size for QT2/QT3. Large enough
// that real campaign workloads always have >L matches (so every system returns
// exactly L), small enough that no system refuses it.
const traceSearchLimit = 100

// qt3Thresholds are the QT3 duration cutoffs. All sit strictly inside
// (childMaxDurationMS, rootMaxDurationMS) so only root spans can match and
// every threshold has matches.
var qt3Thresholds = []time.Duration{200 * time.Millisecond, 400 * time.Millisecond, 600 * time.Millisecond, 800 * time.Millisecond}

// TraceQueryInstance is one fully-parameterized trace query.
type TraceQueryInstance struct {
	Scenario string

	// QT1: TraceIndex identifies the trace across systems; TraceID is the
	// hex id derived from (GenSeed, TraceIndex).
	TraceIndex int
	TraceID    string

	// QT2.
	Service   string
	Operation string

	// QT3.
	MinDuration time.Duration

	// Search window and page size (QT2/QT3).
	From  time.Time
	To    time.Time
	Limit int
}

// paramsKey is the system-independent identity used by the equality gate.
func (qi TraceQueryInstance) paramsKey() string {
	switch qi.Scenario {
	case ScenarioQT1Lookup:
		return fmt.Sprintf("trace=%d", qi.TraceIndex)
	case ScenarioQT2ServiceOp:
		return fmt.Sprintf("svc=%s op=%s limit=%d", qi.Service, qi.Operation, qi.Limit)
	case ScenarioQT3Duration:
		return fmt.Sprintf("svc=%s mindur=%s limit=%d", qi.Service, qi.MinDuration, qi.Limit)
	default:
		return qi.Scenario
	}
}

// TraceQueryRunOptions drives one trace query phase against one target.
type TraceQueryRunOptions struct {
	Scenarios  []string
	Iterations int
	// Seed drives instance parameters (which trace/service/operation/threshold).
	Seed uint64
	// GenSeed is the seed the workload was ingested with; QT1 recomputes trace
	// IDs from it. Conflating it with Seed silently turns QT1 into a
	// nonexistent-trace lookup.
	GenSeed uint64
	// Traces is the ingested trace count (from the ingest summary): the QT1
	// index space.
	Traces int
	// From/To is the ingest window (from the ingest summary).
	From time.Time
	To   time.Time
	// QPS paces the run (0 = back-to-back).
	QPS int
}

// traceInstance derives the parameters for (scenario, iteration).
func traceInstance(scenario string, iter int, opts TraceQueryRunOptions) TraceQueryInstance {
	rng := rand.New(rand.NewPCG(opts.Seed, uint64(iter)<<8^hashScenario(scenario)))
	// Pad the search window by a margin on each side. The window is the full
	// ingest span; the margin only guards the boundary against per-system time
	// rounding (Tempo uses second bounds, Jaeger microseconds) so a trace
	// stamped in the last fractional second is never excluded. paramsKey omits
	// the window, so the margin is invisible to the equality gate and applies
	// identically to every system.
	const windowMargin = 5 * time.Second
	qi := TraceQueryInstance{
		Scenario: scenario,
		From:     opts.From.Add(-windowMargin),
		To:       opts.To.Add(windowMargin),
		Limit:    traceSearchLimit,
	}
	switch scenario {
	case ScenarioQT1Lookup:
		n := opts.Traces
		if n <= 0 {
			n = 1
		}
		qi.TraceIndex = rng.IntN(n)
		gen := NewTracesGenerator(TracesGenConfig{Seed: opts.GenSeed})
		id := gen.TraceID(qi.TraceIndex)
		qi.TraceID = hex.EncodeToString(id[:])
	case ScenarioQT2ServiceOp:
		qi.Service = genServices[rng.IntN(len(genServices))]
		qi.Operation = rootOpName(rng.IntN(traceRootOpCount))
	case ScenarioQT3Duration:
		// Scoped to a service (the realistic "slow traces in service X" query)
		// so every backend can express it: Tempo and amber accept a bare
		// duration predicate, but the Jaeger search API VictoriaTraces exposes
		// requires a service. At campaign scale a service still has far more
		// than L traces above any threshold, so the count==L gate holds.
		qi.Service = genServices[rng.IntN(len(genServices))]
		qi.MinDuration = qt3Thresholds[rng.IntN(len(qt3Thresholds))]
	}
	return qi
}

// traceQueryExecutor turns an instance into a request and extracts the gate
// count (span count for QT1, returned-trace count for QT2/QT3).
type traceQueryExecutor interface {
	Execute(ctx context.Context, client *http.Client, target TargetConfig, qi TraceQueryInstance) (count int, err error)
}

func traceExecutorFor(t TargetConfig) (traceQueryExecutor, error) {
	switch t.Kind {
	case "amber":
		return amberTraceQuerier{}, nil
	case "tempo":
		return tempoQuerier{}, nil
	case "victoriatraces":
		return jaegerQuerier{}, nil
	default:
		return nil, fmt.Errorf("trace query: no executor for kind %q", t.Kind)
	}
}

// RunTraceQueries executes the scenario set and returns per-instance outcomes
// in the shared ResultFile shape, so compare reuses the logs/metrics gate.
func RunTraceQueries(ctx context.Context, target TargetConfig, opts TraceQueryRunOptions) ([]QueryOutcome, error) {
	exec, err := traceExecutorFor(target)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 120 * time.Second}

	var pacer *time.Ticker
	if opts.QPS > 0 {
		pacer = time.NewTicker(time.Second / time.Duration(opts.QPS))
		defer pacer.Stop()
	}

	var out []QueryOutcome
	for _, scenario := range opts.Scenarios {
		for iter := 0; iter < opts.Iterations; iter++ {
			if pacer != nil {
				select {
				case <-pacer.C:
				case <-ctx.Done():
					return out, ctx.Err()
				}
			}
			qi := traceInstance(scenario, iter, opts)
			start := time.Now()
			count, err := exec.Execute(ctx, client, target, qi)
			oc := QueryOutcome{
				Scenario:  scenario,
				Iteration: iter,
				Params:    qi.paramsKey(),
				Count:     count,
				Latency:   time.Since(start),
			}
			if err != nil {
				oc.Error = err.Error()
			}
			out = append(out, oc)
		}
	}
	return out, nil
}

// --- amber (native HTTP API) ---

type amberTraceQuerier struct{}

func (amberTraceQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi TraceQueryInstance) (int, error) {
	switch qi.Scenario {
	case ScenarioQT1Lookup:
		body, err := getBody(ctx, client, target, "/api/v1/traces/"+qi.TraceID)
		if err != nil {
			return 0, err
		}
		var resp struct {
			SpanCount int `json:"span_count"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber trace lookup: decode: %w", err)
		}
		return resp.SpanCount, nil

	default: // QT2, QT3: trace search
		params := url.Values{}
		params.Set("from", qi.From.UTC().Format(time.RFC3339Nano))
		params.Set("to", qi.To.UTC().Format(time.RFC3339Nano))
		params.Set("limit", strconv.Itoa(qi.Limit))
		if qi.Scenario == ScenarioQT2ServiceOp {
			params.Set("service", qi.Service)
			params.Set("operation", qi.Operation)
		} else {
			params.Set("service", qi.Service)
			params.Set("min_duration", qi.MinDuration.String())
		}
		body, err := getBody(ctx, client, target, "/api/v1/traces?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Traces []json.RawMessage `json:"traces"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber trace search: decode: %w", err)
		}
		return len(resp.Traces), nil
	}
}

// --- Tempo (TraceQL search + OTLP-JSON trace fetch) ---

type tempoQuerier struct{}

func (tempoQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi TraceQueryInstance) (int, error) {
	switch qi.Scenario {
	case ScenarioQT1Lookup:
		// Tempo serves protobuf by default; ask for OTLP JSON.
		body, err := getBodyAccept(ctx, client, target, "/api/traces/"+qi.TraceID, "application/json")
		if err != nil {
			return 0, err
		}
		return countOTLPJSONSpans(body)

	default: // QT2, QT3: TraceQL search
		var q string
		if qi.Scenario == ScenarioQT2ServiceOp {
			q = fmt.Sprintf(`{resource.service.name="%s" && name="%s"}`, qi.Service, qi.Operation)
		} else {
			q = fmt.Sprintf(`{resource.service.name="%s" && duration > %dms}`, qi.Service, qi.MinDuration.Milliseconds())
		}
		params := url.Values{}
		params.Set("q", q)
		params.Set("limit", strconv.Itoa(qi.Limit))
		params.Set("start", strconv.FormatInt(qi.From.Unix(), 10))
		params.Set("end", strconv.FormatInt(qi.To.Unix(), 10))
		body, err := getBody(ctx, client, target, "/api/search?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Traces []json.RawMessage `json:"traces"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("tempo search: decode: %w", err)
		}
		return len(resp.Traces), nil
	}
}

// --- VictoriaTraces (Jaeger-compatible query API) ---

type jaegerQuerier struct{}

func (jaegerQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi TraceQueryInstance) (int, error) {
	prefix := target.JaegerPrefix
	if prefix == "" {
		prefix = "/select/jaeger"
	}
	switch qi.Scenario {
	case ScenarioQT1Lookup:
		body, err := getBody(ctx, client, target, prefix+"/api/traces/"+qi.TraceID)
		if err != nil {
			return 0, err
		}
		return countJaegerSpans(body)

	default: // QT2, QT3: Jaeger search (microsecond time bounds)
		params := url.Values{}
		params.Set("limit", strconv.Itoa(qi.Limit))
		params.Set("start", strconv.FormatInt(qi.From.UnixMicro(), 10))
		params.Set("end", strconv.FormatInt(qi.To.UnixMicro(), 10))
		if qi.Scenario == ScenarioQT2ServiceOp {
			params.Set("service", qi.Service)
			params.Set("operation", qi.Operation)
		} else {
			// Jaeger search requires a service; QT3 carries one (see
			// traceInstance) and applies the duration threshold via minDuration.
			params.Set("service", qi.Service)
			params.Set("minDuration", qi.MinDuration.String())
		}
		body, err := getBody(ctx, client, target, prefix+"/api/traces?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Data []json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("jaeger search: decode: %w", err)
		}
		return len(resp.Data), nil
	}
}

// countOTLPJSONSpans counts spans in a Tempo OTLP-JSON trace response
// ({"batches":[{"scopeSpans":[{"spans":[...]}]}]}); older Tempo nests under
// instrumentationLibrarySpans, handled too.
func countOTLPJSONSpans(body []byte) (int, error) {
	var resp struct {
		Batches []struct {
			ScopeSpans []struct {
				Spans []json.RawMessage `json:"spans"`
			} `json:"scopeSpans"`
			InstrumentationLibrarySpans []struct {
				Spans []json.RawMessage `json:"spans"`
			} `json:"instrumentationLibrarySpans"`
		} `json:"batches"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("otlp-json trace: decode: %w", err)
	}
	n := 0
	for _, b := range resp.Batches {
		for _, ss := range b.ScopeSpans {
			n += len(ss.Spans)
		}
		for _, ss := range b.InstrumentationLibrarySpans {
			n += len(ss.Spans)
		}
	}
	return n, nil
}

// countJaegerSpans counts spans in a Jaeger trace response
// ({"data":[{"spans":[...]}]}).
func countJaegerSpans(body []byte) (int, error) {
	var resp struct {
		Data []struct {
			Spans []json.RawMessage `json:"spans"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("jaeger trace: decode: %w", err)
	}
	n := 0
	for _, d := range resp.Data {
		n += len(d.Spans)
	}
	return n, nil
}

// getBodyAccept is getBody with an explicit Accept header (Tempo's trace-by-id
// endpoint serves protobuf unless asked for JSON).
func getBodyAccept(ctx context.Context, client *http.Client, target TargetConfig, path, accept string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	if target.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+target.APIKey)
	}
	if target.OrgID != "" {
		req.Header.Set("X-Scope-OrgID", target.OrgID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("status %d: %.200s", resp.StatusCode, body)
	}
	return body, nil
}
