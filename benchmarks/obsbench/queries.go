package obsbench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Log query scenarios per METHODOLOGY.md §5. Q5 (trace-ID lookup) belongs to
// the traces phase — the log dataset carries no trace IDs by design.
//
// Every instance derives its parameters from (seed, iteration), so two
// systems given the same seed answer the *same* query sequence — that is
// what makes the result-count equality gate (§6) meaningful.
const (
	ScenarioQ1Point = "q1-point"    // service + level
	ScenarioQ2Range = "q2-range"    // service over a sub-window
	ScenarioQ3FTS   = "q3-fts"      // common-token full-text
	ScenarioQ4Rare  = "q4-rare-fts" // rare-token full-text
)

var AllScenarios = []string{ScenarioQ1Point, ScenarioQ2Range, ScenarioQ3FTS, ScenarioQ4Rare}

// q3Tokens appear in genBodies templates, so every system has real matches.
var q3Tokens = []string{"timeout", "retrying", "payment", "cache"}

// QueryInstance is one fully-parameterized query.
type QueryInstance struct {
	Scenario string
	Service  string
	Level    string
	Token    string
	From     time.Time
	To       time.Time
	Limit    int

	// windowFrac is q2's window start as 1/1000ths of the ingest window —
	// the system-independent identity of the sub-window (absolute times
	// differ per system because each ingested at a different wall-clock).
	windowFrac int64
}

// QueryOutcome is the per-instance record written to the results file. Count
// is the number of returned entries (capped by Limit) — the equality-gate
// quantity. Total carries the system's own total-hits figure when the API
// reports one (amber does; informational only).
type QueryOutcome struct {
	Scenario  string        `json:"scenario"`
	Iteration int           `json:"iteration"`
	Params    string        `json:"params"`
	Count     int           `json:"count"`
	Total     int           `json:"total,omitempty"`
	Latency   time.Duration `json:"latency_ns"`
	Error     string        `json:"error,omitempty"`
}

// QueryRunOptions drives one query phase against one target.
type QueryRunOptions struct {
	Scenarios  []string
	Iterations int
	// Seed drives instance parameters (which service, which sub-window).
	Seed uint64
	// DatasetSeed is the seed the dataset was generated with; Q4 derives the
	// rare token from it. Conflating the two seeds silently turns Q4 into a
	// zero-match query.
	DatasetSeed uint64
	From        time.Time
	To          time.Time
	Limit       int
	// QPS paces the run (0 = back-to-back).
	QPS int
}

// instance derives the parameters for (scenario, iteration) from the seed.
// The PRNG is re-seeded per instance so systems can run different scenario
// subsets and still agree on any shared instance.
func instance(scenario string, iter int, opts QueryRunOptions) QueryInstance {
	rng := rand.New(rand.NewPCG(opts.Seed, uint64(iter)<<8^hashScenario(scenario)))
	qi := QueryInstance{
		Scenario: scenario,
		From:     opts.From,
		To:       opts.To,
		Limit:    opts.Limit,
	}
	switch scenario {
	case ScenarioQ1Point:
		qi.Service = genServices[rng.IntN(len(genServices))]
		qi.Level = "ERROR"
	case ScenarioQ2Range:
		qi.Service = genServices[rng.IntN(len(genServices))]
		// Random quarter sub-window of the ingest window. The fraction (not
		// the absolute time) is the cross-system identity of the instance:
		// every system ingests the same dataset over its own wall-clock
		// window, so equal fractions select comparable slices.
		span := opts.To.Sub(opts.From)
		if span > 0 {
			frac := rng.Int64N(750) // start offset in 0..74.9% of the window
			qi.windowFrac = frac
			off := time.Duration(frac * int64(span) / 1000)
			qi.From = opts.From.Add(off)
			qi.To = qi.From.Add(span / 4)
		}
	case ScenarioQ3FTS:
		// Token+service: with bare tokens (4 distinct values) most
		// iterations repeat within result-cache TTLs and the latency
		// distribution measures the cache, not the query path.
		qi.Token = q3Tokens[rng.IntN(len(q3Tokens))]
		qi.Service = genServices[rng.IntN(len(genServices))]
	case ScenarioQ4Rare:
		qi.Token = RareToken(opts.DatasetSeed)
	}
	return qi
}

func hashScenario(s string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// paramsKey identifies the logical query across systems: absolute times are
// deliberately excluded (each system ingests at its own wall-clock), the
// q2 sub-window is identified by its fraction.
func (qi QueryInstance) paramsKey() string {
	return fmt.Sprintf("svc=%s lvl=%s tok=%s winfrac=%d limit=%d",
		qi.Service, qi.Level, qi.Token, qi.windowFrac, qi.Limit)
}

// queryExecutor turns an instance into a request and extracts the entry
// count from the response. One per system kind.
type queryExecutor interface {
	Execute(ctx context.Context, client *http.Client, target TargetConfig, qi QueryInstance) (count, total int, err error)
}

func executorFor(t TargetConfig) (queryExecutor, error) {
	switch t.Kind {
	case "amber":
		return amberQuerier{}, nil
	case "loki":
		return lokiQuerier{}, nil
	case "victorialogs":
		return victoriaLogsQuerier{}, nil
	default:
		return nil, fmt.Errorf("query: no executor for kind %q", t.Kind)
	}
}

// RunQueries executes the scenario set and returns per-instance outcomes.
func RunQueries(ctx context.Context, target TargetConfig, opts QueryRunOptions) ([]QueryOutcome, error) {
	exec, err := executorFor(target)
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
			qi := instance(scenario, iter, opts)
			start := time.Now()
			count, total, err := exec.Execute(ctx, client, target, qi)
			oc := QueryOutcome{
				Scenario:  scenario,
				Iteration: iter,
				Params:    qi.paramsKey(),
				Count:     count,
				Total:     total,
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

// --- amber ---

type amberQuerier struct{}

func (amberQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi QueryInstance) (int, int, error) {
	params := url.Values{}
	if qi.Service != "" {
		params.Set("service", qi.Service)
	}
	if qi.Level != "" {
		params.Set("level", qi.Level)
	}
	if qi.Token != "" {
		params.Set("q", qi.Token)
	}
	if !qi.From.IsZero() {
		params.Set("from", qi.From.UTC().Format(time.RFC3339Nano))
	}
	if !qi.To.IsZero() {
		params.Set("to", qi.To.UTC().Format(time.RFC3339Nano))
	}
	params.Set("limit", strconv.Itoa(qi.Limit))

	body, err := getBody(ctx, client, target, "/api/v1/logs?"+params.Encode())
	if err != nil {
		return 0, 0, err
	}
	var resp struct {
		Entries   []json.RawMessage `json:"entries"`
		TotalHits int               `json:"total_hits"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0, fmt.Errorf("amber: decode: %w", err)
	}
	return len(resp.Entries), resp.TotalHits, nil
}

// --- loki ---

type lokiQuerier struct{}

func (lokiQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi QueryInstance) (int, int, error) {
	logql := ""
	switch {
	case qi.Service != "" && qi.Level != "":
		logql = fmt.Sprintf(`{service=%q, level=%q}`, qi.Service, qi.Level)
	case qi.Service != "":
		logql = fmt.Sprintf(`{service=%q}`, qi.Service)
	default:
		// LogQL requires a non-empty matcher even for pure text search.
		logql = `{service=~".+"}`
	}
	if qi.Token != "" {
		logql += fmt.Sprintf(` |= %q`, qi.Token)
	}

	params := url.Values{}
	params.Set("query", logql)
	params.Set("limit", strconv.Itoa(qi.Limit))
	if !qi.From.IsZero() {
		params.Set("start", strconv.FormatInt(qi.From.UnixNano(), 10))
	}
	if !qi.To.IsZero() {
		params.Set("end", strconv.FormatInt(qi.To.UnixNano(), 10))
	}

	body, err := getBody(ctx, client, target, "/loki/api/v1/query_range?"+params.Encode())
	if err != nil {
		return 0, 0, err
	}
	var resp struct {
		Data struct {
			Result []struct {
				Values [][2]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0, fmt.Errorf("loki: decode: %w", err)
	}
	count := 0
	for _, stream := range resp.Data.Result {
		count += len(stream.Values)
	}
	return count, 0, nil
}

// --- victorialogs ---

type victoriaLogsQuerier struct{}

func (victoriaLogsQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi QueryInstance) (int, int, error) {
	// LogsQL: field filters AND'ed, bare quoted token = word search in _msg.
	q := ""
	if qi.Service != "" {
		q += fmt.Sprintf("service:=%q ", qi.Service)
	}
	if qi.Level != "" {
		q += fmt.Sprintf("level:=%q ", qi.Level)
	}
	if qi.Token != "" {
		q += fmt.Sprintf("%q ", qi.Token)
	}
	if !qi.From.IsZero() && !qi.To.IsZero() {
		q += fmt.Sprintf("_time:[%s, %s] ",
			qi.From.UTC().Format(time.RFC3339Nano), qi.To.UTC().Format(time.RFC3339Nano))
	}
	if q == "" {
		q = "*"
	}

	params := url.Values{}
	params.Set("query", q)
	params.Set("limit", strconv.Itoa(qi.Limit))

	body, err := getBody(ctx, client, target, "/select/logsql/query?"+params.Encode())
	if err != nil {
		return 0, 0, err
	}
	// NDJSON stream: one line per matched entry.
	count := 0
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) > 0 {
			count++
		}
	}
	return count, 0, nil
}

func getBody(ctx context.Context, client *http.Client, target TargetConfig, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	if target.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+target.APIKey)
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
