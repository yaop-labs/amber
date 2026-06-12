package obsbench

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Metrics query scenarios per METHODOLOGY.md §5 (QM1–QM4). As with logs,
// every instance derives its parameters from (seed, iteration) and identifies
// itself by window *fractions*, never absolute times — each system ingested
// over its own wall clock, so equal fractions select comparable slices and
// the equality gate (§6) stays meaningful.
//
// The gate quantity for metrics is the result-group cardinality (number of
// series/groups returned), not values: rate math differs across engines by
// design (PromQL extrapolates, amber doesn't), and that difference is
// documented rather than averaged away.
const (
	ScenarioQM1Rate     = "qm1-rate"       // instant rate grouped by service (10 groups)
	ScenarioQM2Range    = "qm2-range-rate" // range rate with steps, by service
	ScenarioQM3Quantile = "qm3-quantile"   // p95/p99 over exponential histograms, by service
	ScenarioQM4GroupBy  = "qm4-groupby"    // instant rate grouped by route (1k groups)
)

var AllMetricsScenarios = []string{ScenarioQM1Rate, ScenarioQM2Range, ScenarioQM3Quantile, ScenarioQM4GroupBy}

// MetricsQueryInstance is one fully-parameterized metrics query. From/To/Eval
// are absolute (per-system); the *Frac fields are the cross-system identity.
type MetricsQueryInstance struct {
	Scenario string
	Metric   string
	By       string
	Window   time.Duration
	Quantile float64

	// Eval is the instant-query evaluation point (QM1/QM3/QM4).
	Eval time.Time
	// From/To/Step define the QM2 range grid.
	From time.Time
	To   time.Time
	Step time.Duration

	evalFrac  int64
	startFrac int64
}

// metricsWindow is the rate/quantile window shared by all scenarios.
const metricsWindow = 5 * time.Minute

// metricsInstance derives the parameters for (scenario, iteration).
func metricsInstance(scenario string, iter int, opts MetricsQueryRunOptions) MetricsQueryInstance {
	rng := rand.New(rand.NewPCG(opts.Seed, uint64(iter)<<8^hashScenario(scenario)))
	span := opts.To.Sub(opts.From)
	qi := MetricsQueryInstance{Scenario: scenario, Window: metricsWindow}

	// evalAt places an instant query inside the back 70% of the ingest
	// window, so the rate window always has data behind it.
	evalAt := func() {
		qi.evalFrac = 300 + rng.Int64N(701) // 300..1000
		qi.Eval = opts.From.Add(time.Duration(qi.evalFrac * int64(span) / 1000))
	}
	switch scenario {
	case ScenarioQM1Rate:
		qi.Metric, qi.By = MetricCounterName, "service"
		evalAt()
	case ScenarioQM2Range:
		qi.Metric, qi.By = MetricCounterName, "service"
		qi.Window = time.Minute
		qi.startFrac = rng.Int64N(500) // sub-window start in 0..49.9%
		qi.From = opts.From.Add(time.Duration(qi.startFrac * int64(span) / 1000))
		qi.To = qi.From.Add(span / 2)
		qi.Step = (span / 2 / 20).Round(time.Second)
		if qi.Step < time.Second {
			qi.Step = time.Second
		}
	case ScenarioQM3Quantile:
		qi.Metric, qi.By = MetricHistName, "service"
		qi.Quantile = []float64{0.95, 0.99}[rng.IntN(2)]
		evalAt()
	case ScenarioQM4GroupBy:
		qi.Metric, qi.By = MetricCounterName, "route"
		evalAt()
	}
	return qi
}

// paramsKey identifies the logical query across systems (no absolute times).
func (qi MetricsQueryInstance) paramsKey() string {
	return fmt.Sprintf("metric=%s by=%s win=%s q=%g evalfrac=%d startfrac=%d",
		qi.Metric, qi.By, qi.Window, qi.Quantile, qi.evalFrac, qi.startFrac)
}

// MetricsQueryRunOptions drives one metrics query phase against one target.
type MetricsQueryRunOptions struct {
	Scenarios  []string
	Iterations int
	Seed       uint64
	// From/To is the ingest window (from the ingest summary).
	From time.Time
	To   time.Time
	// QPS paces the run (0 = back-to-back).
	QPS int
}

// metricsQueryExecutor turns an instance into a request and extracts the
// result-group count.
type metricsQueryExecutor interface {
	Execute(ctx context.Context, client *http.Client, target TargetConfig, qi MetricsQueryInstance) (count int, err error)
}

func metricsExecutorFor(t TargetConfig) (metricsQueryExecutor, error) {
	switch t.Kind {
	case "amber":
		return amberMetricsQuerier{}, nil
	case "prometheus", "victoriametrics", "mimir":
		return promQuerier{}, nil
	default:
		return nil, fmt.Errorf("metrics query: no executor for kind %q", t.Kind)
	}
}

// RunMetricsQueries executes the scenario set and returns per-instance
// outcomes in the shared ResultFile shape, so compare reuses the same
// equality gate as logs.
func RunMetricsQueries(ctx context.Context, target TargetConfig, opts MetricsQueryRunOptions) ([]QueryOutcome, error) {
	exec, err := metricsExecutorFor(target)
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
			qi := metricsInstance(scenario, iter, opts)
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

type amberMetricsQuerier struct{}

func (amberMetricsQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi MetricsQueryInstance) (int, error) {
	switch qi.Scenario {
	case ScenarioQM2Range:
		params := url.Values{}
		params.Set("metric", qi.Metric)
		params.Set("window", qi.Window.String())
		params.Set("step", qi.Step.String())
		params.Set("by", qi.By)
		params.Set("from", strconv.FormatInt(qi.From.UnixMilli(), 10))
		params.Set("to", strconv.FormatInt(qi.To.UnixMilli(), 10))
		body, err := getBody(ctx, client, target, "/api/v1/metrics/rate_range?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Steps []struct {
				Rates map[string]float64 `json:"rates"`
			} `json:"steps"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber rate_range: decode: %w", err)
		}
		groups := make(map[string]struct{})
		for _, s := range resp.Steps {
			for k := range s.Rates {
				groups[k] = struct{}{}
			}
		}
		return len(groups), nil

	case ScenarioQM3Quantile:
		params := url.Values{}
		params.Set("metric", qi.Metric)
		params.Set("q", strconv.FormatFloat(qi.Quantile, 'g', -1, 64))
		params.Set("window", qi.Window.String())
		params.Set("end", strconv.FormatInt(qi.Eval.UnixMilli(), 10))
		params.Set("by", qi.By)
		body, err := getBody(ctx, client, target, "/api/v1/metrics/quantile?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Quantiles map[string]float64 `json:"quantiles"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber quantile: decode: %w", err)
		}
		return len(resp.Quantiles), nil

	default: // QM1, QM4: instant rate by label
		params := url.Values{}
		params.Set("metric", qi.Metric)
		params.Set("window", qi.Window.String())
		params.Set("end", strconv.FormatInt(qi.Eval.UnixMilli(), 10))
		params.Set("by", qi.By)
		body, err := getBody(ctx, client, target, "/api/v1/metrics/rate?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp struct {
			Rates map[string]float64 `json:"rates"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber rate: decode: %w", err)
		}
		return len(resp.Rates), nil
	}
}

// --- PromQL (Prometheus, VictoriaMetrics, Mimir) ---

type promQuerier struct{}

func (promQuerier) Execute(ctx context.Context, client *http.Client, target TargetConfig, qi MetricsQueryInstance) (int, error) {
	winSec := int64(qi.Window.Seconds())
	switch qi.Scenario {
	case ScenarioQM2Range:
		q := fmt.Sprintf(`sum by (%s) (rate(%s[%ds]))`, qi.By, qi.Metric, winSec)
		params := url.Values{}
		params.Set("query", q)
		params.Set("start", strconv.FormatInt(qi.From.Unix(), 10))
		params.Set("end", strconv.FormatInt(qi.To.Unix(), 10))
		params.Set("step", strconv.FormatInt(int64(qi.Step.Seconds()), 10))
		body, err := getBody(ctx, client, target, target.PromPrefix+"/api/v1/query_range?"+params.Encode())
		if err != nil {
			return 0, err
		}
		return promSeriesCount(body)

	default: // QM1, QM3, QM4: instant queries
		var q string
		if qi.Scenario == ScenarioQM3Quantile {
			// Native-histogram form; systems without native-histogram
			// support return empty and fail the equality gate — that is a
			// finding, not a harness error.
			q = fmt.Sprintf(`histogram_quantile(%g, sum by (%s) (rate(%s[%ds])))`,
				qi.Quantile, qi.By, qi.Metric, winSec)
		} else {
			q = fmt.Sprintf(`sum by (%s) (rate(%s[%ds]))`, qi.By, qi.Metric, winSec)
		}
		params := url.Values{}
		params.Set("query", q)
		params.Set("time", strconv.FormatInt(qi.Eval.Unix(), 10))
		body, err := getBody(ctx, client, target, target.PromPrefix+"/api/v1/query?"+params.Encode())
		if err != nil {
			return 0, err
		}
		return promSeriesCount(body)
	}
}

// promSeriesCount returns the number of series/groups in a PromQL response,
// instant vector and range matrix alike.
func promSeriesCount(body []byte) (int, error) {
	var resp struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("promql: decode: %w", err)
	}
	if resp.Status != "success" {
		return 0, fmt.Errorf("promql: status %q: %s", resp.Status, resp.Error)
	}
	return len(resp.Data.Result), nil
}
