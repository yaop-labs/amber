package obsbench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// CountQueryableMetricSamples returns the durable/queryable *scalar* sample
// count — the metrics side of the loss-accounting gate (METHODOLOGY.md §4):
// sent == acked == queryable before any number is recorded.
//
// Scalar samples only: every system stores one stored sample per scalar OTLP
// datapoint, so the count is exact and uniform. Exponential-histogram points
// have no system-independent stored-sample identity (amber stores per-tick
// sketches, Prometheus/Mimir one native-histogram sample, VictoriaMetrics a
// bucket expansion), so histograms are verified per system and documented,
// not gated here.
func CountQueryableMetricSamples(ctx context.Context, target TargetConfig, from, to time.Time) (uint64, error) {
	client := &http.Client{Timeout: 120 * time.Second}
	switch target.Kind {
	case "amber":
		return amberMetricSampleCount(ctx, client, target)
	case "prometheus", "victoriametrics", "mimir":
		return promMetricSampleCount(ctx, client, target, from, to)
	default:
		return 0, fmt.Errorf("metrics verify: no counter for kind %q", target.Kind)
	}
}

// amberMetricSampleCount reads the scalar store's sealed + buffered sample
// counters from /api/v1/metrics/stats — exact and O(1).
func amberMetricSampleCount(ctx context.Context, client *http.Client, target TargetConfig) (uint64, error) {
	body, err := getBody(ctx, client, target, "/api/v1/metrics/stats")
	if err != nil {
		return 0, err
	}
	var resp struct {
		Samples         int64 `json:"samples"`
		BufferedSamples int64 `json:"buffered_samples"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("amber metrics stats: %w", err)
	}
	return uint64(resp.Samples) + uint64(resp.BufferedSamples), nil
}

// promMetricSampleCount counts stored scalar samples with one instant
// count_over_time query whose window covers the whole ingest run (plus slack
// for clock skew between loadgen and server).
func promMetricSampleCount(ctx context.Context, client *http.Client, target TargetConfig, from, to time.Time) (uint64, error) {
	window := to.Sub(from) + 5*time.Minute
	q := fmt.Sprintf(`sum(count_over_time({__name__=~"%s|%s"}[%ds]))`,
		MetricCounterName, MetricGaugeName, int64(window.Seconds()))
	params := url.Values{}
	params.Set("query", q)
	params.Set("time", strconv.FormatInt(to.Unix()+60, 10))

	body, err := getBody(ctx, client, target, target.PromPrefix+"/api/v1/query?"+params.Encode())
	if err != nil {
		return 0, err
	}
	v, n, err := promVectorSum(body)
	if err != nil {
		return 0, fmt.Errorf("%s count: %w", target.Kind, err)
	}
	if n == 0 {
		return 0, nil
	}
	return uint64(v), nil
}

// promVectorSum parses a PromQL instant-vector response and returns the sum
// of its sample values plus the entry count.
func promVectorSum(body []byte) (float64, int, error) {
	var resp struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, 0, err
	}
	if resp.Status != "success" {
		return 0, 0, fmt.Errorf("status %q", resp.Status)
	}
	var sum float64
	for _, r := range resp.Data.Result {
		s, ok := r.Value[1].(string)
		if !ok {
			return 0, 0, fmt.Errorf("unexpected value shape")
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, 0, err
		}
		sum += f
	}
	return sum, len(resp.Data.Result), nil
}
