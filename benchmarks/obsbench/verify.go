package obsbench

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// CountQueryable returns the system's durable/queryable log-record count.
// This is the right-hand side of the loss accounting: sent == acked ==
// queryable is the gate every ingest run must pass before its numbers count
// (METHODOLOGY.md §4–5), and the post-restart count is what the kill -9 test
// compares against acked.
func CountQueryable(ctx context.Context, target TargetConfig) (uint64, error) {
	client := &http.Client{Timeout: 120 * time.Second}
	switch target.Kind {
	case "amber":
		return amberCount(ctx, client, target)
	case "victorialogs":
		return victoriaLogsCount(ctx, client, target)
	case "loki":
		return lokiCount(ctx, client, target)
	default:
		return 0, fmt.Errorf("verify: no counter for kind %q", target.Kind)
	}
}

// amberCount reads segments.total_records from the admin stats endpoint —
// exact (sealed + active) and O(1), unlike a full-scan query whose
// total_hits is allowed to skip segments once the result heap fills.
func amberCount(ctx context.Context, client *http.Client, target TargetConfig) (uint64, error) {
	body, err := getBody(ctx, client, target, "/api/v1/admin/stats")
	if err != nil {
		return 0, err
	}
	var resp struct {
		Segments struct {
			TotalRecords uint64 `json:"total_records"`
		} `json:"segments"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("amber stats: %w", err)
	}
	return resp.Segments.TotalRecords, nil
}

func victoriaLogsCount(ctx context.Context, client *http.Client, target TargetConfig) (uint64, error) {
	params := url.Values{}
	params.Set("query", "* | stats count(*) as hits")
	body, err := getBody(ctx, client, target, "/select/logsql/query?"+params.Encode())
	if err != nil {
		return 0, err
	}
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		// VictoriaLogs renders stats values as JSON strings.
		var line struct {
			Hits string `json:"hits"`
		}
		if err := json.Unmarshal(sc.Bytes(), &line); err != nil {
			return 0, fmt.Errorf("victorialogs stats: %w", err)
		}
		return strconv.ParseUint(line.Hits, 10, 64)
	}
	return 0, fmt.Errorf("victorialogs stats: empty response")
}

func lokiCount(ctx context.Context, client *http.Client, target TargetConfig) (uint64, error) {
	// Instant query over a lookback wide enough to cover any campaign run.
	params := url.Values{}
	params.Set("query", `sum(count_over_time({service=~".+"}[30d]))`)
	params.Set("time", strconv.FormatInt(time.Now().UnixNano(), 10))
	body, err := getBody(ctx, client, target, "/loki/api/v1/query?"+params.Encode())
	if err != nil {
		return 0, err
	}
	var resp struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return 0, fmt.Errorf("loki count: %w", err)
	}
	if len(resp.Data.Result) == 0 {
		return 0, nil
	}
	s, ok := resp.Data.Result[0].Value[1].(string)
	if !ok {
		return 0, fmt.Errorf("loki count: unexpected value shape")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("loki count: %w", err)
	}
	return uint64(f), nil
}
