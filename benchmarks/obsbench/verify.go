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
// (METHODOLOGY.md), and the post-restart count is what the kill -9 test
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

// amberCount walks the real paginated query API and verifies both Amber entry
// IDs and benchmark seq identities are unique. total_hits is intentionally not
// used: it is a lower bound once top-k pruning starts.
func amberCount(ctx context.Context, client *http.Client, target TargetConfig) (uint64, error) {
	const pageSize = 10_000
	type attr struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type entry struct {
		ID    json.RawMessage `json:"id"`
		Attrs []attr          `json:"attrs"`
	}
	type queryResponse struct {
		Entries    []entry `json:"entries"`
		NextCursor string  `json:"next_cursor"`
	}

	entryIDs := make(map[string]struct{})
	seqs := make(map[uint64]struct{})
	cursors := make(map[string]struct{})
	cursor := ""
	for {
		params := url.Values{}
		params.Set("limit", strconv.Itoa(pageSize))
		if cursor != "" {
			params.Set("cursor", cursor)
		}
		body, err := getBody(ctx, client, target, "/api/v1/logs?"+params.Encode())
		if err != nil {
			return 0, err
		}
		var resp queryResponse
		if err := json.Unmarshal(body, &resp); err != nil {
			return 0, fmt.Errorf("amber query page: %w", err)
		}
		for _, item := range resp.Entries {
			id := string(item.ID)
			if id == "" || id == "null" {
				return 0, fmt.Errorf("amber query: entry without ID")
			}
			if _, duplicate := entryIDs[id]; duplicate {
				return 0, fmt.Errorf("amber query: duplicate entry ID %s", id)
			}
			entryIDs[id] = struct{}{}

			var seqText string
			for _, a := range item.Attrs {
				if a.Key == "seq" {
					seqText = a.Value
					break
				}
			}
			if seqText == "" {
				return 0, fmt.Errorf("amber query: entry %s has no benchmark seq", id)
			}
			seq, err := strconv.ParseUint(seqText, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("amber query: entry %s bad seq %q: %w", id, seqText, err)
			}
			if _, duplicate := seqs[seq]; duplicate {
				return 0, fmt.Errorf("amber query: duplicate benchmark seq %d", seq)
			}
			seqs[seq] = struct{}{}
		}

		if resp.NextCursor == "" {
			break
		}
		if len(resp.Entries) == 0 {
			return 0, fmt.Errorf("amber query: empty page returned cursor")
		}
		if _, duplicate := cursors[resp.NextCursor]; duplicate {
			return 0, fmt.Errorf("amber query: cursor cycle")
		}
		cursors[resp.NextCursor] = struct{}{}
		cursor = resp.NextCursor
	}
	if len(entryIDs) != len(seqs) {
		return 0, fmt.Errorf("amber query: %d entry IDs but %d seq IDs", len(entryIDs), len(seqs))
	}
	return uint64(len(entryIDs)), nil
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
