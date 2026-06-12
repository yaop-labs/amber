package obsbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// IngestOptions drives one ingest run (W1 burst when Rate==0, W2 sustained
// otherwise — METHODOLOGY.md §5).
type IngestOptions struct {
	Workers   int
	BatchSize int
	// Rate caps total records/second across workers. 0 = unlimited (burst).
	Rate int
	// Limit stops after N records (0 = whole dataset).
	Limit uint64
}

// IngestResult is the run summary. AckedRecords counts records the server
// actually accepted — from the response body when the system reports
// per-record accept/reject counts (amber), otherwise whole 2xx batches.
// The gap to SentRecords plus the verify step's queryable count is the loss
// accounting (METHODOLOGY.md §4).
type IngestResult struct {
	Target string `json:"target"`
	// StartedAt/FinishedAt bound the ingest window; the query phase derives
	// its time ranges from them (records carry send-time timestamps).
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	SentRecords   uint64        `json:"sent_records"`
	AckedRecords  uint64        `json:"acked_records"`
	FailedBatches uint64        `json:"failed_batches"`
	Duration      time.Duration `json:"duration_ns"`
	RecPerSec     float64       `json:"rec_per_sec"`
	LatencyP50    time.Duration `json:"latency_p50_ns"`
	LatencyP95    time.Duration `json:"latency_p95_ns"`
	LatencyP99    time.Duration `json:"latency_p99_ns"`
	// Errors maps "status 500" / transport error strings to counts; errors
	// are first-class results, never footnotes (METHODOLOGY.md §6).
	Errors map[string]uint64 `json:"errors,omitempty"`
}

// Ingest streams the dataset into the target. Batch latencies are recorded
// client-side, end to end.
func Ingest(ctx context.Context, targetName string, target TargetConfig, datasetPath string, opts IngestOptions) (IngestResult, error) {
	builder, err := builderFor(target)
	if err != nil {
		return IngestResult{}, err
	}
	reader, err := OpenDataset(datasetPath)
	if err != nil {
		return IngestResult{}, err
	}
	defer reader.Close()

	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}

	client := &http.Client{Timeout: 60 * time.Second}
	batches := make(chan []Record, opts.Workers*2)

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
			for batch := range batches {
				path, contentType, body, err := builder.Build(batch, time.Now())
				if err != nil {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, "build: "+err.Error())
					continue
				}
				start := time.Now()
				status, respBody, err := post(ctx, client, target, path, contentType, body)
				elapsed := time.Since(start)
				if err != nil {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, "transport: "+err.Error())
					continue
				}
				accepted, ok := countAccepted(target.Kind, status, respBody, len(batch))
				if !ok {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, fmt.Sprintf("status %d", status))
					continue
				}
				acked.Add(uint64(accepted))
				if accepted < len(batch) {
					recordErr(&mu, errCounts, "partial reject")
				}
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}()
	}

	// Token bucket over batches: Rate is records/s, so one batch costs
	// BatchSize tokens.
	var limiter *time.Ticker
	if opts.Rate > 0 {
		interval := time.Duration(float64(opts.BatchSize) / float64(opts.Rate) * float64(time.Second))
		if interval <= 0 {
			interval = time.Nanosecond
		}
		limiter = time.NewTicker(interval)
		defer limiter.Stop()
	}

	start := time.Now()
	var sent uint64
	batch := make([]Record, 0, opts.BatchSize)
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		if limiter != nil {
			select {
			case <-limiter.C:
			case <-ctx.Done():
				return false
			}
		}
		cp := make([]Record, len(batch))
		copy(cp, batch)
		select {
		case batches <- cp:
		case <-ctx.Done():
			return false
		}
		sent += uint64(len(batch))
		batch = batch[:0]
		return true
	}

readLoop:
	for {
		if opts.Limit > 0 && sent+uint64(len(batch)) >= opts.Limit {
			break
		}
		rec, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			close(batches)
			wg.Wait()
			return IngestResult{}, err
		}
		batch = append(batch, rec)
		if len(batch) >= opts.BatchSize {
			if !flush() {
				break readLoop
			}
		}
	}
	flush()
	close(batches)
	wg.Wait()
	duration := time.Since(start)

	res := IngestResult{
		Target:        targetName,
		StartedAt:     start,
		FinishedAt:    start.Add(duration),
		SentRecords:   sent,
		AckedRecords:  acked.Load(),
		FailedBatches: failedBatches.Load(),
		Duration:      duration,
		Errors:        errCounts,
	}
	if duration > 0 {
		res.RecPerSec = float64(res.AckedRecords) / duration.Seconds()
	}
	res.LatencyP50, res.LatencyP95, res.LatencyP99 = Percentiles(latencies)
	return res, nil
}

func post(ctx context.Context, client *http.Client, target TargetConfig, path, contentType string, body []byte) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", contentType)
	if target.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+target.APIKey)
	}
	if target.OrgID != "" {
		req.Header.Set("X-Scope-OrgID", target.OrgID)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	_ = resp.Body.Close()
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// countAccepted extracts the per-batch accepted-record count. Amber answers
// partial queue-full rejection with a 503 that still carries
// {"accepted":N,"rejected":M} — trusting the status code alone undercounts
// acked records on backpressure (and a 202 with rejections would overcount).
// Other systems ack all-or-nothing by status code.
func countAccepted(kind string, status int, body []byte, batchLen int) (int, bool) {
	if kind == "amber" {
		var resp struct {
			Accepted *int `json:"accepted"`
		}
		if json.Unmarshal(body, &resp) == nil && resp.Accepted != nil {
			return *resp.Accepted, true
		}
	}
	if status >= 200 && status <= 299 {
		return batchLen, true
	}
	return 0, false
}

func recordErr(mu *sync.Mutex, counts map[string]uint64, key string) {
	mu.Lock()
	counts[key]++
	mu.Unlock()
}

// Percentiles reports p50/p95/p99 over client-measured latencies.
func Percentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	at := func(q float64) time.Duration {
		idx := int(q * float64(len(latencies)-1))
		return latencies[idx]
	}
	return at(0.50), at(0.95), at(0.99)
}
