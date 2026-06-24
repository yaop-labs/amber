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

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Metrics ingest runner: tick-driven OTLP push (METHODOLOGY.md, I1-I3).
// Every system under test accepts the same OTLP protobuf payload - the
// same-client fairness baseline; only the mount path differs per system.
//
// One tick emits one sample per active series. Payload building is
// sequential (cumulative generator state mutates between ticks); only the
// HTTP sends are parallel. If a tick cannot start on schedule because the
// previous tick's sends are still queued, it is counted as an overrun -
// the metrics twin of the log harness's rate cap being honest about
// backpressure instead of silently stretching the interval.

// otlpMetricsPath returns the system's OTLP metrics mount.
func otlpMetricsPath(t TargetConfig) string {
	if t.OTLPMetricsPath != "" {
		return t.OTLPMetricsPath
	}
	switch t.Kind {
	case "mimir":
		return "/otlp/v1/metrics"
	case "victoriametrics":
		return "/opentelemetry/v1/metrics"
	case "prometheus":
		return "/api/v1/otlp/v1/metrics"
	default: // amber
		return "/v1/metrics"
	}
}

// MetricsIngestOptions drives one metrics ingest run.
type MetricsIngestOptions struct {
	Gen MetricsGenConfig
	// Interval between ticks (default 10s - METHODOLOGY I1).
	Interval time.Duration
	// Ticks is the run length in ticks (required, > 0).
	Ticks int
	// ChurnEvery is the I2 epoch length (default 5m); epochs only take
	// effect when Gen.ChurnPercent > 0.
	ChurnEvery time.Duration
	Workers    int
	// BatchSeries is the number of series per OTLP request (default 2000).
	BatchSeries int
}

// MetricsIngestResult is the run summary; the JSON file is the publishable
// artifact and supplies the query phase's time window plus the verify step's
// expected scalar-sample count.
type MetricsIngestResult struct {
	Target       string    `json:"target"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	Ticks        int       `json:"ticks"`
	IntervalMS   int64     `json:"interval_ms"`
	ScalarSeries int       `json:"scalar_series"`
	HistSeries   int       `json:"hist_series"`
	// SentScalar/SentHist count datapoints handed to senders; Acked counts
	// what the server accepted (amber reports per-point accept/reject in the
	// response body, the rest ack whole batches by status code).
	SentScalar    uint64            `json:"sent_scalar_samples"`
	SentHist      uint64            `json:"sent_hist_samples"`
	AckedSamples  uint64            `json:"acked_samples"`
	FailedBatches uint64            `json:"failed_batches"`
	OverrunTicks  int               `json:"overrun_ticks"`
	Duration      time.Duration     `json:"duration_ns"`
	SamplesPerSec float64           `json:"samples_per_sec"`
	LatencyP50    time.Duration     `json:"latency_p50_ns"`
	LatencyP95    time.Duration     `json:"latency_p95_ns"`
	LatencyP99    time.Duration     `json:"latency_p99_ns"`
	Errors        map[string]uint64 `json:"errors,omitempty"`
}

type metricsBatch struct {
	body   []byte
	points int
}

// IngestMetrics runs the tick loop against the target.
func IngestMetrics(ctx context.Context, targetName string, target TargetConfig, opts MetricsIngestOptions, logf func(string, ...any)) (MetricsIngestResult, error) {
	if opts.Ticks <= 0 {
		return MetricsIngestResult{}, fmt.Errorf("metrics ingest: Ticks must be > 0")
	}
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Second
	}
	if opts.ChurnEvery <= 0 {
		opts.ChurnEvery = 5 * time.Minute
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.BatchSeries <= 0 {
		opts.BatchSeries = 2000
	}
	gen := NewMetricsGenerator(opts.Gen)
	path := otlpMetricsPath(target)
	client := &http.Client{Timeout: 60 * time.Second}

	batches := make(chan metricsBatch, opts.Workers*2)
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
				status, respBody, err := post(ctx, client, target, path, "application/x-protobuf", b.body)
				elapsed := time.Since(start)
				if err != nil {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, "transport: "+err.Error())
					continue
				}
				accepted, ok := countAcceptedMetrics(target.Kind, status, respBody, b.points)
				if !ok {
					failedBatches.Add(1)
					recordErr(&mu, errCounts, fmt.Sprintf("status %d", status))
					continue
				}
				acked.Add(uint64(accepted))
				if accepted < b.points {
					recordErr(&mu, errCounts, "partial reject")
				}
				mu.Lock()
				latencies = append(latencies, elapsed)
				mu.Unlock()
			}
		}()
	}

	res := MetricsIngestResult{
		Target:       targetName,
		Ticks:        opts.Ticks,
		IntervalMS:   opts.Interval.Milliseconds(),
		ScalarSeries: gen.ScalarCount(),
		HistSeries:   gen.HistCount(),
	}
	start := time.Now()
	res.StartedAt = start
	startNano := uint64(start.UnixNano())

tickLoop:
	for tick := 0; tick < opts.Ticks; tick++ {
		due := start.Add(time.Duration(tick) * opts.Interval)
		now := time.Now()
		if d := now.Sub(due); d > opts.Interval/2 {
			res.OverrunTicks++
			if logf != nil {
				logf("tick %d overrun by %s", tick, d.Round(time.Millisecond))
			}
		} else if now.Before(due) {
			select {
			case <-time.After(due.Sub(now)):
			case <-ctx.Done():
				break tickLoop
			}
		}

		gen.SetEpoch(int(time.Since(start) / opts.ChurnEvery))
		gen.BeginTick(tick)
		tsNano := uint64(time.Now().UnixNano())

		for lo := 0; lo < gen.ScalarCount(); lo += opts.BatchSeries {
			hi := min(lo+opts.BatchSeries, gen.ScalarCount())
			body, err := buildScalarOTLP(gen, lo, hi, tick, startNano, tsNano)
			if err != nil {
				return res, fmt.Errorf("metrics ingest: build scalar: %w", err)
			}
			select {
			case batches <- metricsBatch{body: body, points: hi - lo}:
				res.SentScalar += uint64(hi - lo)
			case <-ctx.Done():
				break tickLoop
			}
		}
		for lo := 0; lo < gen.HistCount(); lo += opts.BatchSeries {
			hi := min(lo+opts.BatchSeries, gen.HistCount())
			body, err := buildHistOTLP(gen, lo, hi, startNano, tsNano)
			if err != nil {
				return res, fmt.Errorf("metrics ingest: build hist: %w", err)
			}
			select {
			case batches <- metricsBatch{body: body, points: hi - lo}:
				res.SentHist += uint64(hi - lo)
			case <-ctx.Done():
				break tickLoop
			}
		}
		if logf != nil && (tick+1)%30 == 0 {
			logf("tick %d/%d acked=%d failed=%d", tick+1, opts.Ticks, acked.Load(), failedBatches.Load())
		}
	}
	close(batches)
	wg.Wait()

	res.Duration = time.Since(start)
	res.FinishedAt = start.Add(res.Duration)
	res.AckedSamples = acked.Load()
	res.FailedBatches = failedBatches.Load()
	res.Errors = errCounts
	if res.Duration > 0 {
		res.SamplesPerSec = float64(res.AckedSamples) / res.Duration.Seconds()
	}
	res.LatencyP50, res.LatencyP95, res.LatencyP99 = Percentiles(latencies)
	if err := ctx.Err(); err != nil {
		return res, err
	}
	return res, nil
}

// countAcceptedMetrics mirrors countAccepted for the OTLP metrics endpoint:
// amber answers {"accepted":N,"rejected":M}, everyone else acks the whole
// request by status code.
func countAcceptedMetrics(kind string, status int, body []byte, points int) (int, bool) {
	if kind == "amber" {
		var resp struct {
			Accepted *int `json:"accepted"`
		}
		if json.Unmarshal(body, &resp) == nil && resp.Accepted != nil {
			return *resp.Accepted, true
		}
	}
	if status >= 200 && status <= 299 {
		return points, true
	}
	return 0, false
}

// buildScalarOTLP renders scalar series [lo, hi) at the current tick state,
// counters as cumulative monotonic sums. Every label - service included -
// rides as a datapoint attribute: resource attributes like service.name go
// through system-specific renames (Prometheus and Mimir fold them into
// job/target_info), which would break the cross-system label identity the
// equality gate depends on.
func buildScalarOTLP(gen *MetricsGenerator, lo, hi, tick int, startNano, tsNano uint64) ([]byte, error) {
	var counters, gauges []*metricspb.NumberDataPoint
	for i := lo; i < hi; i++ {
		_, lbl, value, isCounter := gen.ScalarPoint(i, tick)
		dp := &metricspb.NumberDataPoint{
			StartTimeUnixNano: startNano,
			TimeUnixNano:      tsNano,
			Attributes:        labelAttrs(lbl),
			Value:             &metricspb.NumberDataPoint_AsInt{AsInt: value},
		}
		if isCounter {
			counters = append(counters, dp)
		} else {
			gauges = append(gauges, dp)
		}
	}

	var metrics []*metricspb.Metric
	if len(counters) > 0 {
		metrics = append(metrics, &metricspb.Metric{
			Name: MetricCounterName,
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
				IsMonotonic:            true,
				DataPoints:             counters,
			}},
		})
	}
	if len(gauges) > 0 {
		metrics = append(metrics, &metricspb.Metric{
			Name: MetricGaugeName,
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: gauges}},
		})
	}
	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource:     &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: metrics}},
		}},
	}
	return proto.Marshal(req)
}

// buildHistOTLP renders exponential-histogram series [lo, hi). Cumulative
// temporality: Prometheus-lineage systems drop delta histograms.
func buildHistOTLP(gen *MetricsGenerator, lo, hi int, startNano, tsNano uint64) ([]byte, error) {
	dps := make([]*metricspb.ExponentialHistogramDataPoint, 0, hi-lo)
	for i := lo; i < hi; i++ {
		lbl, h := gen.HistPoint(i)
		counts := make([]uint64, len(h.Counts))
		copy(counts, h.Counts)
		dps = append(dps, &metricspb.ExponentialHistogramDataPoint{
			StartTimeUnixNano: startNano,
			TimeUnixNano:      tsNano,
			Attributes:        labelAttrs(lbl),
			Scale:             h.Scale,
			Count:             h.Count,
			Sum:               proto.Float64(h.Sum),
			Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
				Offset:       h.Offset,
				BucketCounts: counts,
			},
		})
	}

	req := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{},
			ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{{
				Name: MetricHistName,
				Data: &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					DataPoints:             dps,
				}},
			}}}},
		}},
	}
	return proto.Marshal(req)
}

func labelAttrs(l SeriesLabels) []*commonpb.KeyValue {
	attrs := []*commonpb.KeyValue{
		{Key: "service", Value: strVal(l.Service)},
		{Key: "host", Value: strVal(l.Host)},
		{Key: "route", Value: strVal(l.Route)},
		{Key: "shard", Value: strVal(l.Shard)},
	}
	if l.Gen >= 0 {
		attrs = append(attrs, &commonpb.KeyValue{Key: "gen", Value: strVal(fmt.Sprintf("g%d", l.Gen))})
	}
	return attrs
}
