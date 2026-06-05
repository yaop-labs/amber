package main

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// telemetryCollector polls amber's /metrics endpoint (Prometheus exposition)
// on a fixed cadence and stores per-scrape snapshots of the gauges/counters
// the harness reports on: GC, heap, goroutines, active series, ingest
// counters. We do NOT scrape histograms — they're cheap on the binary side
// but rendering quantiles from cumulative buckets needs delta math we don't
// need here.

type telemetrySample struct {
	At              time.Time
	HeapAllocBytes  float64
	HeapInuseBytes  float64
	HeapSysBytes    float64
	GCRuns          float64 // counter, cumulative
	GCPauseLastSec  float64
	GCPauseTotalSec float64 // counter, cumulative
	Goroutines      float64
	ActiveSeries    float64
	HeadSeries      float64
	HeadSamples     float64
	Blocks          float64
	IngestAccepted  float64 // counter sum across all kind labels
	IngestRejected  float64
	IngestUnsuppd   float64
	Mallocs         float64            // counter
	Frees           float64            // counter
	Raw             map[string]float64 // every numeric series the scrape returned
}

type telemetryCollector struct {
	addr     string
	interval time.Duration
	client   *http.Client

	mu      sync.Mutex
	samples []telemetrySample
	done    chan struct{}
}

func newTelemetryCollector(addr string, interval time.Duration, client *http.Client) *telemetryCollector {
	return &telemetryCollector{
		addr:     addr,
		interval: interval,
		client:   client,
		done:     make(chan struct{}),
	}
}

func (t *telemetryCollector) Run(ctx context.Context) {
	defer close(t.done)
	tick := time.NewTicker(t.interval)
	defer tick.Stop()
	// First scrape immediately, then on interval.
	t.scrapeOnce()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.scrapeOnce()
		}
	}
}

func (t *telemetryCollector) Wait() { <-t.done }

func (t *telemetryCollector) Samples() []telemetrySample {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]telemetrySample, len(t.samples))
	copy(out, t.samples)
	return out
}

func (t *telemetryCollector) scrapeOnce() {
	req, err := http.NewRequest(http.MethodGet, t.addr+"/metrics", nil)
	if err != nil {
		return
	}
	resp, err := t.client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return
	}

	raw := make(map[string]float64, 64)
	var sample telemetrySample
	sample.At = time.Now()
	sample.Raw = raw

	// Sum-by-name accumulators for the labelled counters we care about.
	ingestAccepted := 0.0
	ingestRejected := 0.0
	ingestUnsupp := 0.0

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		// Format: name{labels} value [timestamp]
		// We split off the value, then take the name up to '{' or ' '.
		valStart := strings.LastIndexByte(line, ' ')
		if valStart < 0 {
			continue
		}
		valStr := line[valStart+1:]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}
		head := line[:valStart]
		nameEnd := strings.IndexAny(head, "{ ")
		if nameEnd < 0 {
			nameEnd = len(head)
		}
		name := head[:nameEnd]
		raw[head] = val

		switch name {
		case "amber_go_heap_alloc_bytes":
			sample.HeapAllocBytes = val
		case "amber_go_heap_inuse_bytes":
			sample.HeapInuseBytes = val
		case "amber_go_heap_sys_bytes":
			sample.HeapSysBytes = val
		case "amber_go_gc_runs_total":
			sample.GCRuns = val
		case "amber_go_gc_pause_last_seconds":
			sample.GCPauseLastSec = val
		case "amber_go_gc_pause_total_seconds":
			sample.GCPauseTotalSec = val
		case "amber_go_goroutines":
			sample.Goroutines = val
		case "amber_go_mallocs_total":
			sample.Mallocs = val
		case "amber_go_frees_total":
			sample.Frees = val
		case "amber_metrics_active_series":
			sample.ActiveSeries = val
		case "amber_metrics_store_head_series":
			sample.HeadSeries = val
		case "amber_metrics_store_head_samples":
			sample.HeadSamples = val
		case "amber_metrics_store_blocks":
			sample.Blocks = val
		case "amber_metrics_ingest_accepted_total":
			ingestAccepted += val
		case "amber_metrics_ingest_rejected_total":
			ingestRejected += val
		case "amber_metrics_ingest_unsupported_total":
			ingestUnsupp += val
		}
	}
	if err := scanner.Err(); err != nil {
		// Partial scrape; we still record what we parsed before the error.
		// Quietly: a single broken scrape during a long run is not worth
		// alarming the operator, the gap will show in the timeline.
		_ = err
	}
	sample.IngestAccepted = ingestAccepted
	sample.IngestRejected = ingestRejected
	sample.IngestUnsuppd = ingestUnsupp

	t.mu.Lock()
	t.samples = append(t.samples, sample)
	t.mu.Unlock()
}
