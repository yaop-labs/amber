package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type report struct {
	cfg config
	mix *mix
	run runResult
	tel []telemetrySample
}

func buildReport(cfg config, m *mix, r runResult, tel []telemetrySample) *report {
	return &report{cfg: cfg, mix: m, run: r, tel: tel}
}

func (r *report) Render() string {
	var b strings.Builder
	r.renderHeader(&b)
	r.renderThroughput(&b)
	r.renderLatency(&b)
	r.renderTelemetry(&b)
	return b.String()
}

func (r *report) renderHeader(b *strings.Builder) {
	fmt.Fprintln(b, "=== loadharness report ===")
	fmt.Fprintf(b, "target:           %s\n", r.cfg.Addr)
	fmt.Fprintf(b, "duration:         %s (warmup %s discarded)\n", r.cfg.Duration, r.cfg.Warmup)
	fmt.Fprintf(b, "target rate:      %.0f samples/sec (open-loop)\n", r.cfg.RatePerSec)
	fmt.Fprintf(b, "series:           %d (stable %.0f%%, churn %.0f%%/%s)\n",
		r.cfg.Series, r.cfg.StableFraction*100, r.cfg.ChurnRate*100, r.cfg.ChurnInterval)
	fmt.Fprintf(b, "mix:              %s\n", r.mix.String())
	fmt.Fprintf(b, "concurrency:      %d\n", r.cfg.Concurrency)
	fmt.Fprintf(b, "batch:            %d samples/req\n", r.cfg.Batch)
	fmt.Fprintf(b, "telemetry:        %d scrapes (%s)\n", len(r.tel), r.cfg.TelemetryInterval)
	fmt.Fprintln(b)
}

func (r *report) renderThroughput(b *strings.Builder) {
	wall := r.run.endWall.Sub(r.run.startWall).Seconds()
	if wall <= 0 {
		wall = 1
	}
	intendedTotal := r.run.sent + r.run.queueDrops // every tick the scheduler attempted
	achievedSamplesPerSec := float64(r.run.completed) / wall
	achievedReqPerSec := achievedSamplesPerSec / float64(r.cfg.Batch)

	fmt.Fprintln(b, "--- throughput ---")
	fmt.Fprintf(b, "wall time:           %.2fs\n", wall)
	fmt.Fprintf(b, "target req rate:     %.0f req/sec (%.0f samples/sec at batch=%d)\n",
		r.cfg.RatePerSec, r.cfg.RatePerSec*float64(r.cfg.Batch), r.cfg.Batch)
	fmt.Fprintf(b, "scheduler attempts:  %d samples (%.0f samples/sec — what an OTLP collector would push)\n",
		intendedTotal, float64(intendedTotal)/wall)
	fmt.Fprintf(b, "enqueued:            %d samples (%.0f/sec) — admitted to worker queue\n",
		r.run.sent, float64(r.run.sent)/wall)
	fmt.Fprintf(b, "achieved (200 OK):   %d samples (%.0f samples/sec, %.1f req/sec)\n",
		r.run.completed, achievedSamplesPerSec, achievedReqPerSec)
	if r.run.errors > 0 {
		fmt.Fprintf(b, "errors:              %d samples\n", r.run.errors)
	}
	if r.run.queueDrops > 0 {
		dropPct := 100 * float64(r.run.queueDrops) / float64(intendedTotal)
		fmt.Fprintf(b, "queue drops:         %d samples (%.1f%% of attempts) — binary past knee\n",
			r.run.queueDrops, dropPct)
	}
	if r.run.rotations > 0 {
		fmt.Fprintf(b, "churn rotations:     %d\n", r.run.rotations)
	}
	fmt.Fprintln(b)
}

func (r *report) renderLatency(b *strings.Builder) {
	fmt.Fprintln(b, "--- latency (intended-time anchored; CO-honest) ---")
	if r.run.histSteady.Count() == 0 {
		fmt.Fprintln(b, "  (no steady-state samples — warmup covered the entire run?)")
		return
	}
	all := r.run.histAll
	st := r.run.histSteady
	fmt.Fprintf(b, "                       all run            steady state (post-warmup)\n")
	fmt.Fprintf(b, "  count:               %-18d %d\n", all.Count(), st.Count())
	fmt.Fprintf(b, "  mean:                %-18s %s\n", fmtDur(time.Duration(all.Mean())), fmtDur(time.Duration(st.Mean())))
	fmt.Fprintf(b, "  p50:                 %-18s %s\n", fmtDur(time.Duration(all.Quantile(0.5))), fmtDur(time.Duration(st.Quantile(0.5))))
	fmt.Fprintf(b, "  p90:                 %-18s %s\n", fmtDur(time.Duration(all.Quantile(0.9))), fmtDur(time.Duration(st.Quantile(0.9))))
	fmt.Fprintf(b, "  p99:                 %-18s %s\n", fmtDur(time.Duration(all.Quantile(0.99))), fmtDur(time.Duration(st.Quantile(0.99))))
	fmt.Fprintf(b, "  p999:                %-18s %s\n", fmtDur(time.Duration(all.Quantile(0.999))), fmtDur(time.Duration(st.Quantile(0.999))))
	fmt.Fprintf(b, "  max:                 %-18s %s\n", fmtDur(time.Duration(all.Max())), fmtDur(time.Duration(st.Max())))
	fmt.Fprintln(b)
}

func (r *report) renderTelemetry(b *strings.Builder) {
	fmt.Fprintln(b, "--- binary telemetry ---")
	if len(r.tel) == 0 {
		fmt.Fprintln(b, "  (no scrapes — /metrics unreachable?)")
		fmt.Fprintln(b)
		return
	}
	first := r.tel[0]
	last := r.tel[len(r.tel)-1]

	gcRuns := last.GCRuns - first.GCRuns
	gcPause := (last.GCPauseTotalSec - first.GCPauseTotalSec)
	wall := last.At.Sub(first.At).Seconds()
	avgPauseMs := 0.0
	if gcRuns > 0 {
		avgPauseMs = (gcPause / gcRuns) * 1000
	}
	gcRate := 0.0
	if wall > 0 {
		gcRate = gcRuns / wall
	}

	fmt.Fprintf(b, "  scrapes:           %d over %.1fs\n", len(r.tel), wall)
	fmt.Fprintf(b, "  goroutines:        first=%.0f  last=%.0f  peak=%.0f\n",
		first.Goroutines, last.Goroutines, peak(r.tel, func(s telemetrySample) float64 { return s.Goroutines }))
	fmt.Fprintf(b, "  heap_alloc_MiB:    first=%.1f  last=%.1f  peak=%.1f\n",
		first.HeapAllocBytes/(1<<20), last.HeapAllocBytes/(1<<20),
		peak(r.tel, func(s telemetrySample) float64 { return s.HeapAllocBytes })/(1<<20))
	fmt.Fprintf(b, "  heap_inuse_MiB:    first=%.1f  last=%.1f  peak=%.1f\n",
		first.HeapInuseBytes/(1<<20), last.HeapInuseBytes/(1<<20),
		peak(r.tel, func(s telemetrySample) float64 { return s.HeapInuseBytes })/(1<<20))
	fmt.Fprintf(b, "  gc_runs:           %.0f total over %.1fs (%.2f/s, avg pause %.3f ms)\n",
		gcRuns, wall, gcRate, avgPauseMs)
	fmt.Fprintf(b, "  active_series:     first=%.0f  last=%.0f  peak=%.0f\n",
		first.ActiveSeries, last.ActiveSeries,
		peak(r.tel, func(s telemetrySample) float64 { return s.ActiveSeries }))
	fmt.Fprintf(b, "  head_samples:      last=%.0f  peak=%.0f\n",
		last.HeadSamples,
		peak(r.tel, func(s telemetrySample) float64 { return s.HeadSamples }))
	fmt.Fprintf(b, "  blocks_sealed:     last=%.0f\n", last.Blocks)
	fmt.Fprintf(b, "  binary accepted:   %.0f samples (delta over run)\n",
		last.IngestAccepted-first.IngestAccepted)
	if last.IngestRejected-first.IngestRejected > 0 {
		fmt.Fprintf(b, "  binary rejected:   %.0f samples\n", last.IngestRejected-first.IngestRejected)
	}
	if last.IngestUnsuppd-first.IngestUnsuppd > 0 {
		fmt.Fprintf(b, "  binary unsuppd:    %.0f samples\n", last.IngestUnsuppd-first.IngestUnsuppd)
	}
	fmt.Fprintln(b)
}

func peak(samples []telemetrySample, get func(telemetrySample) float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	p := get(samples[0])
	for _, s := range samples[1:] {
		if v := get(s); v > p {
			p = v
		}
	}
	return p
}

// Sort helper for stable iteration in tests/future-debug.
var _ = sort.Strings

func fmtDur(d time.Duration) string {
	switch {
	case d < time.Microsecond:
		return fmt.Sprintf("%dns", d)
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
}
