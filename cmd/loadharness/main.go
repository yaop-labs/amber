// Command loadharness drives a LIVE amber binary over its real OTLP/HTTP
// ingest endpoint, with open-loop arrival scheduling, configurable cardinality
// and churn, and parallel binary-side telemetry scraping. It exists to answer
// the questions the in-process micro-benchmarks cannot: sustained throughput,
// honest tail latency, GC under steady state, churn behaviour.
//
// See LOAD_HARNESS_BRIEF.md for the design and the experiments this harness
// is supposed to enable.
//
// This is a MEASUREMENT tool — it does not change amber, it does not "fix"
// perf. If a number is bad, that is the signal; the fix belongs to a
// downstream PR that uses these numbers as the regression baseline.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

var (
	flagAddr        = flag.String("addr", "http://localhost:8080", "amber HTTP base URL (no trailing slash)")
	flagRate        = flag.Float64("rate", 100, "target arrival rate (HTTP requests per second, open-loop). total samples/sec = rate × batch.")
	flagDuration    = flag.Duration("duration", 60*time.Second, "total run duration (including warmup)")
	flagWarmup      = flag.Duration("warmup", 10*time.Second, "warmup duration to discard from latency stats")
	flagSeries      = flag.Int("series", 1000, "number of distinct active series at any moment")
	flagStableFrac  = flag.Float64("stable-frac", 0.5, "fraction of series that are stable (the rest churn)")
	flagChurnRate   = flag.Float64("churn-rate", 0, "fraction of churning series rotated per --churn-interval (0 = no churn)")
	flagChurnEvery  = flag.Duration("churn-interval", 10*time.Second, "how often to rotate the ephemeral pool")
	flagBatch       = flag.Int("batch", 100, "samples per OTLP request (one request = one POST)")
	flagConcurrency = flag.Int("concurrency", runtime.NumCPU(), "max in-flight HTTP requests")
	flagTelemetry   = flag.Duration("telemetry-interval", 1*time.Second, "/metrics scrape period")
	flagOut         = flag.String("o", "", "report output path (default: stdout)")
	flagMix         = flag.String("mix", "counter:0.7,gauge:0.2,exphist:0.1", "metric-type mix; weights normalised")
	flagSeed        = flag.Uint64("seed", 42, "PRNG seed")
)

func main() {
	os.Exit(run())
}

func run() int {
	flag.Parse()
	if *flagRate <= 0 || *flagDuration <= 0 || *flagSeries <= 0 || *flagBatch <= 0 {
		fmt.Fprintln(os.Stderr, "rate, duration, series, batch must all be > 0")
		return 2
	}
	if *flagWarmup >= *flagDuration {
		fmt.Fprintln(os.Stderr, "warmup must be shorter than duration")
		return 2
	}

	cfg := config{
		Addr:              *flagAddr,
		RatePerSec:        *flagRate,
		Duration:          *flagDuration,
		Warmup:            *flagWarmup,
		Series:            *flagSeries,
		StableFraction:    *flagStableFrac,
		ChurnRate:         *flagChurnRate,
		ChurnInterval:     *flagChurnEvery,
		Batch:             *flagBatch,
		Concurrency:       *flagConcurrency,
		TelemetryInterval: *flagTelemetry,
		MixSpec:           *flagMix,
		Seed:              *flagSeed,
	}
	mix, err := parseMix(cfg.MixSpec)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid --mix: %v\n", err)
		return 2
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Sanity check: amber up?
	if err := healthCheck(cfg.Addr); err != nil {
		fmt.Fprintf(os.Stderr, "amber not reachable at %s: %v\nstart amber first (e.g. ./amber config.yaml) and try again.\n", cfg.Addr, err)
		return 1
	}

	fmt.Printf("→ loadharness\n")
	fmt.Printf("  target:        %s\n", cfg.Addr)
	fmt.Printf("  rate:          %.0f samples/sec (open-loop)\n", cfg.RatePerSec)
	fmt.Printf("  duration:      %s (warmup %s)\n", cfg.Duration, cfg.Warmup)
	fmt.Printf("  series:        %d (stable %.0f%%, churn %.0f%% of ephemeral per %s)\n",
		cfg.Series, cfg.StableFraction*100, cfg.ChurnRate*100, cfg.ChurnInterval)
	fmt.Printf("  mix:           %s\n", mix.String())
	fmt.Printf("  concurrency:   %d\n", cfg.Concurrency)
	fmt.Printf("  telemetry:     /metrics every %s\n", cfg.TelemetryInterval)
	fmt.Println()

	catalog := newCatalog(cfg.Series, cfg.StableFraction, cfg.Seed)
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.Concurrency * 2,
			MaxIdleConnsPerHost: cfg.Concurrency * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	tel := newTelemetryCollector(cfg.Addr, cfg.TelemetryInterval, httpClient)
	telCtx, telCancel := context.WithCancel(ctx)
	go tel.Run(telCtx)

	gen := newGenerator(cfg, mix, catalog, httpClient)
	result := gen.Run(ctx)

	telCancel()
	tel.Wait()
	telSamples := tel.Samples()

	report := buildReport(cfg, mix, result, telSamples)
	if *flagOut == "" {
		fmt.Println(report.Render())
	} else {
		if err := os.WriteFile(*flagOut, []byte(report.Render()), 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "write report: %v\n", err)
			return 1
		}
		fmt.Printf("report written to %s\n", *flagOut)
	}
	return 0
}

type config struct {
	Addr              string
	RatePerSec        float64
	Duration          time.Duration
	Warmup            time.Duration
	Series            int
	StableFraction    float64
	ChurnRate         float64
	ChurnInterval     time.Duration
	Batch             int
	Concurrency       int
	TelemetryInterval time.Duration
	MixSpec           string
	Seed              uint64
}

func healthCheck(addr string) error {
	req, err := http.NewRequest(http.MethodGet, addr+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
