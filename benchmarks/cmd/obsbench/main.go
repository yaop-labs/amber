// Command obsbench is the benchmark harness CLI behind
// benchmarks/METHODOLOGY.md. Subcommands cover the campaign phases:
//
//	preflight — environment gate (mem/disk/swap/governor)
//	datagen   — seeded zstd-compressed log dataset
//	sample    — 1 Hz external RSS/PSS sampler → CSV
//	ingest    — load generation against one target
//
// Each run's raw outputs (JSON summaries, sampler CSVs) are the publishable
// artifacts; tables are derived from them, never typed by hand.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/yaop-labs/amber/benchmarks/obsbench"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "preflight":
		err = cmdPreflight(os.Args[2:])
	case "datagen":
		err = cmdDatagen(os.Args[2:])
	case "sample":
		err = cmdSample(os.Args[2:])
	case "ingest":
		err = cmdIngest(os.Args[2:])
	case "query":
		err = cmdQuery(os.Args[2:])
	case "compare":
		err = cmdCompare(os.Args[2:])
	case "verify":
		err = cmdVerify(os.Args[2:])
	case "kill9":
		err = cmdKill9(os.Args[2:])
	case "metrics-ingest":
		err = cmdMetricsIngest(os.Args[2:])
	case "metrics-query":
		err = cmdMetricsQuery(os.Args[2:])
	case "metrics-verify":
		err = cmdMetricsVerify(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "obsbench:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: obsbench <preflight|datagen|sample|ingest|query|compare|verify|kill9|metrics-ingest|metrics-query|metrics-verify> [flags]`)
}

// loadTarget resolves -config/-target, the boilerplate every subcommand shares.
func loadTarget(configPath, targetName string) (obsbench.TargetConfig, error) {
	if targetName == "" {
		return obsbench.TargetConfig{}, fmt.Errorf("-target is required")
	}
	cfg, err := obsbench.LoadConfig(configPath)
	if err != nil {
		return obsbench.TargetConfig{}, err
	}
	target, ok := cfg.Targets[targetName]
	if !ok {
		return obsbench.TargetConfig{}, fmt.Errorf("target %q not in %s", targetName, configPath)
	}
	return target, nil
}

func cmdMetricsIngest(args []string) error {
	fs := flag.NewFlagSet("metrics-ingest", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	seed := fs.Uint64("seed", 1, "series/value seed (same seed across systems = same workload)")
	scalarSeries := fs.Int("scalar-series", 100_000, "active scalar series (half counters, half gauges)")
	histSeries := fs.Int("hist-series", 10_000, "exponential-histogram series")
	routes := fs.Int("routes", 1000, "route label cardinality (QM4 group-by target)")
	churnPercent := fs.Int("churn-percent", 0, "percent of series replaced each churn epoch (I2; 0 = off)")
	churnEvery := fs.Duration("churn-every", 5*time.Minute, "churn epoch length")
	interval := fs.Duration("interval", 10*time.Second, "tick interval (one sample per series per tick)")
	ticks := fs.Int("ticks", 0, "run length in ticks (required)")
	workers := fs.Int("workers", 4, "concurrent senders")
	batch := fs.Int("batch", 2000, "series per OTLP request")
	out := fs.String("out", "", "write JSON summary here in addition to stdout")
	_ = fs.Parse(args)

	target, err := loadTarget(*configPath, *targetName)
	if err != nil {
		return fmt.Errorf("metrics-ingest: %w", err)
	}
	if *ticks <= 0 {
		return fmt.Errorf("metrics-ingest: -ticks is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := obsbench.IngestMetrics(ctx, *targetName, target, obsbench.MetricsIngestOptions{
		Gen: obsbench.MetricsGenConfig{
			Seed:         *seed,
			ScalarSeries: *scalarSeries,
			HistSeries:   *histSeries,
			Routes:       *routes,
			ChurnPercent: *churnPercent,
		},
		Interval:    *interval,
		Ticks:       *ticks,
		ChurnEvery:  *churnEvery,
		Workers:     *workers,
		BatchSeries: *batch,
	}, func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, "metrics-ingest: "+format+"\n", args...)
	})
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}
	if *out != "" {
		data, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func cmdMetricsQuery(args []string) error {
	fs := flag.NewFlagSet("metrics-query", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	scenarios := fs.String("scenarios", "all", "comma-separated scenario list or 'all'")
	iterations := fs.Int("iterations", 30, "instances per scenario")
	seed := fs.Uint64("seed", 1, "instance-parameter seed (same seed across systems = same queries)")
	qps := fs.Int("qps", 0, "pace queries (0 = back-to-back)")
	ingestSummary := fs.String("ingest-summary", "", "metrics ingest summary JSON; supplies the time window")
	fromFlag := fs.String("from", "", "window start (RFC3339; overrides ingest summary)")
	toFlag := fs.String("to", "", "window end (RFC3339; overrides ingest summary)")
	out := fs.String("out", "", "results JSON path (required for compare)")
	_ = fs.Parse(args)

	target, err := loadTarget(*configPath, *targetName)
	if err != nil {
		return fmt.Errorf("metrics-query: %w", err)
	}

	opts := obsbench.MetricsQueryRunOptions{
		Iterations: *iterations,
		Seed:       *seed,
		QPS:        *qps,
	}
	if *scenarios == "all" {
		opts.Scenarios = obsbench.AllMetricsScenarios
	} else {
		opts.Scenarios = strings.Split(*scenarios, ",")
	}

	if *ingestSummary != "" {
		data, err := os.ReadFile(*ingestSummary)
		if err != nil {
			return err
		}
		var ir obsbench.MetricsIngestResult
		if err := json.Unmarshal(data, &ir); err != nil {
			return fmt.Errorf("metrics ingest summary: %w", err)
		}
		opts.From, opts.To = ir.StartedAt, ir.FinishedAt
	}
	if *fromFlag != "" {
		if opts.From, err = time.Parse(time.RFC3339, *fromFlag); err != nil {
			return fmt.Errorf("-from: %w", err)
		}
	}
	if *toFlag != "" {
		if opts.To, err = time.Parse(time.RFC3339, *toFlag); err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}
	if opts.From.IsZero() || opts.To.IsZero() {
		return fmt.Errorf("metrics-query: time window required (-ingest-summary or -from/-to)")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	outcomes, err := obsbench.RunMetricsQueries(ctx, target, opts)
	if err != nil {
		return err
	}

	rf := obsbench.ResultFile{Target: *targetName, Outcomes: outcomes}
	if *out != "" {
		if err := obsbench.WriteResultFile(*out, rf); err != nil {
			return err
		}
	}
	printQuerySummary(rf)
	return nil
}

func cmdMetricsVerify(args []string) error {
	fs := flag.NewFlagSet("metrics-verify", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	ingestSummary := fs.String("ingest-summary", "", "metrics ingest summary JSON (required): expected scalar count + window")
	_ = fs.Parse(args)

	target, err := loadTarget(*configPath, *targetName)
	if err != nil {
		return fmt.Errorf("metrics-verify: %w", err)
	}
	if *ingestSummary == "" {
		return fmt.Errorf("metrics-verify: -ingest-summary is required")
	}
	data, err := os.ReadFile(*ingestSummary)
	if err != nil {
		return err
	}
	var ir obsbench.MetricsIngestResult
	if err := json.Unmarshal(data, &ir); err != nil {
		return fmt.Errorf("metrics ingest summary: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	got, err := obsbench.CountQueryableMetricSamples(ctx, target, ir.StartedAt, ir.FinishedAt)
	if err != nil {
		return err
	}
	fmt.Printf("queryable_scalar=%d sent_scalar=%d (hist sent=%d, verified per system)\n",
		got, ir.SentScalar, ir.SentHist)
	if got != ir.SentScalar {
		return fmt.Errorf("metrics-verify: scalar count mismatch (silent loss or duplication) — numbers from this run are void")
	}
	return nil
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	ingestSummary := fs.String("ingest-summary", "", "ingest summary JSON; supplies the expected count")
	expected := fs.Uint64("expected", 0, "expected record count (overrides ingest summary)")
	_ = fs.Parse(args)

	if *targetName == "" {
		return fmt.Errorf("verify: -target is required")
	}
	cfg, err := obsbench.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	target, ok := cfg.Targets[*targetName]
	if !ok {
		return fmt.Errorf("verify: target %q not in %s", *targetName, *configPath)
	}

	want := *expected
	if *ingestSummary != "" && want == 0 {
		data, err := os.ReadFile(*ingestSummary)
		if err != nil {
			return err
		}
		var ir obsbench.IngestResult
		if err := json.Unmarshal(data, &ir); err != nil {
			return fmt.Errorf("ingest summary: %w", err)
		}
		want = ir.AckedRecords
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	got, err := obsbench.CountQueryable(ctx, target)
	if err != nil {
		return err
	}
	fmt.Printf("queryable=%d expected=%d\n", got, want)
	if want > 0 && got != want {
		return fmt.Errorf("verify: count mismatch (silent loss or duplication) — numbers from this run are void")
	}
	return nil
}

func cmdKill9(args []string) error {
	fs := flag.NewFlagSet("kill9", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	dataset := fs.String("dataset", "dataset.ndjson.zst", "dataset path")
	serverCmd := fs.String("server-cmd", "", "command starting the system under test (required)")
	readyURL := fs.String("ready-url", "", "readiness URL polled for 200 (required)")
	dataDir := fs.String("data-dir", "", "SUT data dir, wiped before each run (required)")
	logDir := fs.String("log-dir", "kill9-logs", "server stdout/stderr captures")
	runs := fs.Int("runs", 5, "independent kill cycles")
	rate := fs.Int("rate", 20_000, "steady ingest records/second")
	workers := fs.Int("workers", 4, "ingest workers")
	batch := fs.Int("batch", 500, "records per batch")
	killMin := fs.Duration("kill-after-min", 5*time.Second, "earliest kill point")
	killMax := fs.Duration("kill-after-max", 20*time.Second, "latest kill point")
	seed := fs.Uint64("seed", 1, "kill-point seed")
	out := fs.String("out", "", "JSON result path")
	_ = fs.Parse(args)

	if *targetName == "" || *serverCmd == "" || *readyURL == "" || *dataDir == "" {
		return fmt.Errorf("kill9: -target, -server-cmd, -ready-url and -data-dir are required")
	}
	cfg, err := obsbench.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	target, ok := cfg.Targets[*targetName]
	if !ok {
		return fmt.Errorf("kill9: target %q not in %s", *targetName, *configPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := obsbench.RunKill9(ctx, obsbench.Kill9Options{
		ServerCmd:    strings.Fields(*serverCmd),
		ReadyURL:     *readyURL,
		DataDir:      *dataDir,
		ServerLogDir: *logDir,
		Target:       target,
		TargetName:   *targetName,
		DatasetPath:  *dataset,
		Ingest:       obsbench.IngestOptions{Workers: *workers, BatchSize: *batch, Rate: *rate},
		Runs:         *runs,
		KillAfterMin: *killMin,
		KillAfterMax: *killMax,
		Seed:         *seed,
		Logf: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "kill9: "+format+"\n", args...)
		},
	})
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}
	if *out != "" {
		data, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func cmdPreflight(args []string) error {
	fs := flag.NewFlagSet("preflight", flag.ExitOnError)
	dataDir := fs.String("data-dir", ".", "filesystem the systems under test write to")
	_ = fs.Parse(args)

	results := obsbench.Preflight(*dataDir)
	fatal := false
	for _, r := range results {
		status := "ok  "
		if !r.OK {
			status = "FAIL"
			if r.Fatal {
				fatal = true
			}
		}
		fmt.Printf("%s %-24s %s\n", status, r.Name, r.Note)
	}
	if fatal {
		return fmt.Errorf("preflight failed; fix the environment before benchmarking")
	}
	return nil
}

func cmdDatagen(args []string) error {
	fs := flag.NewFlagSet("datagen", flag.ExitOnError)
	out := fs.String("out", "dataset.ndjson.zst", "output path")
	count := fs.Uint64("count", 20_000_000, "records to generate")
	seed := fs.Uint64("seed", 1, "PRNG seed (same seed = byte-identical dataset)")
	rareEvery := fs.Uint64("rare-every", 200_000, "inject rare FTS token every N records (0 = off)")
	_ = fs.Parse(args)

	start := time.Now()
	err := obsbench.Generate(*out, obsbench.GenConfig{
		Count:          *count,
		Seed:           *seed,
		RareTokenEvery: *rareEvery,
	}, func(done uint64) {
		fmt.Fprintf(os.Stderr, "datagen: %dM/%dM\n", done/1_000_000, *count/1_000_000)
	})
	if err != nil {
		return err
	}
	fmt.Printf("generated %d records in %s (rare token: %s)\n",
		*count, time.Since(start).Round(time.Second), obsbench.RareToken(*seed))
	return nil
}

func cmdSample(args []string) error {
	fs := flag.NewFlagSet("sample", flag.ExitOnError)
	match := fs.String("match", "", "regexp over /proc/<pid>/cmdline selecting processes to sample (required)")
	interval := fs.Duration("interval", time.Second, "sampling interval")
	out := fs.String("out", "", "CSV output path (default stdout)")
	_ = fs.Parse(args)

	if *match == "" {
		return fmt.Errorf("sample: -match is required")
	}
	re, err := regexp.Compile(*match)
	if err != nil {
		return fmt.Errorf("sample: -match: %w", err)
	}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sampler := &obsbench.MemSampler{Match: re, Interval: *interval, Out: w}
	return sampler.Run(ctx)
}

func cmdQuery(args []string) error {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	scenarios := fs.String("scenarios", "all", "comma-separated scenario list or 'all'")
	iterations := fs.Int("iterations", 50, "instances per scenario")
	seed := fs.Uint64("seed", 1, "instance-parameter seed (same seed across systems = same queries)")
	datasetSeed := fs.Uint64("dataset-seed", 1, "seed the dataset was generated with (drives the Q4 rare token)")
	limit := fs.Int("limit", 100, "result limit per query")
	qps := fs.Int("qps", 0, "pace queries (0 = back-to-back)")
	ingestSummary := fs.String("ingest-summary", "", "ingest summary JSON; supplies the time window")
	fromFlag := fs.String("from", "", "window start (RFC3339; overrides ingest summary)")
	toFlag := fs.String("to", "", "window end (RFC3339; overrides ingest summary)")
	out := fs.String("out", "", "results JSON path (required for compare)")
	_ = fs.Parse(args)

	if *targetName == "" {
		return fmt.Errorf("query: -target is required")
	}
	cfg, err := obsbench.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	target, ok := cfg.Targets[*targetName]
	if !ok {
		return fmt.Errorf("query: target %q not in %s", *targetName, *configPath)
	}

	opts := obsbench.QueryRunOptions{
		Iterations:  *iterations,
		Seed:        *seed,
		DatasetSeed: *datasetSeed,
		Limit:       *limit,
		QPS:         *qps,
	}
	if *scenarios == "all" {
		opts.Scenarios = obsbench.AllScenarios
	} else {
		opts.Scenarios = strings.Split(*scenarios, ",")
	}

	if *ingestSummary != "" {
		data, err := os.ReadFile(*ingestSummary)
		if err != nil {
			return err
		}
		var ir obsbench.IngestResult
		if err := json.Unmarshal(data, &ir); err != nil {
			return fmt.Errorf("ingest summary: %w", err)
		}
		opts.From, opts.To = ir.StartedAt, ir.FinishedAt
	}
	if *fromFlag != "" {
		if opts.From, err = time.Parse(time.RFC3339, *fromFlag); err != nil {
			return fmt.Errorf("-from: %w", err)
		}
	}
	if *toFlag != "" {
		if opts.To, err = time.Parse(time.RFC3339, *toFlag); err != nil {
			return fmt.Errorf("-to: %w", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	outcomes, err := obsbench.RunQueries(ctx, target, opts)
	if err != nil {
		return err
	}

	rf := obsbench.ResultFile{Target: *targetName, Outcomes: outcomes}
	if *out != "" {
		if err := obsbench.WriteResultFile(*out, rf); err != nil {
			return err
		}
	}
	printQuerySummary(rf)
	return nil
}

func printQuerySummary(rf obsbench.ResultFile) {
	type agg struct {
		lat         []time.Duration
		errs, total int
		countMin    int
		countMax    int
	}
	byScenario := map[string]*agg{}
	for _, oc := range rf.Outcomes {
		a := byScenario[oc.Scenario]
		if a == nil {
			a = &agg{countMin: -1}
			byScenario[oc.Scenario] = a
		}
		a.total++
		if oc.Error != "" {
			a.errs++
			continue
		}
		a.lat = append(a.lat, oc.Latency)
		if a.countMin == -1 || oc.Count < a.countMin {
			a.countMin = oc.Count
		}
		if oc.Count > a.countMax {
			a.countMax = oc.Count
		}
	}
	for scenario, a := range byScenario {
		p50, p95, p99 := obsbench.Percentiles(a.lat)
		fmt.Printf("%-12s n=%d errs=%d count=[%d..%d] p50=%s p95=%s p99=%s\n",
			scenario, a.total, a.errs, a.countMin, a.countMax,
			p50.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	}
}

func cmdCompare(args []string) error {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	_ = fs.Parse(args)
	paths := fs.Args()
	if len(paths) < 2 {
		return fmt.Errorf("compare: need at least two result files")
	}
	files := make([]obsbench.ResultFile, 0, len(paths))
	for _, p := range paths {
		rf, err := obsbench.ReadResultFile(p)
		if err != nil {
			return err
		}
		files = append(files, rf)
	}
	mismatches := obsbench.CompareCounts(files)
	if len(mismatches) == 0 {
		fmt.Println("equality gate: all shared query instances agree")
		return nil
	}
	for _, m := range mismatches {
		fmt.Printf("MISMATCH %s iter=%d %s\n", m.Scenario, m.Iteration, m.Params)
		for target, count := range m.Counts {
			line := fmt.Sprintf("  %-16s count=%d", target, count)
			if e := m.Errors[target]; e != "" {
				line += " error=" + e
			}
			fmt.Println(line)
		}
	}
	return fmt.Errorf("equality gate: %d mismatching instances — latency comparison void for them", len(mismatches))
}

func cmdIngest(args []string) error {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	configPath := fs.String("config", "systems.yaml", "targets config")
	targetName := fs.String("target", "", "target name from config (required)")
	dataset := fs.String("dataset", "dataset.ndjson.zst", "dataset path")
	workers := fs.Int("workers", 8, "concurrent senders")
	batch := fs.Int("batch", 500, "records per batch")
	rate := fs.Int("rate", 0, "records/second cap (0 = max throughput)")
	limit := fs.Uint64("limit", 0, "stop after N records (0 = whole dataset)")
	out := fs.String("out", "", "write JSON summary here in addition to stdout")
	_ = fs.Parse(args)

	if *targetName == "" {
		return fmt.Errorf("ingest: -target is required")
	}
	cfg, err := obsbench.LoadConfig(*configPath)
	if err != nil {
		return err
	}
	target, ok := cfg.Targets[*targetName]
	if !ok {
		return fmt.Errorf("ingest: target %q not in %s", *targetName, *configPath)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	res, err := obsbench.Ingest(ctx, *targetName, target, *dataset, obsbench.IngestOptions{
		Workers:   *workers,
		BatchSize: *batch,
		Rate:      *rate,
		Limit:     *limit,
	})
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}
	if *out != "" {
		data, _ := json.MarshalIndent(res, "", "  ")
		if err := os.WriteFile(*out, data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
