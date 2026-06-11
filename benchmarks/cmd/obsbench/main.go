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
	fmt.Fprintln(os.Stderr, `usage: obsbench <preflight|datagen|sample|ingest> [flags]`)
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
	count := fs.Uint64("count", 100_000_000, "records to generate")
	seed := fs.Uint64("seed", 1, "PRNG seed (same seed = byte-identical dataset)")
	rareEvery := fs.Uint64("rare-every", 1_000_000, "inject rare FTS token every N records (0 = off)")
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
