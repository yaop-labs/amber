package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/yaop-labs/amber/internal/client"
)

const metricsUsage = `amberctl metrics — query metrics

subcommands:
  rate <metric>     compute the per-second rate of a counter

example:
  amberctl metrics rate http_requests_total --window 5m --by job
`

func cmdMetrics(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		writef(out, "%s", metricsUsage)
		return nil
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "rate":
		return cmdMetricsRate(ctx, rest, out)
	case "help", "-h", "--help":
		writef(out, "%s", metricsUsage)
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for metrics (run \"amberctl metrics help\")", sub)
	}
}

func cmdMetricsRate(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("metrics rate", flag.ContinueOnError)
	cf := registerCommon(fs)
	var (
		window = fs.Duration("window", 5*time.Minute, "rate window, e.g. 1m, 5m, 1h")
		by     = fs.String("by", "", "group rate by this label (empty = single total)")
		end    = fs.String("end", "", "evaluation time (RFC3339 or unix ms; default now)")
		sel    stringSlice
	)
	fs.Var(&sel, "selector", "extra label matcher key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("metrics rate: missing metric name (usage: amberctl metrics rate <metric>)")
	}
	metric := fs.Arg(0)

	q := client.MetricRateQuery{
		Metric: metric,
		Window: *window,
		By:     *by,
	}
	if *end != "" {
		t, err := parseEndTime(*end)
		if err != nil {
			return fmt.Errorf("--end: %w", err)
		}
		q.End = t
	}
	selectors, err := parseAttrs(sel)
	if err != nil {
		return err
	}
	q.Selector = selectors

	res, err := cf.newClient().MetricRate(ctx, q)
	if err != nil {
		return err
	}
	switch {
	case cf.ndjson, cf.json:
		return writeJSON(out, res)
	default:
		renderMetricRate(out, res)
		return nil
	}
}

// parseEndTime accepts either RFC3339 or raw unix milliseconds. The CLI's
// "now-relative" forms (5m, 1h) are not supported here — RangeSelector.Window
// already captures the lookback, and adding ambiguity (does "5m" mean window
// or evaluation offset?) would only confuse.
func parseEndTime(raw string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t, nil
	}
	// Try unix millis.
	var ms int64
	if _, err := fmt.Sscanf(raw, "%d", &ms); err == nil && ms > 0 {
		return time.UnixMilli(ms), nil
	}
	return time.Time{}, fmt.Errorf("want RFC3339 or unix ms, got %q", raw)
}

func renderMetricRate(out io.Writer, res *client.MetricRateResult) {
	end := time.UnixMilli(res.EndMillis).UTC().Format(time.RFC3339)
	writef(out, "metric=%s window=%dms end=%s\n", res.Metric, res.WindowMS, end)
	if len(res.Rates) == 0 {
		writef(out, "(no series matched)\n")
		return
	}
	keys := make([]string, 0, len(res.Rates))
	for k := range res.Rates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	header := "TOTAL"
	if res.By != "" {
		header = res.By
	}
	_, _ = fmt.Fprintf(tw, "%s\tRATE/S\n", header)
	for _, k := range keys {
		label := k
		if label == "" {
			label = "(total)"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%.4f\n", label, res.Rates[k])
	}
	_ = tw.Flush()
}
