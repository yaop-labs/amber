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
  list                  list metric names available in the head index
  rate <metric>         compute the per-second rate of a counter
  quantile <metric>     compute a quantile over a histogram metric
  stats                 show metric store size, block count, time range

examples:
  amberctl metrics list
  amberctl metrics rate http_requests_total --window 5m --by job
  amberctl metrics quantile rpc_latency_seconds --q 0.95 --by job
  amberctl metrics stats
`

func cmdMetrics(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		writef(out, "%s", metricsUsage)
		return nil
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "list":
		return cmdMetricsList(ctx, rest, out)
	case "rate":
		return cmdMetricsRate(ctx, rest, out)
	case "quantile":
		return cmdMetricsQuantile(ctx, rest, out)
	case "stats":
		return cmdMetricsStats(ctx, rest, out)
	case "help", "-h", "--help":
		writef(out, "%s", metricsUsage)
		return nil
	default:
		return fmt.Errorf("unknown subcommand %q for metrics (run \"amberctl metrics help\")", sub)
	}
}

func cmdMetricsList(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("metrics list", flag.ContinueOnError)
	cf := registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cat, err := cf.newClient().MetricNames(ctx)
	if err != nil {
		return err
	}
	if cf.ndjson || cf.json {
		return writeJSON(out, cat)
	}
	if len(cat.Metrics) == 0 && len(cat.Histograms) == 0 {
		writef(out, "(no metrics)\n")
		return nil
	}
	// Scalar and histogram namespaces have distinct read paths (rate vs
	// quantile). Tag each so users know which subcommand to reach for.
	for _, n := range cat.Metrics {
		writef(out, "%s\tscalar\n", n)
	}
	for _, n := range cat.Histograms {
		writef(out, "%s\thistogram\n", n)
	}
	return nil
}

func cmdMetricsStats(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("metrics stats", flag.ContinueOnError)
	cf := registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	stats, err := cf.newClient().MetricStats(ctx)
	if err != nil {
		return err
	}
	if cf.ndjson || cf.json {
		return writeJSON(out, stats)
	}
	renderMetricStats(out, stats)
	return nil
}

func renderMetricStats(out io.Writer, s *client.MetricStoreStats) {
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintf(tw, "scalar.blocks\t%d\n", s.Blocks)
	_, _ = fmt.Fprintf(tw, "scalar.series\t%d\n", s.Series)
	_, _ = fmt.Fprintf(tw, "scalar.samples\t%d\n", s.Samples)
	_, _ = fmt.Fprintf(tw, "scalar.bytes\t%d\n", s.Bytes)
	_, _ = fmt.Fprintf(tw, "scalar.buffered_series\t%d\n", s.BufferedSeries)
	_, _ = fmt.Fprintf(tw, "scalar.buffered_samples\t%d\n", s.BufferedSamples)
	writeStatsTimeRange(tw, "scalar", s.MinTimeMS, s.MaxTimeMS)
	_, _ = fmt.Fprintf(tw, "histogram.blocks\t%d\n", s.Histogram.Blocks)
	_, _ = fmt.Fprintf(tw, "histogram.series\t%d\n", s.Histogram.Series)
	_, _ = fmt.Fprintf(tw, "histogram.bytes\t%d\n", s.Histogram.Bytes)
	writeStatsTimeRange(tw, "histogram", s.Histogram.MinTimeMS, s.Histogram.MaxTimeMS)
	_ = tw.Flush()
}

func writeStatsTimeRange(tw io.Writer, prefix string, minMS, maxMS *int64) {
	if minMS != nil && maxMS != nil {
		_, _ = fmt.Fprintf(tw, "%s.min_time\t%s\n", prefix, time.UnixMilli(*minMS).UTC().Format(time.RFC3339))
		_, _ = fmt.Fprintf(tw, "%s.max_time\t%s\n", prefix, time.UnixMilli(*maxMS).UTC().Format(time.RFC3339))
	} else {
		_, _ = fmt.Fprintf(tw, "%s.min_time\t-\n", prefix)
		_, _ = fmt.Fprintf(tw, "%s.max_time\t-\n", prefix)
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

func cmdMetricsQuantile(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("metrics quantile", flag.ContinueOnError)
	cf := registerCommon(fs)
	var (
		q      = fs.Float64("q", 0.95, "quantile in [0, 1], e.g. 0.5, 0.95, 0.99")
		window = fs.Duration("window", 0, "lookback window, e.g. 5m, 1h (zero = quantile over everything)")
		by     = fs.String("by", "", "group quantile by this label (empty = single value)")
		end    = fs.String("end", "", "evaluation time (RFC3339 or unix ms; default now). only used with --window")
		sel    stringSlice
	)
	fs.Var(&sel, "selector", "extra label matcher key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("metrics quantile: missing metric name (usage: amberctl metrics quantile <metric>)")
	}
	metric := fs.Arg(0)

	query := client.MetricQuantileQuery{
		Metric:   metric,
		Quantile: *q,
		Window:   *window,
		By:       *by,
	}
	if *end != "" {
		t, err := parseEndTime(*end)
		if err != nil {
			return fmt.Errorf("--end: %w", err)
		}
		query.End = t
	}
	selectors, err := parseAttrs(sel)
	if err != nil {
		return err
	}
	query.Selector = selectors

	res, err := cf.newClient().MetricQuantile(ctx, query)
	if err != nil {
		return err
	}
	switch {
	case cf.ndjson, cf.json:
		return writeJSON(out, res)
	default:
		renderMetricQuantile(out, res)
		return nil
	}
}

func renderMetricQuantile(out io.Writer, res *client.MetricQuantileResult) {
	// Header line mirrors rate's: surface metric, q, and the actual window
	// the server applied (zero means unbounded — we render that as "-" so
	// the reader doesn't confuse it with "0ms window").
	windowStr := "-"
	endStr := "-"
	if res.WindowMS > 0 {
		windowStr = fmt.Sprintf("%dms", res.WindowMS)
		endStr = time.UnixMilli(res.EndMillis).UTC().Format(time.RFC3339)
	}
	writef(out, "metric=%s q=%g window=%s end=%s\n", res.Metric, res.Quantile, windowStr, endStr)
	if len(res.Quantiles) == 0 {
		writef(out, "(no series matched)\n")
		return
	}
	keys := make([]string, 0, len(res.Quantiles))
	for k := range res.Quantiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	header := "TOTAL"
	if res.By != "" {
		header = res.By
	}
	_, _ = fmt.Fprintf(tw, "%s\tVALUE\n", header)
	for _, k := range keys {
		label := k
		if label == "" {
			label = "(total)"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%.4f\n", label, res.Quantiles[k])
	}
	_ = tw.Flush()
}
