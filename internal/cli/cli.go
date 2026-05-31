// Package cli implements amberctl's one-shot, scriptable commands on top of
// internal/client. It is intentionally built on the stdlib flag package — the
// command surface is small and amber already carries a heavy dependency set,
// so a framework like cobra is not worth the weight.
package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/client"
)

const usageText = `amberctl — command-line client for an amber instance

usage:
  amberctl <command> [flags]

commands:
  logs        query log entries (supports -f/--follow live tail)
  traces      list trace summaries
  trace <id>  show one trace's span waterfall with attached logs
  services    list known service names
  stats       show storage and memory statistics
  tui         launch the interactive terminal UI

common flags (all commands):
  --addr      amber address (env AMBER_ADDR, default http://localhost:8080)
  --api-key   bearer token (env AMBER_API_KEY)
  --json      emit JSON
  --ndjson    emit newline-delimited JSON (logs/traces)

run "amberctl <command> -h" for command-specific flags.
`

// Run dispatches a single amberctl invocation. args excludes the program name.
func Run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		writef(out, "%s", usageText)
		return nil
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "logs":
		return cmdLogs(ctx, rest, out)
	case "traces":
		return cmdTraces(ctx, rest, out)
	case "trace":
		return cmdTrace(ctx, rest, out)
	case "services":
		return cmdServices(ctx, rest, out)
	case "stats":
		return cmdStats(ctx, rest, out)
	case "help", "-h", "--help":
		writef(out, "%s", usageText)
		return nil
	default:
		return fmt.Errorf("unknown command %q (run \"amberctl help\")", cmd)
	}
}

// commonFlags holds the connection/output options every command shares.
type commonFlags struct {
	addr   string
	apiKey string
	json   bool
	ndjson bool
}

func registerCommon(fs *flag.FlagSet) *commonFlags {
	cf := &commonFlags{}
	fs.StringVar(&cf.addr, "addr", envOr("AMBER_ADDR", client.DefaultAddr), "amber server address")
	fs.StringVar(&cf.apiKey, "api-key", os.Getenv("AMBER_API_KEY"), "bearer API key")
	fs.BoolVar(&cf.json, "json", false, "emit JSON")
	fs.BoolVar(&cf.ndjson, "ndjson", false, "emit newline-delimited JSON")
	return cf
}

func (cf *commonFlags) newClient() *client.Client {
	return client.New(cf.addr, client.WithAPIKey(cf.apiKey))
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// stringSlice collects repeatable --attr k=v flags.
type stringSlice []string

func (s *stringSlice) String() string { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func cmdLogs(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("logs", flag.ContinueOnError)
	cf := registerCommon(fs)
	var (
		service  = fs.String("service", "", "filter by service (comma-separated)")
		level    = fs.String("level", "", "filter by level (comma-separated)")
		host     = fs.String("host", "", "filter by host (comma-separated)")
		q        = fs.String("q", "", "full-text search")
		since    = fs.String("since", "", "relative time window, e.g. 15m, 6h, 7d")
		from     = fs.String("from", "", "start time (RFC3339 or 'YYYY-MM-DD HH:MM:SS')")
		to       = fs.String("to", "", "end time")
		limit    = fs.Int("limit", 100, "max entries")
		attrs    stringSlice
		follow   bool
		interval = fs.Duration("interval", 2*time.Second, "poll interval for --follow")
	)
	fs.Var(&attrs, "attr", "attribute filter key=value (repeatable)")
	fs.BoolVar(&follow, "f", false, "follow / live tail")
	fs.BoolVar(&follow, "follow", false, "follow / live tail")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fromT, toT, err := resolveRange(*since, *from, *to, time.Now())
	if err != nil {
		return err
	}
	attrMap, err := parseAttrs(attrs)
	if err != nil {
		return err
	}

	qy := client.LogQuery{
		Services: splitComma(*service),
		Levels:   splitComma(*level),
		Hosts:    splitComma(*host),
		FullText: *q,
		From:     fromT,
		To:       toT,
		Limit:    *limit,
		Attrs:    attrMap,
	}
	c := cf.newClient()

	if follow {
		return followLogs(ctx, c, qy, *interval, out)
	}

	res, err := c.Logs(ctx, qy)
	if err != nil {
		return err
	}
	switch {
	case cf.ndjson:
		return writeNDJSON(out, res.Entries)
	case cf.json:
		return writeJSON(out, res)
	default:
		renderLogs(out, res)
		return nil
	}
}

// followLogs polls for new entries, advancing the window to the newest
// timestamp seen and de-duplicating only the entries that sit exactly on that
// boundary — so the seen-set stays bounded regardless of how long it runs.
func followLogs(ctx context.Context, c *client.Client, base client.LogQuery, interval time.Duration, out io.Writer) error {
	if base.From.IsZero() {
		base.From = time.Now()
	}
	if base.Limit == 0 {
		base.Limit = 100
	}
	boundary := make(map[string]bool)
	from := base.From

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		q := base
		q.From = from
		res, err := c.Logs(ctx, q)
		if err != nil {
			return err
		}
		entries := res.Entries
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].Timestamp.Before(entries[j].Timestamp)
		})

		var maxTS time.Time
		for _, e := range entries {
			if boundary[e.ID] {
				continue
			}
			renderLogLine(out, e)
			if e.Timestamp.After(maxTS) {
				maxTS = e.Timestamp
			}
		}
		if !maxTS.IsZero() {
			// Rebuild the boundary set for the new high-water mark.
			next := make(map[string]bool)
			for _, e := range entries {
				if e.Timestamp.Equal(maxTS) {
					next[e.ID] = true
				}
			}
			boundary = next
			from = maxTS
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func cmdTraces(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("traces", flag.ContinueOnError)
	cf := registerCommon(fs)
	var (
		service = fs.String("service", "", "filter by service (comma-separated)")
		since   = fs.String("since", "", "relative time window, e.g. 15m, 6h")
		from    = fs.String("from", "", "start time")
		to      = fs.String("to", "", "end time")
		limit   = fs.Int("limit", 20, "max traces")
		offset  = fs.Int("offset", 0, "result offset")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	fromT, toT, err := resolveRange(*since, *from, *to, time.Now())
	if err != nil {
		return err
	}
	res, err := cf.newClient().Traces(ctx, client.TraceQuery{
		Services: splitComma(*service),
		From:     fromT,
		To:       toT,
		Limit:    *limit,
		Offset:   *offset,
	})
	if err != nil {
		return err
	}
	if cf.json || cf.ndjson {
		return writeJSON(out, res)
	}
	renderTraces(out, res)
	return nil
}

func cmdTrace(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("trace", flag.ContinueOnError)
	cf := registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("trace: missing trace id (usage: amberctl trace <id>)")
	}
	res, err := cf.newClient().Trace(ctx, fs.Arg(0))
	if err != nil {
		return err
	}
	if cf.json || cf.ndjson {
		return writeJSON(out, res)
	}
	renderTrace(out, res)
	return nil
}

func cmdServices(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("services", flag.ContinueOnError)
	cf := registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	svcs, err := cf.newClient().Services(ctx)
	if err != nil {
		return err
	}
	if cf.json || cf.ndjson {
		return writeJSON(out, svcs)
	}
	renderServices(out, svcs)
	return nil
}

func cmdStats(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	cf := registerCommon(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	s, err := cf.newClient().Stats(ctx)
	if err != nil {
		return err
	}
	if cf.json || cf.ndjson {
		return writeJSON(out, s)
	}
	renderStats(out, s)
	return nil
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseAttrs(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	m := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("--attr %q: want key=value", p)
		}
		m[k] = v
	}
	return m, nil
}
