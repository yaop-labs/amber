package cli

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseSince(t *testing.T) {
	cases := map[string]time.Duration{
		"15m": 15 * time.Minute,
		"6h":  6 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"2w":  2 * 7 * 24 * time.Hour,
		"90s": 90 * time.Second,
	}
	for in, want := range cases {
		got, err := parseSince(in)
		if err != nil {
			t.Errorf("parseSince(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("parseSince(%q) = %v, want %v", in, got, want)
		}
	}
	if _, err := parseSince("nonsense"); err == nil {
		t.Error("parseSince(nonsense): expected error")
	}
}

func TestResolveRangeSinceAndFromExclusive(t *testing.T) {
	if _, _, err := resolveRange("1h", "2026-01-01", "", time.Now()); err == nil {
		t.Error("expected error when --since and --from both set")
	}
}

func TestResolveRangeSince(t *testing.T) {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	from, to, err := resolveRange("1h", "", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if !from.Equal(now.Add(-time.Hour)) {
		t.Errorf("from = %v, want %v", from, now.Add(-time.Hour))
	}
	if !to.IsZero() {
		t.Errorf("to = %v, want zero", to)
	}
}

func TestParseAttrs(t *testing.T) {
	m, err := parseAttrs([]string{"env=prod", "region=eu"})
	if err != nil {
		t.Fatal(err)
	}
	if m["env"] != "prod" || m["region"] != "eu" {
		t.Errorf("attrs = %v", m)
	}
	if _, err := parseAttrs([]string{"bad"}); err == nil {
		t.Error("expected error for attr without '='")
	}
}

func TestSplitComma(t *testing.T) {
	got := splitComma("a, b ,,c")
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("splitComma = %v", got)
	}
	if splitComma("") != nil {
		t.Error("splitComma(\"\") should be nil")
	}
}

func TestRunLogsPlain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Write([]byte(`{"entries":[{"ID":"01","Timestamp":"2026-05-31T12:00:00Z","Level":"ERROR","Service":"api","Body":"boom","TraceID":"abcdef1234"}],"total_hits":1,"took_ms":2}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), []string{"logs", "--addr", srv.URL, "--service", "api"}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"ERROR", "api", "boom", "trace:abcdef12", "1 hits"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"bogus"}, &buf); err == nil {
		t.Error("expected error for unknown command")
	}
}

func TestRunTraceMissingID(t *testing.T) {
	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"trace"}, &buf); err == nil {
		t.Error("expected error when trace id missing")
	}
}

func TestRunMetricsRatePlain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/metrics/rate" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("metric"); got != "http_requests_total" {
			t.Errorf("metric param = %q", got)
		}
		if got := r.URL.Query().Get("window"); got != "5m0s" {
			t.Errorf("window param = %q", got)
		}
		w.Write([]byte(`{"metric":"http_requests_total","window_ms":300000,"end_ms":0,"by":"job","rates":{"api":1.25,"worker":0.5}}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(), []string{"metrics", "rate", "--addr", srv.URL, "--by", "job", "http_requests_total"}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"http_requests_total", "api", "1.2500", "worker", "0.5000"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunMetricsRateMissingMetric(t *testing.T) {
	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "rate"}, &buf); err == nil {
		t.Error("expected error when metric name missing")
	}
}

func TestRunMetricsList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/metrics" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Write([]byte(`{"metrics":["alpha_total","zeta_total"]}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "list", "--addr", srv.URL}, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// New format: each line carries a kind column ("scalar" or "histogram")
	// so the user knows which subcommand (rate vs quantile) reads it.
	for _, want := range []string{"alpha_total\tscalar", "zeta_total\tscalar"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

// TestRunMetricsListWithHistograms verifies the histogram namespace renders
// distinctly from scalar, so the user can tell which subcommand to use.
func TestRunMetricsListWithHistograms(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"metrics":["http_requests_total"],"histograms":["rpc_latency_seconds"]}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "list", "--addr", srv.URL}, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"http_requests_total\tscalar", "rpc_latency_seconds\thistogram"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunMetricsList_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"metrics":[],"histograms":[]}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "list", "--addr", srv.URL}, &buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "no metrics") {
		t.Errorf("expected '(no metrics)' for empty list, got:\n%s", buf.String())
	}
}

func TestRunMetricsStats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/metrics/stats" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		w.Write([]byte(`{"blocks":2,"series":10,"samples":1000,"bytes":4096,"buffered_series":3,"buffered_samples":42,"min_time_ms":1700000000000,"max_time_ms":1700001000000,"histogram":{"blocks":5,"series":17,"bytes":2048,"min_time_ms":1700000500000,"max_time_ms":1700000900000}}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "stats", "--addr", srv.URL}, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// Scalar half mirrors the old assertions but with the new prefix.
	for _, want := range []string{"scalar.blocks", "scalar.series", "scalar.samples", "scalar.buffered_samples", "42"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
	// Histogram half is the new surface; check both counters and time range
	// rendered as RFC3339 (proves nullable pointers were honored).
	for _, want := range []string{"histogram.blocks", "histogram.series", "17", "histogram.min_time"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunMetricsStats_EmptyTimeRange(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"blocks":0,"series":0,"samples":0,"bytes":0,"buffered_series":0,"buffered_samples":0}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "stats", "--addr", srv.URL}, &buf); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// Both halves must render a dash when their respective store has no
	// time range — the CLI relies on this to distinguish "empty" from
	// "actual data at epoch 0". tabwriter pads with spaces, so we check
	// each line carries the dash.
	for _, line := range []string{"scalar.min_time", "histogram.min_time"} {
		want := line
		if !strings.Contains(out, want) {
			t.Errorf("output missing line %q:\n%s", want, out)
			continue
		}
		idx := strings.Index(out, want)
		// Walk forward to the end of this line and check it ends in "-".
		end := strings.Index(out[idx:], "\n")
		if end < 0 {
			end = len(out) - idx
		}
		if !strings.HasSuffix(strings.TrimSpace(out[idx:idx+end]), "-") {
			t.Errorf("expected dash on %q line, got %q", line, out[idx:idx+end])
		}
	}
}

func TestRunMetricsQuantilePlain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/metrics/quantile" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		qp := r.URL.Query()
		if got := qp.Get("metric"); got != "rpc_latency_seconds" {
			t.Errorf("metric param = %q", got)
		}
		if got := qp.Get("q"); got != "0.95" {
			t.Errorf("q param = %q", got)
		}
		if got := qp.Get("by"); got != "job" {
			t.Errorf("by param = %q", got)
		}
		// Window flag was zero on the CLI side → must be absent so the
		// server treats the query as unbounded.
		if qp.Has("window") {
			t.Errorf("window should be absent when --window is zero, got %q", qp.Get("window"))
		}
		w.Write([]byte(`{"metric":"rpc_latency_seconds","quantile":0.95,"by":"job","quantiles":{"api":12.5,"worker":7.25}}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(),
		[]string{"metrics", "quantile", "--addr", srv.URL, "--q", "0.95", "--by", "job", "rpc_latency_seconds"},
		&buf)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	for _, want := range []string{"rpc_latency_seconds", "q=0.95", "api", "12.5000", "worker", "7.2500"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunMetricsQuantileWithWindowEcho(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("window"); got != "5m0s" {
			t.Errorf("window param = %q", got)
		}
		w.Write([]byte(`{"metric":"rpc_latency_seconds","quantile":0.5,"window_ms":300000,"end_ms":1700000000000,"quantiles":{"":1.42}}`))
	}))
	defer srv.Close()

	var buf bytes.Buffer
	err := Run(context.Background(),
		[]string{"metrics", "quantile", "--addr", srv.URL, "--window", "5m", "rpc_latency_seconds"},
		&buf)
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	// Bounded query: window must render as "300000ms", end as RFC3339, and
	// the no-by group key "" must collapse to "(total)".
	for _, want := range []string{"window=300000ms", "end=2023", "(total)", "1.4200"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestRunMetricsQuantileMissingMetric(t *testing.T) {
	var buf bytes.Buffer
	if err := Run(context.Background(), []string{"metrics", "quantile"}, &buf); err == nil {
		t.Error("expected error when metric name missing")
	}
}
