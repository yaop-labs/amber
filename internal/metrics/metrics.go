// Package metrics is a hand-rolled Prometheus text-exposition exporter for
// Amber. It keeps the dependency surface to stdlib only — runtime/process
// collectors are intentionally omitted; add them only when an operator
// asks for them by name and we know what we're paying for.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Counter is a monotonic integer counter. All Amber counters are integer
// events (entries dropped, builds failed); float storage would just hide
// truncation bugs.
type Counter struct {
	v atomic.Uint64
}

func (c *Counter) Inc()         { c.v.Add(1) }
func (c *Counter) Add(n uint64) { c.v.Add(n) }
func (c *Counter) Get() uint64  { return c.v.Load() }

// CounterVec shards a counter by a fixed list of label keys. Children are
// created on first WithLabelValues call.
type CounterVec struct {
	name   string
	help   string
	labels []string

	mu       sync.RWMutex
	children map[string]*Counter
}

func NewCounterVec(name, help string, labels ...string) *CounterVec {
	return &CounterVec{
		name:     name,
		help:     help,
		labels:   labels,
		children: make(map[string]*Counter),
	}
}

func (cv *CounterVec) WithLabelValues(values ...string) *Counter {
	if len(values) != len(cv.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d labels, got %d", cv.name, len(cv.labels), len(values)))
	}
	// NUL is illegal inside Prom label values, so it's a safe key separator.
	key := strings.Join(values, "\x00")

	cv.mu.RLock()
	if c, ok := cv.children[key]; ok {
		cv.mu.RUnlock()
		return c
	}
	cv.mu.RUnlock()

	cv.mu.Lock()
	defer cv.mu.Unlock()
	if c, ok := cv.children[key]; ok {
		return c
	}
	c := &Counter{}
	cv.children[key] = c
	return c
}

type funcMetric struct {
	name string
	help string
	typ  string // "counter" | "gauge"
	fn   func() float64
}

var (
	regMu       sync.RWMutex
	counterVecs []*CounterVec
	funcMetrics []funcMetric
)

func RegisterCounterVec(cv *CounterVec) {
	regMu.Lock()
	defer regMu.Unlock()
	counterVecs = append(counterVecs, cv)
}

func RegisterGaugeFunc(name, help string, fn func() float64) {
	regMu.Lock()
	defer regMu.Unlock()
	funcMetrics = append(funcMetrics, funcMetric{name, help, "gauge", fn})
}

func RegisterCounterFunc(name, help string, fn func() float64) {
	regMu.Lock()
	defer regMu.Unlock()
	funcMetrics = append(funcMetrics, funcMetric{name, help, "counter", fn})
}

// Pre-declared counters used directly from ingest/bootstrap hot paths.
var (
	IngestAccepted  = NewCounterVec("amber_ingest_accepted_total", "Entries successfully written to a segment.", "kind")
	IngestDropped   = NewCounterVec("amber_ingest_dropped_total", "Entries dropped before reaching storage.", "kind", "reason")
	SealIndexErrors = NewCounterVec("amber_seal_index_errors_total", "Index builds that failed during segment seal, after retries.", "kind", "index")
)

func init() {
	RegisterCounterVec(IngestAccepted)
	RegisterCounterVec(IngestDropped)
	RegisterCounterVec(SealIndexErrors)
}

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		regMu.RLock()
		defer regMu.RUnlock()
		for _, cv := range counterVecs {
			writeCounterVec(w, cv)
		}
		for _, fm := range funcMetrics {
			writeFunc(w, fm)
		}
	})
}

// fprintf wraps fmt.Fprintf and intentionally drops the error: a scraper
// disconnect mid-response is normal and there's nothing to recover.
func fprintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func writeCounterVec(w io.Writer, cv *CounterVec) {
	fprintf(w, "# HELP %s %s\n", cv.name, cv.help)
	fprintf(w, "# TYPE %s counter\n", cv.name)

	cv.mu.RLock()
	keys := make([]string, 0, len(cv.children))
	for k := range cv.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c := cv.children[k]
		values := strings.Split(k, "\x00")
		writeSample(w, cv.name, cv.labels, values, strconv.FormatUint(c.Get(), 10))
	}
	cv.mu.RUnlock()
}

func writeFunc(w io.Writer, fm funcMetric) {
	fprintf(w, "# HELP %s %s\n", fm.name, fm.help)
	fprintf(w, "# TYPE %s %s\n", fm.name, fm.typ)
	fprintf(w, "%s %s\n", fm.name, formatFloat(fm.fn()))
}

func writeSample(w io.Writer, name string, labels, values []string, value string) {
	if len(labels) == 0 {
		fprintf(w, "%s %s\n", name, value)
		return
	}
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(values[i]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	b.WriteByte(' ')
	b.WriteString(value)
	b.WriteByte('\n')
	_, _ = io.WriteString(w, b.String())
}

func formatFloat(v float64) string {
	if v == float64(int64(v)) {
		return strconv.FormatInt(int64(v), 10)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// escapeLabel applies the Prom text-format escape rules: backslash, quote,
// newline. Anything else passes through verbatim.
func escapeLabel(s string) string {
	if !strings.ContainsAny(s, "\\\"\n") {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
