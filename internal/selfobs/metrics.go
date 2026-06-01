// Package selfobs is a hand-rolled Prometheus text-exposition exporter for
// Amber. It keeps the dependency surface to stdlib only — runtime/process
// collectors are intentionally omitted; add them only when an operator
// asks for them by name and we know what we're paying for.
package selfobs

import (
	"fmt"
	"io"
	"math"
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

// Default bucket boundaries (upper bounds, seconds). Two presets — short for
// sub-second hot paths (queries, WAL writes) and long for multi-second
// background work (segment seal). Operators usually want histograms with
// boundaries that match their alert thresholds; if these don't, switch to
// a per-instance NewHistogramVecWithBuckets.
var (
	// ShortLatencyBuckets: 0.5 ms .. 5 s. Tail focused, covers everything
	// from in-memory cache hits to a slow query that touched cold S3.
	ShortLatencyBuckets = []float64{
		0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025,
		0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
	}
	// LongLatencyBuckets: 0.1 s .. 60 s. For background work where 1 s is
	// already "fast" and 30 s is a meaningful slow.
	LongLatencyBuckets = []float64{
		0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 60,
	}
)

// Histogram is a fixed-bucket cumulative histogram. Bucket counts are
// inclusive upper bounds in seconds and stored as atomic uint64s; observed
// values fall into every bucket whose le >= value. The +Inf bucket is
// implicit (== Count).
type Histogram struct {
	buckets []float64
	counts  []atomic.Uint64
	count   atomic.Uint64
	// sum is stored as uint64 bits of a float64 so it stays lock-free; the
	// CAS loop in Observe handles concurrent updates.
	sumBits atomic.Uint64
}

func newHistogram(buckets []float64) *Histogram {
	h := &Histogram{
		buckets: append([]float64(nil), buckets...),
		counts:  make([]atomic.Uint64, len(buckets)),
	}
	return h
}

// Observe records v (in seconds). Negative values are silently dropped —
// a negative latency is always a bug at the call site, not data worth
// preserving.
func (h *Histogram) Observe(v float64) {
	if v < 0 {
		return
	}
	for i, ub := range h.buckets {
		if v <= ub {
			h.counts[i].Add(1)
		}
	}
	h.count.Add(1)
	for {
		old := h.sumBits.Load()
		newSum := math.Float64frombits(old) + v
		if h.sumBits.CompareAndSwap(old, math.Float64bits(newSum)) {
			return
		}
	}
}

// HistogramVec shards a histogram by a fixed list of label keys.
type HistogramVec struct {
	name    string
	help    string
	labels  []string
	buckets []float64

	mu       sync.RWMutex
	children map[string]*Histogram
}

func NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	return &HistogramVec{
		name:     name,
		help:     help,
		labels:   labels,
		buckets:  append([]float64(nil), buckets...),
		children: make(map[string]*Histogram),
	}
}

func (hv *HistogramVec) WithLabelValues(values ...string) *Histogram {
	if len(values) != len(hv.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d labels, got %d", hv.name, len(hv.labels), len(values)))
	}
	key := strings.Join(values, "\x00")

	hv.mu.RLock()
	if h, ok := hv.children[key]; ok {
		hv.mu.RUnlock()
		return h
	}
	hv.mu.RUnlock()

	hv.mu.Lock()
	defer hv.mu.Unlock()
	if h, ok := hv.children[key]; ok {
		return h
	}
	h := newHistogram(hv.buckets)
	hv.children[key] = h
	return h
}

type funcMetric struct {
	name string
	help string
	typ  string // "counter" | "gauge"
	fn   func() float64
}

var (
	regMu         sync.RWMutex
	counterVecs   []*CounterVec
	histogramVecs []*HistogramVec
	funcMetrics   []funcMetric
)

func RegisterCounterVec(cv *CounterVec) {
	regMu.Lock()
	defer regMu.Unlock()
	counterVecs = append(counterVecs, cv)
}

func RegisterHistogramVec(hv *HistogramVec) {
	regMu.Lock()
	defer regMu.Unlock()
	histogramVecs = append(histogramVecs, hv)
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

	// Query path. cache="hit"|"miss" so dashboards derive hit rate without
	// a second metric; errors are a separate counter so a stuck dependency
	// doesn't inflate the success-side rate.
	QueryTotal    = NewCounterVec("amber_query_total", "Queries executed, partitioned by cache outcome.", "kind", "cache")
	QueryErrors   = NewCounterVec("amber_query_errors_total", "Queries that returned an error to the caller.", "kind")
	QueryDuration = NewHistogramVec("amber_query_duration_seconds", "End-to-end query handling time (validation through result encode).", ShortLatencyBuckets, "kind")

	// WAL write latency. Inputs go through SegmentManager.Write/WriteBatch
	// which fsyncs to the WAL before touching the active segment, so this
	// metric is the floor of ingest tail latency.
	WALWrites        = NewCounterVec("amber_wal_writes_total", "WAL append operations.", "op")
	WALWriteDuration = NewHistogramVec("amber_wal_write_duration_seconds", "Latency of WAL append (single record or batch).", ShortLatencyBuckets, "op")

	// Seal duration covers the synchronous portion of rotate(): close +
	// fsync of the data file, plus index builds invoked via the onSeal
	// callback. Background S3 upload is excluded — it has its own lifecycle.
	SealDuration = NewHistogramVec("amber_seal_duration_seconds", "Time spent sealing a segment (sync + index builds).", LongLatencyBuckets, "kind")

	// Retention. reason="max_age"|"max_segments"|"max_total_bytes".
	RetentionEvictions = NewCounterVec("amber_retention_evictions_total", "Sealed segments deleted by retention.", "reason")

	// RetentionLocalEvictions counts segments whose local copy was removed by
	// the local-tier pass, leaving the remote (S3) copy intact. Distinct from
	// RetentionEvictions, which deletes the segment globally.
	// kind="logs"|"spans", reason="local_max_age"|"local_max_bytes".
	RetentionLocalEvictions = NewCounterVec("amber_retention_local_evictions_total", "Segments whose local copy was evicted, remote copy retained.", "kind", "reason")

	// Cold-segment reads: a query against an evicted segment that had to
	// fetch from the remote store before serving. Inflation here is the
	// signal to raise local_max_age / local_max_bytes.
	QueryColdSegmentReads    = NewCounterVec("amber_query_cold_segment_reads_total", "Queries that triggered a fetch from the remote segment store.", "kind")
	QueryColdSegmentFetchDur = NewHistogramVec("amber_query_cold_segment_fetch_duration_seconds", "Time spent fetching an evicted segment from the remote store.", LongLatencyBuckets, "kind")
)

func init() {
	RegisterCounterVec(IngestAccepted)
	RegisterCounterVec(IngestDropped)
	RegisterCounterVec(SealIndexErrors)
	RegisterCounterVec(QueryTotal)
	RegisterCounterVec(QueryErrors)
	RegisterCounterVec(WALWrites)
	RegisterCounterVec(RetentionEvictions)
	RegisterCounterVec(RetentionLocalEvictions)
	RegisterCounterVec(QueryColdSegmentReads)
	RegisterHistogramVec(QueryDuration)
	RegisterHistogramVec(WALWriteDuration)
	RegisterHistogramVec(SealDuration)
	RegisterHistogramVec(QueryColdSegmentFetchDur)
}

func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		regMu.RLock()
		defer regMu.RUnlock()
		for _, cv := range counterVecs {
			writeCounterVec(w, cv)
		}
		for _, hv := range histogramVecs {
			writeHistogramVec(w, hv)
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

// writeHistogramVec emits the prom text format for one histogram:
//
//	name_bucket{<labels>,le="<ub>"} <cumulative>
//	name_bucket{<labels>,le="+Inf"} <count>
//	name_count{<labels>} <count>
//	name_sum{<labels>}   <sum_seconds>
//
// Bucket counts are cumulative because Observe writes to every bucket whose
// le >= v; that matches prom_histogram_quantile expectations directly.
func writeHistogramVec(w io.Writer, hv *HistogramVec) {
	fprintf(w, "# HELP %s %s\n", hv.name, hv.help)
	fprintf(w, "# TYPE %s histogram\n", hv.name)

	hv.mu.RLock()
	keys := make([]string, 0, len(hv.children))
	for k := range hv.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := hv.children[k]
		values := strings.Split(k, "\x00")
		bucketLabels := append([]string(nil), hv.labels...)
		bucketLabels = append(bucketLabels, "le")
		for i, ub := range hv.buckets {
			vals := append([]string(nil), values...)
			vals = append(vals, formatBucket(ub))
			writeSample(w, hv.name+"_bucket", bucketLabels, vals, strconv.FormatUint(h.counts[i].Load(), 10))
		}
		// +Inf bucket: always equals total count.
		vals := append([]string(nil), values...)
		vals = append(vals, "+Inf")
		count := h.count.Load()
		writeSample(w, hv.name+"_bucket", bucketLabels, vals, strconv.FormatUint(count, 10))
		writeSample(w, hv.name+"_count", hv.labels, values, strconv.FormatUint(count, 10))
		sum := math.Float64frombits(h.sumBits.Load())
		writeSample(w, hv.name+"_sum", hv.labels, values, formatFloat(sum))
	}
	hv.mu.RUnlock()
}

// formatBucket renders a bucket upper bound. Plain decimal — avoids
// scientific notation so prom_histogram_quantile string-matches buckets.
func formatBucket(ub float64) string {
	return strconv.FormatFloat(ub, 'f', -1, 64)
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
