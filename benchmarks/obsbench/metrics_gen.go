package obsbench

import (
	"fmt"
	"math"
)

// Metrics workload generator behind METHODOLOGY.md (Metrics): I1 steady
// gauges+counters, I2 series churn, I3 exponential histograms. Unlike the log
// dataset there is no pre-generated file: a metrics workload is a *schedule*
// (which series emit a sample at which tick), so the generator synthesizes
// samples on the fly, fully determined by (config, tick index). Two runs with
// the same config produce identical series sets and identical values - the
// reproducibility contract that makes the cross-system equality gate
// meaningful.
//
// Counter values are cumulative (OTLP cumulative temporality): Prometheus,
// Mimir and VictoriaMetrics reject or misread delta sums. Cumulative state
// lives in the generator and advances via BeginTick, which the ingest runner
// must call once per tick, in order.
const (
	MetricCounterName = "http_requests_total"
	MetricGaugeName   = "queue_depth"
	MetricHistName    = "http_request_duration_ms"
)

// MetricsGenConfig parameterizes the metrics workload. The route label is the
// QM4 group-by target: Routes distinct values spread evenly over the series.
type MetricsGenConfig struct {
	Seed uint64
	// ScalarSeries is the active scalar series count: the first half are
	// counters (http_requests_total), the second half gauges (queue_depth).
	ScalarSeries int
	// HistSeries is the exponential-histogram series count (I3).
	HistSeries int
	// Routes is the cardinality of the route label (default 1000).
	Routes int
	// ChurnPercent of scalar series are replaced each epoch (I2): their gen
	// label changes and counter state resets, exactly like a restarted
	// instance. 0 disables churn.
	ChurnPercent int
}

func (c MetricsGenConfig) withDefaults() MetricsGenConfig {
	if c.ScalarSeries == 0 {
		c.ScalarSeries = 100_000
	}
	if c.HistSeries == 0 {
		c.HistSeries = 10_000
	}
	if c.Routes == 0 {
		c.Routes = 1000
	}
	return c
}

// SeriesLabels is one series identity without the metric name. Gen < 0 means
// the series is stable (no gen label).
type SeriesLabels struct {
	Service string
	Host    string
	Route   string
	Shard   string
	Gen     int
}

// histBuckets is the per-series bucket count of the synthetic exponential
// histograms. Fixed so cumulative state stays flat arrays.
const histBuckets = 8

// histScale is the OTLP exponential-histogram scale (base 2^(1/4)).
const histScale = 2

// ExpHistPoint is one cumulative exponential-histogram sample.
type ExpHistPoint struct {
	Scale  int32
	Offset int32
	Counts []uint64
	Sum    float64
	Count  uint64
}

// MetricsGenerator synthesizes the deterministic metrics stream. Not safe for
// concurrent use; the ingest runner serializes BeginTick/SetEpoch and the
// point accessors are read-only between them.
type MetricsGenerator struct {
	cfg   MetricsGenConfig
	epoch int

	// counters holds cumulative values for series [0, CounterCount).
	counters []int64
	// histCounts/histSums/histTotals hold cumulative histogram state,
	// histBuckets buckets per series.
	histCounts []uint64
	histSums   []float64
	histTotals []uint64
}

func NewMetricsGenerator(cfg MetricsGenConfig) *MetricsGenerator {
	cfg = cfg.withDefaults()
	g := &MetricsGenerator{cfg: cfg}
	g.counters = make([]int64, g.CounterCount())
	g.histCounts = make([]uint64, cfg.HistSeries*histBuckets)
	g.histSums = make([]float64, cfg.HistSeries)
	g.histTotals = make([]uint64, cfg.HistSeries)
	return g
}

func (g *MetricsGenerator) Config() MetricsGenConfig { return g.cfg }

// CounterCount is the number of counter series (first half of the scalar
// index space); GaugeCount covers the rest.
func (g *MetricsGenerator) CounterCount() int { return g.cfg.ScalarSeries / 2 }
func (g *MetricsGenerator) GaugeCount() int   { return g.cfg.ScalarSeries - g.CounterCount() }
func (g *MetricsGenerator) ScalarCount() int  { return g.cfg.ScalarSeries }
func (g *MetricsGenerator) HistCount() int    { return g.cfg.HistSeries }

// SetEpoch applies churn (I2): on an epoch change every churnable series gets
// a fresh gen label and its cumulative state resets. The runner derives the
// epoch from wall time (elapsed / churn interval) and calls this before
// BeginTick.
func (g *MetricsGenerator) SetEpoch(epoch int) {
	if epoch == g.epoch {
		return
	}
	g.epoch = epoch
	for i := 0; i < g.CounterCount(); i++ {
		if g.churns(i) {
			g.counters[i] = 0
		}
	}
	for i := 0; i < g.cfg.HistSeries; i++ {
		if g.churns(i) {
			for b := 0; b < histBuckets; b++ {
				g.histCounts[i*histBuckets+b] = 0
			}
			g.histSums[i] = 0
			g.histTotals[i] = 0
		}
	}
}

// churns reports whether series index i belongs to the churned subset.
func (g *MetricsGenerator) churns(i int) bool {
	return g.cfg.ChurnPercent > 0 && i%100 < g.cfg.ChurnPercent
}

// BeginTick advances all cumulative state to tick. Must be called once per
// tick, in increasing tick order, before reading any point of that tick.
func (g *MetricsGenerator) BeginTick(tick int) {
	for i := range g.counters {
		g.counters[i] += int64(1 + g.hash(uint64(i), uint64(tick), 0xc0)%50)
	}
	for i := 0; i < g.cfg.HistSeries; i++ {
		for b := 0; b < histBuckets; b++ {
			n := g.hash(uint64(i), uint64(tick), 0x80|uint64(b)) % 20
			g.histCounts[i*histBuckets+b] += n
			g.histTotals[i] += n
			g.histSums[i] += float64(n) * bucketMidpoint(g.histOffset(i)+int32(b))
		}
	}
}

// ScalarPoint returns the (name, labels, value, isCounter) of scalar series i
// at the current tick state. Gauge values are tick-local, counters cumulative.
func (g *MetricsGenerator) ScalarPoint(i, tick int) (string, SeriesLabels, int64, bool) {
	if i < g.CounterCount() {
		return MetricCounterName, g.labels(i), g.counters[i], true
	}
	v := int64(g.hash(uint64(i), uint64(tick), 0x6a) % 10_001)
	return MetricGaugeName, g.labels(i), v, false
}

// HistPoint returns the cumulative exponential-histogram sample of hist
// series i at the current tick state. Counts is a live view - encode before
// the next BeginTick.
func (g *MetricsGenerator) HistPoint(i int) (SeriesLabels, ExpHistPoint) {
	return g.labels(i), ExpHistPoint{
		Scale:  histScale,
		Offset: g.histOffset(i),
		Counts: g.histCounts[i*histBuckets : (i+1)*histBuckets],
		Sum:    g.histSums[i],
		Count:  g.histTotals[i],
	}
}

// histOffset spreads the per-series bucket window so quantiles differ across
// series (QM3 grouped quantiles would otherwise be a constant).
func (g *MetricsGenerator) histOffset(i int) int32 {
	return int32(g.hash(uint64(i), 0, 0x0f) % 16)
}

// bucketMidpoint is the geometric midpoint of exponential bucket idx at
// histScale: (2^(1/2^scale))^(idx+0.5).
func bucketMidpoint(idx int32) float64 {
	base := math.Exp2(1.0 / float64(int64(1)<<histScale))
	return math.Pow(base, float64(idx)+0.5)
}

// labels derives the series identity from its index. Uniqueness: (route,
// shard) = (i mod Routes, i div Routes) is unique per i; service and host are
// functions of i layered on top for realistic grouping cardinalities.
func (g *MetricsGenerator) labels(i int) SeriesLabels {
	l := SeriesLabels{
		Service: genServices[i%len(genServices)],
		Host:    genHosts[(i/10)%len(genHosts)],
		Route:   fmt.Sprintf("route-%04d", i%g.cfg.Routes),
		Shard:   fmt.Sprintf("shard-%03d", i/g.cfg.Routes),
		Gen:     -1,
	}
	if g.churns(i) {
		l.Gen = g.epoch
	}
	return l
}

// hash is a splitmix64-style mix over (seed, series, tick, salt) - the
// deterministic value source for everything non-cumulative.
func (g *MetricsGenerator) hash(i, tick, salt uint64) uint64 {
	x := g.cfg.Seed ^ i*0x9e3779b97f4a7c15 ^ tick<<24 ^ salt<<56
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
