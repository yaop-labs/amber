package obsbench

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Trace workload generator behind METHODOLOGY.md §5 (Traces): T1 ingest
// (~10 spans/trace), QT1 trace-ID lookup, QT2 service+operation search, QT3
// duration-threshold search. Like the metrics workload there is no dataset
// file — a trace is fully determined by (seed, trace index), so two runs with
// the same config produce byte-identical traces. That reproducibility is what
// lets the cross-system equality gate match instances across systems.
//
// The generator emits *relative* structure only: trace and span IDs,
// parent links, operations, per-span durations and error status. The absolute
// start time of each trace is stamped by the ingest runner at send time, so
// every system indexes the workload over its own wall clock (the same trick
// the metrics runner uses for tsNano). A query phase then selects sub-windows
// by fraction of the ingest window, never by absolute time.
//
// Two invariants keep the search gates honest and must hold when editing:
//
//   - Single service per trace. Every span of a trace carries the same
//     resource service.name, so QT2's `service+operation` predicate is
//     unambiguous (a Jaeger/TraceQL service filter selects traces that have a
//     span from that service; with one service per trace there is no
//     cross-service leakage).
//   - Disjoint root/child operation namespaces, and only the root span is
//     "long". QT2 searches a root operation (rootOpName) which no child span
//     can carry, so a name match resolves to exactly one trace role. QT3's
//     duration threshold (>= traceMinThreshold) sits above every child
//     duration, so only the root span can satisfy it. Both keep the matching
//     trace set a function of the generator alone.
const (
	traceRootOpCount  = 8
	traceChildOpCount = 12
	// childMaxDurationMS bounds every non-root span; traceMinThreshold (the
	// smallest QT3 threshold) must stay strictly above it so duration search
	// only ever matches root spans.
	childMaxDurationMS = 49
	traceMinThreshold  = 100 * time.Millisecond
	// rootMaxDurationMS bounds the root span; QT3 thresholds live inside
	// (traceMinThreshold, rootMaxDurationMS) so every threshold has matches.
	rootMaxDurationMS = 1000
)

// TracesGenConfig parameterizes the trace workload.
type TracesGenConfig struct {
	Seed uint64
	// SpansPerTrace is the span count of every trace (default 10 — METHODOLOGY
	// T1 "~10 spans/trace"). Fixed per run so QT1's span-count gate is exact.
	SpansPerTrace int
	// ErrorPercent of traces carry an error status on their root span (default
	// 5). Informational; no QT scenario gates on it.
	ErrorPercent int
}

func (c TracesGenConfig) withDefaults() TracesGenConfig {
	if c.SpansPerTrace <= 0 {
		c.SpansPerTrace = 10
	}
	if c.ErrorPercent == 0 {
		c.ErrorPercent = 5
	}
	return c
}

// GenSpan is one span's relative structure. ParentIndex is the index of the
// parent span within the same trace, or -1 for the root.
type GenSpan struct {
	Index       int
	ParentIndex int
	Operation   string
	Duration    time.Duration
	Error       bool
}

// IsRoot reports whether the span is the trace root.
func (s GenSpan) IsRoot() bool { return s.ParentIndex < 0 }

// TracesGenerator synthesizes the deterministic trace stream. Stateless beyond
// its config — every accessor is a pure function of (seed, trace index), so it
// is safe for concurrent use by the ingest workers.
type TracesGenerator struct {
	cfg TracesGenConfig
}

func NewTracesGenerator(cfg TracesGenConfig) *TracesGenerator {
	return &TracesGenerator{cfg: cfg.withDefaults()}
}

func (g *TracesGenerator) Config() TracesGenConfig { return g.cfg }
func (g *TracesGenerator) SpansPerTrace() int      { return g.cfg.SpansPerTrace }

// TraceID is the 16-byte trace identifier for trace i. QT1 recomputes it from
// (seed, i) to look the trace up, so this must stay stable for a given seed.
func (g *TracesGenerator) TraceID(i int) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[0:8], g.hash(uint64(i), 0, 0x71))
	binary.BigEndian.PutUint64(id[8:16], g.hash(uint64(i), 0, 0x72))
	return id
}

// SpanID is the 8-byte span identifier for span s of trace i. Non-zero by
// construction (the high salt byte guarantees it) so parent links never alias
// the zero "no parent" sentinel.
func (g *TracesGenerator) SpanID(i, span int) [8]byte {
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], g.hash(uint64(i), uint64(span)+1, 0x5a)|1)
	return id
}

// Service is the single resource service.name of trace i.
func (g *TracesGenerator) Service(i int) string {
	return genServices[g.hash(uint64(i), 0, 0x53)%uint64(len(genServices))]
}

// RootOperation is the operation name carried by trace i's root span — the
// QT2 search target.
func (g *TracesGenerator) RootOperation(i int) string {
	return rootOpName(int(g.hash(uint64(i), 0, 0x52) % traceRootOpCount))
}

// Trace returns the full relative structure of trace i: the root span at
// index 0 followed by SpansPerTrace-1 children, each attached to an earlier
// span so the result is a connected tree.
func (g *TracesGenerator) Trace(i int) []GenSpan {
	n := g.cfg.SpansPerTrace
	spans := make([]GenSpan, n)
	isErr := int(g.hash(uint64(i), 0, 0xe7)%100) < g.cfg.ErrorPercent
	spans[0] = GenSpan{
		Index:       0,
		ParentIndex: -1,
		Operation:   g.RootOperation(i),
		Duration:    time.Duration(1+g.hash(uint64(i), 0, 0xd0)%rootMaxDurationMS) * time.Millisecond,
		Error:       isErr,
	}
	for s := 1; s < n; s++ {
		// Parent is any earlier span: a deterministic tree, not a flat fan-out.
		parent := int(g.hash(uint64(i), uint64(s), 0xba) % uint64(s))
		spans[s] = GenSpan{
			Index:       s,
			ParentIndex: parent,
			Operation:   childOpName(int(g.hash(uint64(i), uint64(s), 0xc1) % traceChildOpCount)),
			Duration:    time.Duration(1+g.hash(uint64(i), uint64(s), 0xc2)%childMaxDurationMS) * time.Millisecond,
		}
	}
	return spans
}

// rootOpName and childOpName draw from disjoint namespaces (see the invariant
// note above): no child operation can ever equal a root operation.
func rootOpName(k int) string  { return fmt.Sprintf("HTTP GET /v%d/resource", k) }
func childOpName(k int) string { return fmt.Sprintf("internal.call.%d", k) }

// hash is a splitmix64-style mix over (seed, trace, span, salt) — the
// deterministic source for every non-structural value, matching the metrics
// generator's mixer so the two phases share one reproducibility story.
func (g *TracesGenerator) hash(i, span, salt uint64) uint64 {
	x := g.cfg.Seed ^ i*0x9e3779b97f4a7c15 ^ span<<24 ^ salt<<56
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
