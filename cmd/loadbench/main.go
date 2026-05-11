// Command loadbench drives a single-node Amber stack with a deterministic
// N-record synthetic workload and reports write throughput, idle memory after
// sealing, and read-latency percentiles across five canonical query shapes.
//
// Intended for regression detection across releases, not cross-product
// comparison. For that, drive the public API via cmd/loadgen.
//
// Usage:
//
//	go run ./cmd/loadbench -n 10000000 -tmpdir /mnt/scratch
//	go run ./cmd/loadbench -n 100000 -o smoke.txt                     # smoke run
//	go run ./cmd/loadbench -n 10000000 -restart                       # cold-start RSS
//	go run ./cmd/loadbench -n 10000000 -warmup-iters 10               # stable p99
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/query"
	rt "github.com/hnlbs/amber/internal/runtime"
)

var (
	flagN           = flag.Int("n", 10_000_000, "number of records to ingest")
	flagBatch       = flag.Int("batch", 1024, "Batcher.BatchSize (records flushed per WriteBatch)")
	flagQueue       = flag.Int("queue", 64*1024, "Batcher.QueueSize")
	flagTmpDir      = flag.String("tmpdir", "", "data dir (default: OS tmpdir, removed at exit)")
	flagQueryIters  = flag.Int("query-iters", 100, "iterations per query scenario")
	flagSeed        = flag.Uint64("seed", 42, "PRNG seed for record content")
	flagServices    = flag.Int("services", 5, "cardinality of service field")
	flagHosts       = flag.Int("hosts", 20, "cardinality of host field")
	flagSealWait    = flag.Duration("seal-wait", 60*time.Second, "max wait for all sealed segments to have indexes built")
	flagOut         = flag.String("o", "", "output file (default: stdout)")
	flagKeep        = flag.Bool("keep", false, "do not delete tmpdir at exit")
	flagRestart     = flag.Bool("restart", false, "close+reopen engine before M/R phases; reports cold-start RSS instead of post-ingest RSS")
	flagWarmupIters = flag.Int("warmup-iters", 5, "dry-run query iterations before timing; primes segment reader handles and settles GC")
)

const (
	// Body templates all carry the common token "request" — searched in R3.
	// Templates avoid overlap so FTS index isn't degenerate.
	commonToken = "request"
	// Rare token planted in rareTokenRate fraction of bodies — searched in R4.
	rareToken     = "anomalous-pattern-7f2"
	rareTokenRate = 0.001 // 0.1% — selective enough that FTS lookup beats scan
)

var bodyTemplates = []string{
	"GET /api/v1/users request handled in 45ms response 200",
	"POST /api/v1/orders request validated bill total 199",
	"redis cache miss request reload key user_42 latency 12ms",
	"postgres slow request 1240ms tx_id 99 conn pool full",
	"worker dequeued request job batch process 5000 items",
	"scheduler tick request rebalance shards 7 nodes 3",
	"auth request token verified subject api-gateway scope read",
	"payment request authorized card last4 4242 amount 89usd",
	"notification request email queued recipient user_88",
	"billing request charge subscription tier pro renew monthly",
}

func main() {
	flag.Parse()

	dir := *flagTmpDir
	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "amber-loadbench-")
		if err != nil {
			fatal("mkdir tmp: %v", err)
		}
		if !*flagKeep {
			defer func() { _ = os.RemoveAll(dir) }()
		}
	}

	out := io.Writer(os.Stdout)
	if *flagOut != "" {
		f, err := os.Create(*flagOut)
		if err != nil {
			fatal("create %s: %v", *flagOut, err)
		}
		defer f.Close()
		out = f
	}

	report := &reporter{w: out}
	report.printf("loadbench: n=%d batch=%d queue=%d services=%d hosts=%d seed=%d restart=%v warmup=%d\n",
		*flagN, *flagBatch, *flagQueue, *flagServices, *flagHosts, *flagSeed, *flagRestart, *flagWarmupIters)
	report.printf("data dir: %s (keep=%v)\n\n", dir, *flagKeep)

	// Logger discards everything except warnings/errors so we don't pollute
	// stdout. Errors still surface for triage.
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	opts := rt.Options{
		DataDir: dir,
		Logger:  logger,
		Ingest: rt.IngestOptions{
			BatchSize:    *flagBatch,
			BatchTimeout: 100 * time.Millisecond,
			QueueSize:    *flagQueue,
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stack, err := rt.New(ctx, opts)
	if err != nil {
		cancel()
		fatal("rt.New: %v", err)
	}

	// W — Write throughput.
	w := runWrite(stack, *flagN, *flagServices, *flagHosts, *flagSeed)
	report.section("W — Write throughput", w.lines())

	// Force-seal active segment so the dataset is fully sealed before queries.
	// Then wait for onSeal index builds to land .bidx/.fidx/.filt on disk.
	if err := stack.LogManager.Rotate(); err != nil {
		fatal("rotate: %v", err)
	}
	waited, sealedSegs := waitForSealing(stack, *flagSealWait)
	report.printf("seal wait: %v across %d sealed segments\n\n", waited.Round(time.Millisecond), sealedSegs)

	// -restart: close and reopen the engine so bootstrap re-loads indexes from
	// disk. M and R then reflect cold-start state, not the post-ingest process
	// that already has everything in memory from the write phase. Without this,
	// VmRSS is dominated by ingest-time allocations that have not been GC'd yet.
	if *flagRestart {
		cancel()
		closeCtx, cancelClose := context.WithTimeout(context.Background(), 60*time.Second)
		if err := stack.Close(closeCtx); err != nil {
			fmt.Fprintf(os.Stderr, "restart close: %v\n", err)
		}
		cancelClose()

		var newErr error
		ctx, cancel = context.WithCancel(context.Background())
		stack, newErr = rt.New(ctx, opts)
		if newErr != nil {
			cancel()
			fatal("rt.New (restart): %v", newErr)
		}
		report.printf("restart: engine reopened (cold-start RSS follows)\n\n")
	}

	// M — Storage + idle RSS after GC.
	m := measureMemory(stack, dir, *flagRestart)
	report.section("M — Storage and idle memory", m.lines())

	// R — Read latency p50/p99 across 5 scenarios.
	r := runReads(stack, *flagQueryIters, *flagWarmupIters, w)
	report.section("R — Read latency", r.lines())

	cancel()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancelClose()
	if err := stack.Close(closeCtx); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
	}
}

// --- Phase W -----------------------------------------------------------------

type writeResult struct {
	n          int
	duration   time.Duration
	dropped    int
	throughput float64
	baseTS     int64
	spacingNs  int64
}

func (w writeResult) lines() []string {
	return []string{
		fmt.Sprintf("ingested:   %d records", w.n),
		fmt.Sprintf("duration:   %v", w.duration.Round(time.Millisecond)),
		fmt.Sprintf("throughput: %.0f rec/s", w.throughput),
		fmt.Sprintf("dropped:    %d (queue_full or rejected)", w.dropped),
	}
}

func runWrite(stack *rt.Stack, n, nServices, nHosts int, seed uint64) writeResult {
	services := make([]string, nServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%02d", i)
	}
	levels := []model.Level{model.LevelDebug, model.LevelInfo, model.LevelWarn, model.LevelError, model.LevelFatal}

	// PCG seeded deterministically; only used for rare-token sprinkle.
	// Not a security context — math/rand/v2 is correct here.
	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15)) //nolint:gosec

	// Stretch the dataset across 24h synthetic time so R2's 1h time-range
	// query selects a meaningful sub-slice. Spacing in ns is exact.
	const totalSpan = 24 * time.Hour
	spacingNs := int64(totalSpan) / int64(n)
	if spacingNs < 1 {
		spacingNs = 1
	}
	baseTS := time.Now().Add(-totalSpan).UnixNano()
	dropped := 0
	t0 := time.Now()

	for i := 0; i < n; i++ {
		entry := generateEntry(i, services, nHosts, levels, baseTS, spacingNs, rng)
		// Retry on transient queue_full so producer doesn't outrun consumer
		// at the rim. Bounded so a hard wedge doesn't hang the bench.
		var err error
		for attempt := 0; attempt < 1000; attempt++ {
			err = stack.Batcher.SendLog(entry)
			if err == nil {
				break
			}
			time.Sleep(20 * time.Microsecond)
		}
		if err != nil {
			dropped++
		}
	}

	// Drain: poll until queue is empty for two consecutive checks. processBatch
	// might still be mid-flight after queue drains; the second-empty observation
	// covers that without a fixed sleep.
	drainStart := time.Now()
	emptyHits := 0
	for time.Since(drainStart) < 5*time.Minute {
		if stack.Batcher.QueueLen() == 0 {
			emptyHits++
			if emptyHits >= 2 {
				break
			}
		} else {
			emptyHits = 0
		}
		time.Sleep(50 * time.Millisecond)
	}

	dur := time.Since(t0)
	return writeResult{
		n:          n,
		duration:   dur,
		dropped:    dropped,
		throughput: float64(n) / dur.Seconds(),
		baseTS:     baseTS,
		spacingNs:  spacingNs,
	}
}

func generateEntry(idx int, services []string, nHosts int, levels []model.Level, baseTS, spacingNs int64, rng *rand.Rand) model.LogEntry {
	var id model.EntryID
	binary.BigEndian.PutUint64(id[8:], uint64(idx))

	// Every 100th entry shares the same trace_id (target for R5). Rest get
	// unique trace_ids. Both are deterministic from idx.
	var traceID model.TraceID
	if idx%100 == 0 {
		binary.BigEndian.PutUint64(traceID[8:], 1) // target trace
	} else {
		binary.BigEndian.PutUint64(traceID[8:], uint64(idx)+1<<32)
	}

	body := bodyTemplates[idx%len(bodyTemplates)]
	if rng.Float64() < rareTokenRate {
		body = body + " " + rareToken
	}

	// Service and level use co-prime strides so the (service, level) cross
	// product is fully covered — otherwise idx%5 for both gives only 5 pairs
	// out of 25 possible.
	return model.LogEntry{
		ID:        id,
		Timestamp: time.Unix(0, baseTS+int64(idx)*spacingNs),
		Level:     levels[(idx/len(services))%len(levels)],
		Service:   services[idx%len(services)],
		Host:      fmt.Sprintf("host-%03d", idx%nHosts),
		Body:      body,
		TraceID:   traceID,
		Attrs:     []model.Attr{{Key: "env", Value: "prod"}},
	}
}

// --- Sealing wait ------------------------------------------------------------

// waitForSealing polls until every sealed segment has its .bidx and .fidx
// sidecars on disk, or the deadline expires. onSeal runs in a goroutine and
// has no public completion signal — checking file presence is the simplest
// race-free way to know index builds are done.
//
// .filt (bitmap ribbon) and .fts.filt are prefilters; their absence makes
// queries slower but not incorrect, so we don't gate on them.
func waitForSealing(stack *rt.Stack, timeout time.Duration) (time.Duration, int) {
	t0 := time.Now()
	for {
		segs := stack.LogManager.Segments() // returns sealed only
		ready := 0
		for _, s := range segs {
			segPath := filepath.Join(stack.LogDir, s.FileName)
			if exists(segPath+".bidx") && exists(segPath+".fidx") {
				ready++
			}
		}
		if len(segs) > 0 && ready == len(segs) {
			return time.Since(t0), len(segs)
		}
		if time.Since(t0) > timeout {
			fmt.Fprintf(os.Stderr, "warn: seal wait timed out, %d/%d segments ready\n", ready, len(segs))
			return time.Since(t0), len(segs)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// --- Phase M -----------------------------------------------------------------

type memResult struct {
	segBytes     int64
	walBytes     int64
	indexBytes   int64
	heapAlloc    uint64
	heapInuse    uint64
	sysBytes     uint64
	vmRSS        uint64
	gcCycles     uint32
	gcPauseTotal time.Duration
	// cold is true when measured after a close+reopen cycle (-restart flag).
	// False means post-ingest: bootstrap indexes are still hot from the write
	// phase and VmRSS is inflated relative to a real cold start.
	cold bool
}

func (m memResult) lines() []string {
	out := []string{
		fmt.Sprintf("segments:   %s", humanBytes(m.segBytes)),
		fmt.Sprintf("indexes:    %s (bidx + fidx + filt + fts.filt sidecars)", humanBytes(m.indexBytes)),
		fmt.Sprintf("wal:        %s", humanBytes(m.walBytes)),
		fmt.Sprintf("heap alloc: %s", humanBytes(int64(m.heapAlloc))),
		fmt.Sprintf("heap inuse: %s", humanBytes(int64(m.heapInuse))),
		fmt.Sprintf("sys total:  %s", humanBytes(int64(m.sysBytes))),
	}
	if m.vmRSS > 0 {
		rssLabel := "post-ingest"
		if m.cold {
			rssLabel = "cold-start"
		}
		out = append(out, fmt.Sprintf("VmRSS:      %s (%s, linux /proc)", humanBytes(int64(m.vmRSS)), rssLabel))
	}
	out = append(out, fmt.Sprintf("gc:         %d cycles, %v total pause", m.gcCycles, m.gcPauseTotal.Round(time.Microsecond)))
	return out
}

func measureMemory(stack *rt.Stack, dir string, cold bool) memResult {
	// Two GCs are the standard idiom for "settle" — the first frees young
	// garbage, the second sweeps newly-promoted objects. FreeOSMemory hints to
	// the OS to return pages so VmRSS reflects live state, not high-water.
	runtime.GC()
	runtime.GC()
	debug.FreeOSMemory()

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	r := memResult{
		heapAlloc:    ms.HeapAlloc,
		heapInuse:    ms.HeapInuse,
		sysBytes:     ms.Sys,
		gcCycles:     ms.NumGC,
		gcPauseTotal: time.Duration(ms.PauseTotalNs),
		vmRSS:        readRSS(),
		cold:         cold,
	}

	// Walk dir to size segments / indexes / WAL.
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		size := info.Size()
		name := info.Name()
		switch {
		case strings.HasSuffix(name, ".alog"):
			r.segBytes += size
		case strings.HasSuffix(name, ".bidx"), strings.HasSuffix(name, ".fidx"),
			strings.HasSuffix(name, ".filt"), strings.HasSuffix(name, ".fts.filt"):
			r.indexBytes += size
		case name == "amber.wal":
			r.walBytes += size
		}
		return nil
	})
	_ = stack
	return r
}

// readRSS returns resident set size from /proc/self/status on linux, or 0
// elsewhere. Falls back to 0 silently — we report MemStats unconditionally.
func readRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		var kb uint64
		_, _ = fmt.Sscanf(fields[1], "%d", &kb)
		return kb * 1024
	}
	return 0
}

// --- Phase R -----------------------------------------------------------------

type queryStats struct {
	name       string
	hits       int
	segScanned int
	segTotal   int
	p50, p99   time.Duration
}

func (q queryStats) line() string {
	return fmt.Sprintf("%-30s p50=%-9v p99=%-9v hits=%-6d seg=%d/%d",
		q.name, q.p50.Round(time.Microsecond), q.p99.Round(time.Microsecond),
		q.hits, q.segScanned, q.segTotal)
}

type readsResult struct {
	scenarios []queryStats
}

func (r readsResult) lines() []string {
	out := make([]string, 0, len(r.scenarios))
	for _, s := range r.scenarios {
		out = append(out, s.line())
	}
	return out
}

func runReads(stack *rt.Stack, iters, warmup int, w writeResult) readsResult {
	// Dataset spans [baseTS, baseTS + n*spacingNs]. Take a 1h window from the
	// middle so R2 actually exercises pruning instead of covering everything
	// or nothing.
	dataFrom := time.Unix(0, w.baseTS)
	midOffset := time.Duration(w.spacingNs * int64(w.n) / 2)
	queryFrom := dataFrom.Add(midOffset - 30*time.Minute)
	queryTo := dataFrom.Add(midOffset + 30*time.Minute)

	var targetTrace model.TraceID
	binary.BigEndian.PutUint64(targetTrace[8:], 1)

	scenarios := []struct {
		name string
		q    *query.LogQuery
	}{
		{"R1 service+level (bitmap AND)", &query.LogQuery{Services: []string{"svc-00"}, Levels: []string{"ERROR"}, Limit: 100}},
		{"R2 time range 1h slice", &query.LogQuery{From: queryFrom, To: queryTo, Limit: 100}},
		{"R3 FTS common token", &query.LogQuery{FullText: commonToken, Limit: 100}},
		{"R4 FTS rare token", &query.LogQuery{FullText: rareToken, Limit: 100}},
		{"R5 trace_id lookup", &query.LogQuery{TraceID: targetTrace, Limit: 100}},
	}

	out := readsResult{scenarios: make([]queryStats, 0, len(scenarios))}
	for _, sc := range scenarios {
		out.scenarios = append(out.scenarios, runScenario(stack, sc.name, sc.q, iters, warmup))
	}
	return out
}

// runScenario measures query latency over iters cold executions.
//
// warmup dry-run iterations run first (with ClearResultCache each time) to
// prime segment reader handles and let Go's allocator reach steady state.
// These iterations are discarded. The measured iters still clear the result
// cache between each run so they reflect cold query execution, not cache hits.
func runScenario(stack *rt.Stack, name string, q *query.LogQuery, iters, warmup int) queryStats {
	for i := 0; i < warmup; i++ {
		stack.Executor.ClearResultCache()
		_, _ = stack.Executor.ExecLog(context.Background(), q)
	}

	samples := make([]time.Duration, 0, iters)
	var lastResult *query.LogResult
	for i := 0; i < iters; i++ {
		stack.Executor.ClearResultCache()
		t0 := time.Now()
		res, err := stack.Executor.ExecLog(context.Background(), q)
		dur := time.Since(t0)
		if err != nil {
			fmt.Fprintf(os.Stderr, "warn: %s: %v\n", name, err)
			continue
		}
		samples = append(samples, dur)
		lastResult = res
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	out := queryStats{name: name}
	if len(samples) > 0 {
		out.p50 = samples[len(samples)*50/100]
		out.p99 = samples[len(samples)*99/100]
	}
	if lastResult != nil {
		out.hits = lastResult.TotalHits
		out.segScanned = lastResult.SegScanned
		out.segTotal = lastResult.SegTotal
	}
	return out
}

// --- Output helpers ----------------------------------------------------------

type reporter struct{ w io.Writer }

// Output writes ignore errors: writer is stdout or a freshly-opened file,
// failures there don't affect bench correctness and there's nothing to recover.
func (r *reporter) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(r.w, format, args...)
}

func (r *reporter) section(title string, lines []string) {
	_, _ = fmt.Fprintf(r.w, "%s\n", title)
	_, _ = fmt.Fprintf(r.w, "%s\n", strings.Repeat("-", len(title)))
	for _, l := range lines {
		_, _ = fmt.Fprintf(r.w, "  %s\n", l)
	}
	_, _ = fmt.Fprintln(r.w)
}

func humanBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "loadbench: "+format+"\n", args...)
	os.Exit(1)
}
