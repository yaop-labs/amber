package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// generator drives an open-loop OTLP/HTTP ingest stream against amber.
//
// Open-loop discipline (the part that prevents coordinated omission):
// the scheduler issues "intended send time" Ti on a fixed cadence
// (Ti+1 = Ti + 1/rate), regardless of whether previous requests have
// completed. Each event lands on a bounded queue; workers drain it and
// record latency = now(at completion) - Ti. If the binary stalls, the
// queue back-pressures the scheduler and stall events accumulate proper
// latency — they don't disappear from the dataset.
//
// A common bug: latency = now(at completion) - now(at send dispatch).
// That hides the queueing delay caused by the stall; the binary looks
// faster than it is. We carefully do NOT do that.

type generator struct {
	cfg      config
	mix      *mix
	catalog  *catalog
	client   *http.Client
	startWall time.Time

	hist       *latencyHist
	sent       atomic.Uint64
	completed  atomic.Uint64
	errors     atomic.Uint64
	queueDrops atomic.Uint64

	// Per-worker counter used as latencyHist shard key, so observe calls
	// hash to distinct shards and we minimise mutex contention.
	workerIDs atomic.Uint64
}

type runResult struct {
	cfg          config
	startWall    time.Time
	endWall      time.Time
	warmupCutoff time.Time
	sent         uint64
	completed    uint64
	errors       uint64
	queueDrops   uint64
	rotations    uint64
	histAll      latencySnapshot
	histSteady   latencySnapshot
}

func newGenerator(cfg config, m *mix, cat *catalog, client *http.Client) *generator {
	return &generator{
		cfg:     cfg,
		mix:     m,
		catalog: cat,
		client:  client,
		hist:    newLatencyHist(cfg.Concurrency),
	}
}

func (g *generator) Run(ctx context.Context) runResult {
	g.startWall = time.Now()
	endWall := g.startWall.Add(g.cfg.Duration)
	warmupCutoff := g.startWall.Add(g.cfg.Warmup)

	// Two histograms: one all-time (for sanity), one only after warmup
	// (the reportable one). We achieve this by splitting on dispatch.
	histSteady := newLatencyHist(g.cfg.Concurrency)

	// Work queue: bounded so that scheduler back-pressure becomes visible
	// to the binary side. If we used an unbounded chan the scheduler
	// would run away while binary stalls and we'd allocate forever.
	queueCap := g.cfg.Concurrency * 4
	work := make(chan workItem, queueCap)

	var wg sync.WaitGroup
	for i := 0; i < g.cfg.Concurrency; i++ {
		wg.Add(1)
		workerID := g.workerIDs.Add(1)
		go g.worker(ctx, &wg, work, histSteady, warmupCutoff, workerID)
	}

	// Churn rotation in background.
	if g.cfg.ChurnRate > 0 && g.cfg.ChurnInterval > 0 {
		go g.churnLoop(ctx, endWall)
	}

	// Scheduler. Intended-send-time Ti = startWall + tick * reqInterval.
	// rate is REQUESTS per second, so reqInterval = 1/rate. Total samples/sec
	// = rate × batch — this matches how a real OTLP collector pushes (one
	// HTTP POST per batch, batch sized by collector config). We do NOT
	// divide rate by batch internally because then the schedule would only
	// fire (rate/batch) ticks/sec and absolute throughput in the report
	// would silently scale with batch size, which made the smoke test lie.
	reqInterval := time.Duration(float64(time.Second) / g.cfg.RatePerSec)
	if reqInterval <= 0 {
		reqInterval = time.Nanosecond
	}
	samplesPerReq := g.cfg.Batch

	var tick uint64
	rng := rand.New(rand.NewPCG(g.cfg.Seed, g.cfg.Seed*1009))

	// Reuse a single Timer instead of allocating one per tick via time.After.
	// On a tight rate (10k+ req/s) the timer-allocation noise was visible
	// in p99.
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		intendedAt := g.startWall.Add(reqInterval * time.Duration(tick))
		if intendedAt.After(endWall) {
			break
		}
		if d := time.Until(intendedAt); d > 0 {
			timer.Reset(d)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				goto done
			case <-timer.C:
			}
		} else if ctx.Err() != nil {
			break
		}

		item := workItem{
			intendedAt: intendedAt,
			batchSeed:  rng.Uint64(),
		}
		select {
		case work <- item:
			g.sent.Add(uint64(samplesPerReq))
		default:
			// Queue full: binary is stalling. We record the drop but do
			// NOT block the scheduler — blocking re-introduces the CO
			// bug from a different angle. The drop count itself is a
			// signal that the binary is past its knee.
			g.queueDrops.Add(uint64(samplesPerReq))
		}
		tick++
	}
done:
	close(work)
	wg.Wait()

	return runResult{
		cfg:          g.cfg,
		startWall:    g.startWall,
		endWall:      time.Now(),
		warmupCutoff: warmupCutoff,
		sent:         g.sent.Load(),
		completed:    g.completed.Load(),
		errors:       g.errors.Load(),
		queueDrops:   g.queueDrops.Load(),
		rotations:    g.catalog.Rotations(),
		histAll:      g.hist.Snapshot(),
		histSteady:   histSteady.Snapshot(),
	}
}

type workItem struct {
	intendedAt time.Time
	batchSeed  uint64
}

func (g *generator) worker(ctx context.Context, wg *sync.WaitGroup, work <-chan workItem, steady *latencyHist, warmupCutoff time.Time, workerID uint64) {
	defer wg.Done()
	rng := rand.New(rand.NewPCG(workerID, g.cfg.Seed^workerID))
	bodyBuf := bytes.NewBuffer(make([]byte, 0, 4096))

	for item := range work {
		if ctx.Err() != nil {
			return
		}
		bodyBuf.Reset()
		g.buildOTLPBatch(bodyBuf, item.batchSeed, rng)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, g.cfg.Addr+"/v1/metrics", bytes.NewReader(bodyBuf.Bytes()))
		if err != nil {
			g.errors.Add(uint64(g.cfg.Batch))
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := g.client.Do(req)
		now := time.Now()
		if err != nil {
			g.errors.Add(uint64(g.cfg.Batch))
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			g.errors.Add(uint64(g.cfg.Batch))
			continue
		}
		latency := now.Sub(item.intendedAt).Nanoseconds()
		g.hist.Observe(latency, workerID)
		if item.intendedAt.After(warmupCutoff) {
			steady.Observe(latency, workerID)
		}
		g.completed.Add(uint64(g.cfg.Batch))
	}
}

func (g *generator) churnLoop(ctx context.Context, endWall time.Time) {
	t := time.NewTicker(g.cfg.ChurnInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if now.After(endWall) {
				return
			}
			g.catalog.Rotate(g.cfg.ChurnRate)
		}
	}
}

// --- OTLP JSON shape (reused — and simpler — than cmd/loadgen since this
// harness does not need to mix traces/logs). We emit one batch per request,
// covering --batch samples across whatever series we sampled. service.name
// goes on Resource so amber's grouping path is exercised.

type otlpKV struct {
	Key   string  `json:"key"`
	Value otlpVal `json:"value"`
}
type otlpVal struct {
	StringValue string `json:"stringValue,omitempty"`
}
type otlpNumPoint struct {
	Attributes   []otlpKV `json:"attributes,omitempty"`
	TimeUnixNano string   `json:"timeUnixNano"`
	AsInt        string   `json:"asInt,omitempty"`
	AsDouble     *float64 `json:"asDouble,omitempty"`
}
type otlpSum struct {
	DataPoints             []otlpNumPoint `json:"dataPoints"`
	AggregationTemporality int            `json:"aggregationTemporality"`
	IsMonotonic            bool           `json:"isMonotonic"`
}
type otlpGauge struct {
	DataPoints []otlpNumPoint `json:"dataPoints"`
}
type otlpExpBuckets struct {
	Offset       int32    `json:"offset"`
	BucketCounts []uint64 `json:"bucketCounts"`
}
type otlpExpPoint struct {
	Attributes   []otlpKV       `json:"attributes,omitempty"`
	TimeUnixNano string         `json:"timeUnixNano"`
	Count        uint64         `json:"count"`
	Sum          *float64       `json:"sum,omitempty"`
	Scale        int32          `json:"scale"`
	Positive     otlpExpBuckets `json:"positive"`
}
type otlpExpHist struct {
	DataPoints             []otlpExpPoint `json:"dataPoints"`
	AggregationTemporality int            `json:"aggregationTemporality"`
}
type otlpMetric struct {
	Name                 string       `json:"name"`
	Sum                  *otlpSum     `json:"sum,omitempty"`
	Gauge                *otlpGauge   `json:"gauge,omitempty"`
	ExponentialHistogram *otlpExpHist `json:"exponentialHistogram,omitempty"`
}
type otlpScope struct {
	Metrics []otlpMetric `json:"metrics"`
}
type otlpResource struct {
	Resource     map[string]any `json:"resource"`
	ScopeMetrics []otlpScope    `json:"scopeMetrics"`
}
type otlpExport struct {
	ResourceMetrics []otlpResource `json:"resourceMetrics"`
}

func (g *generator) buildOTLPBatch(buf *bytes.Buffer, seed uint64, rng *rand.Rand) {
	now := time.Now()
	tsStr := fmt.Sprintf("%d", now.UnixNano())

	// Group points by service.name (matches what a real OTLP collector would
	// do) so per-resource handling code-paths fire.
	byService := make(map[string][]otlpMetric, 8)

	for i := 0; i < g.cfg.Batch; i++ {
		// Zipfian-ish skew: rank ~ floor(N / U^1.2). Heavy head, long tail.
		idx := zipfRank(g.cfg.Series, rng)
		lbl := g.catalog.Pick(idx)
		kind := g.mix.Pick(rng.Float64())
		metric := g.buildOne(lbl, kind, tsStr, rng, seed^uint64(i))
		byService[lbl.Service] = append(byService[lbl.Service], metric)
	}

	rms := make([]otlpResource, 0, len(byService))
	for svc, ms := range byService {
		rms = append(rms, otlpResource{
			Resource: map[string]any{
				"attributes": []otlpKV{{Key: "service.name", Value: otlpVal{StringValue: svc}}},
			},
			ScopeMetrics: []otlpScope{{Metrics: ms}},
		})
	}
	enc := json.NewEncoder(buf)
	_ = enc.Encode(otlpExport{ResourceMetrics: rms})
}

func (g *generator) buildOne(lbl seriesLabels, kind metricKind, tsStr string, rng *rand.Rand, salt uint64) otlpMetric {
	attrs := []otlpKV{
		{Key: "host", Value: otlpVal{StringValue: lbl.Host}},
		{Key: "region", Value: otlpVal{StringValue: lbl.Region}},
	}
	if lbl.Pod != "" {
		attrs = append(attrs, otlpKV{Key: "pod", Value: otlpVal{StringValue: lbl.Pod}})
	}
	switch kind {
	case kindCounter:
		// Monotonically increasing value derived from salt so the same series
		// gets a deterministic but advancing stream. Real rate() over this
		// will be ~stable for stable series, which is what the dashboard
		// query in 3.5 expects.
		val := int64(salt & 0x0FFFFFFF)
		return otlpMetric{
			Name: "harness_requests_total",
			Sum: &otlpSum{
				AggregationTemporality: 2, IsMonotonic: true,
				DataPoints: []otlpNumPoint{{
					Attributes:   attrs,
					TimeUnixNano: tsStr,
					AsInt:        fmt.Sprintf("%d", val),
				}},
			},
		}
	case kindGauge:
		v := 50 + rng.Float64()*50
		return otlpMetric{
			Name: "harness_cpu_usage",
			Gauge: &otlpGauge{
				DataPoints: []otlpNumPoint{{
					Attributes:   attrs,
					TimeUnixNano: tsStr,
					AsDouble:     &v,
				}},
			},
		}
	default:
		// Exp histogram. Cheap shape: 8 positive buckets weighted toward the
		// centre. Sum derived from rng so summary/synopsis vary.
		sum := 0.5 + rng.Float64()
		return otlpMetric{
			Name: "harness_latency_seconds",
			ExponentialHistogram: &otlpExpHist{
				AggregationTemporality: 2,
				DataPoints: []otlpExpPoint{{
					Attributes:   attrs,
					TimeUnixNano: tsStr,
					Count:        30,
					Sum:          &sum,
					Scale:        2,
					Positive: otlpExpBuckets{
						Offset:       int32(rng.IntN(4)),
						BucketCounts: []uint64{1, 2, 4, 8, 8, 4, 2, 1},
					},
				}},
			},
		}
	}
}

// zipfRank returns a series index in [0, n) with heavier weight on small
// indices. Inverse transform on a u^0.6 distribution — heavy head, long
// tail. Not a real Zipf, just a smooth skew good enough for "hot core +
// tail" workloads.
func zipfRank(n int, rng *rand.Rand) int {
	if n <= 1 {
		return 0
	}
	u := rng.Float64()
	idx := int((1.0 - math.Pow(u, 0.6)) * float64(n))
	if idx >= n {
		idx = n - 1
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}
