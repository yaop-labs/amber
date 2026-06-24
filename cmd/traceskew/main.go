// Command traceskew stress-tests the trace query path under a REALISTIC,
// SKEWED workload - the shape the uniform obsbench dataset hides. It generates
// traces with a Zipfian service distribution (a few hot services, a long tail of
// rare ones) and power-law spans-per-trace, ingests them into a live amber over
// OTLP/HTTP, then queries /api/v1/traces for services across the rarity spectrum
// and reports latency per bucket. The question it answers: does the covering-
// index + per-trace early-termination win hold for a HOT service (huge fraction
// of spans) and a RARE one (must scan many segments to find a page of traces),
// or does it degrade at the tail?
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"time"

	"google.golang.org/protobuf/proto"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func main() {
	addr := flag.String("addr", "http://127.0.0.1:18800", "amber base URL")
	nTraces := flag.Int("traces", 300_000, "total traces to ingest")
	nServices := flag.Int("services", 50, "distinct services")
	zipfS := flag.Float64("zipf", 1.1, "Zipf skew exponent (higher = more skewed)")
	maxSpans := flag.Int("max-spans", 200, "power-law cap on spans per trace")
	seed := flag.Int64("seed", 1, "rng seed")
	batchTraces := flag.Int("batch", 200, "traces per OTLP request")
	qIters := flag.Int("q-iters", 30, "query iterations per probed service")
	settle := flag.Duration("settle", 8*time.Second, "wait after ingest before querying")
	flag.Parse()

	rng := rand.New(rand.NewSource(*seed))
	svcZipf := rand.NewZipf(rng, *zipfS, 1.0, uint64(*nServices-1))
	spanZipf := rand.NewZipf(rng, 1.6, 1.0, uint64(*maxSpans-1))

	services := make([]string, *nServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%03d", i)
	}
	spanShare := make([]int64, *nServices) // spans emitted per service
	traceShare := make([]int64, *nServices)

	client := &http.Client{Timeout: 30 * time.Second}
	base := time.Now().Add(-90 * time.Minute).UnixNano()
	window := int64(80 * time.Minute)
	step := window / int64(*nTraces)
	if step <= 0 {
		step = 1
	}

	fmt.Printf("ingesting %d traces, %d services, zipf=%.2f, max-spans=%d -> %s\n",
		*nTraces, *nServices, *zipfS, *maxSpans, *addr)
	start := time.Now()
	var totalSpans int64

	batch := make(map[string][]*tracepb.Span) // service -> spans, per request
	flush := func() {
		if len(batch) == 0 {
			return
		}
		rs := make([]*tracepb.ResourceSpans, 0, len(batch))
		for svc, spans := range batch {
			rs = append(rs, &tracepb.ResourceSpans{
				Resource:   &resourcepb.Resource{Attributes: []*commonpb.KeyValue{strAttr("service.name", svc)}},
				ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
			})
		}
		body, err := proto.Marshal(&collectortrace.ExportTraceServiceRequest{ResourceSpans: rs})
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}
		post(client, *addr+"/v1/traces", body)
		batch = make(map[string][]*tracepb.Span)
	}

	for ti := 0; ti < *nTraces; ti++ {
		traceStart := base + int64(ti)*step
		rootSvc := int(svcZipf.Uint64())
		traceShare[rootSvc]++

		nSpans := int(spanZipf.Uint64()) + 1
		tid := make([]byte, 16)
		rng.Read(tid)
		rootID := make([]byte, 8)
		rng.Read(rootID)

		for s := 0; s < nSpans; s++ {
			svc := rootSvc
			if s > 0 && rng.Float64() > 0.6 {
				svc = int(svcZipf.Uint64()) // fan-out to another service
			}
			spanShare[svc]++
			totalSpans++
			sid := make([]byte, 8)
			rng.Read(sid)
			var parent []byte
			if s > 0 {
				parent = rootID
			}
			off := int64(s) * int64(time.Millisecond)
			dur := int64(rng.Intn(20)+1) * int64(time.Millisecond)
			status := tracepb.Status_STATUS_CODE_OK
			if rng.Intn(20) == 0 {
				status = tracepb.Status_STATUS_CODE_ERROR
			}
			batch[services[svc]] = append(batch[services[svc]], &tracepb.Span{
				TraceId:           tid,
				SpanId:            sid,
				ParentSpanId:      parent,
				Name:              fmt.Sprintf("op-%d", s%8),
				StartTimeUnixNano: uint64(traceStart + off),
				EndTimeUnixNano:   uint64(traceStart + off + dur),
				Status:            &tracepb.Status{Code: status},
			})
		}
		if (ti+1)%*batchTraces == 0 {
			flush()
		}
	}
	flush()
	fmt.Printf("ingested %d spans in %s (%.0f spans/s)\n",
		totalSpans, time.Since(start).Round(time.Millisecond),
		float64(totalSpans)/time.Since(start).Seconds())

	fmt.Printf("settling %s...\n", *settle)
	time.Sleep(*settle)

	// Probe services across the rarity spectrum by span share.
	ranks := bySpanShare(spanShare)
	probes := pickProbes(ranks)
	fmt.Printf("\n%-10s %8s %8s   %8s %8s %8s   %s\n",
		"service", "spans%", "traces", "p50ms", "p95ms", "p99ms", "returned")
	for _, idx := range probes {
		svc := services[idx]
		lats, returned := probe(client, *addr, svc, *qIters)
		if len(lats) == 0 {
			continue
		}
		sort.Float64s(lats)
		fmt.Printf("%-10s %7.2f%% %8d   %8.2f %8.2f %8.2f   %d\n",
			svc, 100*float64(spanShare[idx])/float64(totalSpans), traceShare[idx],
			lats[len(lats)/2], pct(lats, 0.95), pct(lats, 0.99), returned)
	}
}

func strAttr(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func post(c *http.Client, url string, body []byte) {
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := c.Do(req)
	if err != nil {
		log.Fatalf("post: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Fatalf("post status %d", resp.StatusCode)
	}
}

// probe issues iters trace-search queries for one service and returns the
// latencies (ms) and the trace count from the last response.
func probe(c *http.Client, addr, svc string, iters int) ([]float64, int) {
	q := addr + "/api/v1/traces?limit=20&service=" + url.QueryEscape(svc)
	lats := make([]float64, 0, iters)
	returned := 0
	for i := 0; i < iters; i++ {
		t0 := time.Now()
		resp, err := c.Get(q)
		if err != nil {
			log.Printf("query %s: %v", svc, err)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
		returned = bytes.Count(b, []byte(`"trace_id"`))
	}
	return lats, returned
}

func bySpanShare(share []int64) []int {
	idx := make([]int, len(share))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return share[idx[a]] > share[idx[b]] })
	return idx
}

// pickProbes selects a spread across the rarity spectrum: hottest, a few mid,
// and the rarest with at least one trace.
func pickProbes(ranks []int) []int {
	if len(ranks) == 0 {
		return nil
	}
	picks := []int{ranks[0], ranks[len(ranks)/4], ranks[len(ranks)/2], ranks[3*len(ranks)/4], ranks[len(ranks)-1]}
	seen := map[int]bool{}
	out := make([]int, 0, len(picks))
	for _, p := range picks {
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out
}

func pct(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}
