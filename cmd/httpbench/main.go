// Command httpbench measures amber ingest and query throughput via HTTP,
// using the same synthetic dataset as loadbench so results are comparable
// to lokibench. This gives an apples-to-apples comparison with Loki
// (both go through HTTP + JSON serialization).
//
// Usage:
//
//	go run ./cmd/amber /tmp/amber-bench-server.yaml &
//	go run ./cmd/httpbench -n 10000000 -amber http://localhost:4317
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"sort"
	"time"
)

var (
	flagN        = 10_000_000
	flagBatch    = 1000
	flagAmber    = "http://localhost:4317"
	flagServices = 5
	flagHosts    = 20
	flagSeed     = uint64(42)
)

const (
	commonToken   = "request"
	rareToken     = "anomalous-pattern-7f2"
	rareTokenRate = 0.001
	totalSpan     = 24 * time.Hour
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

var levelNames = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

type logRecord struct {
	Level   string `json:"level"`
	Service string `json:"service"`
	Host    string `json:"host"`
	Body    string `json:"body"`
}

func main() {
	for i := 1; i < len(os.Args)-1; i++ {
		switch os.Args[i] {
		case "-n":
			fmt.Sscanf(os.Args[i+1], "%d", &flagN) //nolint:errcheck
		case "-amber":
			flagAmber = os.Args[i+1]
		case "-batch":
			fmt.Sscanf(os.Args[i+1], "%d", &flagBatch) //nolint:errcheck
		}
	}

	fmt.Printf("httpbench (amber via HTTP): n=%d batch=%d amber=%s\n\n",
		flagN, flagBatch, flagAmber)

	services := make([]string, flagServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%02d", i)
	}

	rng := rand.New(rand.NewPCG(flagSeed, flagSeed^0x9E3779B97F4A7C15)) //nolint:gosec

	spacingNs := int64(totalSpan) / int64(flagN)
	baseTS := time.Now().Add(-totalSpan).UnixNano()

	client := &http.Client{Timeout: 30 * time.Second}

	// W — Write throughput via HTTP POST /api/v1/logs.
	fmt.Println("W — Write throughput (HTTP)")
	fmt.Println("--------------------------")

	dropped := 0
	t0 := time.Now()

	buf := make([]logRecord, 0, flagBatch)
	for i := 0; i < flagN; i++ {
		svc := services[i%flagServices]
		level := levelNames[(i/flagServices)%len(levelNames)]
		host := fmt.Sprintf("host-%03d", i%flagHosts)
		body := bodyTemplates[i%len(bodyTemplates)]
		if rng.Float64() < rareTokenRate {
			body = body + " " + rareToken
		}

		buf = append(buf, logRecord{
			Level:   level,
			Service: svc,
			Host:    host,
			Body:    body,
		})

		if len(buf) >= flagBatch || i == flagN-1 {
			payload, _ := json.Marshal(buf)
			resp, err := client.Post(flagAmber+"/api/v1/logs", "application/json", bytes.NewReader(payload))
			if err != nil {
				fmt.Fprintf(os.Stderr, "push [%d]: %v\n", i, err)
				dropped += len(buf)
			} else {
				io.Copy(io.Discard, resp.Body) //nolint:errcheck
				resp.Body.Close()
				if resp.StatusCode != 202 {
					dropped += len(buf)
				}
			}
			buf = buf[:0]
		}
	}

	dur := time.Since(t0)
	fmt.Printf("  ingested:   %d records\n", flagN-dropped)
	fmt.Printf("  duration:   %v\n", dur.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f rec/s\n", float64(flagN-dropped)/dur.Seconds())
	fmt.Printf("  dropped:    %d\n\n", dropped)

	// Drain — wait for batcher to flush.
	fmt.Println("draining batcher...")
	time.Sleep(2 * time.Second)

	// Force rotate via admin.
	resp, _ := client.Post(flagAmber+"/api/v1/admin/rotate", "application/json", nil)
	if resp != nil {
		resp.Body.Close()
	}
	time.Sleep(5 * time.Second)

	// R — Read latency via GET /api/v1/logs.
	fmt.Println("R — Read latency (HTTP, 100 iterations)")
	fmt.Println("----------------------------------------")

	midOffset := time.Duration(spacingNs * int64(flagN) / 2)
	dataFrom := time.Unix(0, baseTS)
	queryFrom := dataFrom.Add(midOffset - 30*time.Minute)
	queryTo := dataFrom.Add(midOffset + 30*time.Minute)

	scenarios := []struct {
		name   string
		params url.Values
	}{
		{"R1 service+level (bitmap AND)", url.Values{
			"service": {"svc-00"}, "level": {"ERROR"}, "limit": {"100"},
		}},
		{"R2 time range 1h slice", url.Values{
			"from":  {fmt.Sprintf("%d", queryFrom.UnixNano())},
			"to":    {fmt.Sprintf("%d", queryTo.UnixNano())},
			"limit": {"100"},
		}},
		{"R3 FTS common token", url.Values{
			"q": {commonToken}, "limit": {"100"},
		}},
		{"R4 FTS rare token", url.Values{
			"q": {rareToken}, "limit": {"100"},
		}},
		{"R5 host lookup", url.Values{
			"host": {"host-000"}, "limit": {"100"},
		}},
	}

	const iters = 100
	for _, sc := range scenarios {
		samples := make([]time.Duration, 0, iters)
		var lastHits int

		for i := 0; i < iters; i++ {
			qURL := flagAmber + "/api/v1/logs?" + sc.params.Encode()
			t0 := time.Now()
			resp, err := client.Get(qURL)
			elapsed := time.Since(t0)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var result struct {
				TotalHits int `json:"total_hits"`
			}
			_ = json.Unmarshal(body, &result)
			lastHits = result.TotalHits
			samples = append(samples, elapsed)
		}

		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		p50 := samples[len(samples)*50/100]
		p99 := samples[len(samples)*99/100]
		fmt.Printf("  %-36s p50=%-9v p99=%-9v hits=%d\n",
			sc.name, p50.Round(time.Microsecond), p99.Round(time.Microsecond), lastHits)
	}
	fmt.Println()

	// Storage footprint.
	fmt.Println("M — Storage")
	fmt.Println("-----------")
	printDirSize("/tmp/amber-http-bench")
}

func printDirSize(dir string) {
	var total int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		info, _ := e.Info()
		if info != nil {
			total += info.Size()
		}
	}
	fmt.Printf("  data dir:   %s\n\n", humanBytes(total))
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
