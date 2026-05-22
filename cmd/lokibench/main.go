// Command lokibench sends N log records to a local Loki instance via the
// Push API and measures write throughput, storage footprint, and read latency
// across query shapes comparable to loadbench (amber's own benchmark).
//
// Designed to run against a single-node Loki started with a minimal config.
// Uses the same synthetic dataset as loadbench: 5 services, 20 hosts,
// deterministic bodies with a rare token sprinkled at 0.1%.
//
// Usage:
//
//	go run ./cmd/lokibench -n 10000000 -loki http://localhost:3100
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"
)

var (
	flagN        = 10_000_000
	flagBatch    = 1000
	flagLoki     = "http://localhost:3100"
	flagDataDir  = "/tmp/loki-bench/data"
	flagServices = 5
	flagHosts    = 20
	flagSeed     = uint64(42)
)

const (
	commonToken   = "request"
	rareToken     = "anomalous-pattern-7f2"
	rareTokenRate = 0.001
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

var levels = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}

func main() {
	for i, arg := range os.Args[1:] {
		switch {
		case arg == "-n" && i+1 < len(os.Args)-1:
			fmt.Sscanf(os.Args[i+2], "%d", &flagN) //nolint:errcheck
		case arg == "-loki" && i+1 < len(os.Args)-1:
			flagLoki = os.Args[i+2]
		case arg == "-batch" && i+1 < len(os.Args)-1:
			fmt.Sscanf(os.Args[i+2], "%d", &flagBatch) //nolint:errcheck
		}
	}

	fmt.Printf("lokibench: n=%d batch=%d loki=%s services=%d hosts=%d seed=%d\n\n",
		flagN, flagBatch, flagLoki, flagServices, flagHosts, flagSeed)

	services := make([]string, flagServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%02d", i)
	}

	rng := rand.New(rand.NewPCG(flagSeed, flagSeed^0x9E3779B97F4A7C15)) //nolint:gosec

	const totalSpan = 24 * time.Hour
	spacingNs := int64(totalSpan) / int64(flagN)
	baseTS := time.Now().Add(-totalSpan).UnixNano()

	client := &http.Client{Timeout: 30 * time.Second}

	// W — Write throughput.
	fmt.Println("W — Write throughput")
	fmt.Println("--------------------")

	dropped := 0
	t0 := time.Now()

	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	type lokiPush struct {
		Streams []lokiStream `json:"streams"`
	}

	for base := 0; base < flagN; base += flagBatch {
		end := min(base+flagBatch, flagN)

		// Collect records grouped by service.
		byService := make(map[string][][2]string, flagServices)

		for i := base; i < end; i++ {
			svc := services[i%flagServices]
			level := levels[(i/flagServices)%len(levels)]
			host := fmt.Sprintf("host-%03d", i%flagHosts)
			body := bodyTemplates[i%len(bodyTemplates)]
			if rng.Float64() < rareTokenRate {
				body = body + " " + rareToken
			}
			ts := baseTS + int64(i)*spacingNs
			// Loki timestamp: nanoseconds as string.
			tsStr := fmt.Sprintf("%d", ts)
			line := fmt.Sprintf("level=%s host=%s svc=%s %s", level, host, svc, body)
			_ = level
			byService[svc] = append(byService[svc], [2]string{tsStr, line})
		}

		streams := make([]lokiStream, 0, flagServices)
		for svc, vals := range byService {
			streams = append(streams, lokiStream{
				Stream: map[string]string{"service": svc},
				Values: vals,
			})
		}

		payload, err := json.Marshal(lokiPush{Streams: streams})
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal: %v\n", err)
			dropped += end - base
			continue
		}

		resp, err := client.Post(flagLoki+"/loki/api/v1/push", "application/json", bytes.NewReader(payload))
		if err != nil {
			fmt.Fprintf(os.Stderr, "push [%d..%d]: %v\n", base, end, err)
			dropped += end - base
			continue
		}
		body2, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 204 {
			fmt.Fprintf(os.Stderr, "push [%d..%d]: status %d: %s\n", base, end, resp.StatusCode, string(body2))
			dropped += end - base
		}
	}

	dur := time.Since(t0)
	throughput := float64(flagN-dropped) / dur.Seconds()
	fmt.Printf("  ingested:   %d records\n", flagN-dropped)
	fmt.Printf("  duration:   %v\n", dur.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f rec/s\n", throughput)
	fmt.Printf("  dropped:    %d\n\n", dropped)

	// Wait for flush — Loki flushes chunks periodically.
	// Hit the flush endpoint to force it.
	fmt.Println("flushing Loki chunks...")
	resp, err := client.Post(flagLoki+"/flush", "application/json", nil)
	if err == nil {
		resp.Body.Close()
	}
	time.Sleep(3 * time.Second)

	// M — Storage footprint.
	fmt.Println("M — Storage and memory")
	fmt.Println("----------------------")

	var chunkBytes, indexBytes, walBytes int64
	_ = filepath.Walk(flagDataDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil || info.IsDir() {
			return nil
		}
		size := info.Size()
		name := info.Name()
		switch {
		case strings.Contains(path, "chunks"):
			chunkBytes += size
		case strings.Contains(path, "index") || strings.HasSuffix(name, ".tsdb"):
			indexBytes += size
		case strings.Contains(path, "wal"):
			walBytes += size
		}
		return nil
	})

	runtime.GC()
	runtime.GC()

	rss := readRSS()
	fmt.Printf("  chunks:     %s\n", humanBytes(chunkBytes))
	fmt.Printf("  index:      %s\n", humanBytes(indexBytes))
	fmt.Printf("  wal:        %s\n", humanBytes(walBytes))
	fmt.Printf("  loki VmRSS: %s (external process — see /proc)\n\n", humanBytes(int64(rss)))

	// R — Read latency.
	fmt.Println("R — Read latency (100 iterations each)")
	fmt.Println("---------------------------------------")

	midOffset := time.Duration(spacingNs * int64(flagN) / 2)
	dataFrom := time.Unix(0, baseTS)
	queryFrom := dataFrom.Add(midOffset - 30*time.Minute)
	queryTo := dataFrom.Add(midOffset + 30*time.Minute)

	scenarios := []struct {
		name  string
		query string
	}{
		{"R1 service+level (label filter)", `{service="svc-00"} |= "level=ERROR"`},
		{"R2 time range 1h slice", `{service=~".+"}`},
		{"R3 FTS common token", `{service=~".+"} |= "` + commonToken + `"`},
		{"R4 FTS rare token", `{service=~".+"} |= "` + rareToken + `"`},
		{"R5 trace-equivalent (host)", `{service=~".+"} |= "host-000"`},
	}

	const iters = 100
	for _, sc := range scenarios {
		samples := make([]time.Duration, 0, iters)
		var lastHits int

		for range iters {
			var (
				start = queryFrom
				end2  = queryTo
			)
			if sc.name == "R1 service+level (label filter)" ||
				sc.name == "R3 FTS common token" ||
				sc.name == "R4 FTS rare token" ||
				sc.name == "R5 trace-equivalent (host)" {
				// Full-range query for these.
				start = dataFrom
				end2 = dataFrom.Add(totalSpan)
			}

			hits, dur2 := runLokiQuery(client, flagLoki, sc.query, start, end2, 100)
			samples = append(samples, dur2)
			lastHits = hits
		}

		slices.Sort(samples)
		p50 := samples[len(samples)*50/100]
		p99 := samples[len(samples)*99/100]
		fmt.Printf("  %-38s p50=%-9v p99=%-9v hits=%d\n",
			sc.name, p50.Round(time.Microsecond), p99.Round(time.Microsecond), lastHits)
	}
	fmt.Println()
}

func runLokiQuery(client *http.Client, base, query string, start, end time.Time, limit int) (int, time.Duration) {
	url := fmt.Sprintf("%s/loki/api/v1/query_range?query=%s&start=%d&end=%d&limit=%d",
		base,
		encodeQuery(query),
		start.UnixNano(),
		end.UnixNano(),
		limit,
	)

	t0 := time.Now()
	resp, err := client.Get(url)
	dur := time.Since(t0)
	if err != nil {
		return 0, dur
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var result struct {
		Data struct {
			Result []struct {
				Values [][]any `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &result)
	hits := 0
	for _, r := range result.Data.Result {
		hits += len(r.Values)
	}
	return hits, dur
}

func encodeQuery(q string) string {
	var buf strings.Builder
	for _, c := range q {
		switch c {
		case ' ':
			buf.WriteString("%20")
		case '{':
			buf.WriteString("%7B")
		case '}':
			buf.WriteString("%7D")
		case '"':
			buf.WriteString("%22")
		case '=':
			buf.WriteString("%3D")
		case '~':
			buf.WriteString("%7E")
		case '|':
			buf.WriteString("%7C")
		case '+':
			buf.WriteString("%2B")
		default:
			buf.WriteRune(c)
		}
	}
	return buf.String()
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

func readRSS() uint64 {
	// Read Loki's RSS from /proc by finding its PID.
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		if !strings.Contains(string(cmdline), "loki-linux-amd64") {
			continue
		}
		status, err := os.ReadFile("/proc/" + e.Name() + "/status")
		if err != nil {
			continue
		}
		for line := range strings.SplitSeq(string(status), "\n") {
			if !strings.HasPrefix(line, "VmRSS:") {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 2 {
				return 0
			}
			var kb uint64
			fmt.Sscanf(fields[1], "%d", &kb) //nolint:errcheck
			return kb * 1024
		}
	}
	return 0
}
