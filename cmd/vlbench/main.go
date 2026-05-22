// Command vlbench measures VictoriaLogs ingest and query throughput using
// the same synthetic dataset as loadbench and lokibench. Enables apples-to-
// apples comparison with amber and Loki.
//
// VictoriaLogs push: POST /insert/jsonline (newline-delimited JSON)
// VictoriaLogs query: GET /select/logsql/query?query=...&start=...&end=...&limit=...
//
// Usage:
//
//	/tmp/vlogs-bench/victoria-logs-prod -storageDataPath=/tmp/vlogs-bench/data -httpListenAddr=127.0.0.1:9428 &
//	go run ./cmd/vlbench -n 10000000 -vl http://localhost:9428
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
	"path/filepath"
	"slices"
	"strings"
	"time"
)

var (
	flagN     = 10_000_000
	flagBatch = 1000
	flagVL    = "http://localhost:9428"
	flagSeed  = uint64(42)
)

const (
	commonToken   = "request"
	rareToken     = "anomalous-pattern-7f2"
	rareTokenRate = 0.001
	totalSpan     = 24 * time.Hour
	dataDir       = "/tmp/vlogs-bench/data"
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

func main() {
	for i := 1; i < len(os.Args)-1; i++ {
		switch os.Args[i] {
		case "-n":
			fmt.Sscanf(os.Args[i+1], "%d", &flagN) //nolint:errcheck
		case "-vl":
			flagVL = os.Args[i+1]
		case "-batch":
			fmt.Sscanf(os.Args[i+1], "%d", &flagBatch) //nolint:errcheck
		}
	}

	const nServices = 5
	const nHosts = 20
	services := make([]string, nServices)
	for i := range services {
		services[i] = fmt.Sprintf("svc-%02d", i)
	}

	rng := rand.New(rand.NewPCG(flagSeed, flagSeed^0x9E3779B97F4A7C15)) //nolint:gosec

	spacingNs := int64(totalSpan) / int64(flagN)
	baseTS := time.Now().Add(-totalSpan).UnixNano()

	client := &http.Client{Timeout: 30 * time.Second}

	fmt.Printf("vlbench: n=%d batch=%d vl=%s\n\n", flagN, flagBatch, flagVL)

	// W — Write throughput via POST /insert/jsonline
	fmt.Println("W — Write throughput")
	fmt.Println("--------------------")

	dropped := 0
	t0 := time.Now()

	var buf bytes.Buffer
	for i := 0; i < flagN; i++ {
		svc := services[i%nServices]
		level := levelNames[(i/nServices)%len(levelNames)]
		host := fmt.Sprintf("host-%03d", i%nHosts)
		body := bodyTemplates[i%len(bodyTemplates)]
		if rng.Float64() < rareTokenRate {
			body = body + " " + rareToken
		}
		ts := baseTS + int64(i)*spacingNs

		// VictoriaLogs jsonline format: one JSON object per line.
		// _msg is the log message, _time is RFC3339Nano, rest are fields.
		rec := map[string]string{
			"_time":   time.Unix(0, ts).UTC().Format(time.RFC3339Nano),
			"_msg":    body,
			"service": svc,
			"level":   level,
			"host":    host,
		}
		line, _ := json.Marshal(rec)
		buf.Write(line)
		buf.WriteByte('\n')

		if buf.Len() >= flagBatch*150 || i == flagN-1 {
			resp, err := client.Post(
				flagVL+"/insert/jsonline?_stream_fields=service",
				"application/x-ndjson",
				bytes.NewReader(buf.Bytes()),
			)
			if err != nil {
				fmt.Fprintf(os.Stderr, "push [%d]: %v\n", i, err)
				dropped += flagBatch
			} else {
				io.Copy(io.Discard, resp.Body) //nolint:errcheck
				resp.Body.Close()
				if resp.StatusCode != 200 {
					dropped += flagBatch
				}
			}
			buf.Reset()
		}
	}

	dur := time.Since(t0)
	fmt.Printf("  ingested:   %d records\n", flagN-dropped)
	fmt.Printf("  duration:   %v\n", dur.Round(time.Millisecond))
	fmt.Printf("  throughput: %.0f rec/s\n", float64(flagN-dropped)/dur.Seconds())
	fmt.Printf("  dropped:    %d\n\n", dropped)

	// Force flush.
	time.Sleep(3 * time.Second)

	// M — Storage footprint.
	fmt.Println("M — Storage")
	fmt.Println("-----------")
	var total int64
	_ = filepath.Walk(dataDir, func(_ string, info os.FileInfo, err error) error { //nolint:errcheck
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	fmt.Printf("  data dir:   %s\n", humanBytes(total))
	fmt.Printf("  vl VmRSS:   %s\n\n", vlRSS())

	// R — Read latency.
	fmt.Println("R — Read latency (100 iterations)")
	fmt.Println("----------------------------------")

	midOffset := time.Duration(spacingNs * int64(flagN) / 2)
	dataFrom := time.Unix(0, baseTS)
	queryFrom := dataFrom.Add(midOffset - 30*time.Minute)
	queryTo := dataFrom.Add(midOffset + 30*time.Minute)
	fullFrom := dataFrom
	fullTo := dataFrom.Add(totalSpan)

	scenarios := []struct {
		name  string
		query string
		from  time.Time
		to    time.Time
	}{
		// R1: label filter (AND) — LogsQL: field:value AND field:value
		{"R1 service+level (AND)", `service:svc-00 AND level:ERROR`, fullFrom, fullTo},
		// R2: time range — narrow window, any service
		{"R2 time range 1h slice", `*`, queryFrom, queryTo},
		// R3: FTS common token
		{"R3 FTS common token", `"` + commonToken + `"`, fullFrom, fullTo},
		// R4: FTS rare token
		{"R4 FTS rare token", `"` + rareToken + `"`, fullFrom, fullTo},
		// R5: host lookup (equivalent of trace_id lookup — unique field)
		{"R5 host lookup", `host:host-000`, fullFrom, fullTo},
	}

	const iters = 100
	for _, sc := range scenarios {
		samples := make([]time.Duration, 0, iters)
		var lastHits int

		for range iters {
			hits, elapsed := runVLQuery(client, sc.query, sc.from, sc.to)
			samples = append(samples, elapsed)
			lastHits = hits
		}

		slices.Sort(samples)
		p50 := samples[len(samples)*50/100]
		p99 := samples[len(samples)*99/100]
		fmt.Printf("  %-34s p50=%-9v p99=%-9v hits=%d\n",
			sc.name, p50.Round(time.Microsecond), p99.Round(time.Microsecond), lastHits)
	}
}

func runVLQuery(client *http.Client, query string, start, end time.Time) (int, time.Duration) {
	params := url.Values{
		"query": {query},
		"start": {start.UTC().Format(time.RFC3339)},
		"end":   {end.UTC().Format(time.RFC3339)},
		"limit": {"100"},
	}
	reqURL := flagVL + "/select/logsql/query?" + params.Encode()

	t0 := time.Now()
	resp, err := client.Get(reqURL)
	elapsed := time.Since(t0)
	if err != nil {
		return 0, elapsed
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	// VictoriaLogs returns newline-delimited JSON objects.
	hits := 0
	for _, line := range bytes.Split(body, []byte("\n")) {
		if len(bytes.TrimSpace(line)) > 0 {
			hits++
		}
	}
	return hits, elapsed
}

func vlRSS() string {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return "unknown"
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		if !strings.Contains(string(cmdline), "victoria-logs") {
			continue
		}
		status, err := os.ReadFile("/proc/" + e.Name() + "/status")
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "VmRSS:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					var kb uint64
					fmt.Sscanf(fields[1], "%d", &kb) //nolint:errcheck
					return humanBytes(int64(kb * 1024))
				}
			}
		}
	}
	return "unknown"
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
