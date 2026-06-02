package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
)

func hexToBase64(h string) string {
	b, err := hex.DecodeString(h)
	if err != nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

var (
	addr      = flag.String("addr", "http://localhost:8080", "amber address")
	count     = flag.Int("n", 1000, "number of log entries to send")
	traces    = flag.Int("traces", 50, "number of traces to generate")
	batchSize = flag.Int("batch", 100, "entries per HTTP request")
	metrics   = flag.Int("metrics", 200, "number of metric points to send (0 disables)")
)

var services = []string{
	"api-gateway", "auth-service", "payment", "worker", "scheduler",
	"postgres-proxy", "redis-cache", "notification", "billing", "analytics",
}

var callTree = map[string][]string{
	"api-gateway":    {"auth-service", "billing"},
	"auth-service":   {"redis-cache", "postgres-proxy"},
	"billing":        {"payment", "notification"},
	"payment":        {"postgres-proxy"},
	"notification":   {},
	"redis-cache":    {},
	"postgres-proxy": {},
}

var hosts = []string{"node-01", "node-02", "node-03", "node-04", "node-05"}

type logTemplate struct {
	level string
	body  string
}

var logTemplates = []logTemplate{
	{"DEBUG", "GET /api/v1/users 200 latency=12ms"},
	{"INFO", "POST /api/v1/orders 201 order_id=ord_8821 latency=45ms"},
	{"INFO", "job processed queue=payments duration=234ms status=ok"},
	{"INFO", "new user registered email=alice@example.com plan=pro"},
	{"INFO", "database migration applied version=20260301_add_indexes"},
	{"INFO", "cache miss key=user:session:abc123 loading from db"},
	{"WARN", "rate limit exceeded ip=192.168.1.42 limit=100/min"},
	{"WARN", "high memory usage heap=87% threshold=80%"},
	{"WARN", "SQL slow query duration=3421ms table=orders"},
	{"ERROR", "connection refused to postgres:5432 retrying attempt=3"},
	{"ERROR", "timeout waiting for redis response latency=5001ms"},
	{"ERROR", "failed to send notification email=user@example.com err=smtp timeout"},
	{"ERROR", "health check failed service=downstream-api status=503"},
	{"ERROR", "payment declined card_last4=4242 reason=insufficient_funds"},
	{"FATAL", "panic: nil pointer dereference in handler.go:142"},
}

var operations = map[string][]string{
	"api-gateway":    {"HTTP GET /users", "HTTP POST /orders", "HTTP GET /billing"},
	"auth-service":   {"ValidateToken", "RefreshToken", "CheckPermissions"},
	"billing":        {"CreateInvoice", "GetBalance", "ProcessRefund"},
	"payment":        {"ChargeCard", "Authorize", "Capture"},
	"notification":   {"SendEmail", "SendSMS", "PushNotification"},
	"postgres-proxy": {"Query", "Insert", "Update"},
	"redis-cache":    {"GET", "SET", "DEL"},
}

var attrs = []map[string]string{
	{"env": "prod", "region": "eu-west-1"},
	{"env": "prod", "region": "us-east-1"},
	{"env": "staging", "region": "ru-west-1"},
}

type logEntry struct {
	Level   string            `json:"level"`
	Service string            `json:"service"`
	Host    string            `json:"host"`
	Body    string            `json:"body"`
	TraceID string            `json:"trace_id,omitempty"`
	SpanID  string            `json:"span_id,omitempty"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

func randHex(n int) string {
	b := make([]byte, n)
	for i := 0; i < n; i += 8 {
		u := rand.Uint64()
		for j := 0; j < 8 && i+j < n; j++ {
			b[i+j] = byte(u >> (j * 8))
		}
	}
	return hex.EncodeToString(b)
}

func main() {
	flag.Parse()

	fmt.Printf("→ amber loadgen\n")
	fmt.Printf("  target:  %s\n", *addr)
	fmt.Printf("  logs:    %d\n", *count)
	fmt.Printf("  traces:  %d\n", *traces)
	fmt.Printf("  metrics: %d\n", *metrics)
	fmt.Println()

	if err := healthCheck(*addr); err != nil {
		fmt.Fprintf(os.Stderr, "health check failed: %v\nis amber running at %s?\n", err, *addr)
		os.Exit(1)
	}
	fmt.Println("[ok] amber is up")

	client := &http.Client{Timeout: 30 * time.Second}
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))

	fmt.Printf("\n-> generating %d traces...\n", *traces)

	var traceLogs []logEntry
	now := time.Now()

	for i := range *traces {
		traceID := randHex(16)
		hasError := rng.IntN(5) == 0
		rootSvc := services[rng.IntN(3)]
		spans, logs := buildTrace(rng, traceID, rootSvc, "", now.Add(-time.Duration(rng.IntN(3600))*time.Second), 0, hasError)

		if err := sendOTLPTraces(client, *addr, spans); err != nil {
			fmt.Fprintf(os.Stderr, "  trace %d error: %v\n", i+1, err)
		}
		traceLogs = append(traceLogs, logs...)
	}
	fmt.Printf("[ok] traces sent\n")

	fmt.Printf("-> sending %d correlated logs...\n", len(traceLogs))
	for i := 0; i < len(traceLogs); i += *batchSize {
		end := min(i+*batchSize, len(traceLogs))
		if err := sendBatch(client, *addr, traceLogs[i:end]); err != nil {
			fmt.Fprintf(os.Stderr, "  batch error: %v\n", err)
		}
	}
	fmt.Printf("[ok] correlated logs sent\n")

	fmt.Printf("\n-> generating %d standalone logs...\n", *count)
	sent := 0
	for sent < *count {
		size := min(*batchSize, *count-sent)
		batch := make([]logEntry, size)
		for i := range batch {
			tpl := logTemplates[rng.IntN(len(logTemplates))]
			batch[i] = logEntry{
				Level:   tpl.level,
				Service: services[rng.IntN(len(services))],
				Host:    hosts[rng.IntN(len(hosts))],
				Body:    tpl.body,
				Attrs:   attrs[rng.IntN(len(attrs))],
			}
		}
		if err := sendBatch(client, *addr, batch); err != nil {
			fmt.Fprintf(os.Stderr, "  batch error: %v\n", err)
		}
		sent += size
		if sent%500 == 0 || sent == *count {
			fmt.Printf("sent %d / %d\n", sent, *count)
		}
	}

	if *metrics > 0 {
		fmt.Printf("\n-> generating %d metric points...\n", *metrics)
		if err := sendOTLPMetrics(client, *addr, rng, *metrics); err != nil {
			fmt.Fprintf(os.Stderr, "  metrics error: %v\n", err)
		} else {
			fmt.Printf("[ok] metrics sent\n")
		}
	}

	fmt.Printf("\n[ok] done! open http://localhost:8080 to explore\n")
}

func buildTrace(rng *rand.Rand, traceID, service, parentSpanID string, startAt time.Time, depth int, forceError bool) ([]otlpSpan, []logEntry) {
	if depth > 3 {
		return nil, nil
	}

	spanID := randHex(8)
	dur := time.Duration(10+rng.IntN(490)) * time.Millisecond
	if depth == 0 {
		dur = time.Duration(200+rng.IntN(1800)) * time.Millisecond
	}

	ops := operations[service]
	if len(ops) == 0 {
		ops = []string{"process"}
	}
	op := ops[rng.IntN(len(ops))]

	isError := forceError && depth == 0 || rng.IntN(20) == 0

	sp := otlpSpan{
		TraceID:           hexToBase64(traceID),
		SpanID:            hexToBase64(spanID),
		ParentSpanID:      hexToBase64(parentSpanID),
		Name:              op,
		StartTimeUnixNano: fmt.Sprintf("%d", startAt.UnixNano()),
		EndTimeUnixNano:   fmt.Sprintf("%d", startAt.Add(dur).UnixNano()),
		Status:            otlpStatus{Code: 1},
		Attributes: []otlpAttr{
			{Key: "service.version", Value: otlpVal{StringValue: "1.0.0"}},
		},
	}
	if isError {
		sp.Status = otlpStatus{Code: 2, Message: "internal error"}
	}

	logCount := 1 + rng.IntN(3)
	var logs []logEntry
	for i := range logCount {
		tpl := logTemplates[rng.IntN(len(logTemplates))]
		lvl := tpl.level
		body := tpl.body
		if isError && i == logCount-1 {
			lvl = "ERROR"
			body = "operation failed: " + body
		}
		logs = append(logs, logEntry{
			Level:   lvl,
			Service: service,
			Host:    hosts[rng.IntN(len(hosts))],
			Body:    body,
			TraceID: traceID,
			SpanID:  spanID,
			Attrs:   attrs[rng.IntN(len(attrs))],
		})
	}

	allSpans := []otlpSpan{sp}

	deps := callTree[service]
	if len(deps) > 0 && depth < 2 {
		childCount := 1 + rng.IntN(min(2, len(deps)))
		childStart := startAt.Add(time.Duration(10+rng.IntN(50)) * time.Millisecond)
		for i := range childCount {
			dep := deps[i%len(deps)]
			childSpans, childLogs := buildTrace(rng, traceID, dep, spanID, childStart, depth+1, false)
			allSpans = append(allSpans, childSpans...)
			logs = append(logs, childLogs...)
			childStart = childStart.Add(time.Duration(30+rng.IntN(100)) * time.Millisecond)
		}
	}

	return allSpans, logs
}

type otlpVal struct {
	StringValue string `json:"stringValue,omitempty"`
}
type otlpAttr struct {
	Key   string  `json:"key"`
	Value otlpVal `json:"value"`
}
type otlpStatus struct {
	Code    int    `json:"code"`
	Message string `json:"message,omitempty"`
}
type otlpSpan struct {
	TraceID           string     `json:"traceId"`
	SpanID            string     `json:"spanId"`
	ParentSpanID      string     `json:"parentSpanId,omitempty"`
	Name              string     `json:"name"`
	StartTimeUnixNano string     `json:"startTimeUnixNano"`
	EndTimeUnixNano   string     `json:"endTimeUnixNano"`
	Status            otlpStatus `json:"status"`
	Attributes        []otlpAttr `json:"attributes,omitempty"`
}

func sendOTLPTraces(client *http.Client, addr string, spans []otlpSpan) error {
	byService := make(map[string][]otlpSpan)
	byService["mixed"] = append(byService["mixed"], spans...)

	type scopeSpans struct {
		Spans []otlpSpan `json:"spans"`
	}
	type resourceSpan struct {
		Resource   map[string]any `json:"resource"`
		ScopeSpans []scopeSpans   `json:"scopeSpans"`
	}
	type exportReq struct {
		ResourceSpans []resourceSpan `json:"resourceSpans"`
	}

	svcSpans := make(map[string][]otlpSpan)
	for _, sp := range spans {
		svc := spanService(sp.Name)
		svcSpans[svc] = append(svcSpans[svc], sp)
	}

	var resourceSpans []resourceSpan
	for svc, sps := range svcSpans {
		resourceSpans = append(resourceSpans, resourceSpan{
			Resource: map[string]any{
				"attributes": []otlpAttr{
					{Key: "service.name", Value: otlpVal{StringValue: svc}},
				},
			},
			ScopeSpans: []scopeSpans{{Spans: sps}},
		})
	}

	req := exportReq{ResourceSpans: resourceSpans}
	data, _ := json.Marshal(req)
	resp, err := client.Post(addr+"/v1/traces", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return nil
}

func spanService(op string) string {
	for svc, ops := range operations {
		for _, o := range ops {
			if o == op {
				return svc
			}
		}
	}
	return "unknown"
}

func sendBatch(client *http.Client, addr string, batch []logEntry) error {
	data, err := json.Marshal(batch)
	if err != nil {
		return err
	}
	resp, err := client.Post(addr+"/api/v1/logs", "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return nil
}

func healthCheck(addr string) error {
	resp, err := http.Get(addr + "/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}
