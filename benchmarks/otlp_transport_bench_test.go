package benchmarks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	gogrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	apigrpc "github.com/yaop-labs/amber/internal/api/grpc"
	apihttp "github.com/yaop-labs/amber/internal/api/http"
	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/indexer"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/storage"
)

const (
	otlpBenchAcceptedRecords = 256
	otlpBenchWireRecords     = otlpBenchAcceptedRecords + 1
	otlpBenchAPIKey          = "benchmark-secret"
)

// BenchmarkOTLPTransport measures the complete acknowledged OTLP write path:
// loopback transport, protobuf codec, conversion, primary WAL/segment/index
// durability, accepted-request subset cloning, and canonical journal durability.
// Each wire request contains one invalid record so the post-run validation can
// prove that both primary storage and the journal retained only the accepted
// 256-record subset.
func BenchmarkOTLPTransport(b *testing.B) {
	b.Run("HTTP/Logs256", benchmarkOTLPHTTPLogs)
	b.Run("HTTP/Traces256", benchmarkOTLPHTTPTraces)
	b.Run("gRPC/Logs256", benchmarkOTLPGRPCLogs)
	b.Run("gRPC/Traces256", benchmarkOTLPGRPCTraces)
}

type otlpTransportBench struct {
	root        string
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	batcher     *ingest.Batcher
	journal     *otlpv4.Journal
	log         *slog.Logger
}

func newOTLPTransportBench(b *testing.B) *otlpTransportBench {
	b.Helper()

	root := b.TempDir()
	logDir := filepath.Join(root, "logs")
	spanDir := filepath.Join(root, "spans")
	policy := storage.RotationPolicy{MaxRecords: 10_000_000, MaxBytes: 1 << 40}
	logManager, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		b.Fatalf("open log manager: %v", err)
	}
	spanManager, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		_ = logManager.Close()
		b.Fatalf("open span manager: %v", err)
	}
	journal, err := otlpv4.OpenJournal(root, policy)
	if err != nil {
		_ = logManager.Close()
		_ = spanManager.Close()
		b.Fatalf("open OTLP journal: %v", err)
	}

	logSparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()
	guard := ingest.NewCardinalityGuard(64, 4096, 1024, 10_000)
	active := indexer.New(logManager, spanManager)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	batcher := ingest.NewBatcher(ingest.Deps{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		Indexer:     active,
		Guard:       guard,
		Logger:      logger,
	}, ingest.Config{
		BatchSize:        otlpBenchAcceptedRecords,
		BatchTimeout:     time.Hour,
		QueueSize:        4096,
		BreakerThreshold: 10,
	})
	batcher.Start(context.Background())

	h := &otlpTransportBench{
		root:        root,
		logManager:  logManager,
		spanManager: spanManager,
		logSparse:   logSparse,
		batcher:     batcher,
		journal:     journal,
		log:         logger,
	}
	b.Cleanup(func() {
		_ = batcher.Close(context.Background())
		_ = journal.Close()
		_ = logManager.Close()
		_ = spanManager.Close()
	})
	return h
}

func (h *otlpTransportBench) httpServer(b *testing.B) (*httptest.Server, *http.Client) {
	b.Helper()
	mux := http.NewServeMux()
	apihttp.RegisterRoutes(mux, apihttp.RoutesDeps{
		Batcher:     h.batcher,
		LogManager:  h.logManager,
		LogSparse:   h.logSparse,
		OTLPJournal: h.journal,
		Logger:      h.log,
	}, apihttp.RoutesConfig{
		APIKeys:         []config.NamedAPIKey{{Name: "benchmark", Key: otlpBenchAPIKey}},
		MaxRequestBytes: 32 << 20,
	})
	server := httptest.NewServer(mux)
	client := server.Client()
	b.Cleanup(func() {
		client.CloseIdleConnections()
		server.Close()
	})

	// Establish the keep-alive connection outside the timed region.
	resp, err := client.Get(server.URL + "/healthz")
	if err != nil {
		b.Fatalf("warm HTTP connection: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return server, client
}

func (h *otlpTransportBench) grpcConn(b *testing.B) (*gogrpc.ClientConn, *gogrpc.Server) {
	b.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen gRPC: %v", err)
	}
	server := apigrpc.NewServerWithJournal(h.batcher, nil, h.journal, 32<<20, h.log)
	go func() { _ = server.Serve(listener) }()

	conn, err := gogrpc.NewClient(
		"passthrough:///"+listener.Addr().String(),
		gogrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		_ = listener.Close()
		b.Fatalf("dial gRPC: %v", err)
	}
	b.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return conn, server
}

func benchmarkOTLPHTTPLogs(b *testing.B) {
	h := newOTLPTransportBench(b)
	server, client := h.httpServer(b)
	request := makeOTLPLogRequest()
	wireBytes := proto.Size(request)

	b.SetBytes(int64(wireBytes))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		body, err := proto.Marshal(request)
		if err != nil {
			b.Fatal(err)
		}
		rejected, err := postOTLPLogs(client, server.URL+"/v1/logs", body)
		if err != nil {
			b.Fatal(err)
		}
		if rejected != 1 {
			b.Fatalf("rejected logs = %d, want 1", rejected)
		}
	}
	b.StopTimer()
	reportOTLPRecords(b)
	h.verify(b, otlpv4.SignalLogs)
}

func benchmarkOTLPHTTPTraces(b *testing.B) {
	h := newOTLPTransportBench(b)
	server, client := h.httpServer(b)
	request := makeOTLPTraceRequest()
	wireBytes := proto.Size(request)

	b.SetBytes(int64(wireBytes))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		setTraceRequestSequence(request, uint64(i))
		body, err := proto.Marshal(request)
		if err != nil {
			b.Fatal(err)
		}
		rejected, err := postOTLPTraces(client, server.URL+"/v1/traces", body)
		if err != nil {
			b.Fatal(err)
		}
		if rejected != 1 {
			b.Fatalf("rejected spans = %d, want 1", rejected)
		}
	}
	b.StopTimer()
	reportOTLPRecords(b)
	h.verify(b, otlpv4.SignalTraces)
}

func benchmarkOTLPGRPCLogs(b *testing.B) {
	h := newOTLPTransportBench(b)
	conn, _ := h.grpcConn(b)
	client := collectorlogs.NewLogsServiceClient(conn)
	if _, err := client.Export(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); err != nil {
		b.Fatalf("warm gRPC connection: %v", err)
	}
	request := makeOTLPLogRequest()

	b.SetBytes(int64(proto.Size(request)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		resp, err := client.Export(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		if rejected := resp.GetPartialSuccess().GetRejectedLogRecords(); rejected != 1 {
			b.Fatalf("rejected logs = %d, want 1", rejected)
		}
	}
	b.StopTimer()
	reportOTLPRecords(b)
	h.verify(b, otlpv4.SignalLogs)
}

func benchmarkOTLPGRPCTraces(b *testing.B) {
	h := newOTLPTransportBench(b)
	conn, _ := h.grpcConn(b)
	client := collectortrace.NewTraceServiceClient(conn)
	if _, err := client.Export(context.Background(), &collectortrace.ExportTraceServiceRequest{}); err != nil {
		b.Fatalf("warm gRPC connection: %v", err)
	}
	request := makeOTLPTraceRequest()

	b.SetBytes(int64(proto.Size(request)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		setTraceRequestSequence(request, uint64(i))
		resp, err := client.Export(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		if rejected := resp.GetPartialSuccess().GetRejectedSpans(); rejected != 1 {
			b.Fatalf("rejected spans = %d, want 1", rejected)
		}
	}
	b.StopTimer()
	reportOTLPRecords(b)
	h.verify(b, otlpv4.SignalTraces)
}

func reportOTLPRecords(b *testing.B) {
	b.Helper()
	b.ReportMetric(
		float64(b.N*otlpBenchAcceptedRecords)/b.Elapsed().Seconds(),
		"records/sec",
	)
}

func postOTLPLogs(client *http.Client, endpoint string, body []byte) (int64, error) {
	req, err := newOTLPHTTPRequest(endpoint, body)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("OTLP logs HTTP status %d: %s", resp.StatusCode, responseBody)
	}
	var result collectorlogs.ExportLogsServiceResponse
	if err := proto.Unmarshal(responseBody, &result); err != nil {
		return 0, err
	}
	return result.GetPartialSuccess().GetRejectedLogRecords(), nil
}

func postOTLPTraces(client *http.Client, endpoint string, body []byte) (int64, error) {
	req, err := newOTLPHTTPRequest(endpoint, body)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("OTLP traces HTTP status %d: %s", resp.StatusCode, responseBody)
	}
	var result collectortrace.ExportTraceServiceResponse
	if err := proto.Unmarshal(responseBody, &result); err != nil {
		return 0, err
	}
	return result.GetPartialSuccess().GetRejectedSpans(), nil
}

func newOTLPHTTPRequest(endpoint string, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		endpoint,
		bytes.NewReader(body),
	)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+otlpBenchAPIKey)
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/x-protobuf")
	return req, nil
}

func (h *otlpTransportBench) verify(b *testing.B, signal otlpv4.Signal) {
	b.Helper()
	manager := h.logManager
	if signal == otlpv4.SignalTraces {
		manager = h.spanManager
	}
	primaryStats, err := manager.Stats()
	if err != nil {
		b.Fatalf("primary stats: %v", err)
	}
	wantPrimary := uint64(b.N * otlpBenchAcceptedRecords)
	if primaryStats.TotalRecords != wantPrimary {
		b.Fatalf("primary records = %d, want %d", primaryStats.TotalRecords, wantPrimary)
	}

	journalStats, err := h.journal.Stats()
	if err != nil {
		b.Fatalf("journal stats: %v", err)
	}
	if journalStats.TotalRecords != uint64(b.N) {
		b.Fatalf("journal records = %d, want %d", journalStats.TotalRecords, b.N)
	}
	if err := h.journal.Close(); err != nil {
		b.Fatalf("close journal: %v", err)
	}

	errReplayStop := errors.New("benchmark replay checked")
	replayed := false
	err = otlpv4.Replay(context.Background(), h.root, func(envelope otlpv4.Envelope) error {
		replayed = true
		if envelope.Signal() != signal || envelope.Fidelity() != otlpv4.FidelityOTLP {
			b.Fatalf("journal envelope = %s/%d, want %s/%d",
				envelope.Signal(), envelope.Fidelity(), signal, otlpv4.FidelityOTLP)
		}
		request, requestErr := envelope.Request()
		if requestErr != nil {
			b.Fatalf("decode journal request: %v", requestErr)
		}
		var records int
		switch typed := request.(type) {
		case *collectorlogs.ExportLogsServiceRequest:
			records = countOTLPLogRecords(typed)
		case *collectortrace.ExportTraceServiceRequest:
			records = countOTLPSpans(typed)
		default:
			b.Fatalf("unexpected journal request %T", request)
		}
		if records != otlpBenchAcceptedRecords {
			b.Fatalf("journal subset records = %d, want %d", records, otlpBenchAcceptedRecords)
		}
		return errReplayStop
	})
	if !errors.Is(err, errReplayStop) {
		b.Fatalf("replay first journal record: %v", err)
	}
	if !replayed {
		b.Fatal("journal replay returned no records")
	}
}

func makeOTLPLogRequest() *collectorlogs.ExportLogsServiceRequest {
	records := make([]*logspb.LogRecord, 0, otlpBenchWireRecords)
	baseTime := uint64(time.Now().UnixNano())
	for i := range otlpBenchAcceptedRecords {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		binary.BigEndian.PutUint64(traceID[8:], uint64(i+1))
		binary.BigEndian.PutUint64(spanID, uint64(i+1))
		records = append(records, &logspb.LogRecord{
			TimeUnixNano:   baseTime + uint64(i),
			SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_INFO,
			SeverityText:   "INFO",
			Body:           otlpStringValue("request completed"),
			Attributes: []*commonpb.KeyValue{
				otlpStringAttr("http.method", "GET"),
				otlpStringAttr("http.route", "/api/v1/items"),
				otlpIntAttr("http.status_code", 200),
			},
			TraceId: traceID,
			SpanId:  spanID,
		})
	}
	records = append(records, &logspb.LogRecord{
		TimeUnixNano: baseTime,
		Body:         otlpStringValue("invalid trace id"),
		TraceId:      []byte{1},
	})
	return &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: otlpBenchResource(),
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: records,
			}},
		}},
	}
}

func makeOTLPTraceRequest() *collectortrace.ExportTraceServiceRequest {
	spans := make([]*tracepb.Span, 0, otlpBenchWireRecords)
	baseTime := uint64(time.Now().UnixNano())
	for i := range otlpBenchAcceptedRecords {
		traceID := make([]byte, 16)
		spanID := make([]byte, 8)
		binary.BigEndian.PutUint64(traceID[8:], uint64(i+1))
		binary.BigEndian.PutUint64(spanID, uint64(i+1))
		spans = append(spans, &tracepb.Span{
			TraceId:           traceID,
			SpanId:            spanID,
			Name:              "GET /api/v1/items",
			Kind:              tracepb.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: baseTime + uint64(i),
			EndTimeUnixNano:   baseTime + uint64(i) + uint64(25*time.Millisecond),
			Attributes: []*commonpb.KeyValue{
				otlpStringAttr("http.method", "GET"),
				otlpStringAttr("http.route", "/api/v1/items"),
				otlpIntAttr("http.status_code", 200),
			},
			Status: &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
		})
	}
	spans = append(spans, &tracepb.Span{
		TraceId:           make([]byte, 16),
		SpanId:            []byte{1},
		Name:              "invalid span id",
		StartTimeUnixNano: baseTime,
		EndTimeUnixNano:   baseTime + 1,
	})
	return &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: otlpBenchResource(),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: spans,
			}},
		}},
	}
}

func setTraceRequestSequence(request *collectortrace.ExportTraceServiceRequest, sequence uint64) {
	var record uint64
	for _, resource := range request.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			for _, span := range scope.Spans {
				if len(span.SpanId) != 8 {
					continue
				}
				record++
				id := sequence*otlpBenchAcceptedRecords + record
				binary.BigEndian.PutUint64(span.TraceId[8:], id)
				binary.BigEndian.PutUint64(span.SpanId, id)
			}
		}
	}
}

func otlpBenchResource() *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		otlpStringAttr("service.name", "api-gateway"),
		otlpStringAttr("host.name", "bench-host-001"),
		otlpStringAttr("deployment.environment", "benchmark"),
	}}
}

func otlpStringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: otlpStringValue(value)}
}

func otlpIntAttr(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key: key,
		Value: &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{IntValue: value},
		},
	}
}

func otlpStringValue(value string) *commonpb.AnyValue {
	return &commonpb.AnyValue{
		Value: &commonpb.AnyValue_StringValue{StringValue: value},
	}
}

func countOTLPLogRecords(request *collectorlogs.ExportLogsServiceRequest) int {
	var count int
	for _, resource := range request.ResourceLogs {
		for _, scope := range resource.ScopeLogs {
			count += len(scope.LogRecords)
		}
	}
	return count
}

func countOTLPSpans(request *collectortrace.ExportTraceServiceRequest) int {
	var count int
	for _, resource := range request.ResourceSpans {
		for _, scope := range resource.ScopeSpans {
			count += len(scope.Spans)
		}
	}
	return count
}
