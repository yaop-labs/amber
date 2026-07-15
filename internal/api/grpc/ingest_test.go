package grpc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/storage"
)

type fakeSender struct {
	logErr        error
	spanErr       error
	logBreakerOn  bool
	spanBreakerOn bool
	flushLogErr   error
	flushSpanErr  error
}

func (f fakeSender) SendLog(model.LogEntry) error       { return f.logErr }
func (f fakeSender) SendSpan(model.SpanEntry) error     { return f.spanErr }
func (f fakeSender) SendOTLPLog(model.LogEntry) error   { return f.logErr }
func (f fakeSender) SendOTLPSpan(model.SpanEntry) error { return f.spanErr }
func (f fakeSender) IsBreakerOpen() bool                { return f.logBreakerOn || f.spanBreakerOn }
func (f fakeSender) IsLogBreakerOpen() bool             { return f.logBreakerOn }
func (f fakeSender) IsSpanBreakerOpen() bool            { return f.spanBreakerOn }
func (f fakeSender) FlushLogs(context.Context) error    { return f.flushLogErr }
func (f fakeSender) FlushSpans(context.Context) error   { return f.flushSpanErr }

func TestLogsExportReturnsUnavailableOnRetryableRejection(t *testing.T) {
	s := &logsServer{
		batcher: fakeSender{logErr: ingest.ErrQueueFull},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	req := &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}}}}}},
	}}}
	if _, err := s.Export(context.Background(), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err %v)", status.Code(err), err)
	}
}

func TestLogsExportReturnsUnavailableWhenDurabilityBarrierFails(t *testing.T) {
	s := &logsServer{
		batcher: fakeSender{flushLogErr: errors.New("fsync failed")},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := s.Export(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("code = %v, want Unavailable (err %v)", status.Code(err), err)
	}
}

func TestLogsExportPreservesContextDeadline(t *testing.T) {
	s := &logsServer{
		batcher: fakeSender{flushLogErr: context.DeadlineExceeded},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if _, err := s.Export(context.Background(), &collectorlogs.ExportLogsServiceRequest{}); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("code = %v, want DeadlineExceeded (err %v)", status.Code(err), err)
	}
}

func TestLogsExportReturnsPartialSuccessOnRejectedRecords(t *testing.T) {
	s := &logsServer{
		batcher: fakeSender{logErr: errors.New("queue unavailable")},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	resp, err := s.Export(context.Background(), &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
				Key:   "service.name",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}},
			}}},
			ScopeLogs: []*logspb.ScopeLogs{{
				LogRecords: []*logspb.LogRecord{{SeverityText: "ERROR", Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "boom"}}}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if resp.GetPartialSuccess() == nil {
		t.Fatal("expected partial success response")
	}
	if got := resp.GetPartialSuccess().GetRejectedLogRecords(); got != 1 {
		t.Fatalf("rejected_log_records = %d, want 1", got)
	}
	if got := resp.GetPartialSuccess().GetErrorMessage(); got == "" {
		t.Fatal("expected partial success error message")
	}
}

func TestTracesExportReturnsPartialSuccessOnRejectedSpans(t *testing.T) {
	s := &tracesServer{
		batcher: fakeSender{spanErr: errors.New("queue unavailable")},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	resp, err := s.Export(context.Background(), &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
				Key:   "service.name",
				Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}},
			}}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Spans: []*tracepb.Span{{
					TraceId:           []byte("0123456789abcdef"),
					SpanId:            []byte("span-123"),
					Name:              "GET /v1/test",
					StartTimeUnixNano: 1,
					EndTimeUnixNano:   2,
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if resp.GetPartialSuccess() == nil {
		t.Fatal("expected partial success response")
	}
	if got := resp.GetPartialSuccess().GetRejectedSpans(); got != 1 {
		t.Fatalf("rejected_spans = %d, want 1", got)
	}
	if got := resp.GetPartialSuccess().GetErrorMessage(); got == "" {
		t.Fatal("expected partial success error message")
	}
}

func TestTracesExportJournalRetainsAcceptedSubset(t *testing.T) {
	root := t.TempDir()
	journal, err := otlpv4.OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	s := &tracesServer{batcher: fakeSender{}, journal: journal, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	accepted := &tracepb.Span{
		TraceId: []byte("0123456789abcdef"), SpanId: []byte("span-123"), ParentSpanId: []byte("parent-1"),
		Name: "GET /rich", Kind: tracepb.Span_SPAN_KIND_SERVER, StartTimeUnixNano: 1, EndTimeUnixNano: 2,
		Events: []*tracepb.Span_Event{{TimeUnixNano: 2, Name: "done"}},
	}
	rejected := proto.Clone(accepted).(*tracepb.Span)
	rejected.SpanId = []byte{1}
	req := &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		SchemaUrl: "https://example.test/resource", ScopeSpans: []*tracepb.ScopeSpans{{SchemaUrl: "https://example.test/scope", Spans: []*tracepb.Span{accepted, rejected}}},
	}}}
	response, err := s.Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("partial success = %v", response.GetPartialSuccess())
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	want := otlpv4.TracesSubset(req, map[*tracepb.Span]struct{}{accepted: {}})
	count := 0
	if err := otlpv4.Replay(context.Background(), root, func(envelope otlpv4.Envelope) error {
		count++
		message, err := envelope.Request()
		if err != nil {
			return err
		}
		if !proto.Equal(message, want) {
			t.Fatalf("replayed traces differ: %v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("journal record count = %d", count)
	}
}
