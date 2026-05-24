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

	"github.com/yaop-labs/amber/internal/model"
)

type fakeSender struct {
	logErr    error
	spanErr   error
	breakerOn bool
}

func (f fakeSender) SendLog(model.LogEntry) error   { return f.logErr }
func (f fakeSender) SendSpan(model.SpanEntry) error { return f.spanErr }
func (f fakeSender) IsBreakerOpen() bool            { return f.breakerOn }

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
