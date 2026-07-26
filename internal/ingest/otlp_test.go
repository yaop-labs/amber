package ingest

import (
	"reflect"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/amber/internal/model"
)

func TestOTLPLogToEntryRejectsMalformedOptionalIDs(t *testing.T) {
	for _, tc := range []struct {
		name string
		log  *logspb.LogRecord
	}{
		{name: "trace", log: &logspb.LogRecord{TraceId: []byte{1}}},
		{name: "span", log: &logspb.LogRecord{SpanId: []byte{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := OTLPLogToEntry(tc.log, "svc", "host"); err == nil {
				t.Fatal("expected invalid ID error")
			}
		})
	}
}

func TestOTLPSpanToEntryRejectsMalformedIDs(t *testing.T) {
	validTrace := make([]byte, 16)
	validSpan := make([]byte, 8)
	for _, tc := range []struct {
		name string
		span *tracepb.Span
	}{
		{name: "missing trace", span: &tracepb.Span{SpanId: validSpan}},
		{name: "missing span", span: &tracepb.Span{TraceId: validTrace}},
		{name: "parent", span: &tracepb.Span{TraceId: validTrace, SpanId: validSpan, ParentSpanId: []byte{1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := OTLPSpanToEntry(tc.span, "svc"); err == nil {
				t.Fatal("expected invalid ID error")
			}
		})
	}
}

func TestOTLPLogToEntryUsesSeverityNumberForCustomText(t *testing.T) {
	entry, err := OTLPLogToEntry(&logspb.LogRecord{
		SeverityText:   "NOTICE",
		SeverityNumber: logspb.SeverityNumber_SEVERITY_NUMBER_WARN2,
		Body:           &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "x"}},
	}, "svc", "host")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Level != model.LevelWarn {
		t.Fatalf("level = %v, want WARN", entry.Level)
	}
}

func TestOTLPSpanToEntryPreservesNonNilAttrs(t *testing.T) {
	span := &tracepb.Span{
		TraceId: make([]byte, 16),
		SpanId:  make([]byte, 8),
		Attributes: []*commonpb.KeyValue{
			{Key: "env", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "prod"}}},
			nil,
			{Key: "attempt", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 3}}},
		},
	}

	entry, err := OTLPSpanToEntry(span, "svc")
	if err != nil {
		t.Fatal(err)
	}
	want := []model.Attr{{Key: "env", Value: "prod"}, {Key: "attempt", Value: "3"}}
	if !reflect.DeepEqual(entry.Attrs, want) {
		t.Fatalf("attrs = %#v, want %#v", entry.Attrs, want)
	}
}

var benchmarkOTLPSpanEntry model.SpanEntry

func BenchmarkOTLPSpanToEntry(b *testing.B) {
	span := &tracepb.Span{
		TraceId:           make([]byte, 16),
		SpanId:            make([]byte, 8),
		ParentSpanId:      make([]byte, 8),
		Name:              "GET /api/v1/users",
		StartTimeUnixNano: 1,
		EndTimeUnixNano:   2,
		Attributes: []*commonpb.KeyValue{
			{Key: "env", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "prod"}}},
			{Key: "region", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "us-east-1"}}},
			{Key: "version", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "v1.42.0"}}},
		},
	}

	b.ReportAllocs()
	for b.Loop() {
		entry, err := OTLPSpanToEntry(span, "api-gateway")
		if err != nil {
			b.Fatal(err)
		}
		benchmarkOTLPSpanEntry = entry
	}
}
