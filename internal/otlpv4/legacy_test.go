package otlpv4

import (
	"bytes"
	"testing"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/model"
)

func TestNormalizedLogV3MatchesStoredProjection(t *testing.T) {
	source := richLogsRequest().ResourceLogs[0]
	service, host := ingest.ExtractResource(source.Resource.Attributes)
	entry, err := ingest.OTLPLogToEntry(source.ScopeLogs[0].LogRecords[0], service, host)
	if err != nil {
		t.Fatal(err)
	}
	entry.ID = model.EntryID{1, 2, 3}

	envelope, err := NormalizedLogV3(entry)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Fidelity() != FidelityNormalizedV3 || envelope.Signal() != SignalLogs {
		t.Fatalf("envelope metadata = (%v, %v)", envelope.Signal(), envelope.Fidelity())
	}
	message, err := envelope.Request()
	if err != nil {
		t.Fatal(err)
	}
	request := message.(*collectorlogs.ExportLogsServiceRequest)
	if len(request.ResourceLogs) != 1 || len(request.ResourceLogs[0].ScopeLogs) != 1 ||
		len(request.ResourceLogs[0].ScopeLogs[0].LogRecords) != 1 {
		t.Fatalf("normalized request shape = %v", request)
	}
	record := request.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if record.TimeUnixNano != uint64(entry.Timestamp.UnixNano()) || record.Body.GetStringValue() != entry.Body ||
		record.SeverityNumber != logspb.SeverityNumber_SEVERITY_NUMBER_WARN || record.SeverityText != "WARN" {
		t.Fatalf("normalized log record = %v", record)
	}
	if !bytes.Equal(record.TraceId, entry.TraceID[:]) || !bytes.Equal(record.SpanId, entry.SpanID[:]) {
		t.Fatal("normalized log IDs differ")
	}
	if got := request.ResourceLogs[0].Resource.Attributes; len(got) != 1 || got[0].Key != "service.name" || got[0].Value.GetStringValue() != service {
		t.Fatalf("normalized resource attrs = %v", got)
	}
	if len(record.Attributes) != 1 || record.Attributes[0].Value.GetStringValue() != "true" {
		t.Fatalf("normalized log attrs = %v", record.Attributes)
	}
}

func TestNormalizedSpanV3MatchesStoredProjection(t *testing.T) {
	source := richTracesRequest().ResourceSpans[0]
	service, _ := ingest.ExtractResource(source.Resource.Attributes)
	entry, err := ingest.OTLPSpanToEntry(source.ScopeSpans[0].Spans[0], service)
	if err != nil {
		t.Fatal(err)
	}
	entry.ID = model.EntryID{4, 5, 6}

	envelope, err := NormalizedSpanV3(entry)
	if err != nil {
		t.Fatal(err)
	}
	message, err := envelope.Request()
	if err != nil {
		t.Fatal(err)
	}
	request := message.(*collectortrace.ExportTraceServiceRequest)
	span := request.ResourceSpans[0].ScopeSpans[0].Spans[0]
	if span.Name != entry.Operation || span.Kind != tracepb.Span_SPAN_KIND_UNSPECIFIED ||
		span.Status.GetCode() != tracepb.Status_STATUS_CODE_ERROR || span.Status.GetMessage() != "" {
		t.Fatalf("normalized span = %v", span)
	}
	if !bytes.Equal(span.TraceId, entry.TraceID[:]) || !bytes.Equal(span.SpanId, entry.SpanID[:]) ||
		!bytes.Equal(span.ParentSpanId, entry.ParentID[:]) {
		t.Fatal("normalized span IDs differ")
	}
	if span.StartTimeUnixNano != uint64(entry.StartTime.UnixNano()) || span.EndTimeUnixNano != uint64(entry.EndTime.UnixNano()) {
		t.Fatal("normalized span times differ")
	}
	if len(span.Events) != 0 || len(span.Links) != 0 || span.TraceState != "" {
		t.Fatal("normalized span invented unavailable fields")
	}
}

func TestNormalizedV3IsDeterministicAndRejectsUnknownEnums(t *testing.T) {
	entry := model.LogEntry{
		Timestamp: time.Unix(0, 123), Level: model.LevelInfo, Service: "api", Host: "node",
		Body: "hello", Attrs: []model.Attr{{Key: "k", Value: "v"}},
	}
	first, err := NormalizedLogV3(entry)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NormalizedLogV3(entry)
	if err != nil {
		t.Fatal(err)
	}
	a, err := first.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	b, err := second.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("same v3 entry produced different envelope bytes")
	}
	entry.Level = model.Level(255)
	if _, err := NormalizedLogV3(entry); err == nil {
		t.Fatal("NormalizedLogV3() error = nil for unknown level")
	}
	if _, err := NormalizedSpanV3(model.SpanEntry{Status: model.SpanStatus(255)}); err == nil {
		t.Fatal("NormalizedSpanV3() error = nil for unknown status")
	}
}
