package otlpv4

import (
	"fmt"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/amber/internal/model"
)

// NormalizedLogNative projects a native Amber log into canonical OTLP.
func NormalizedLogNative(entry model.LogEntry) (Envelope, error) {
	return normalizedLog(entry, FidelityNormalizedNative)
}

func normalizedLog(entry model.LogEntry, fidelity Fidelity) (Envelope, error) {
	severity, err := normalizedSeverity(entry.Level)
	if err != nil {
		return Envelope{}, err
	}
	record := &logspb.LogRecord{
		TimeUnixNano:   uint64(entry.Timestamp.UnixNano()),
		SeverityNumber: severity,
		SeverityText:   entry.Level.String(),
		Body:           normalizedStringValue(entry.Body),
		Attributes:     normalizedAttrs(entry.Attrs),
	}
	if !model.IsZeroTraceID(entry.TraceID) {
		record.TraceId = append([]byte(nil), entry.TraceID[:]...)
	}
	if !model.IsZeroSpanID(entry.SpanID) {
		record.SpanId = append([]byte(nil), entry.SpanID[:]...)
	}
	request := &collectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{{
			Resource:  &resourcepb.Resource{Attributes: logResourceAttrs(entry.Service, entry.Host)},
			ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{record}}},
		}},
	}
	return New(SignalLogs, fidelity, request)
}

// NormalizedSpanNative projects a native Amber span into canonical OTLP.
func NormalizedSpanNative(entry model.SpanEntry) (Envelope, error) {
	return normalizedSpan(entry, FidelityNormalizedNative)
}

func normalizedSpan(entry model.SpanEntry, fidelity Fidelity) (Envelope, error) {
	status, err := normalizedStatus(entry.Status)
	if err != nil {
		return Envelope{}, err
	}
	span := &tracepb.Span{
		TraceId:           append([]byte(nil), entry.TraceID[:]...),
		SpanId:            append([]byte(nil), entry.SpanID[:]...),
		Name:              entry.Operation,
		StartTimeUnixNano: uint64(entry.StartTime.UnixNano()),
		EndTimeUnixNano:   uint64(entry.EndTime.UnixNano()),
		Attributes:        normalizedAttrs(entry.Attrs),
		Status:            status,
	}
	if !model.IsZeroSpanID(entry.ParentID) {
		span.ParentSpanId = append([]byte(nil), entry.ParentID[:]...)
	}
	request := &collectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource:   &resourcepb.Resource{Attributes: serviceResourceAttrs(entry.Service)},
			ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{span}}},
		}},
	}
	return New(SignalTraces, fidelity, request)
}

func normalizedSeverity(level model.Level) (logspb.SeverityNumber, error) {
	switch level {
	case model.LevelTrace:
		return logspb.SeverityNumber_SEVERITY_NUMBER_TRACE, nil
	case model.LevelDebug:
		return logspb.SeverityNumber_SEVERITY_NUMBER_DEBUG, nil
	case model.LevelInfo:
		return logspb.SeverityNumber_SEVERITY_NUMBER_INFO, nil
	case model.LevelWarn:
		return logspb.SeverityNumber_SEVERITY_NUMBER_WARN, nil
	case model.LevelError:
		return logspb.SeverityNumber_SEVERITY_NUMBER_ERROR, nil
	case model.LevelFatal:
		return logspb.SeverityNumber_SEVERITY_NUMBER_FATAL, nil
	default:
		return 0, fmt.Errorf("otlpv4: unsupported native log level %d", level)
	}
}

func normalizedStatus(status model.SpanStatus) (*tracepb.Status, error) {
	switch status {
	case model.SpanStatusUnset:
		return nil, nil
	case model.SpanStatusOK:
		return &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK}, nil
	case model.SpanStatusError:
		return &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR}, nil
	default:
		return nil, fmt.Errorf("otlpv4: unsupported native span status %d", status)
	}
}

func normalizedAttrs(attrs []model.Attr) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		out = append(out, &commonpb.KeyValue{Key: attr.Key, Value: normalizedStringValue(attr.Value)})
	}
	return out
}

func logResourceAttrs(service, host string) []*commonpb.KeyValue {
	attrs := serviceResourceAttrs(service)
	if host != "" {
		attrs = append(attrs, &commonpb.KeyValue{Key: "host.name", Value: normalizedStringValue(host)})
	}
	return attrs
}

func serviceResourceAttrs(service string) []*commonpb.KeyValue {
	if service == "" {
		return nil
	}
	return []*commonpb.KeyValue{{Key: "service.name", Value: normalizedStringValue(service)}}
}

func normalizedStringValue(value string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
}
