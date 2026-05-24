package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/yaop-labs/amber/internal/model"
)

func ExtractResource(attrs []*commonpb.KeyValue) (service, host string) {
	for _, kv := range attrs {
		switch kv.Key {
		case "service.name":
			service = kv.Value.GetStringValue()
		case "host.name":
			host = kv.Value.GetStringValue()
		}
	}
	return
}

func OTLPLogToEntry(lr *logspb.LogRecord, service, host string) (model.LogEntry, error) {
	level, _ := model.LevelFromString(lr.SeverityText)

	body := AnyValueToString(lr.Body)

	attrs := make([]model.Attr, 0, len(lr.Attributes))
	for _, kv := range lr.Attributes {
		attrs = append(attrs, model.Attr{
			Key:   kv.Key,
			Value: AnyValueToString(kv.Value),
		})
	}

	entry, err := model.NewLogEntry(level, service, host, body, attrs...)
	if err != nil {
		return model.LogEntry{}, err
	}

	if lr.TimeUnixNano > 0 {
		entry.Timestamp = time.Unix(0, int64(lr.TimeUnixNano))
	}

	if len(lr.TraceId) == 16 {
		copy(entry.TraceID[:], lr.TraceId)
	}
	if len(lr.SpanId) == 8 {
		copy(entry.SpanID[:], lr.SpanId)
	}

	return entry, nil
}

func OTLPSpanToEntry(sp *tracepb.Span, service string) (model.SpanEntry, error) {
	var traceID model.TraceID
	var spanID model.SpanID
	var parentID model.SpanID

	if len(sp.TraceId) == 16 {
		copy(traceID[:], sp.TraceId)
	}
	if len(sp.SpanId) == 8 {
		copy(spanID[:], sp.SpanId)
	}
	if len(sp.ParentSpanId) == 8 {
		copy(parentID[:], sp.ParentSpanId)
	}

	entry, err := model.NewSpanEntry(traceID, spanID, parentID, service, sp.Name)
	if err != nil {
		return model.SpanEntry{}, err
	}

	entry.StartTime = time.Unix(0, int64(sp.StartTimeUnixNano))
	entry.EndTime = time.Unix(0, int64(sp.EndTimeUnixNano))
	entry.Status = OTLPStatusToModel(sp.Status)

	for _, kv := range sp.Attributes {
		entry.Attrs = append(entry.Attrs, model.Attr{
			Key:   kv.Key,
			Value: AnyValueToString(kv.Value),
		})
	}

	return entry, nil
}

func OTLPStatusToModel(s *tracepb.Status) model.SpanStatus {
	if s == nil {
		return model.SpanStatusUnset
	}
	switch s.Code {
	case tracepb.Status_STATUS_CODE_OK:
		return model.SpanStatusOK
	case tracepb.Status_STATUS_CODE_ERROR:
		return model.SpanStatusError
	default:
		return model.SpanStatusUnset
	}
}

func AnyValueToString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return val.StringValue
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}
