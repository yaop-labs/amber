package otlpv4

import (
	"bytes"
	"testing"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestEnvelopeSemanticRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		signal  Signal
		request proto.Message
	}{
		{name: "logs", signal: SignalLogs, request: richLogsRequest()},
		{name: "traces", signal: SignalTraces, request: richTracesRequest()},
		{name: "metrics", signal: SignalMetrics, request: richMetricsRequest()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := New(test.signal, FidelityOTLP, test.request)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}
			encoded, err := envelope.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			parsed, err := Parse(encoded)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if parsed.Signal() != test.signal || parsed.Fidelity() != FidelityOTLP {
				t.Fatalf("metadata = (%v, %v), want (%v, %v)", parsed.Signal(), parsed.Fidelity(), test.signal, FidelityOTLP)
			}
			replayed, err := parsed.Request()
			if err != nil {
				t.Fatalf("Request() error = %v", err)
			}
			if !proto.Equal(test.request, replayed) {
				t.Fatalf("replayed request differs:\nwant: %v\ngot:  %v", test.request, replayed)
			}
			encodedAgain, err := parsed.MarshalBinary()
			if err != nil {
				t.Fatalf("second MarshalBinary() error = %v", err)
			}
			if !bytes.Equal(encoded, encodedAgain) {
				t.Fatal("deterministic envelope bytes changed after replay")
			}
		})
	}
}

func TestEnvelopePreservesUnknownProtobufFields(t *testing.T) {
	request := richLogsRequest()
	raw, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	raw = protowire.AppendTag(raw, 127, protowire.VarintType)
	raw = protowire.AppendVarint(raw, 42)
	withUnknown := &collectorlogs.ExportLogsServiceRequest{}
	if err := proto.Unmarshal(raw, withUnknown); err != nil {
		t.Fatal(err)
	}

	envelope, err := New(SignalLogs, FidelityOTLP, withUnknown)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := envelope.Request()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(withUnknown.ProtoReflect().GetUnknown(), replayed.ProtoReflect().GetUnknown()) {
		t.Fatal("unknown protobuf fields were not preserved")
	}
}

func TestEnvelopeRejectsCorruptionAndTypeMismatch(t *testing.T) {
	if _, err := New(SignalLogs, FidelityOTLP, richTracesRequest()); err == nil {
		t.Fatal("New() error = nil for mismatched signal")
	}
	if _, err := New(SignalLogs, Fidelity(2), richLogsRequest()); err == nil {
		t.Fatal("New() error = nil for removed fidelity")
	}
	envelope, err := New(SignalLogs, FidelityNormalizedNative, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := envelope.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	encoded[len(encoded)-1] ^= 0xff
	if _, err := Parse(encoded); err == nil {
		t.Fatal("Parse() error = nil for corrupt envelope")
	}
	if _, err := Parse(append(encoded, 0)); err == nil {
		t.Fatal("Parse() error = nil for trailing data")
	}
}

func richLogsRequest() *collectorlogs.ExportLogsServiceRequest {
	return &collectorlogs.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource:  richResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.40.0",
		ScopeLogs: []*logspb.ScopeLogs{{
			Scope:     richScope(),
			SchemaUrl: "https://example.test/log-schema",
			LogRecords: []*logspb.LogRecord{{
				TimeUnixNano:           1_700_000_000_123_456_789,
				ObservedTimeUnixNano:   1_700_000_000_123_456_999,
				SeverityNumber:         logspb.SeverityNumber_SEVERITY_NUMBER_WARN2,
				SeverityText:           "warning-custom",
				Body:                   kvListValue("message", stringValue("disk pressure")),
				Attributes:             []*commonpb.KeyValue{{Key: "retry", Value: boolValue(true)}},
				DroppedAttributesCount: 3,
				Flags:                  1,
				TraceId:                bytes.Repeat([]byte{0x11}, 16),
				SpanId:                 bytes.Repeat([]byte{0x22}, 8),
				EventName:              "com.example.disk.pressure",
			}},
		}},
	}}}
}

func richTracesRequest() *collectortrace.ExportTraceServiceRequest {
	return &collectortrace.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		Resource:  richResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.40.0",
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope:     richScope(),
			SchemaUrl: "https://example.test/trace-schema",
			Spans: []*tracepb.Span{{
				TraceId:                bytes.Repeat([]byte{0x31}, 16),
				SpanId:                 bytes.Repeat([]byte{0x32}, 8),
				TraceState:             "vendor=value",
				ParentSpanId:           bytes.Repeat([]byte{0x33}, 8),
				Flags:                  0x301,
				Name:                   "POST /checkout",
				Kind:                   tracepb.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano:      1_700_000_000_123_456_789,
				EndTimeUnixNano:        1_700_000_000_223_456_789,
				Attributes:             []*commonpb.KeyValue{{Key: "http.status_code", Value: intValue(503)}},
				DroppedAttributesCount: 1,
				Events: []*tracepb.Span_Event{{
					TimeUnixNano: 1_700_000_000_200_000_000, Name: "exception",
					Attributes:             []*commonpb.KeyValue{{Key: "exception.type", Value: stringValue("io.EOF")}},
					DroppedAttributesCount: 2,
				}},
				DroppedEventsCount: 3,
				Links: []*tracepb.Span_Link{{
					TraceId: bytes.Repeat([]byte{0x41}, 16), SpanId: bytes.Repeat([]byte{0x42}, 8),
					TraceState: "linked=value", Flags: 1,
					Attributes:             []*commonpb.KeyValue{{Key: "link.type", Value: stringValue("batch")}},
					DroppedAttributesCount: 4,
				}},
				DroppedLinksCount: 5,
				Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_ERROR, Message: "upstream unavailable"},
			}},
		}},
	}}}
}

func richMetricsRequest() *collectormetrics.ExportMetricsServiceRequest {
	return &collectormetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		Resource:  richResource(),
		SchemaUrl: "https://opentelemetry.io/schemas/1.40.0",
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope:     richScope(),
			SchemaUrl: "https://example.test/metric-schema",
			Metrics: []*metricspb.Metric{{
				Name: "request.duration", Description: "request duration", Unit: "s",
				Metadata: []*commonpb.KeyValue{{Key: "prometheus.type", Value: stringValue("gauge")}},
				Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{{
					Attributes:        []*commonpb.KeyValue{{Key: "route", Value: stringValue("/checkout")}},
					StartTimeUnixNano: 1_700_000_000_000_000_001,
					TimeUnixNano:      1_700_000_000_123_456_789,
					Value:             &metricspb.NumberDataPoint_AsDouble{AsDouble: 0.125125},
					Exemplars: []*metricspb.Exemplar{{
						FilteredAttributes: []*commonpb.KeyValue{{Key: "sampled", Value: boolValue(true)}},
						TimeUnixNano:       1_700_000_000_120_000_000,
						Value:              &metricspb.Exemplar_AsDouble{AsDouble: 0.125},
						TraceId:            bytes.Repeat([]byte{0x51}, 16),
						SpanId:             bytes.Repeat([]byte{0x52}, 8),
					}},
					Flags: 1,
				}}}},
			}},
		}},
	}}}
}

func richResource() *resourcepb.Resource {
	return &resourcepb.Resource{
		Attributes: []*commonpb.KeyValue{
			{Key: "service.name", Value: stringValue("checkout")},
			{Key: "service.instance.id", Value: intValue(42)},
		},
		DroppedAttributesCount: 2,
	}
}

func richScope() *commonpb.InstrumentationScope {
	return &commonpb.InstrumentationScope{
		Name: "github.com/example/checkout", Version: "v1.2.3",
		Attributes:             []*commonpb.KeyValue{{Key: "build.debug", Value: boolValue(false)}},
		DroppedAttributesCount: 1,
	}
}

func stringValue(value string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
}

func intValue(value int64) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}}
}

func boolValue(value bool) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: value}}
}

func kvListValue(key string, value *commonpb.AnyValue) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{
		Values: []*commonpb.KeyValue{{Key: key, Value: value}},
	}}}
}
