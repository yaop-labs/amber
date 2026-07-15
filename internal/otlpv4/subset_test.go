package otlpv4

import (
	"testing"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestSignalSubsetsRetainOnlyAcceptedChildren(t *testing.T) {
	logs := richLogsRequest()
	rejectedLog := proto.Clone(logs.ResourceLogs[0].ScopeLogs[0].LogRecords[0]).(*logspb.LogRecord)
	logs.ResourceLogs[0].ScopeLogs[0].LogRecords = append(logs.ResourceLogs[0].ScopeLogs[0].LogRecords, rejectedLog)
	acceptedLog := logs.ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	logSubset := LogsSubset(logs, map[*logspb.LogRecord]struct{}{acceptedLog: {}})
	if got := logSubset.ResourceLogs[0].ScopeLogs[0].LogRecords; len(got) != 1 || !proto.Equal(got[0], acceptedLog) {
		t.Fatalf("log subset = %v", got)
	}

	traces := richTracesRequest()
	rejectedSpan := proto.Clone(traces.ResourceSpans[0].ScopeSpans[0].Spans[0]).(*tracepb.Span)
	traces.ResourceSpans[0].ScopeSpans[0].Spans = append(traces.ResourceSpans[0].ScopeSpans[0].Spans, rejectedSpan)
	acceptedSpan := traces.ResourceSpans[0].ScopeSpans[0].Spans[0]
	traceSubset := TracesSubset(traces, map[*tracepb.Span]struct{}{acceptedSpan: {}})
	if got := traceSubset.ResourceSpans[0].ScopeSpans[0].Spans; len(got) != 1 || !proto.Equal(got[0], acceptedSpan) {
		t.Fatalf("trace subset = %v", got)
	}
}

func TestMetricsSubsetPreservesHierarchyAndUnknownFields(t *testing.T) {
	metrics := richMetricsRequest()
	metric := metrics.ResourceMetrics[0].ScopeMetrics[0].Metrics[0]
	accepted := metric.GetGauge().DataPoints[0]
	rejected := proto.Clone(accepted).(*metricspb.NumberDataPoint)
	metric.GetGauge().DataPoints = append(metric.GetGauge().DataPoints, rejected)
	unknown := protowire.AppendTag(nil, 127, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 9)
	metrics.ProtoReflect().SetUnknown(unknown)
	metrics.ResourceMetrics[0].ProtoReflect().SetUnknown(unknown)

	subset := MetricsSubset(metrics, map[proto.Message]struct{}{accepted: {}})
	points := subset.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints
	if len(points) != 1 || !proto.Equal(points[0], accepted) {
		t.Fatalf("metric subset points = %v", points)
	}
	if string(subset.ProtoReflect().GetUnknown()) != string(unknown) ||
		string(subset.ResourceMetrics[0].ProtoReflect().GetUnknown()) != string(unknown) {
		t.Fatal("metric subset lost unknown hierarchy fields")
	}
}
