package otlpv4

import (
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// LogsSubset clones req and retains only accepted records. Cloning preserves
// known and unknown protobuf fields at every retained hierarchy level.
func LogsSubset(req *collectorlogs.ExportLogsServiceRequest, accepted map[*logspb.LogRecord]struct{}) *collectorlogs.ExportLogsServiceRequest {
	if req == nil || len(accepted) == 0 {
		return nil
	}
	out := proto.Clone(req).(*collectorlogs.ExportLogsServiceRequest)
	resources := out.ResourceLogs[:0]
	for resourceIndex, originalResource := range req.ResourceLogs {
		clonedResource := out.ResourceLogs[resourceIndex]
		if originalResource == nil || clonedResource == nil {
			continue
		}
		scopes := clonedResource.ScopeLogs[:0]
		for scopeIndex, originalScope := range originalResource.ScopeLogs {
			clonedScope := clonedResource.ScopeLogs[scopeIndex]
			if originalScope == nil || clonedScope == nil {
				continue
			}
			records := clonedScope.LogRecords[:0]
			for recordIndex, originalRecord := range originalScope.LogRecords {
				if _, ok := accepted[originalRecord]; ok {
					records = append(records, clonedScope.LogRecords[recordIndex])
				}
			}
			clonedScope.LogRecords = records
			if len(records) != 0 {
				scopes = append(scopes, clonedScope)
			}
		}
		clonedResource.ScopeLogs = scopes
		if len(scopes) != 0 {
			resources = append(resources, clonedResource)
		}
	}
	out.ResourceLogs = resources
	return out
}

// TracesSubset is LogsSubset for accepted spans.
func TracesSubset(req *collectortrace.ExportTraceServiceRequest, accepted map[*tracepb.Span]struct{}) *collectortrace.ExportTraceServiceRequest {
	if req == nil || len(accepted) == 0 {
		return nil
	}
	out := proto.Clone(req).(*collectortrace.ExportTraceServiceRequest)
	resources := out.ResourceSpans[:0]
	for resourceIndex, originalResource := range req.ResourceSpans {
		clonedResource := out.ResourceSpans[resourceIndex]
		if originalResource == nil || clonedResource == nil {
			continue
		}
		scopes := clonedResource.ScopeSpans[:0]
		for scopeIndex, originalScope := range originalResource.ScopeSpans {
			clonedScope := clonedResource.ScopeSpans[scopeIndex]
			if originalScope == nil || clonedScope == nil {
				continue
			}
			spans := clonedScope.Spans[:0]
			for spanIndex, originalSpan := range originalScope.Spans {
				if _, ok := accepted[originalSpan]; ok {
					spans = append(spans, clonedScope.Spans[spanIndex])
				}
			}
			clonedScope.Spans = spans
			if len(spans) != 0 {
				scopes = append(scopes, clonedScope)
			}
		}
		clonedResource.ScopeSpans = scopes
		if len(scopes) != 0 {
			resources = append(resources, clonedResource)
		}
	}
	out.ResourceSpans = resources
	return out
}

// MetricsSubset clones req and retains only accepted data points. accepted
// keys are original NumberDataPoint, HistogramDataPoint, or
// ExponentialHistogramDataPoint messages.
func MetricsSubset(req *collectormetrics.ExportMetricsServiceRequest, accepted map[proto.Message]struct{}) *collectormetrics.ExportMetricsServiceRequest {
	if req == nil || len(accepted) == 0 {
		return nil
	}
	out := proto.Clone(req).(*collectormetrics.ExportMetricsServiceRequest)
	resources := out.ResourceMetrics[:0]
	for resourceIndex, originalResource := range req.ResourceMetrics {
		clonedResource := out.ResourceMetrics[resourceIndex]
		if originalResource == nil || clonedResource == nil {
			continue
		}
		scopes := clonedResource.ScopeMetrics[:0]
		for scopeIndex, originalScope := range originalResource.ScopeMetrics {
			clonedScope := clonedResource.ScopeMetrics[scopeIndex]
			if originalScope == nil || clonedScope == nil {
				continue
			}
			metrics := clonedScope.Metrics[:0]
			for metricIndex, originalMetric := range originalScope.Metrics {
				clonedMetric := clonedScope.Metrics[metricIndex]
				if originalMetric == nil || clonedMetric == nil {
					continue
				}
				if filterMetricPoints(originalMetric, clonedMetric, accepted) {
					metrics = append(metrics, clonedMetric)
				}
			}
			clonedScope.Metrics = metrics
			if len(metrics) != 0 {
				scopes = append(scopes, clonedScope)
			}
		}
		clonedResource.ScopeMetrics = scopes
		if len(scopes) != 0 {
			resources = append(resources, clonedResource)
		}
	}
	out.ResourceMetrics = resources
	return out
}

func filterMetricPoints(original, cloned *metricspb.Metric, accepted map[proto.Message]struct{}) bool {
	switch originalData := original.Data.(type) {
	case *metricspb.Metric_Gauge:
		clonedData := cloned.GetGauge()
		if originalData.Gauge == nil || clonedData == nil {
			return false
		}
		clonedData.DataPoints = filterNumberPoints(originalData.Gauge.GetDataPoints(), clonedData.GetDataPoints(), accepted)
		return len(clonedData.DataPoints) != 0
	case *metricspb.Metric_Sum:
		clonedData := cloned.GetSum()
		if originalData.Sum == nil || clonedData == nil {
			return false
		}
		clonedData.DataPoints = filterNumberPoints(originalData.Sum.GetDataPoints(), clonedData.GetDataPoints(), accepted)
		return len(clonedData.DataPoints) != 0
	case *metricspb.Metric_Histogram:
		clonedData := cloned.GetHistogram()
		if originalData.Histogram == nil || clonedData == nil {
			return false
		}
		clonedData.DataPoints = filterHistogramPoints(originalData.Histogram.GetDataPoints(), clonedData.GetDataPoints(), accepted)
		return len(clonedData.DataPoints) != 0
	case *metricspb.Metric_ExponentialHistogram:
		clonedData := cloned.GetExponentialHistogram()
		if originalData.ExponentialHistogram == nil || clonedData == nil {
			return false
		}
		clonedData.DataPoints = filterExponentialPoints(originalData.ExponentialHistogram.GetDataPoints(), clonedData.GetDataPoints(), accepted)
		return len(clonedData.DataPoints) != 0
	default:
		return false
	}
}

func filterNumberPoints(original, cloned []*metricspb.NumberDataPoint, accepted map[proto.Message]struct{}) []*metricspb.NumberDataPoint {
	out := cloned[:0]
	for i, point := range original {
		if _, ok := accepted[point]; ok {
			out = append(out, cloned[i])
		}
	}
	return out
}

func filterHistogramPoints(original, cloned []*metricspb.HistogramDataPoint, accepted map[proto.Message]struct{}) []*metricspb.HistogramDataPoint {
	out := cloned[:0]
	for i, point := range original {
		if _, ok := accepted[point]; ok {
			out = append(out, cloned[i])
		}
	}
	return out
}

func filterExponentialPoints(original, cloned []*metricspb.ExponentialHistogramDataPoint, accepted map[proto.Message]struct{}) []*metricspb.ExponentialHistogramDataPoint {
	out := cloned[:0]
	for i, point := range original {
		if _, ok := accepted[point]; ok {
			out = append(out, cloned[i])
		}
	}
	return out
}
