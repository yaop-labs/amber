package otlpv4

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	memodel "github.com/yaop-labs/amber/internal/metricsengine/model"
)

const scaleLabel = "__scale__"

// NormalizedMetricSampleV3 projects one legacy scalar sample into the smallest
// OTLP request that preserves its stored labels, timestamp, type, and value.
func NormalizedMetricSampleV3(sample memodel.Sample) (Envelope, error) {
	return normalizedMetricSample(sample, FidelityNormalizedV3)
}

// NormalizedMetricSampleNative projects one native scalar metric sample.
func NormalizedMetricSampleNative(sample memodel.Sample) (Envelope, error) {
	return normalizedMetricSample(sample, FidelityNormalizedNative)
}

func normalizedMetricSample(sample memodel.Sample, fidelity Fidelity) (Envelope, error) {
	context, err := splitMetricLabels(sample.Labels)
	if err != nil {
		return Envelope{}, err
	}
	point := &metricspb.NumberDataPoint{
		TimeUnixNano: millisToNanos(sample.Timestamp),
		Attributes:   context.point,
	}
	if context.hasScale {
		point.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: float64(sample.Value) / float64(context.scale)}
	} else {
		point.Value = &metricspb.NumberDataPoint_AsInt{AsInt: sample.Value}
	}
	metric := &metricspb.Metric{Name: context.name}
	switch sample.Type {
	case memodel.MetricTypeGauge:
		metric.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{point}}}
	case memodel.MetricTypeCounter:
		metric.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			DataPoints:             []*metricspb.NumberDataPoint{point},
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
		}}
	default:
		return Envelope{}, fmt.Errorf("otlpv4: unsupported v3 scalar metric type %d", sample.Type)
	}
	return normalizedMetricEnvelope(context, metric, fidelity)
}

// NormalizedMetricSketchV3 projects one legacy histogram tick into OTLP. The
// stored sketch payload is retained; unavailable OTLP metadata remains absent.
func NormalizedMetricSketchV3(sample engine.SketchSample) (Envelope, error) {
	return normalizedMetricSketch(sample, FidelityNormalizedV3)
}

// NormalizedMetricSketchNative projects one native histogram tick.
func NormalizedMetricSketchNative(sample engine.SketchSample) (Envelope, error) {
	return normalizedMetricSketch(sample, FidelityNormalizedNative)
}

func normalizedMetricSketch(sample engine.SketchSample, fidelity Fidelity) (Envelope, error) {
	if (sample.Exp == nil) == (sample.Explicit == nil) {
		return Envelope{}, errors.New("otlpv4: v3 sketch must set exactly one histogram payload")
	}
	context, err := splitMetricLabels(sample.Labels)
	if err != nil {
		return Envelope{}, err
	}
	if context.hasScale {
		return Envelope{}, errors.New("otlpv4: histogram has unexpected scale label")
	}
	metric := &metricspb.Metric{Name: context.name}
	if histogram := sample.Explicit; histogram != nil {
		point := &metricspb.HistogramDataPoint{
			Attributes:     context.point,
			TimeUnixNano:   millisToNanos(sample.Timestamp),
			Count:          histogram.Count,
			BucketCounts:   append([]uint64(nil), histogram.Counts...),
			ExplicitBounds: append([]float64(nil), histogram.Bounds...),
			Sum:            proto.Float64(histogram.Sum),
		}
		if histogram.HasMinMax {
			point.Min = proto.Float64(histogram.Min)
			point.Max = proto.Float64(histogram.Max)
		}
		metric.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
			DataPoints:             []*metricspb.HistogramDataPoint{point},
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		}}
	} else {
		histogram := sample.Exp
		point := &metricspb.ExponentialHistogramDataPoint{
			Attributes:    context.point,
			TimeUnixNano:  millisToNanos(sample.Timestamp),
			Count:         histogram.Count,
			Sum:           proto.Float64(histogram.Sum),
			Scale:         histogram.Scale,
			ZeroCount:     histogram.ZeroCount,
			ZeroThreshold: histogram.ZeroThreshold,
			Positive: &metricspb.ExponentialHistogramDataPoint_Buckets{
				Offset: histogram.Positive.Offset, BucketCounts: append([]uint64(nil), histogram.Positive.Counts...),
			},
			Negative: &metricspb.ExponentialHistogramDataPoint_Buckets{
				Offset: histogram.Negative.Offset, BucketCounts: append([]uint64(nil), histogram.Negative.Counts...),
			},
		}
		if histogram.HasMinMax {
			point.Min = proto.Float64(histogram.Min)
			point.Max = proto.Float64(histogram.Max)
		}
		metric.Data = &metricspb.Metric_ExponentialHistogram{ExponentialHistogram: &metricspb.ExponentialHistogram{
			DataPoints:             []*metricspb.ExponentialHistogramDataPoint{point},
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
		}}
	}
	return normalizedMetricEnvelope(context, metric, fidelity)
}

type normalizedMetricContext struct {
	name     string
	hasName  bool
	resource []*commonpb.KeyValue
	scope    []*commonpb.KeyValue
	point    []*commonpb.KeyValue
	scale    int64
	hasScale bool
}

func splitMetricLabels(labels memodel.LabelSet) (normalizedMetricContext, error) {
	var context normalizedMetricContext
	for _, label := range labels.Canonical() {
		switch {
		case label.Name == memodel.MetricNameLabel:
			if context.hasName {
				return normalizedMetricContext{}, errors.New("otlpv4: duplicate v3 metric name label")
			}
			context.name, context.hasName = label.Value, true
		case label.Name == scaleLabel:
			if context.hasScale {
				return normalizedMetricContext{}, errors.New("otlpv4: duplicate v3 metric scale label")
			}
			scale, err := strconv.ParseInt(label.Value, 10, 64)
			if err != nil || scale <= 0 {
				return normalizedMetricContext{}, fmt.Errorf("otlpv4: invalid v3 metric scale %q", label.Value)
			}
			context.scale, context.hasScale = scale, true
		case strings.HasPrefix(label.Name, "resource."):
			context.resource = append(context.resource, metricStringAttr(strings.TrimPrefix(label.Name, "resource."), label.Value))
		case strings.HasPrefix(label.Name, "scope."):
			context.scope = append(context.scope, metricStringAttr(strings.TrimPrefix(label.Name, "scope."), label.Value))
		default:
			context.point = append(context.point, metricStringAttr(label.Name, label.Value))
		}
	}
	if !context.hasName || context.name == "" {
		return normalizedMetricContext{}, errors.New("otlpv4: v3 metric name label is required")
	}
	return context, nil
}

func normalizedMetricEnvelope(context normalizedMetricContext, metric *metricspb.Metric, fidelity Fidelity) (Envelope, error) {
	request := &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: context.resource},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope:   &commonpb.InstrumentationScope{Attributes: context.scope},
				Metrics: []*metricspb.Metric{metric},
			}},
		}},
	}
	return New(SignalMetrics, fidelity, request)
}

func metricStringAttr(name, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: name, Value: normalizedStringValue(value)}
}

func millisToNanos(timestamp int64) uint64 {
	return uint64(timestamp) * 1_000_000
}
