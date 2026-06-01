package otlp

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

type NumberKind uint8

const (
	NumberInt NumberKind = iota + 1
	NumberFloat
)

type MetricKind uint8

const (
	MetricGauge MetricKind = iota + 1
	MetricSum
	MetricHistogram
	MetricExponentialHistogram
)

type Point struct {
	Name       string
	Kind       MetricKind
	Timestamp  int64
	Attributes map[string]string
	IntValue   int64
	FloatValue float64
	NumberKind NumberKind
	Scale      int64
}

type Batch struct {
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Points             []Point
}

func Samples(batch Batch) ([]model.Sample, error) {
	samples := make([]model.Sample, 0, len(batch.Points))
	for _, point := range batch.Points {
		if point.Name == "" {
			return nil, errors.New("otlp: metric name is required")
		}
		value := point.IntValue
		if point.NumberKind == NumberFloat {
			scale := point.Scale
			if scale == 0 {
				scale = 1000
			}
			if scale < 0 {
				return nil, fmt.Errorf("otlp: scale for %q must be positive", point.Name)
			}
			value = int64(math.Round(point.FloatValue * float64(scale)))
		} else if point.NumberKind != NumberInt {
			return nil, fmt.Errorf("otlp: unsupported number kind for %q", point.Name)
		}
		samples = append(samples, model.Sample{
			Labels:    labelsForPoint(batch, point),
			Type:      metricType(point.Kind),
			Timestamp: point.Timestamp,
			Value:     value,
		})
	}
	return samples, nil
}

func labelsForPoint(batch Batch, point Point) model.LabelSet {
	labels := make(model.LabelSet, 0, len(batch.ResourceAttributes)+len(batch.ScopeAttributes)+len(point.Attributes)+2)
	labels = appendAttrs(labels, "resource.", batch.ResourceAttributes)
	labels = appendAttrs(labels, "scope.", batch.ScopeAttributes)
	labels = appendAttrs(labels, "", point.Attributes)
	labels = append(labels, model.Label{Name: "__name__", Value: point.Name})
	if point.NumberKind == NumberFloat {
		scale := point.Scale
		if scale == 0 {
			scale = 1000
		}
		labels = append(labels, model.Label{Name: "__scale__", Value: strconv.FormatInt(scale, 10)})
	}
	return labels.Canonical()
}

func appendAttrs(labels model.LabelSet, prefix string, attrs map[string]string) model.LabelSet {
	for key, value := range attrs {
		labels = append(labels, model.Label{Name: prefix + key, Value: value})
	}
	return labels
}

func metricType(kind MetricKind) model.MetricType {
	switch kind {
	case MetricSum:
		return model.MetricTypeCounter
	case MetricHistogram:
		return model.MetricTypeHistogram
	case MetricExponentialHistogram:
		return model.MetricTypeExponentialHistogram
	default:
		return model.MetricTypeGauge
	}
}
