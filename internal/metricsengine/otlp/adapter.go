// Package otlp adapts OTLP metric points into the engine's storage samples,
// including the float-to-scaled-int64 conversion and its overflow guards.
package otlp

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// NumberKind distinguishes an integer from a floating-point data point.
type NumberKind uint8

const (
	NumberInt NumberKind = iota + 1
	NumberFloat
)

// MetricKind is the OTLP metric instrument type of a point.
type MetricKind uint8

const (
	MetricGauge MetricKind = iota + 1
	MetricSum
	MetricHistogram
	MetricExponentialHistogram
)

// Point is one OTLP data point with its name, attributes, value (int or float
// with the scale used to store floats as int64), and instrument kind.
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

// Batch is a set of OTLP points sharing resource and scope attributes.
type Batch struct {
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	Points             []Point
}

// maxScaledFloat is float64(math.MaxInt64) == 2^63: the magnitude at which a
// scaled float no longer fits the int64 value model.
const maxScaledFloat = float64(math.MaxInt64)

// scaledFloatValue converts a float to the int64 value model as
// round(value*scale). ok is false for NaN, +/-Inf, and values whose scaled
// form overflows int64 - storing those would produce garbage samples.
func scaledFloatValue(value float64, scale int64) (int64, bool) {
	scaled := math.Round(value * float64(scale))
	if math.IsNaN(scaled) || scaled >= maxScaledFloat || scaled <= -maxScaledFloat {
		return 0, false
	}
	return int64(scaled), true
}

// Samples converts a batch to storage samples. Float points that cannot be
// represented in the int64 value model (NaN - OTLP uses it as a staleness
// marker - +/-Inf, and overflow at the point's scale) are skipped, not stored
// as garbage. Use SamplesSkipped to observe how many were dropped.
func Samples(batch Batch) ([]model.Sample, error) {
	samples, _, err := SamplesSkipped(batch)
	return samples, err
}

// SamplesSkipped is Samples plus the count of float points that were dropped
// because their value cannot be encoded (NaN, +/-Inf, int64 overflow).
func SamplesSkipped(batch Batch) ([]model.Sample, int, error) {
	samples := make([]model.Sample, 0, len(batch.Points))
	skipped := 0
	for _, point := range batch.Points {
		if point.Name == "" {
			return nil, 0, errors.New("otlp: metric name is required")
		}
		value := point.IntValue
		if point.NumberKind == NumberFloat {
			scale := point.Scale
			if scale == 0 {
				scale = 1000
			}
			if scale < 0 {
				return nil, 0, fmt.Errorf("otlp: scale for %q must be positive", point.Name)
			}
			v, ok := scaledFloatValue(point.FloatValue, scale)
			if !ok {
				skipped++
				continue
			}
			value = v
		} else if point.NumberKind != NumberInt {
			return nil, 0, fmt.Errorf("otlp: unsupported number kind for %q", point.Name)
		}
		samples = append(samples, model.Sample{
			Labels:    labelsForPoint(batch, point),
			Type:      metricType(point.Kind),
			Timestamp: point.Timestamp,
			Value:     value,
		})
	}
	return samples, skipped, nil
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
