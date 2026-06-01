package http

import (
	"errors"
	"io"
	"net/http"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/amber/metricsengine"
)

// nanosToMillis converts an OTLP timestamp (uint64 unix nanos) into the
// int64 unix milliseconds the metricsengine model expects.
func nanosToMillis(unixNano uint64) int64 {
	return int64(unixNano / 1_000_000)
}

func (h *OTLPHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if h.metricStore == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectormetrics.ExportMetricsServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		writeError(w, http.StatusBadRequest, "decode failed: "+err.Error())
		return
	}

	var accepted, rejected, unsupported int
	for _, rm := range req.ResourceMetrics {
		resourceAttrs := kvToMap(rm.Resource.GetAttributes())
		for _, sm := range rm.ScopeMetrics {
			scopeAttrs := kvToMap(sm.Scope.GetAttributes())
			for _, metric := range sm.Metrics {
				batch := metricsengine.OTLPBatch{
					ResourceAttributes: resourceAttrs,
					ScopeAttributes:    scopeAttrs,
				}
				addPoints, kind := pointsForMetric(metric)
				if !kind.supported {
					unsupported += kind.skipCount
					continue
				}
				batch.Points = addPoints
				if len(batch.Points) == 0 {
					continue
				}
				samples, err := metricsengine.OTLPSamples(batch)
				if err != nil {
					rejected += len(batch.Points)
					h.log.Warn("otlp metric sample conversion failed", "metric", metric.Name, "err", err)
					continue
				}
				if _, err := h.metricStore.AppendBatch(samples); err != nil {
					rejected += len(samples)
					if !errors.Is(err, metricsengine.ErrNoSamples) {
						h.log.Warn("otlp metric append failed", "metric", metric.Name, "err", err)
					}
					continue
				}
				accepted += len(samples)
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":    accepted,
		"rejected":    rejected,
		"unsupported": unsupported,
	})
}

type metricKindStatus struct {
	supported bool
	skipCount int
}

// pointsForMetric flattens a single OTLP Metric into metricsengine OTLP points.
// Only Gauge (number) and Sum (number, monotonic or not — both ride the
// Counter path in metricsengine's adapter) are wired in v0. Histogram and
// ExponentialHistogram are accepted by the protocol but skipped here:
// metricsengine's query layer has no execution path for them yet
// (docs/metricsengine/V0_HANDOFF.md), so writing them would just bloat the
// WAL without any read path catching them.
func pointsForMetric(metric *metricspb.Metric) ([]metricsengine.OTLPPoint, metricKindStatus) {
	switch data := metric.Data.(type) {
	case *metricspb.Metric_Gauge:
		return numberPoints(metric.Name, metricsengine.OTLPMetricGauge, data.Gauge.GetDataPoints()), metricKindStatus{supported: true}
	case *metricspb.Metric_Sum:
		return numberPoints(metric.Name, metricsengine.OTLPMetricSum, data.Sum.GetDataPoints()), metricKindStatus{supported: true}
	case *metricspb.Metric_Histogram:
		return nil, metricKindStatus{supported: false, skipCount: len(data.Histogram.GetDataPoints())}
	case *metricspb.Metric_ExponentialHistogram:
		return nil, metricKindStatus{supported: false, skipCount: len(data.ExponentialHistogram.GetDataPoints())}
	default:
		return nil, metricKindStatus{supported: false, skipCount: 1}
	}
}

func numberPoints(name string, kind metricsengine.OTLPMetricKind, dps []*metricspb.NumberDataPoint) []metricsengine.OTLPPoint {
	points := make([]metricsengine.OTLPPoint, 0, len(dps))
	for _, dp := range dps {
		point := metricsengine.OTLPPoint{
			Name:       name,
			Kind:       kind,
			Timestamp:  nanosToMillis(dp.TimeUnixNano),
			Attributes: kvToMap(dp.Attributes),
		}
		switch v := dp.Value.(type) {
		case *metricspb.NumberDataPoint_AsInt:
			point.NumberKind = metricsengine.OTLPNumberInt
			point.IntValue = v.AsInt
		case *metricspb.NumberDataPoint_AsDouble:
			point.NumberKind = metricsengine.OTLPNumberFloat
			point.FloatValue = v.AsDouble
		default:
			continue
		}
		points = append(points, point)
	}
	return points
}

func kvToMap(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		out[kv.Key] = kv.Value.GetStringValue()
	}
	return out
}
