package http

import (
	"errors"
	"io"
	"net/http"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	meotlp "github.com/yaop-labs/amber/internal/metricsengine/otlp"
	"github.com/yaop-labs/amber/internal/selfobs"
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
	// Histogram series are accumulated across the whole request and flushed
	// as a single block at the end. histogram.Store.WriteBlock creates one
	// file per call, so writing per-metric would explode small-block count
	// for a typical OTLP request carrying ~10s of metric definitions.
	// IDs are reassigned to the global sequence below to avoid collisions
	// across (resource, scope) sub-batches.
	var expAll []histogram.ExpSeries
	var explicitAll []histogram.ExplicitSeries

	for _, rm := range req.ResourceMetrics {
		resourceAttrs := kvToMap(rm.Resource.GetAttributes())
		for _, sm := range rm.ScopeMetrics {
			scopeAttrs := kvToMap(sm.Scope.GetAttributes())
			for _, metric := range sm.Metrics {
				switch data := metric.Data.(type) {
				case *metricspb.Metric_Gauge, *metricspb.Metric_Sum:
					_ = data // type-asserted only to route to scalar path.
					a, r := h.ingestScalar(metric, resourceAttrs, scopeAttrs)
					accepted += a
					rejected += r
				case *metricspb.Metric_Histogram:
					if h.histStore == nil {
						unsupported += len(data.Histogram.GetDataPoints())
						selfobs.MetricsIngestUnsupported.WithLabelValues("histogram").Add(uint64(len(data.Histogram.GetDataPoints())))
						continue
					}
					series := explicitSeriesFor(metric, data.Histogram, resourceAttrs, scopeAttrs)
					accepted += sumExplicitPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("histogram").Add(uint64(sumExplicitPoints(series)))
					explicitAll = append(explicitAll, series...)
				case *metricspb.Metric_ExponentialHistogram:
					if h.histStore == nil {
						unsupported += len(data.ExponentialHistogram.GetDataPoints())
						selfobs.MetricsIngestUnsupported.WithLabelValues("exphistogram").Add(uint64(len(data.ExponentialHistogram.GetDataPoints())))
						continue
					}
					series := expSeriesFor(metric, data.ExponentialHistogram, resourceAttrs, scopeAttrs)
					accepted += sumExpPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("exphistogram").Add(uint64(sumExpPoints(series)))
					expAll = append(expAll, series...)
				default:
					unsupported++
					selfobs.MetricsIngestUnsupported.WithLabelValues("unknown").Add(1)
				}
			}
		}
	}

	if len(expAll) > 0 || len(explicitAll) > 0 {
		assignSeriesIDs(expAll, explicitAll)
		if _, err := h.histStore.WriteBlock(expAll, explicitAll); err != nil {
			// Treat block-write failure as ingest rejection: the histogram
			// data never landed. Count by point total, not series count, so
			// the counter matches scalar semantics.
			pts := sumExpPoints(expAll) + sumExplicitPoints(explicitAll)
			rejected += pts
			accepted -= pts
			selfobs.MetricsIngestRejected.WithLabelValues("hist_write").Add(uint64(pts))
			h.log.Warn("histogram block write failed", "err", err)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":    accepted,
		"rejected":    rejected,
		"unsupported": unsupported,
	})
}

// ingestScalar walks the scalar (Gauge/Sum) path: pointsForMetric → samples
// → metricStore.AppendBatch. Selfobs counters are bumped inline; the return
// values feed only the response-body accept/reject totals.
func (h *OTLPHandler) ingestScalar(metric *metricspb.Metric, resourceAttrs, scopeAttrs map[string]string) (int, int) {
	addPoints, kind := pointsForMetric(metric)
	if !kind.supported || len(addPoints) == 0 {
		return 0, 0
	}
	batch := metricsengine.OTLPBatch{
		ResourceAttributes: resourceAttrs,
		ScopeAttributes:    scopeAttrs,
		Points:             addPoints,
	}
	samples, err := metricsengine.OTLPSamples(batch)
	if err != nil {
		selfobs.MetricsIngestRejected.WithLabelValues("conversion").Add(uint64(len(addPoints)))
		h.log.Warn("otlp metric sample conversion failed", "metric", metric.Name, "err", err)
		return 0, len(addPoints)
	}
	if _, err := h.metricStore.AppendBatch(samples); err != nil {
		selfobs.MetricsIngestRejected.WithLabelValues("append").Add(uint64(len(samples)))
		if !errors.Is(err, metricsengine.ErrNoSamples) {
			h.log.Warn("otlp metric append failed", "metric", metric.Name, "err", err)
		}
		return 0, len(samples)
	}
	selfobs.MetricsIngestAccepted.WithLabelValues(kind.label).Add(uint64(len(samples)))
	return len(samples), 0
}

// expSeriesFor converts one OTLP ExponentialHistogram metric into adapter
// series (grouped by labels). IDs are local-only and reassigned in
// assignSeriesIDs before WriteBlock.
func expSeriesFor(metric *metricspb.Metric, hist *metricspb.ExponentialHistogram, resourceAttrs, scopeAttrs map[string]string) []histogram.ExpSeries {
	dps := hist.GetDataPoints()
	if len(dps) == 0 {
		return nil
	}
	points := make([]meotlp.ExponentialHistogramPoint, 0, len(dps))
	for _, dp := range dps {
		pos := dp.GetPositive()
		neg := dp.GetNegative()
		p := meotlp.ExponentialHistogramPoint{
			Name:           metric.Name,
			Timestamp:      nanosToMillis(dp.TimeUnixNano),
			Attributes:     kvToMap(dp.Attributes),
			Scale:          dp.Scale,
			ZeroCount:      dp.ZeroCount,
			ZeroThreshold:  dp.ZeroThreshold,
			PositiveOffset: pos.GetOffset(),
			PositiveCounts: pos.GetBucketCounts(),
			NegativeOffset: neg.GetOffset(),
			NegativeCounts: neg.GetBucketCounts(),
			Sum:            dp.GetSum(),
			Count:          dp.Count,
			Min:            dp.GetMin(),
			Max:            dp.GetMax(),
			HasMinMax:      dp.Min != nil && dp.Max != nil,
		}
		points = append(points, p)
	}
	batch := meotlp.Batch{ResourceAttributes: resourceAttrs, ScopeAttributes: scopeAttrs}
	return meotlp.ExponentialSeries(batch, points)
}

// explicitSeriesFor mirrors expSeriesFor for the Explicit Histogram case.
func explicitSeriesFor(metric *metricspb.Metric, hist *metricspb.Histogram, resourceAttrs, scopeAttrs map[string]string) []histogram.ExplicitSeries {
	dps := hist.GetDataPoints()
	if len(dps) == 0 {
		return nil
	}
	points := make([]meotlp.ExplicitHistogramPoint, 0, len(dps))
	for _, dp := range dps {
		p := meotlp.ExplicitHistogramPoint{
			Name:           metric.Name,
			Timestamp:      nanosToMillis(dp.TimeUnixNano),
			Attributes:     kvToMap(dp.Attributes),
			ExplicitBounds: dp.ExplicitBounds,
			BucketCounts:   dp.BucketCounts,
			Sum:            dp.GetSum(),
			Count:          dp.Count,
			Min:            dp.GetMin(),
			Max:            dp.GetMax(),
			HasMinMax:      dp.Min != nil && dp.Max != nil,
		}
		points = append(points, p)
	}
	batch := meotlp.Batch{ResourceAttributes: resourceAttrs, ScopeAttributes: scopeAttrs}
	return meotlp.ExplicitSeries(batch, points)
}

// assignSeriesIDs renumbers all series with a contiguous global sequence so
// the adapter's per-call local IDs (1,2,3,...) don't collide across batches.
// histogram.WriteBlock relies on unique SeriesID inside a block.
func assignSeriesIDs(exp []histogram.ExpSeries, explicit []histogram.ExplicitSeries) {
	var next uint64 = 1
	for i := range exp {
		exp[i].ID = next
		next++
	}
	for i := range explicit {
		explicit[i].ID = next
		next++
	}
}

func sumExpPoints(s []histogram.ExpSeries) int {
	n := 0
	for _, ss := range s {
		n += len(ss.Sketches)
	}
	return n
}

func sumExplicitPoints(s []histogram.ExplicitSeries) int {
	n := 0
	for _, ss := range s {
		n += len(ss.Buckets)
	}
	return n
}

type metricKindStatus struct {
	supported bool
	label     string // selfobs label value: "gauge"|"sum"|"unknown"
}

// pointsForMetric flattens an OTLP scalar Metric into metricsengine OTLP
// points. The outer handleMetrics switch dispatches histogram/exp-histogram
// data to the histogram-store path before this is called, so only Gauge/Sum
// reach here; everything else is treated as "unknown".
func pointsForMetric(metric *metricspb.Metric) ([]metricsengine.OTLPPoint, metricKindStatus) {
	switch data := metric.Data.(type) {
	case *metricspb.Metric_Gauge:
		return numberPoints(metric.Name, metricsengine.OTLPMetricGauge, data.Gauge.GetDataPoints()), metricKindStatus{supported: true, label: "gauge"}
	case *metricspb.Metric_Sum:
		return numberPoints(metric.Name, metricsengine.OTLPMetricSum, data.Sum.GetDataPoints()), metricKindStatus{supported: true, label: "sum"}
	default:
		return nil, metricKindStatus{supported: false, label: "unknown"}
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
