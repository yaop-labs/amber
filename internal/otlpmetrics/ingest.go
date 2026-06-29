// Package otlpmetrics is the transport-agnostic OTLP metrics ingestion path.
//
// It converts an OTLP ExportMetricsServiceRequest into metricsengine
// samples/sketches and writes them to the store, returning per-request point
// counts. Both the OTLP/HTTP and OTLP/gRPC endpoints call Ingest so the
// conversion, histogram reassembly, and self-observability counters live in
// exactly one place.
package otlpmetrics

import (
	"errors"
	"log/slog"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	meotlp "github.com/yaop-labs/amber/internal/metricsengine/otlp"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/metricsengine"
)

// Result reports the per-request point counts produced by Ingest.
type Result struct {
	Accepted    int
	Rejected    int
	Unsupported int
}

// Ingest converts req and writes it to store, returning point counts. store
// must be non-nil; callers gate on metric-store availability before calling.
func Ingest(store *metricsengine.Store, req *collectormetrics.ExportMetricsServiceRequest, log *slog.Logger) Result {
	var res Result
	// Histogram series from one request are written as one block.
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
					a, r := ingestScalar(store, metric, resourceAttrs, scopeAttrs, log)
					res.Accepted += a
					res.Rejected += r
				case *metricspb.Metric_Histogram:
					series := explicitSeriesFor(metric, data.Histogram, resourceAttrs, scopeAttrs)
					res.Accepted += sumExplicitPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("histogram").Add(uint64(sumExplicitPoints(series)))
					explicitAll = append(explicitAll, series...)
				case *metricspb.Metric_ExponentialHistogram:
					series := expSeriesFor(metric, data.ExponentialHistogram, resourceAttrs, scopeAttrs)
					res.Accepted += sumExpPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("exphistogram").Add(uint64(sumExpPoints(series)))
					expAll = append(expAll, series...)
				default:
					res.Unsupported++
					selfobs.MetricsIngestUnsupported.WithLabelValues("unknown").Add(1)
				}
			}
		}
	}

	if len(expAll) > 0 || len(explicitAll) > 0 {
		sketches := sketchSamples(expAll, explicitAll)
		if _, err := store.AppendSketches(sketches); err != nil {
			// Treat append failure as ingest rejection: the histogram data
			// never landed. Count by point total, not series count, so the
			// counter matches scalar semantics.
			pts := len(sketches)
			res.Rejected += pts
			res.Accepted -= pts
			selfobs.MetricsIngestRejected.WithLabelValues("hist_write").Add(uint64(pts))
			log.Warn("histogram append failed", "err", err)
		}
	}
	return res
}

// nanosToMillis converts an OTLP timestamp (uint64 unix nanos) into the
// int64 unix milliseconds the metricsengine model expects.
func nanosToMillis(unixNano uint64) int64 {
	return int64(unixNano / 1_000_000)
}

// ingestScalar writes Gauge and Sum points to the scalar metric store.
func ingestScalar(store *metricsengine.Store, metric *metricspb.Metric, resourceAttrs, scopeAttrs map[string]string, log *slog.Logger) (int, int) {
	addPoints, kind := pointsForMetric(metric)
	if !kind.supported || len(addPoints) == 0 {
		return 0, 0
	}
	batch := metricsengine.OTLPBatch{
		ResourceAttributes: resourceAttrs,
		ScopeAttributes:    scopeAttrs,
		Points:             addPoints,
	}
	samples, skipped, err := metricsengine.OTLPSamplesSkipped(batch)
	if err != nil {
		selfobs.MetricsIngestRejected.WithLabelValues("conversion").Add(uint64(len(addPoints)))
		log.Warn("otlp metric sample conversion failed", "metric", metric.Name, "err", err)
		return 0, len(addPoints)
	}
	if skipped > 0 {
		// NaN/+/-Inf/int64-overflow float points: unencodable in the int64
		// value model, dropped by the adapter instead of stored as garbage.
		selfobs.MetricsIngestRejected.WithLabelValues("value_unencodable").Add(uint64(skipped))
	}
	if len(samples) == 0 {
		return 0, skipped
	}
	if _, err := store.AppendBatch(samples); err != nil {
		selfobs.MetricsIngestRejected.WithLabelValues("append").Add(uint64(len(samples)))
		if !errors.Is(err, metricsengine.ErrNoSamples) {
			log.Warn("otlp metric append failed", "metric", metric.Name, "err", err)
		}
		return 0, len(samples) + skipped
	}
	selfobs.MetricsIngestAccepted.WithLabelValues(kind.label).Add(uint64(len(samples)))
	return len(samples), skipped
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

// sketchSamples flattens converted series into per-tick sketch samples for
// the store; series IDs are assigned globally by the store's catalog.
func sketchSamples(exp []histogram.ExpSeries, explicit []histogram.ExplicitSeries) []metricsengine.SketchSample {
	out := make([]metricsengine.SketchSample, 0, sumExpPoints(exp)+sumExplicitPoints(explicit))
	for _, series := range exp {
		for i, ts := range series.Timestamps {
			out = append(out, metricsengine.SketchSample{Labels: series.Labels, Timestamp: ts, Exp: series.Sketches[i]})
		}
	}
	for _, series := range explicit {
		for i, ts := range series.Timestamps {
			out = append(out, metricsengine.SketchSample{Labels: series.Labels, Timestamp: ts, Explicit: series.Buckets[i]})
		}
	}
	return out
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
// points. The Ingest switch dispatches histogram/exp-histogram data to the
// histogram-store path before this is called, so only Gauge/Sum reach here;
// everything else is treated as "unknown".
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
		out[kv.Key] = ingest.AnyValueToString(kv.Value)
	}
	return out
}
