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
	"fmt"
	"log/slog"
	"math"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	meotlp "github.com/yaop-labs/amber/internal/metricsengine/otlp"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/metricsengine"
)

// Result reports the per-request point counts produced by Ingest.
type Result struct {
	Accepted        int
	Rejected        int
	Unsupported     int
	AcceptedRequest *collectormetrics.ExportMetricsServiceRequest
}

// Ingest converts req and writes it to store, returning point counts. Permanent
// per-point conversion failures are reported in Result. Durable write failures
// are returned as errors so transports can fail the whole request and let OTLP
// clients retry.
//
// store must be non-nil; callers gate on metric-store availability before
// calling.
func Ingest(store *metricsengine.Store, req *collectormetrics.ExportMetricsServiceRequest, log *slog.Logger) (res Result, returnErr error) {
	if req == nil {
		return res, errors.New("otlp metrics request is nil")
	}
	acceptedPoints := make(map[proto.Message]struct{})
	defer func() {
		res.AcceptedRequest = otlpv4.MetricsSubset(req, acceptedPoints)
	}()
	// Histogram series from one request are written as one block.
	var expAll []histogram.ExpSeries
	var explicitAll []histogram.ExplicitSeries
	var acceptedHistograms []proto.Message

	for _, rm := range req.ResourceMetrics {
		if rm == nil {
			continue
		}
		resourceAttrs := kvToMap(rm.Resource.GetAttributes())
		for _, sm := range rm.ScopeMetrics {
			if sm == nil {
				continue
			}
			scopeAttrs := kvToMap(sm.Scope.GetAttributes())
			for _, metric := range sm.Metrics {
				if metric == nil {
					recordUnsupported(&res, "unknown", 1)
					continue
				}
				switch data := metric.Data.(type) {
				case *metricspb.Metric_Gauge:
					a, r, points, err := ingestScalar(store, metric, resourceAttrs, scopeAttrs, log)
					res.Accepted += a
					res.Rejected += r
					for _, point := range points {
						acceptedPoints[point] = struct{}{}
					}
					if err != nil {
						return res, err
					}
				case *metricspb.Metric_Sum:
					if !supportedCounterSum(data.Sum) {
						recordUnsupported(&res, "sum_temporality", len(data.Sum.GetDataPoints()))
						continue
					}
					a, r, points, err := ingestScalar(store, metric, resourceAttrs, scopeAttrs, log)
					res.Accepted += a
					res.Rejected += r
					for _, point := range points {
						acceptedPoints[point] = struct{}{}
					}
					if err != nil {
						return res, err
					}
				case *metricspb.Metric_Histogram:
					if !supportedCumulativeTemporality(data.Histogram.GetAggregationTemporality()) {
						recordUnsupported(&res, "histogram_temporality", len(data.Histogram.GetDataPoints()))
						continue
					}
					series := explicitSeriesFor(metric, data.Histogram, resourceAttrs, scopeAttrs)
					res.Accepted += sumExplicitPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("histogram").Add(uint64(sumExplicitPoints(series)))
					explicitAll = append(explicitAll, series...)
					for _, point := range data.Histogram.GetDataPoints() {
						if point != nil {
							acceptedHistograms = append(acceptedHistograms, point)
						}
					}
				case *metricspb.Metric_ExponentialHistogram:
					if !supportedCumulativeTemporality(data.ExponentialHistogram.GetAggregationTemporality()) {
						recordUnsupported(&res, "exphistogram_temporality", len(data.ExponentialHistogram.GetDataPoints()))
						continue
					}
					series := expSeriesFor(metric, data.ExponentialHistogram, resourceAttrs, scopeAttrs)
					res.Accepted += sumExpPoints(series)
					selfobs.MetricsIngestAccepted.WithLabelValues("exphistogram").Add(uint64(sumExpPoints(series)))
					expAll = append(expAll, series...)
					for _, point := range data.ExponentialHistogram.GetDataPoints() {
						if point != nil {
							acceptedHistograms = append(acceptedHistograms, point)
						}
					}
				default:
					recordUnsupported(&res, "unknown", 1)
				}
			}
		}
	}

	if len(expAll) > 0 || len(explicitAll) > 0 {
		sketches := sketchSamples(expAll, explicitAll)
		if _, err := store.AppendSketchesOTLP(sketches); err != nil {
			// Treat append failure as ingest rejection: the histogram data
			// never landed. Count by point total, not series count, so the
			// counter matches scalar semantics.
			pts := len(sketches)
			res.Rejected += pts
			res.Accepted -= pts
			selfobs.MetricsIngestRejected.WithLabelValues("hist_write").Add(uint64(pts))
			log.Warn("histogram append failed", "err", err)
			return res, fmt.Errorf("otlp metrics histogram append: %w", err)
		}
		for _, point := range acceptedHistograms {
			acceptedPoints[point] = struct{}{}
		}
	}
	return res, nil
}

// nanosToMillis converts an OTLP timestamp (uint64 unix nanos) into the
// int64 unix milliseconds the metricsengine model expects.
func nanosToMillis(unixNano uint64) int64 {
	return int64(unixNano / 1_000_000)
}

// ingestScalar writes Gauge and Sum points to the scalar metric store.
func ingestScalar(store *metricsengine.Store, metric *metricspb.Metric, resourceAttrs, scopeAttrs map[string]string, log *slog.Logger) (int, int, []*metricspb.NumberDataPoint, error) {
	addPoints, sourcePoints, kind := pointsForMetric(metric)
	if !kind.supported || len(addPoints) == 0 {
		return 0, 0, nil, nil
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
		return 0, len(addPoints), nil, nil
	}
	if skipped > 0 {
		// NaN/+/-Inf/int64-overflow float points: unencodable in the int64
		// value model, dropped by the adapter instead of stored as garbage.
		selfobs.MetricsIngestRejected.WithLabelValues("value_unencodable").Add(uint64(skipped))
	}
	if len(samples) == 0 {
		return 0, skipped, nil, nil
	}
	if _, err := store.AppendBatchOTLP(samples); err != nil {
		selfobs.MetricsIngestRejected.WithLabelValues("append").Add(uint64(len(samples)))
		if !errors.Is(err, metricsengine.ErrNoSamples) {
			log.Warn("otlp metric append failed", "metric", metric.Name, "err", err)
		}
		return 0, len(samples) + skipped, nil, fmt.Errorf("otlp metrics append %q: %w", metric.Name, err)
	}
	selfobs.MetricsIngestAccepted.WithLabelValues(kind.label).Add(uint64(len(samples)))
	return len(samples), skipped, acceptedScalarPoints(sourcePoints), nil
}

func supportedCounterSum(sum *metricspb.Sum) bool {
	return sum.GetIsMonotonic() && supportedCumulativeTemporality(sum.GetAggregationTemporality())
}

func supportedCumulativeTemporality(temporality metricspb.AggregationTemporality) bool {
	return temporality == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
}

func recordUnsupported(res *Result, label string, points int) {
	if points <= 0 {
		return
	}
	res.Unsupported += points
	selfobs.MetricsIngestUnsupported.WithLabelValues(label).Add(uint64(points))
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
		if dp == nil {
			continue
		}
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
		if dp == nil {
			continue
		}
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
func sketchSamples(exp []histogram.ExpSeries, explicit []histogram.ExplicitSeries) []engine.SketchSample {
	out := make([]engine.SketchSample, 0, sumExpPoints(exp)+sumExplicitPoints(explicit))
	for _, series := range exp {
		for i, ts := range series.Timestamps {
			out = append(out, engine.SketchSample{Labels: series.Labels, Timestamp: ts, Exp: series.Sketches[i]})
		}
	}
	for _, series := range explicit {
		for i, ts := range series.Timestamps {
			out = append(out, engine.SketchSample{Labels: series.Labels, Timestamp: ts, Explicit: series.Buckets[i]})
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
func pointsForMetric(metric *metricspb.Metric) ([]metricsengine.OTLPPoint, []*metricspb.NumberDataPoint, metricKindStatus) {
	switch data := metric.Data.(type) {
	case *metricspb.Metric_Gauge:
		points, sources := numberPoints(metric.Name, metricsengine.OTLPMetricGauge, data.Gauge.GetDataPoints())
		return points, sources, metricKindStatus{supported: true, label: "gauge"}
	case *metricspb.Metric_Sum:
		points, sources := numberPoints(metric.Name, metricsengine.OTLPMetricSum, data.Sum.GetDataPoints())
		return points, sources, metricKindStatus{supported: true, label: "sum"}
	default:
		return nil, nil, metricKindStatus{supported: false, label: "unknown"}
	}
}

func numberPoints(name string, kind metricsengine.OTLPMetricKind, dps []*metricspb.NumberDataPoint) ([]metricsengine.OTLPPoint, []*metricspb.NumberDataPoint) {
	points := make([]metricsengine.OTLPPoint, 0, len(dps))
	sources := make([]*metricspb.NumberDataPoint, 0, len(dps))
	for _, dp := range dps {
		if dp == nil {
			continue
		}
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
		sources = append(sources, dp)
	}
	return points, sources
}

func acceptedScalarPoints(points []*metricspb.NumberDataPoint) []*metricspb.NumberDataPoint {
	accepted := make([]*metricspb.NumberDataPoint, 0, len(points))
	for _, point := range points {
		switch value := point.Value.(type) {
		case *metricspb.NumberDataPoint_AsInt:
			accepted = append(accepted, point)
		case *metricspb.NumberDataPoint_AsDouble:
			scaled := math.Round(value.AsDouble * 1000)
			if !math.IsNaN(scaled) && scaled < float64(math.MaxInt64) && scaled > float64(math.MinInt64) {
				accepted = append(accepted, point)
			}
		}
	}
	return accepted
}

func kvToMap(kvs []*commonpb.KeyValue) map[string]string {
	if len(kvs) == 0 {
		return nil
	}
	out := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		if kv == nil {
			continue
		}
		out[kv.Key] = ingest.AnyValueToString(kv.Value)
	}
	return out
}
