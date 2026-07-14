// Package metricsengine is the public face of the embedded metrics store.
//
// The supported contract is Store (Open*, Append*, query methods, Flush,
// Compact, Stats, Close) plus the types its stable signatures need:
// selectors, query options, bounded aggregate/rate result shapes, and the OTLP
// adapters. Planner/execution internals live in internal/metricsengine and are
// deliberately not re-exported from the facade.
package metricsengine

import (
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/otlp"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
	"github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

type SeriesID = index.SeriesID
type Label = model.Label
type LabelSet = model.LabelSet
type MetricType = model.MetricType
type Sample = model.Sample
type OTLPBatch = otlp.Batch
type OTLPPoint = otlp.Point
type OTLPMetricKind = otlp.MetricKind
type OTLPNumberKind = otlp.NumberKind
type RangeSelector = query.RangeSelector
type Selector = index.Selector
type Matcher = index.Matcher
type MatchOp = index.MatchOp
type QueryOptions = query.Options
type FloatStep = query.FloatStep
type IntStep = query.IntStep
type AggregateStep = query.AggregateStep
type Aggregate = query.Aggregate
type StoreOptions = store.Options
type StoreConfig = store.Config
type StoreStats = store.Stats
type HistogramStats = store.HistogramStats
type CacheStats = store.CacheStats

const (
	MetricTypeGauge                = model.MetricTypeGauge
	MetricTypeCounter              = model.MetricTypeCounter
	MetricTypeHistogram            = model.MetricTypeHistogram
	MetricTypeExponentialHistogram = model.MetricTypeExponentialHistogram
	MetricNameLabel                = model.MetricNameLabel
	MatchEqual                     = index.MatchEqual
	MatchRegexp                    = index.MatchRegexp
	MatchNotEqual                  = index.MatchNotEqual
	MatchNotRegexp                 = index.MatchNotRegexp
	OTLPMetricGauge                = otlp.MetricGauge
	OTLPMetricSum                  = otlp.MetricSum
	OTLPMetricHistogram            = otlp.MetricHistogram
	OTLPMetricExponentialHistogram = otlp.MetricExponentialHistogram
	OTLPNumberInt                  = otlp.NumberInt
	OTLPNumberFloat                = otlp.NumberFloat
)

// Selector construction.
var NewSelector = index.NewSelector
var MetricName = index.MetricName
var LabelEqual = index.LabelEqual
var LabelRegexp = index.LabelRegexp
var LabelNotEqual = index.LabelNotEqual
var LabelNotRegexp = index.LabelNotRegexp

// Query-option construction.
var TimeRange = query.TimeRange
var TimeWindow = query.TimeWindow
var ValueRange = query.ValueRange
var StepMillis = query.StepMillis

// OTLP adapters.
var OTLPSamples = otlp.Samples
var OTLPSamplesSkipped = otlp.SamplesSkipped

var ErrNoSamples = store.ErrNoSamples
var ErrInvalidLabels = store.ErrInvalidLabels
var ErrLabelLimitExceeded = store.ErrLabelLimitExceeded
var ErrActiveSeriesLimitExceeded = store.ErrActiveSeriesLimitExceeded

// Store is the public metrics store facade. It forwards stable append, query,
// stats, and lifecycle operations to the internal implementation without
// exporting planner/execution internals as part of the public method set.
type Store struct {
	inner *store.Store
}

// OpenStore opens a metrics store at dir with default options.
func OpenStore(dir string) (*Store, error) {
	inner, err := store.Open(dir)
	if err != nil {
		return nil, err
	}
	return &Store{inner: inner}, nil
}

// OpenStoreWithOptions opens a metrics store at dir with explicit options.
func OpenStoreWithOptions(dir string, opts StoreOptions) (*Store, error) {
	inner, err := store.OpenWithOptions(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Store{inner: inner}, nil
}

// OpenStoreConfigured opens a metrics store from a config.
func OpenStoreConfigured(cfg StoreConfig) (*Store, error) {
	inner, err := store.OpenConfigured(cfg)
	if err != nil {
		return nil, err
	}
	return &Store{inner: inner}, nil
}

func (s *Store) Append(labels LabelSet, typ MetricType, timestamp int64, value int64) (SeriesID, error) {
	return s.inner.Append(labels, typ, timestamp, value)
}

func (s *Store) AppendBatch(samples []Sample) ([]SeriesID, error) {
	return s.inner.AppendBatch(samples)
}

func (s *Store) AppendScaledFloat(labels LabelSet, typ MetricType, timestamp int64, value float64, scale int64) (SeriesID, error) {
	return s.inner.AppendScaledFloat(labels, typ, timestamp, value, scale)
}

func (s *Store) AppendSketches(samples []engine.SketchSample) ([]SeriesID, error) {
	return s.inner.AppendSketches(samples)
}

func (s *Store) Flush() (string, error) {
	return s.inner.Flush()
}

func (s *Store) Compact() (string, error) {
	return s.inner.Compact()
}

func (s *Store) DeleteBefore(cutoffMillis int64) (int, error) {
	return s.inner.DeleteBefore(cutoffMillis)
}

func (s *Store) Close() error {
	return s.inner.Close()
}

func (s *Store) MetricNames() []string {
	return s.inner.MetricNames()
}

func (s *Store) BlockCount() int {
	return s.inner.BlockCount()
}

func (s *Store) BufferedSeries() int {
	return s.inner.BufferedSeries()
}

func (s *Store) BufferedSamples() int {
	return s.inner.BufferedSamples()
}

func (s *Store) ActiveSeries() int {
	return s.inner.ActiveSeries()
}

func (s *Store) CacheStats() CacheStats {
	return s.inner.CacheStats()
}

func (s *Store) HistogramMetricNames() ([]string, error) {
	return s.inner.HistogramMetricNames()
}

func (s *Store) RateByLabel(selector Selector, opts QueryOptions, label string) (map[string]float64, error) {
	return s.inner.RateByLabel(selector, opts, label)
}

func (s *Store) RateByLabelRange(rangeSelector RangeSelector, endMillis int64, label string) (map[string]float64, error) {
	return s.inner.RateByLabelRange(rangeSelector, endMillis, label)
}

func (s *Store) RateByLabelRangeSteps(rangeSelector RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]FloatStep, error) {
	return s.inner.RateByLabelRangeSteps(rangeSelector, startMillis, endMillis, step, label)
}

func (s *Store) IncreaseByLabel(selector Selector, opts QueryOptions, label string) (map[string]int64, error) {
	return s.inner.IncreaseByLabel(selector, opts, label)
}

func (s *Store) IncreaseByLabelRange(rangeSelector RangeSelector, endMillis int64, label string) (map[string]int64, error) {
	return s.inner.IncreaseByLabelRange(rangeSelector, endMillis, label)
}

func (s *Store) IncreaseByLabelRangeSteps(rangeSelector RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]IntStep, error) {
	return s.inner.IncreaseByLabelRangeSteps(rangeSelector, startMillis, endMillis, step, label)
}

func (s *Store) SumByLabel(selector Selector, opts QueryOptions, label string) (map[string]int64, error) {
	return s.inner.SumByLabel(selector, opts, label)
}

func (s *Store) SumByLabelRangeSteps(rangeSelector RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]IntStep, error) {
	return s.inner.SumByLabelRangeSteps(rangeSelector, startMillis, endMillis, step, label)
}

func (s *Store) AggregateByLabel(selector Selector, opts QueryOptions, label string) (map[string]Aggregate, error) {
	return s.inner.AggregateByLabel(selector, opts, label)
}

func (s *Store) AggregateByLabelRangeSteps(rangeSelector RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]AggregateStep, error) {
	return s.inner.AggregateByLabelRangeSteps(rangeSelector, startMillis, endMillis, step, label)
}

func (s *Store) HistogramQuantile(selector Selector, q float64, tr histogram.TimeRange) (float64, error) {
	return s.inner.HistogramQuantile(selector, q, tr)
}

func (s *Store) HistogramQuantileBy(selector Selector, q float64, tr histogram.TimeRange, by []string) (map[string]float64, error) {
	return s.inner.HistogramQuantileBy(selector, q, tr, by)
}

func (s *Store) Stats() (StoreStats, error) {
	return s.inner.Stats()
}

func (s *Store) HistStats() (HistogramStats, error) {
	return s.inner.HistStats()
}

func (s *Store) LastBackgroundError() error {
	return s.inner.LastBackgroundError()
}

func (s *Store) WALRecoveryStats() wal.RecoverStats {
	return s.inner.WALRecoveryStats()
}

func (s *Store) UnknownWALSeries() int {
	return s.inner.UnknownWALSeries()
}
