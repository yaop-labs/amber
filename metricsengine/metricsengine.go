package metricsengine

import (
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/otlp"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
	"github.com/yaop-labs/amber/internal/metricsengine/store"
)

type Engine = engine.Engine
type Options = engine.Options
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
type QueryOperation = query.Operation
type QueryPlan = query.Plan
type QueryResult = query.Result
type QueryExecutionPath = query.ExecutionPath
type QueryCandidateStats = query.CandidateStats
type QueryPlanCost = query.PlanCost
type QueryPlanCandidate = query.PlanCandidate
type QueryExecutionPlan = query.ExecutionPlan
type FloatStep = query.FloatStep
type IntStep = query.IntStep
type AggregateStep = query.AggregateStep
type Aggregate = query.Aggregate
type Store = store.Store
type StoreOptions = store.Options
type StoreConfig = store.Config
type StoreStats = store.Stats

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
	OpSelect                       = query.OpSelect
	OpSumByLabel                   = query.OpSumByLabel
	OpAggregateByLabel             = query.OpAggregateByLabel
	OpRateByLabel                  = query.OpRateByLabel
	OpIncreaseByLabel              = query.OpIncreaseByLabel
	OpRateByLabelRange             = query.OpRateByLabelRange
	OpIncreaseByLabelRange         = query.OpIncreaseByLabelRange
	OpRateByLabelRangeSteps        = query.OpRateByLabelRangeSteps
	OpIncreaseByLabelRangeSteps    = query.OpIncreaseByLabelRangeSteps
	OpSumByLabelRangeSteps         = query.OpSumByLabelRangeSteps
	OpAggregateByLabelRangeSteps   = query.OpAggregateByLabelRangeSteps
	PathMaterializeSeries          = query.PathMaterializeSeries
	PathHeadOnly                   = query.PathHeadOnly
	PathDirectoryAggregate         = query.PathDirectoryAggregate
	PathBucketAggregate            = query.PathBucketAggregate
	PathSingleBlockStreaming       = query.PathSingleBlockStreaming
	PathMultiBlockStreaming        = query.PathMultiBlockStreaming
	PathCoalescedSummaries         = query.PathCoalescedSummaries
	OTLPMetricGauge                = otlp.MetricGauge
	OTLPMetricSum                  = otlp.MetricSum
	OTLPMetricHistogram            = otlp.MetricHistogram
	OTLPMetricExponentialHistogram = otlp.MetricExponentialHistogram
	OTLPNumberInt                  = otlp.NumberInt
	OTLPNumberFloat                = otlp.NumberFloat
)

func New() *Engine {
	return engine.New()
}

func Open(opts Options) (*Engine, error) {
	return engine.Open(opts)
}

var NewSelector = index.NewSelector
var MetricName = index.MetricName
var LabelEqual = index.LabelEqual
var LabelRegexp = index.LabelRegexp
var LabelNotEqual = index.LabelNotEqual
var LabelNotRegexp = index.LabelNotRegexp
var SelectBlock = query.SelectBlock
var SelectBlockWithDirectory = query.SelectBlockWithDirectory
var SelectSeries = query.SelectSeries
var TimeRange = query.TimeRange
var TimeWindow = query.TimeWindow
var ValueRange = query.ValueRange
var StepMillis = query.StepMillis
var SumByLabel = query.SumByLabel
var SumByLabelSteps = query.SumByLabelSteps
var SumByLabelInBlock = query.SumByLabelInBlock
var AggregateByLabel = query.AggregateByLabel
var AggregateByLabelSteps = query.AggregateByLabelSteps
var AggregateByLabelStepsInBlockWithDirectory = query.AggregateByLabelStepsInBlockWithDirectory
var AggregateByLabelInBlock = query.AggregateByLabelInBlock
var AggregateByLabelInDirectoryBuckets = query.AggregateByLabelInDirectoryBuckets
var Rate = query.Rate
var RateByLabel = query.RateByLabel
var RateByLabelSteps = query.RateByLabelSteps
var RateByLabelStepsInBlockWithDirectory = query.RateByLabelStepsInBlockWithDirectory
var Increase = query.Increase
var IncreaseByLabel = query.IncreaseByLabel
var IncreaseByLabelSteps = query.IncreaseByLabelSteps
var IncreaseByLabelStepsInBlockWithDirectory = query.IncreaseByLabelStepsInBlockWithDirectory
var PlanExecution = query.PlanExecution
var OTLPSamples = otlp.Samples

var ErrNoSamples = store.ErrNoSamples
var ErrInvalidLabels = store.ErrInvalidLabels
var ErrLabelLimitExceeded = store.ErrLabelLimitExceeded
var ErrActiveSeriesLimitExceeded = store.ErrActiveSeriesLimitExceeded
var OpenStore = store.Open
var OpenStoreWithOptions = store.OpenWithOptions
var OpenStoreConfigured = store.OpenConfigured
