// Package amber provides the embedded API.
package amber

import (
	"context"
	"log/slog"
	"time"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/metricsengine"
)

type (
	LogEntry   = model.LogEntry
	SpanEntry  = model.SpanEntry
	Level      = model.Level
	TraceID    = model.TraceID
	SpanID     = model.SpanID
	Attr       = model.Attr
	SpanStatus = model.SpanStatus
)

const (
	LevelTrace = model.LevelTrace
	LevelDebug = model.LevelDebug
	LevelInfo  = model.LevelInfo
	LevelWarn  = model.LevelWarn
	LevelError = model.LevelError
	LevelFatal = model.LevelFatal
)

type (
	LogQuery   = query.LogQuery
	SpanQuery  = query.SpanQuery
	LogResult  = query.LogResult
	SpanResult = query.SpanResult
)

// CardinalityLimits caps per-record attribute cardinality at ingest time.
type CardinalityLimits struct {
	MaxAttrsPerEntry      int
	MaxAttrValueBytes     int
	MaxAttrKeysPerService int
}

// S3Storage configures S3-compatible storage for sealed segments.
type S3Storage struct {
	Bucket   string
	Prefix   string
	Region   string
	Endpoint string

	// ReconcileOnStart adopts sealed remote segments at startup.
	ReconcileOnStart bool
}

// Metrics configures the embedded metrics store.
type Metrics struct {
	Disabled            bool
	Dir                 string
	FlushInterval       time.Duration
	MaxBufferedSamples  int
	MaxActiveSeries     int
	MaxLabelsPerSeries  int
	Retention           time.Duration
	CompactionMinBlocks int
}

// Options configures Open.
type Options struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
	BatchSize         int
	BatchTimeout      time.Duration
	QueueSize         int
	BreakerThreshold  int
	LogIngest         IngestLane
	SpanIngest        IngestLane
	IndexCacheSize    int
	Cardinality       CardinalityLimits
	S3                S3Storage
	Metrics           Metrics
	Logger            *slog.Logger
}

type IngestLane struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

type DB struct {
	stack  *runtime.Stack
	cancel context.CancelFunc
}

func Open(dataDir string, opts ...*Options) (*DB, error) {
	var o *Options
	if len(opts) > 0 {
		o = opts[0]
	}
	if o == nil {
		o = &Options{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	stack, err := runtime.New(ctx, runtime.Options{
		DataDir:        dataDir,
		Logger:         o.Logger,
		IndexCacheSize: o.IndexCacheSize,
		Storage: runtime.StorageOptions{
			SegmentMaxRecords:  o.SegmentMaxRecords,
			SegmentMaxBytes:    o.SegmentMaxBytes,
			S3Bucket:           o.S3.Bucket,
			S3Prefix:           o.S3.Prefix,
			S3Region:           o.S3.Region,
			S3Endpoint:         o.S3.Endpoint,
			S3ReconcileOnStart: o.S3.ReconcileOnStart,
		},
		Ingest: runtime.IngestOptions{
			BatchSize:        o.BatchSize,
			BatchTimeout:     o.BatchTimeout,
			QueueSize:        o.QueueSize,
			BreakerThreshold: o.BreakerThreshold,
			Logs: runtime.IngestLaneOptions{
				BatchSize:        o.LogIngest.BatchSize,
				BatchTimeout:     o.LogIngest.BatchTimeout,
				QueueSize:        o.LogIngest.QueueSize,
				BreakerThreshold: o.LogIngest.BreakerThreshold,
			},
			Spans: runtime.IngestLaneOptions{
				BatchSize:        o.SpanIngest.BatchSize,
				BatchTimeout:     o.SpanIngest.BatchTimeout,
				QueueSize:        o.SpanIngest.QueueSize,
				BreakerThreshold: o.SpanIngest.BreakerThreshold,
			},
		},
		Cardinality: runtime.CardinalityOptions{
			MaxAttrsPerEntry:      o.Cardinality.MaxAttrsPerEntry,
			MaxAttrValueBytes:     o.Cardinality.MaxAttrValueBytes,
			MaxAttrKeysPerService: o.Cardinality.MaxAttrKeysPerService,
		},
		Metrics: runtime.MetricsOptions{
			Disabled:            o.Metrics.Disabled,
			Dir:                 o.Metrics.Dir,
			FlushInterval:       o.Metrics.FlushInterval,
			MaxBufferedSamples:  o.Metrics.MaxBufferedSamples,
			MaxActiveSeries:     o.Metrics.MaxActiveSeries,
			MaxLabelsPerSeries:  o.Metrics.MaxLabelsPerSeries,
			Retention:           o.Metrics.Retention,
			CompactionMinBlocks: o.Metrics.CompactionMinBlocks,
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}

	return &DB{stack: stack, cancel: cancel}, nil
}

func (db *DB) Log(_ context.Context, entry LogEntry) error {
	return db.stack.Batcher.SendLog(entry)
}

func (db *DB) Span(_ context.Context, span SpanEntry) error {
	return db.stack.Batcher.SendSpan(span)
}

func (db *DB) QueryLogs(ctx context.Context, q *LogQuery) (*LogResult, error) {
	return db.stack.Executor.ExecLog(ctx, q)
}

func (db *DB) QuerySpans(ctx context.Context, q *SpanQuery) (*SpanResult, error) {
	return db.stack.Executor.ExecSpan(ctx, q)
}

// TraceResult is the combined output of QueryTrace.
type TraceResult struct {
	Logs  []LogEntry
	Spans []SpanEntry
}

// QueryTrace fetches logs and spans for one trace ID.
func (db *DB) QueryTrace(ctx context.Context, traceID TraceID, limit int) (*TraceResult, error) {
	logs, err := db.stack.Executor.ExecLog(ctx, &LogQuery{TraceID: traceID, Limit: limit})
	if err != nil {
		return nil, err
	}
	spans, err := db.stack.Executor.ExecSpan(ctx, &SpanQuery{TraceID: traceID, Limit: limit})
	if err != nil {
		return nil, err
	}
	return &TraceResult{Logs: logs.Entries, Spans: spans.Spans}, nil
}

// MetricStore returns the embedded metrics store.
func (db *DB) MetricStore() *metricsengine.Store { return db.stack.MetricStore }

// IsReady reports whether bootstrap has finished loading sealed indexes.
func (db *DB) IsReady() bool { return db.stack.IsReady() }

const shutdownTimeout = 30 * time.Second

func (db *DB) Close() error {
	db.cancel()
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	return db.stack.Close(ctx)
}

var (
	NewLogEntry  = model.NewLogEntry
	NewSpanEntry = model.NewSpanEntry
)
