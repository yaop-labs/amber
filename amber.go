// Package amber is the embedded API. Standalone-binary callers should use
// cmd/amber instead. Both share internal/runtime under the hood.
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

// CardinalityLimits caps per-record attribute fan-out at ingest time.
// Zero in any field disables that specific check; the zero value disables all.
type CardinalityLimits struct {
	MaxAttrsPerEntry      int
	MaxAttrValueBytes     int
	MaxAttrKeysPerService int
}

// S3Storage enables S3-compatible object storage for sealed segments and
// their index sidecars. The active segment and WAL remain node-local.
// Reads fall back to S3 on a local cache miss. Empty Bucket disables S3.
// Endpoint overrides the AWS endpoint for MinIO, R2, DO Spaces, etc.
type S3Storage struct {
	Bucket   string
	Prefix   string
	Region   string
	Endpoint string

	// ReconcileOnStart triggers a remote List at startup and adopts sealed
	// segments not yet known locally. Default false: reconcile still runs
	// implicitly when the local meta has no sealed segments.
	ReconcileOnStart bool
}

// Metrics controls the embedded metricsengine store. By default the store
// is enabled and opens at <dataDir>/metrics. Set Disabled to opt out; set
// Dir to relocate. Other fields map onto metricsengine/store.Options and
// keep their zero-value defaults when unset.
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

// Options is the embedded API's configuration surface. It mirrors the
// fields of internal/runtime.Options that matter for callers. Zero values
// fall back to sensible defaults (see runtime/runtime.go).
type Options struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
	BatchSize         int
	BatchTimeout      time.Duration
	QueueSize         int
	BreakerThreshold  int
	IndexCacheSize    int
	Cardinality       CardinalityLimits
	S3                S3Storage
	Metrics           Metrics
	Logger            *slog.Logger
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

// TraceResult is the combined output of QueryTrace: every log entry and
// every span recorded for one trace id. No tree assembly, no ordering
// guarantees beyond what the underlying queries provide — that lives in
// the consumer (UI, TUI, collector-side gateway).
type TraceResult struct {
	Logs  []LogEntry
	Spans []SpanEntry
}

// QueryTrace fetches both the logs and spans for a single trace id in one
// call, saving the caller a round trip. limit is applied independently to
// each side: at most `limit` logs and at most `limit` spans come back.
// A limit of 0 or less is treated as unbounded by the underlying executor's
// default (100 per side).
//
// This is intentionally a thin wrapper. Correlation (waterfall layout,
// log-to-span association by SpanID, span-tree construction) belongs to
// the UI layer; storage stays domain-agnostic.
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

// MetricStore returns the embedded metricsengine store. nil when metrics
// are disabled via Options.Metrics.Disabled. Callers get the live store
// directly — Append, Select, Rate, Aggregate, etc. all live on it. amber
// only owns its lifecycle (open at Open, flush+close at Close).
func (db *DB) MetricStore() *metricsengine.Store { return db.stack.MetricStore }

// IsReady reports whether bootstrap has finished loading sealed indexes.
// Until this returns true, queries may return partial results because some
// segments still lack in-memory ribbon filters and bitmap caches.
// Use as a readiness gate before serving traffic.
func (db *DB) IsReady() bool { return db.stack.IsReady() }

// shutdownTimeout caps how long Close waits for batcher drain + storage
// flush. 30s matches the standalone binary default and is enough for any
// realistic in-flight batch; longer hangs are an FS pathology, not work.
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
