// Package amber provides the embedded API.
package amber

import (
	"context"
	"log/slog"
	"time"

	"github.com/yaop-labs/amber/internal/ingest"
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

type Status struct {
	Ready                bool
	Degraded             bool
	Closing              bool
	DatabaseID           string
	WALRepairEvents      uint64
	IndexBootstrapErrors uint64
	LastSuccessfulBackup *BackupCheckpointStatus
	LastVerifiedRestore  *RestoreCheckpointStatus
	Reasons              []StatusReason
}

type BackupCheckpointStatus struct {
	Checkpoint        string
	SnapshotCreatedAt time.Time
	CompletedAt       time.Time
}

type RestoreCheckpointStatus struct {
	Checkpoint        string
	SnapshotCreatedAt time.Time
	VerifiedAt        time.Time
}

type StatusReason struct {
	Code  string
	Count uint64
}

// CardinalityLimits caps per-record attribute cardinality at ingest time.
type CardinalityLimits struct {
	MaxAttrsPerEntry      int
	MaxAttrValueBytes     int
	MaxAttrKeysPerService int
	MaxServices           int
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
//
// Value model (alpha): samples are stored as int64. Floats are scaled
// integers - round(value x scale), scale 1000 by default - so precision is
// bounded by the scale, and NaN/+/-Inf/overflowing values are rejected at
// ingest. See the "Metrics value model" section in the README.
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

// Retention configures database-owned retention for logs and spans. Local
// limits require S3 and evict only the local cache; global limits remove the
// segment from both local and remote storage.
type Retention struct {
	Logs     StreamRetention
	Spans    StreamRetention
	Journal  JournalRetention
	Interval time.Duration
}

// JournalRetention bounds the physical canonical OTLP replay journal. MaxAge
// uses Amber acceptance time, so old backfilled events are not deleted on
// arrival. All limits are terminal; zero disables the corresponding limit.
type JournalRetention struct {
	MaxAge      time.Duration
	MaxBytes    int64
	MaxSegments int
}

// StreamRetention is the retention policy for one signal stream.
type StreamRetention struct {
	LocalMaxAge   time.Duration
	LocalMaxBytes int64
	MaxAge        time.Duration
	MaxBytes      int64
	MaxSegments   int
}

// Options configures Open.
type Options struct {
	SegmentMaxRecords uint64
	// SegmentMaxBytes counts uncompressed serialized payload and may be
	// exceeded by one batch; it is not a physical file-size cap.
	SegmentMaxBytes  int64
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
	LogIngest        IngestLane
	SpanIngest       IngestLane
	IndexCacheSize   int
	// IndexBootstrapWorkers bounds concurrent startup sidecar builds.
	// Zero uses the conservative default of one.
	IndexBootstrapWorkers int
	Cardinality           CardinalityLimits
	S3                    S3Storage
	Metrics               Metrics
	Retention             Retention
	Logger                *slog.Logger

	// MemoryLimit sets the Go runtime soft memory limit in bytes via
	// debug.SetMemoryLimit. The limit is process-wide: it affects the host
	// application too, overrides GOMEMLIMIT, and stays in effect after
	// Close. Zero (default) leaves the runtime setting untouched.
	MemoryLimit int64
}

// IngestLane overrides the batching and breaker settings for one ingest lane
// (logs or spans); zero fields fall back to the top-level Options values.
type IngestLane struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

// DB is an embedded amber database: logs, spans, and metrics in one process.
// It is safe for concurrent use. Create it with Open and release it with Close.
type DB struct {
	stack  *runtime.Stack
	cancel context.CancelFunc
}

// Open opens (or creates) a database rooted at dataDir. opts may be nil for
// defaults.
func Open(dataDir string, opts *Options) (*DB, error) {
	o := opts
	if o == nil {
		o = &Options{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	stack, err := runtime.New(ctx, runtime.Options{
		DataDir:               dataDir,
		Logger:                o.Logger,
		IndexCacheSize:        o.IndexCacheSize,
		IndexBootstrapWorkers: o.IndexBootstrapWorkers,
		MemoryLimit:           o.MemoryLimit,
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
			MaxServices:           o.Cardinality.MaxServices,
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
		Retention: runtime.RetentionOptions{
			Interval: o.Retention.Interval,
			Journal: runtime.JournalRetentionOptions{
				MaxAge: o.Retention.Journal.MaxAge, MaxBytes: o.Retention.Journal.MaxBytes, MaxSegments: o.Retention.Journal.MaxSegments,
			},
			Logs: runtime.StreamRetentionOptions{
				LocalMaxAge: o.Retention.Logs.LocalMaxAge, LocalMaxBytes: o.Retention.Logs.LocalMaxBytes,
				MaxAge: o.Retention.Logs.MaxAge, MaxTotalBytes: o.Retention.Logs.MaxBytes, MaxSegments: o.Retention.Logs.MaxSegments,
			},
			Spans: runtime.StreamRetentionOptions{
				LocalMaxAge: o.Retention.Spans.LocalMaxAge, LocalMaxBytes: o.Retention.Spans.LocalMaxBytes,
				MaxAge: o.Retention.Spans.MaxAge, MaxTotalBytes: o.Retention.Spans.MaxBytes, MaxSegments: o.Retention.Spans.MaxSegments,
			},
		},
	})
	if err != nil {
		cancel()
		return nil, err
	}

	return &DB{stack: stack, cancel: cancel}, nil
}

// Log enqueues an entry for asynchronous ingest and returns before anything
// reaches the WAL: a nil error means "accepted into the in-process queue",
// not "durable". Entries sitting in the queue or an unflushed batch are lost
// on a crash (kill -9, power loss). Call Flush for a durability barrier, or
// Close for a clean shutdown that drains the queue.
//
// The enqueue itself never blocks (a full queue returns ErrQueueFull), so
// ctx is consulted only up front: a canceled context fails fast.
func (db *DB) Log(ctx context.Context, entry LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return db.stack.Batcher.SendLog(entry)
}

// Span enqueues a span for asynchronous ingest. Same durability and context
// semantics as Log: nil means queued, not persisted; see Flush.
func (db *DB) Span(ctx context.Context, span SpanEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return db.stack.Batcher.SendSpan(span)
}

// Flush blocks until every Log/Span call that returned before Flush started
// has become durable and queryable. Covered serialization, WAL, segment, or
// sync failures are returned to the caller. Use it as a durability barrier
// before process exit or after writes the application cannot afford to lose.
func (db *DB) Flush(ctx context.Context) error {
	return db.stack.Batcher.Flush(ctx)
}

// QueryLogs runs a log query and returns one page of results.
func (db *DB) QueryLogs(ctx context.Context, q *LogQuery) (*LogResult, error) {
	return db.stack.Executor.ExecLog(ctx, q)
}

// QuerySpans runs a span query and returns one page of results.
func (db *DB) QuerySpans(ctx context.Context, q *SpanQuery) (*SpanResult, error) {
	return db.stack.Executor.ExecSpan(ctx, q)
}

// TraceResult is the combined output of QueryTrace.
type TraceResult struct {
	Logs  []LogEntry
	Spans []SpanEntry
}

// QueryTrace fetches logs and spans for one trace ID. The limit applies to
// logs and spans independently: limit=100 can return up to 100 of each.
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

// Status returns structured readiness and degraded-state details.
func (db *DB) Status() Status {
	s := db.stack.Status()
	out := Status{
		Ready: s.Ready, Degraded: s.Degraded, Closing: s.Closing,
		DatabaseID:      s.DatabaseID,
		WALRepairEvents: s.WALRepairEvents, IndexBootstrapErrors: s.IndexBootstrapErrors,
		Reasons: make([]StatusReason, len(s.Reasons)),
	}
	if s.LastSuccessfulBackup != nil {
		out.LastSuccessfulBackup = &BackupCheckpointStatus{
			Checkpoint: s.LastSuccessfulBackup.Checkpoint, SnapshotCreatedAt: s.LastSuccessfulBackup.SnapshotCreatedAt, CompletedAt: s.LastSuccessfulBackup.CompletedAt,
		}
	}
	if s.LastVerifiedRestore != nil {
		out.LastVerifiedRestore = &RestoreCheckpointStatus{
			Checkpoint: s.LastVerifiedRestore.Checkpoint, SnapshotCreatedAt: s.LastVerifiedRestore.SnapshotCreatedAt, VerifiedAt: s.LastVerifiedRestore.VerifiedAt,
		}
	}
	for i, reason := range s.Reasons {
		out.Reasons[i] = StatusReason{Code: reason.Code, Count: reason.Count}
	}
	return out
}

const shutdownTimeout = 30 * time.Second

// Close shuts the database down cleanly: it drains the ingest queue, flushes,
// and releases resources, bounded by an internal timeout.
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

// Ingest errors returned by Log and Span, for errors.Is.
var (
	ErrQueueFull      = ingest.ErrQueueFull
	ErrBreakerOpen    = ingest.ErrBreakerOpen
	ErrCardinality    = ingest.ErrCardinality
	ErrClosed         = ingest.ErrClosed
	ErrRecordTooLarge = ingest.ErrRecordTooLarge
)
