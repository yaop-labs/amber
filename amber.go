// Package amber is the embedded API. Standalone-binary callers should use
// cmd/amber instead. Both share internal/runtime under the hood.
package amber

import (
	"context"
	"log/slog"
	"time"

	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/runtime"
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

// Options keeps the historical flat shape of the embedded API. Internally we
// translate to runtime.Options. Callers who want richer knobs (cardinality
// limits, etc.) can drop down to internal/runtime directly.
type Options struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
	BatchSize         int
	BatchTimeout      time.Duration
	QueueSize         int
	BreakerThreshold  int
	IndexCacheSize    int
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
			SegmentMaxRecords: o.SegmentMaxRecords,
			SegmentMaxBytes:   o.SegmentMaxBytes,
		},
		Ingest: runtime.IngestOptions{
			BatchSize:        o.BatchSize,
			BatchTimeout:     o.BatchTimeout,
			QueueSize:        o.QueueSize,
			BreakerThreshold: o.BreakerThreshold,
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
