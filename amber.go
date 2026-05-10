package amber

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/hnlbs/amber/internal/bootstrap"
	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/storage"
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

func (o *Options) withDefaults() Options {
	if o == nil {
		o = &Options{}
	}
	out := *o
	if out.SegmentMaxRecords == 0 {
		out.SegmentMaxRecords = 1_000_000
	}

	if out.SegmentMaxBytes == 0 {
		out.SegmentMaxBytes = 512 << 20
	}
	if out.BatchSize == 0 {
		out.BatchSize = 1000
	}
	if out.BatchTimeout == 0 {
		out.BatchTimeout = 100 * time.Millisecond
	}
	if out.QueueSize == 0 {
		out.QueueSize = 10_000
	}
	if out.BreakerThreshold == 0 {
		out.BreakerThreshold = 10
	}
	if out.Logger == nil {
		out.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return out
}

type DB struct {
	batcher     *ingest.Batcher
	exec        *query.Executor
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	logDir      string
	spanDir     string
	cancel      context.CancelFunc
	log         *slog.Logger
}

func Open(dataDir string, opts ...*Options) (*DB, error) {
	var o *Options
	if len(opts) > 0 {
		o = opts[0]
	}
	cfg := o.withDefaults()

	logDir := filepath.Join(dataDir, "logs")
	spanDir := filepath.Join(dataDir, "spans")

	policy := storage.RotationPolicy{
		MaxRecords: cfg.SegmentMaxRecords,
		MaxBytes:   cfg.SegmentMaxBytes,
	}

	logManager, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		return nil, err
	}

	spanManager, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		_ = logManager.Close()
		return nil, err
	}

	logSparse, err := index.LoadSparseIndex(logDir)
	if err != nil {
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, err
	}

	spanSparse, err := index.LoadSparseIndex(spanDir)
	if err != nil {
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, err
	}

	exec := query.NewExecutorWithCache(
		logManager, spanManager, logSparse, spanSparse,
		logDir, spanDir, cfg.IndexCacheSize,
	)

	log := cfg.Logger

	bootstrap.LoadSealedIndexes(exec, logManager, spanManager, logDir, spanDir, log)
	bootstrap.SetupSealCallbacks(exec, logManager, spanManager, logDir, spanDir, log)

	ctx, cancel := context.WithCancel(context.Background())

	batcher := ingest.NewBatcher(ingest.Deps{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		Indexer:     exec,
		Logger:      log,
	}, ingest.Config{
		BatchSize:        cfg.BatchSize,
		BatchTimeout:     cfg.BatchTimeout,
		QueueSize:        cfg.QueueSize,
		BreakerThreshold: cfg.BreakerThreshold,
	})
	batcher.Start(ctx)

	return &DB{
		batcher:     batcher,
		exec:        exec,
		logManager:  logManager,
		spanManager: spanManager,
		logSparse:   logSparse,
		spanSparse:  spanSparse,
		logDir:      logDir,
		spanDir:     spanDir,
		cancel:      cancel,
		log:         log,
	}, nil
}

func (db *DB) Log(ctx context.Context, entry LogEntry) error {
	return db.batcher.SendLog(entry)
}

func (db *DB) Span(ctx context.Context, span SpanEntry) error {
	return db.batcher.SendSpan(span)
}

func (db *DB) QueryLogs(ctx context.Context, q *LogQuery) (*LogResult, error) {
	return db.exec.ExecLog(ctx, q)
}

func (db *DB) QuerySpans(ctx context.Context, q *SpanQuery) (*SpanResult, error) {
	return db.exec.ExecSpan(ctx, q)
}

func (db *DB) Close() error {
	db.cancel()
	db.batcher.Wait()

	if err := db.logSparse.Save(db.logDir); err != nil {
		db.log.Error("failed to save log sparse index", "err", err)
	}
	if err := db.spanSparse.Save(db.spanDir); err != nil {
		db.log.Error("failed to save span sparse index", "err", err)
	}

	if err := db.logManager.Close(); err != nil {
		return err
	}
	return db.spanManager.Close()
}

var (
	NewLogEntry  = model.NewLogEntry
	NewSpanEntry = model.NewSpanEntry
)
