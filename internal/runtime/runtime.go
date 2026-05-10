// Package runtime is the shared core stack used by both the standalone binary
// (cmd/amber) and the embedded amber.Open API. It owns storage, indexes,
// query executor, and the ingest batcher — but NOT HTTP/gRPC servers,
// retention, pprof, or signal handling, which live in main.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/hnlbs/amber/internal/bootstrap"
	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/storage"
)

type Options struct {
	DataDir        string
	Logger         *slog.Logger
	Storage        StorageOptions
	Ingest         IngestOptions
	Cardinality    CardinalityOptions
	IndexCacheSize int
}

type StorageOptions struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
}

type IngestOptions struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

type CardinalityOptions struct {
	MaxAttrsPerEntry      int
	MaxAttrValueBytes     int
	MaxAttrKeysPerService int
}

func (o Options) withDefaults() Options {
	out := o
	if out.Storage.SegmentMaxRecords == 0 {
		out.Storage.SegmentMaxRecords = 1_000_000
	}
	if out.Storage.SegmentMaxBytes == 0 {
		out.Storage.SegmentMaxBytes = 512 << 20
	}
	if out.Ingest.BatchSize == 0 {
		out.Ingest.BatchSize = 1000
	}
	if out.Ingest.BatchTimeout == 0 {
		out.Ingest.BatchTimeout = 100 * time.Millisecond
	}
	if out.Ingest.QueueSize == 0 {
		out.Ingest.QueueSize = 10_000
	}
	if out.Logger == nil {
		out.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return out
}

type Stack struct {
	LogManager  *storage.SegmentManager
	SpanManager *storage.SegmentManager
	LogSparse   *index.SparseIndex
	SpanSparse  *index.SparseIndex
	LogDir      string
	SpanDir     string
	Executor    *query.Executor
	Batcher     *ingest.Batcher
	Logger      *slog.Logger
	Ready       *atomic.Bool
}

func New(ctx context.Context, opts Options) (*Stack, error) {
	if opts.DataDir == "" {
		return nil, errors.New("runtime: DataDir required")
	}
	cfg := opts.withDefaults()

	logDir := filepath.Join(cfg.DataDir, "logs")
	spanDir := filepath.Join(cfg.DataDir, "spans")

	policy := storage.RotationPolicy{
		MaxRecords: cfg.Storage.SegmentMaxRecords,
		MaxBytes:   cfg.Storage.SegmentMaxBytes,
	}

	logManager, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		return nil, fmt.Errorf("runtime: open log segment manager: %w", err)
	}

	spanManager, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		_ = logManager.Close()
		return nil, fmt.Errorf("runtime: open span segment manager: %w", err)
	}

	logSparse, err := index.LoadSparseIndex(logDir)
	if err != nil {
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, fmt.Errorf("runtime: load log sparse: %w", err)
	}

	spanSparse, err := index.LoadSparseIndex(spanDir)
	if err != nil {
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, fmt.Errorf("runtime: load span sparse: %w", err)
	}

	exec := query.NewExecutorWithCache(
		logManager, spanManager, logSparse, spanSparse,
		logDir, spanDir, cfg.IndexCacheSize,
	)

	bootstrap.SetupSealCallbacks(exec, logManager, spanManager, logDir, spanDir, cfg.Logger)

	ready := &atomic.Bool{}
	go func() {
		bootstrap.LoadSealedIndexes(exec, logManager, spanManager, logDir, spanDir, cfg.Logger)
		ready.Store(true)
		cfg.Logger.Info("sealed indexes loaded")
	}()

	batcher := ingest.NewBatcher(ingest.Deps{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		Indexer:     exec.ActiveIndex(),
		Logger:      cfg.Logger,
	}, ingest.Config{
		BatchSize:        cfg.Ingest.BatchSize,
		BatchTimeout:     cfg.Ingest.BatchTimeout,
		QueueSize:        cfg.Ingest.QueueSize,
		BreakerThreshold: cfg.Ingest.BreakerThreshold,
	})

	if cfg.Cardinality.MaxAttrsPerEntry > 0 || cfg.Cardinality.MaxAttrValueBytes > 0 || cfg.Cardinality.MaxAttrKeysPerService > 0 {
		batcher.SetCardinalityGuard(ingest.NewCardinalityGuard(
			cfg.Cardinality.MaxAttrsPerEntry,
			cfg.Cardinality.MaxAttrValueBytes,
			cfg.Cardinality.MaxAttrKeysPerService,
		))
	}

	batcher.Start(ctx)

	return &Stack{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		LogDir:      logDir,
		SpanDir:     spanDir,
		Executor:    exec,
		Batcher:     batcher,
		Logger:      cfg.Logger,
		Ready:       ready,
	}, nil
}

// Close drains the batcher (caller must cancel ctx first), saves sparse
// indexes, and closes both segment managers. Returns the first error
// encountered but always attempts every step.
func (s *Stack) Close() error {
	s.Batcher.Wait()

	var firstErr error
	if err := s.LogSparse.Save(s.LogDir); err != nil {
		firstErr = fmt.Errorf("runtime: save log sparse: %w", err)
	}
	if err := s.SpanSparse.Save(s.SpanDir); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("runtime: save span sparse: %w", err)
	}
	if err := s.LogManager.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("runtime: close log manager: %w", err)
	}
	if err := s.SpanManager.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("runtime: close span manager: %w", err)
	}
	return firstErr
}
