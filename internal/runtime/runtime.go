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

	ready *atomic.Bool
}

// IsReady reports whether bootstrap finished loading sealed indexes.
// Exposed as a method so callers can't flip the flag externally.
func (s *Stack) IsReady() bool { return s.ready.Load() }

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

	bootstrap.SetupSealCallbacks(ctx, exec, logManager, spanManager, logDir, spanDir, cfg.Logger)

	ready := &atomic.Bool{}
	go func() {
		bootstrap.LoadSealedIndexes(ctx, exec, logManager, spanManager, logDir, spanDir, cfg.Logger)
		if ctx.Err() == nil {
			ready.Store(true)
			cfg.Logger.Info("sealed indexes loaded")
		}
	}()

	var guard *ingest.CardinalityGuard
	if cfg.Cardinality.MaxAttrsPerEntry > 0 || cfg.Cardinality.MaxAttrValueBytes > 0 || cfg.Cardinality.MaxAttrKeysPerService > 0 {
		guard = ingest.NewCardinalityGuard(
			cfg.Cardinality.MaxAttrsPerEntry,
			cfg.Cardinality.MaxAttrValueBytes,
			cfg.Cardinality.MaxAttrKeysPerService,
		)
	}

	batcher := ingest.NewBatcher(ingest.Deps{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		Indexer:     exec.ActiveIndex(),
		Guard:       guard,
		Logger:      cfg.Logger,
	}, ingest.Config{
		BatchSize:        cfg.Ingest.BatchSize,
		BatchTimeout:     cfg.Ingest.BatchTimeout,
		QueueSize:        cfg.Ingest.QueueSize,
		BreakerThreshold: cfg.Ingest.BreakerThreshold,
	})

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
		ready:       ready,
	}, nil
}

// Close drains the batcher and shuts down storage under ctx's deadline.
// Callers MUST cancel the parent context that fed New() before calling
// Close, so the batcher's run goroutine is already winding down. If ctx
// expires before drain or filesystem ops complete, Close returns ctx.Err();
// background goroutines may still be running against frozen disks, but the
// process can exit. Aggregates all encountered errors via errors.Join.
func (s *Stack) Close(ctx context.Context) error {
	waitDone := make(chan struct{})
	go func() {
		s.Batcher.Wait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-ctx.Done():
		return fmt.Errorf("runtime: batcher drain: %w", ctx.Err())
	}

	closeDone := make(chan error, 1)
	go func() {
		var errs []error
		if err := s.LogSparse.Save(s.LogDir); err != nil {
			errs = append(errs, fmt.Errorf("runtime: save log sparse: %w", err))
		}
		if err := s.SpanSparse.Save(s.SpanDir); err != nil {
			errs = append(errs, fmt.Errorf("runtime: save span sparse: %w", err))
		}
		if err := s.LogManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("runtime: close log manager: %w", err))
		}
		if err := s.SpanManager.Close(); err != nil {
			errs = append(errs, fmt.Errorf("runtime: close span manager: %w", err))
		}
		closeDone <- errors.Join(errs...)
	}()
	select {
	case err := <-closeDone:
		return err
	case <-ctx.Done():
		return fmt.Errorf("runtime: shutdown: %w", ctx.Err())
	}
}
