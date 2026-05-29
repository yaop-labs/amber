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

	"github.com/yaop-labs/amber/internal/bootstrap"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
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
	// S3Bucket enables S3-compatible object storage. When non-empty, sealed
	// segments and their index sidecars are uploaded to S3 after each seal.
	// Reads fall back to S3 on a local cache miss.
	S3Bucket   string
	S3Prefix   string
	S3Region   string
	S3Endpoint string // empty = AWS, non-empty = MinIO/R2/etc.
	// S3ReconcileOnStart forces a remote List at startup. Without it, reconcile
	// still runs but only when the local meta has no sealed segments.
	S3ReconcileOnStart bool
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

// Defaults are sized for a single mid-tier node ingesting modest log volume.
// Operators with very different workloads should override explicitly; the
// numbers are starting points, not optima.
const (
	// 100k records ≈ ~1 MiB compressed per segment (~100s at 1k/s).
	// Heap-threshold pruning skips all but 1-2 segments per query, keeping
	// R p50 in the 50-100ms range. At 1M/seg every query scans the full
	// segment and p50 balloons to 500ms regardless of index quality.
	defaultSegmentMaxRecords uint64 = 100_000

	// 128 MiB: above S3 multipart minimum (5 MiB), safely below the 5 GiB
	// cliff, and sized so a single segment at ~1 MiB compressed hits the
	// record limit long before the byte limit under normal workloads.
	defaultSegmentMaxBytes int64 = 128 << 20

	// Batch of 1000 amortizes WAL/segment write syscalls and zstd block
	// framing without making any single batch large enough to stall ingest
	// while it flushes.
	defaultBatchSize = 1000

	// 100 ms is the batch ceiling: bound on tail latency from queue entry to
	// disk for low-rate workloads where BatchSize is never reached.
	defaultBatchTimeout = 100 * time.Millisecond

	// 10k queue items ≈ 10 batches of 1000. Anything past this is a sign of
	// disk backpressure or a runaway producer; SendLog/SendSpan return
	// ErrQueueFull and the metric counter ticks.
	defaultQueueSize = 10_000
)

func (o Options) withDefaults() Options {
	out := o
	if out.Storage.SegmentMaxRecords == 0 {
		out.Storage.SegmentMaxRecords = defaultSegmentMaxRecords
	}
	if out.Storage.SegmentMaxBytes == 0 {
		out.Storage.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if out.Ingest.BatchSize == 0 {
		out.Ingest.BatchSize = defaultBatchSize
	}
	if out.Ingest.BatchTimeout == 0 {
		out.Ingest.BatchTimeout = defaultBatchTimeout
	}
	if out.Ingest.QueueSize == 0 {
		out.Ingest.QueueSize = defaultQueueSize
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

	logUploader  *uploader
	spanUploader *uploader

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

	var logUp, spanUp *uploader
	if cfg.Storage.S3Bucket != "" {
		s3cfg := storage.S3StoreConfig{
			Bucket:   cfg.Storage.S3Bucket,
			Prefix:   cfg.Storage.S3Prefix,
			Region:   cfg.Storage.S3Region,
			Endpoint: cfg.Storage.S3Endpoint,
		}
		logS3, err := storage.NewS3Store(ctx, storage.S3StoreConfig{
			Bucket: s3cfg.Bucket, Prefix: s3cfg.Prefix,
			Region: s3cfg.Region, Endpoint: s3cfg.Endpoint,
			LocalDir: logDir,
		})
		if err != nil {
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open log s3 store: %w", err)
		}
		spanS3, err := storage.NewS3Store(ctx, storage.S3StoreConfig{
			Bucket: s3cfg.Bucket, Prefix: s3cfg.Prefix + "/spans",
			Region: s3cfg.Region, Endpoint: s3cfg.Endpoint,
			LocalDir: spanDir,
		})
		if err != nil {
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open span s3 store: %w", err)
		}
		logManager.SetStore(logS3)
		spanManager.SetStore(spanS3)

		logUp = newUploader(logManager, logS3, logDir, cfg.Logger)
		spanUp = newUploader(spanManager, spanS3, spanDir, cfg.Logger)
		logUp.Start(ctx)
		spanUp.Start(ctx)

		// Seal callback now just kicks the worker; upload happens off the seal
		// goroutine so an S3 outage cannot stall rotation. The worker re-reads
		// PendingUploads on every wake, so missed signals are harmless.
		logManager.SetOnSealComplete(func(storage.SegmentMeta) { logUp.Enqueue() })
		spanManager.SetOnSealComplete(func(storage.SegmentMeta) { spanUp.Enqueue() })

		// Reconcile: pull sealed segments that exist in S3 but not in local
		// meta. Always runs when local meta is empty (typical fresh node);
		// also runs on every start if the operator opts in. Runs before
		// LoadSealedIndexes so the loader sees adopted segments.
		runLogReconcile := cfg.Storage.S3ReconcileOnStart || len(logManager.Segments()) == 0
		runSpanReconcile := cfg.Storage.S3ReconcileOnStart || len(spanManager.Segments()) == 0
		if runLogReconcile {
			if n, err := bootstrap.ReconcileFromRemote(ctx, logManager, logS3, logDir, cfg.Logger); err != nil {
				cfg.Logger.Warn("log s3 reconcile failed", "err", err)
			} else if n > 0 {
				cfg.Logger.Info("log s3 reconcile adopted segments", "count", n)
			}
		}
		if runSpanReconcile {
			if n, err := bootstrap.ReconcileFromRemote(ctx, spanManager, spanS3, spanDir, cfg.Logger); err != nil {
				cfg.Logger.Warn("span s3 reconcile failed", "err", err)
			} else if n > 0 {
				cfg.Logger.Info("span s3 reconcile adopted segments", "count", n)
			}
		}
	}

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
		LogManager:   logManager,
		SpanManager:  spanManager,
		LogSparse:    logSparse,
		SpanSparse:   spanSparse,
		LogDir:       logDir,
		SpanDir:      spanDir,
		Executor:     exec,
		Batcher:      batcher,
		logUploader:  logUp,
		spanUploader: spanUp,
		ready:        ready,
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

	// Stop uploaders before closing managers: the worker holds a reference to
	// the manager and would race with Close otherwise. We do NOT block on
	// pending uploads here — segments that haven't reached S3 remain in
	// UploadStateLocal and will be retried on next start.
	if s.logUploader != nil {
		s.logUploader.Stop()
	}
	if s.spanUploader != nil {
		s.spanUploader.Stop()
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
