// Package runtime owns the storage, index, query, and ingest stack shared by
// the standalone binary and the embedded API.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/bootstrap"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
)

func joinS3Prefix(parts ...string) string {
	joined := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			joined = append(joined, part)
		}
	}
	return strings.Join(joined, "/")
}

type Options struct {
	DataDir        string
	Logger         *slog.Logger
	Storage        StorageOptions
	Ingest         IngestOptions
	Cardinality    CardinalityOptions
	Metrics        MetricsOptions
	IndexCacheSize int
}

// MetricsOptions configures the embedded metrics store.
// Zero limits use the metricsengine defaults.
type MetricsOptions struct {
	Disabled            bool
	Dir                 string
	FlushInterval       time.Duration
	MaxBufferedSamples  int
	MaxActiveSeries     int
	MaxLabelsPerSeries  int
	Retention           time.Duration
	CompactionMinBlocks int
	// DogfoodInterval enables the in-process selfobs scraper.
	// Zero disables it.
	DogfoodInterval time.Duration
}

type StorageOptions struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
	// S3Bucket enables S3-compatible storage for sealed segments.
	S3Bucket   string
	S3Prefix   string
	S3Region   string
	S3Endpoint string // empty = AWS, non-empty = MinIO/R2/etc.
	// S3ReconcileOnStart adopts sealed remote segments at startup.
	S3ReconcileOnStart bool
}

type IngestOptions struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
	Logs             IngestLaneOptions
	Spans            IngestLaneOptions
}

type IngestLaneOptions struct {
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

const (
	defaultSegmentMaxRecords uint64 = 100_000
	defaultSegmentMaxBytes   int64  = 128 << 20

	defaultBatchSize    = 1000
	defaultBatchTimeout = 100 * time.Millisecond
	defaultQueueSize    = 10_000
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

	// MetricStore is nil when metrics are disabled.
	MetricStore *mestore.Store

	// HistogramStore is nil when metrics are disabled.
	HistogramStore *histogram.Store

	dogfoodStop chan struct{}
	dogfoodDone chan struct{}

	logUploader  *uploader
	spanUploader *uploader

	ready *atomic.Bool

	// bootstrapWG waits for the sealed-index bootstrap goroutine.
	bootstrapWG sync.WaitGroup
}

// IsReady reports whether bootstrap finished loading sealed indexes.
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
			Bucket: s3cfg.Bucket, Prefix: joinS3Prefix(s3cfg.Prefix, "spans"),
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

		exec.SetSegmentStores(logS3, spanS3, cfg.Logger)

		logUp = newUploader(logManager, logS3, logDir, cfg.Logger)
		spanUp = newUploader(spanManager, spanS3, spanDir, cfg.Logger)
		logUp.Start(ctx)
		spanUp.Start(ctx)

		logManager.SetOnSealComplete(func(storage.SegmentMeta) { logUp.Enqueue() })
		spanManager.SetOnSealComplete(func(storage.SegmentMeta) { spanUp.Enqueue() })

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
	s := &Stack{ready: ready}

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
		Invalidator: exec,
		Logger:      cfg.Logger,
	}, ingest.Config{
		BatchSize:        cfg.Ingest.BatchSize,
		BatchTimeout:     cfg.Ingest.BatchTimeout,
		QueueSize:        cfg.Ingest.QueueSize,
		BreakerThreshold: cfg.Ingest.BreakerThreshold,
		Logs: ingest.LaneConfig{
			BatchSize:        cfg.Ingest.Logs.BatchSize,
			BatchTimeout:     cfg.Ingest.Logs.BatchTimeout,
			QueueSize:        cfg.Ingest.Logs.QueueSize,
			BreakerThreshold: cfg.Ingest.Logs.BreakerThreshold,
		},
		Spans: ingest.LaneConfig{
			BatchSize:        cfg.Ingest.Spans.BatchSize,
			BatchTimeout:     cfg.Ingest.Spans.BatchTimeout,
			QueueSize:        cfg.Ingest.Spans.QueueSize,
			BreakerThreshold: cfg.Ingest.Spans.BreakerThreshold,
		},
	})

	var metricStore *mestore.Store
	var histStore *histogram.Store
	if !cfg.Metrics.Disabled {
		metricsDir := cfg.Metrics.Dir
		if metricsDir == "" {
			metricsDir = filepath.Join(cfg.DataDir, "metrics")
		}
		ms, err := mestore.OpenWithOptions(metricsDir, mestore.Options{
			FlushInterval:       cfg.Metrics.FlushInterval,
			MaxBufferedSamples:  cfg.Metrics.MaxBufferedSamples,
			MaxActiveSeries:     cfg.Metrics.MaxActiveSeries,
			MaxLabelsPerSeries:  cfg.Metrics.MaxLabelsPerSeries,
			Retention:           cfg.Metrics.Retention,
			CompactionMinBlocks: cfg.Metrics.CompactionMinBlocks,
		})
		if err != nil {
			if logUp != nil {
				logUp.Stop()
			}
			if spanUp != nil {
				spanUp.Stop()
			}
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open metric store: %w", err)
		}
		metricStore = ms

		hs, err := histogram.OpenStoreWithOptions(filepath.Join(metricsDir, "histograms"), histogram.Options{
			Retention:          cfg.Metrics.Retention,
			MaxActiveSeries:    cfg.Metrics.MaxActiveSeries,
			MaxLabelsPerSeries: cfg.Metrics.MaxLabelsPerSeries,
		})
		if err != nil {
			if logUp != nil {
				logUp.Stop()
			}
			if spanUp != nil {
				spanUp.Stop()
			}
			_ = metricStore.Close()
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open histogram store: %w", err)
		}
		histStore = hs
	}

	s.LogManager = logManager
	s.SpanManager = spanManager
	s.LogSparse = logSparse
	s.SpanSparse = spanSparse
	s.LogDir = logDir
	s.SpanDir = spanDir
	s.Executor = exec
	s.Batcher = batcher
	s.MetricStore = metricStore
	s.HistogramStore = histStore
	s.logUploader = logUp
	s.spanUploader = spanUp

	s.bootstrapWG.Go(func() {
		bootstrap.LoadSealedIndexes(ctx, exec, logManager, spanManager, logDir, spanDir, cfg.Logger)
		if ctx.Err() == nil {
			ready.Store(true)
			cfg.Logger.Info("sealed indexes loaded")
		}
	})

	batcher.Start(ctx)

	if metricStore != nil && cfg.Metrics.DogfoodInterval > 0 {
		s.dogfoodStop = make(chan struct{})
		s.dogfoodDone = make(chan struct{})
		go runDogfoodScraper(cfg.Metrics.DogfoodInterval, metricStore, cfg.Logger, s.dogfoodStop, s.dogfoodDone)
	}

	return s, nil
}

// Close drains the batcher and shuts down storage under ctx's deadline.
// The parent context passed to New must be canceled before Close.
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

	// Stop the dogfood scraper before closing the metric store.
	if s.dogfoodStop != nil {
		close(s.dogfoodStop)
		<-s.dogfoodDone
	}

	// Stop uploaders before closing segment managers.
	if s.logUploader != nil {
		s.logUploader.Stop()
	}
	if s.spanUploader != nil {
		s.spanUploader.Stop()
	}

	// Wait for bootstrap readers before closing segment managers.
	bsDone := make(chan struct{})
	go func() {
		s.bootstrapWG.Wait()
		close(bsDone)
	}()
	select {
	case <-bsDone:
	case <-ctx.Done():
		return fmt.Errorf("runtime: bootstrap drain: %w", ctx.Err())
	}

	closeDone := make(chan error, 1)
	go func() {
		var errs []error
		if s.MetricStore != nil {
			if err := s.MetricStore.Close(); err != nil {
				errs = append(errs, fmt.Errorf("runtime: close metric store: %w", err))
			}
		}
		if s.Executor != nil {
			s.Executor.Close()
		}
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
