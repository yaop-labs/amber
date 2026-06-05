package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	goruntime "runtime"
	"syscall"
	"time"

	ambergrpc "github.com/yaop-labs/amber/internal/api/grpc"
	amberhttp "github.com/yaop-labs/amber/internal/api/http"
	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/retention"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := "config.yaml"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	log := setupLogger(cfg.Log)
	log.Info("amber starting",
		"data_dir", cfg.Storage.DataDir,
		"http_addr", cfg.API.HTTPAddr,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stack, err := runtime.New(ctx, runtime.Options{
		DataDir:        cfg.Storage.DataDir,
		Logger:         log,
		IndexCacheSize: cfg.Storage.IndexCacheSize,
		Storage: runtime.StorageOptions{
			SegmentMaxRecords:  cfg.Storage.SegmentMaxRecords,
			SegmentMaxBytes:    cfg.Storage.SegmentMaxBytes,
			S3Bucket:           cfg.Storage.S3.Bucket,
			S3Prefix:           cfg.Storage.S3.Prefix,
			S3Region:           cfg.Storage.S3.Region,
			S3Endpoint:         cfg.Storage.S3.Endpoint,
			S3ReconcileOnStart: cfg.Storage.S3.ReconcileOnStart,
		},
		Ingest: runtime.IngestOptions{
			BatchSize:        cfg.Ingest.BatchSize,
			BatchTimeout:     cfg.Ingest.BatchTimeout,
			QueueSize:        cfg.Ingest.QueueSize,
			BreakerThreshold: cfg.Ingest.BreakerThreshold,
		},
		Cardinality: runtime.CardinalityOptions{
			MaxAttrsPerEntry:      cfg.Ingest.MaxAttrsPerEntry,
			MaxAttrValueBytes:     cfg.Ingest.MaxAttrValueBytes,
			MaxAttrKeysPerService: cfg.Ingest.MaxAttrKeysPerService,
		},
		Metrics: runtime.MetricsOptions{
			Disabled:            !cfg.Metrics.Enabled,
			Dir:                 cfg.Metrics.Dir,
			FlushInterval:       cfg.Metrics.FlushInterval,
			MaxBufferedSamples:  cfg.Metrics.MaxBufferedSamples,
			MaxActiveSeries:     cfg.Metrics.MaxActiveSeries,
			MaxLabelsPerSeries:  cfg.Metrics.MaxLabelsPerSeries,
			Retention:           cfg.Metrics.Retention,
			CompactionMinBlocks: cfg.Metrics.CompactionMinBlocks,
			DogfoodInterval:     cfg.Metrics.DogfoodInterval,
		},
	})
	if err != nil {
		return err
	}

	selfobs.RegisterGaugeFunc("amber_ingest_queue_length", "Items currently buffered in the ingest queue.", func() float64 {
		return float64(stack.Batcher.QueueLen())
	})
	selfobs.RegisterGaugeFunc("amber_ingest_breaker_open", "1 if the ingest circuit breaker is currently open.", func() float64 {
		if stack.Batcher.IsBreakerOpen() {
			return 1
		}
		return 0
	})
	selfobs.RegisterGaugeFunc("amber_segments_total", "Number of segments tracked by a manager.", func() float64 {
		return float64(stack.LogManager.SegmentCount() + stack.SpanManager.SegmentCount())
	})
	selfobs.RegisterCounterFunc("amber_wal_corrupt_records_total", "Malformed WAL records observed during replay.", func() float64 {
		return float64(stack.LogManager.WALCorruptRecords() + stack.SpanManager.WALCorruptRecords())
	})

	if stack.MetricStore != nil {
		// Cheap state gauges only — Stats() is per-scrape O(blocks) with stat
		// syscalls and footer reads, which would hurt scrape latency. Series
		// count, bytes on disk, and time range stay in amberctl stats.
		selfobs.RegisterGaugeFunc("amber_metrics_store_blocks", "Sealed metric blocks tracked by the manifest.", func() float64 {
			return float64(stack.MetricStore.BlockCount())
		})
		selfobs.RegisterGaugeFunc("amber_metrics_store_head_series", "Series held in the in-memory metrics head, not yet flushed.", func() float64 {
			return float64(stack.MetricStore.BufferedSeries())
		})
		selfobs.RegisterGaugeFunc("amber_metrics_store_head_samples", "Samples held in the in-memory metrics head, not yet flushed.", func() float64 {
			return float64(stack.MetricStore.BufferedSamples())
		})
		selfobs.RegisterGaugeFunc("amber_metrics_active_series", "Total distinct series tracked by the metrics index registry (head + sealed, not yet evicted).", func() float64 {
			return float64(stack.MetricStore.ActiveSeries())
		})
	}

	if cfg.Retention.Logs.Enabled() || cfg.Retention.Spans.Enabled() {
		interval := cfg.Retention.Interval
		if interval == 0 {
			interval = time.Hour
		}
		s3Enabled := cfg.Storage.S3.Bucket != ""

		startCleaner := func(
			stream string,
			scfg config.StreamRetentionConfig,
			mgr *storage.SegmentManager,
			sparse *index.SparseIndex,
			dir string,
			onDelete func(storage.SegmentMeta),
		) {
			if !scfg.Enabled() {
				return
			}
			if scfg.HasLocalTier() && !s3Enabled {
				// Local-tier eviction without a remote copy would just delete
				// data. Refuse loudly rather than silently turning the policy
				// off.
				log.Error("retention local_max_age / local_max_bytes set but storage.s3 is not configured",
					"stream", stream)
				return
			}
			policy := retention.Policy{
				LocalMaxAge:   scfg.LocalMaxAge,
				LocalMaxBytes: scfg.LocalMaxBytes,
				MaxAge:        scfg.MaxAge,
				MaxTotalBytes: scfg.MaxBytes,
				MaxSegments:   scfg.MaxSegments,
			}
			cleaner := retention.NewCleaner(mgr, sparse, policy, dir, stream, log)
			cleaner.SetOnDelete(onDelete)
			if s3Enabled {
				cleaner.RequireUploaded(true)
			}
			go cleaner.StartLoop(interval, ctx.Done())
			log.Info("retention enabled",
				"stream", stream,
				"local_max_age", scfg.LocalMaxAge,
				"local_max_bytes", scfg.LocalMaxBytes,
				"max_age", scfg.MaxAge,
				"max_bytes", scfg.MaxBytes,
				"max_segments", scfg.MaxSegments,
				"interval", interval,
			)
		}

		startCleaner("logs", cfg.Retention.Logs, stack.LogManager, stack.LogSparse, stack.LogDir, stack.Executor.InvalidateLogSegment)
		startCleaner("spans", cfg.Retention.Spans, stack.SpanManager, stack.SpanSparse, stack.SpanDir, stack.Executor.InvalidateSpanSegment)
	}

	if cfg.API.GRPCAddr != "" {
		grpcServer := ambergrpc.NewServer(stack.Batcher, int(cfg.API.MaxRequestBytes), log)
		go func() {
			log.Info("grpc server listening", "addr", cfg.API.GRPCAddr)
			if err := ambergrpc.ListenAndServe(grpcServer, cfg.API.GRPCAddr); err != nil {
				log.Error("grpc server error", "err", err)
			}
		}()
		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
		}()
	}

	if cfg.Debug.Pprof {
		// Enable mutex + block profiling when pprof is on. Without these,
		// the /debug/pprof/mutex and /debug/pprof/block endpoints exist
		// (via pprof.Index) but always return empty samples. Fractions
		// chosen to keep overhead negligible: 5 means every 5th mutex
		// contention event is sampled; block rate=10000 means events
		// blocking ≥10 µs are captured. Both can be lowered to 1 for a
		// dedicated profiling run if needed.
		goruntime.SetMutexProfileFraction(5)
		goruntime.SetBlockProfileRate(10000)

		pprofAddr := cfg.Debug.PprofAddr
		if pprofAddr == "" {
			pprofAddr = "localhost:6060"
		}
		pprofMux := http.NewServeMux()
		pprofMux.HandleFunc("/debug/pprof/", pprof.Index)
		pprofMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		pprofMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		pprofMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		pprofMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		pprofServer := &http.Server{
			Addr:              pprofAddr,
			Handler:           pprofMux,
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Info("pprof listening", "addr", pprofAddr)
			if err := pprofServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("pprof server error", "err", err)
			}
		}()
		go func() {
			<-ctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = pprofServer.Shutdown(shutdownCtx)
		}()
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", selfobs.Handler())
	amberhttp.RegisterRoutes(mux, amberhttp.RoutesDeps{
		Batcher:        stack.Batcher,
		Executor:       stack.Executor,
		LogManager:     stack.LogManager,
		LogSparse:      stack.LogSparse,
		MetricStore:    stack.MetricStore,
		HistogramStore: stack.HistogramStore,
		IsReady:        stack.IsReady,
		Logger:         log,
	}, amberhttp.RoutesConfig{
		APIKeys:         cfg.API.ResolvedAPIKeys(),
		MaxRequestBytes: cfg.API.MaxRequestBytes,
	})

	httpServer := &http.Server{
		Addr:              cfg.API.HTTPAddr,
		Handler:           mux,
		ReadTimeout:       cfg.API.ReadTimeout,
		ReadHeaderTimeout: cfg.API.ReadHeaderTimeout,
		WriteTimeout:      cfg.API.WriteTimeout,
		IdleTimeout:       cfg.API.IdleTimeout,
	}
	go func() {
		log.Info("http server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
		}
	}()

	<-ctx.Done()
	log.Info("shutdown signal received")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Error("http server shutdown error", "err", err)
	}

	batcherTimeout := cfg.Ingest.ShutdownTimeout
	if batcherTimeout <= 0 {
		batcherTimeout = 30 * time.Second
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), batcherTimeout)
	defer closeCancel()
	if err := stack.Close(closeCtx); err != nil {
		log.Error("stack close error", "err", err)
	}

	log.Info("amber stopped")
	return nil
}

func setupLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.Format == "json" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(h)
}
