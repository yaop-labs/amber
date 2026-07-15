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
	"sync"
	"syscall"
	"time"

	ambergrpc "github.com/yaop-labs/amber/internal/api/grpc"
	amberhttp "github.com/yaop-labs/amber/internal/api/http"
	"github.com/yaop-labs/amber/internal/config"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/reef/grpcreef"
	"github.com/yaop-labs/reef/tlsconf"
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
	authCfg := cfg.API.ResolvedBearerConfig()
	httpTLS, err := tlsconf.Server(&cfg.API.Security.TLS)
	if err != nil {
		return fmt.Errorf("configure http reef tls: %w", err)
	}
	httpAuth, err := newHTTPAuth(cfg.API)
	if err != nil {
		return fmt.Errorf("configure http reef auth: %w", err)
	}
	tlsconf.WarnIfPlaintext(log, "amber-http", httpTLS != nil)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	stack, err := runtime.New(ctx, runtime.Options{
		DataDir:               cfg.Storage.DataDir,
		Logger:                log,
		IndexCacheSize:        cfg.Storage.IndexCacheSize,
		IndexBootstrapWorkers: cfg.Runtime.IndexBootstrapWorkers,
		MemoryLimit:           cfg.Runtime.MemoryLimit,
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
			Logs: runtime.IngestLaneOptions{
				BatchSize:        cfg.Ingest.Logs.BatchSize,
				BatchTimeout:     cfg.Ingest.Logs.BatchTimeout,
				QueueSize:        cfg.Ingest.Logs.QueueSize,
				BreakerThreshold: cfg.Ingest.Logs.BreakerThreshold,
			},
			Spans: runtime.IngestLaneOptions{
				BatchSize:        cfg.Ingest.Spans.BatchSize,
				BatchTimeout:     cfg.Ingest.Spans.BatchTimeout,
				QueueSize:        cfg.Ingest.Spans.QueueSize,
				BreakerThreshold: cfg.Ingest.Spans.BreakerThreshold,
			},
		},
		Cardinality: runtime.CardinalityOptions{
			MaxAttrsPerEntry:      cfg.Ingest.MaxAttrsPerEntry,
			MaxAttrValueBytes:     cfg.Ingest.MaxAttrValueBytes,
			MaxAttrKeysPerService: cfg.Ingest.MaxAttrKeysPerService,
			MaxServices:           cfg.Ingest.MaxServices,
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
			CacheBudget:         cfg.Metrics.CacheBudget,
		},
		Retention: runtime.RetentionOptions{
			Interval: cfg.Retention.Interval,
			Logs: runtime.StreamRetentionOptions{
				LocalMaxAge: cfg.Retention.Logs.LocalMaxAge, LocalMaxBytes: cfg.Retention.Logs.LocalMaxBytes,
				MaxAge: cfg.Retention.Logs.MaxAge, MaxTotalBytes: cfg.Retention.Logs.MaxBytes, MaxSegments: cfg.Retention.Logs.MaxSegments,
			},
			Spans: runtime.StreamRetentionOptions{
				LocalMaxAge: cfg.Retention.Spans.LocalMaxAge, LocalMaxBytes: cfg.Retention.Spans.LocalMaxBytes,
				MaxAge: cfg.Retention.Spans.MaxAge, MaxTotalBytes: cfg.Retention.Spans.MaxBytes, MaxSegments: cfg.Retention.Spans.MaxSegments,
			},
		},
	})
	if err != nil {
		return err
	}

	selfobs.RegisterGaugeFunc("amber_ingest_queue_length", "Items currently buffered in the ingest queue.", func() float64 {
		return float64(stack.Batcher.QueueLen())
	})
	selfobs.RegisterGaugeFunc("amber_ingest_log_queue_length", "Log items currently buffered in the ingest queue.", func() float64 {
		return float64(stack.Batcher.LogQueueLen())
	})
	selfobs.RegisterGaugeFunc("amber_ingest_span_queue_length", "Span items currently buffered in the ingest queue.", func() float64 {
		return float64(stack.Batcher.SpanQueueLen())
	})
	selfobs.RegisterGaugeFunc("amber_ingest_breaker_open", "1 if the ingest circuit breaker is currently open.", func() float64 {
		if stack.Batcher.IsBreakerOpen() {
			return 1
		}
		return 0
	})
	selfobs.RegisterGaugeFunc("amber_ingest_log_breaker_open", "1 if the log ingest circuit breaker is currently open.", func() float64 {
		if stack.Batcher.IsLogBreakerOpen() {
			return 1
		}
		return 0
	})
	selfobs.RegisterGaugeFunc("amber_ingest_span_breaker_open", "1 if the span ingest circuit breaker is currently open.", func() float64 {
		if stack.Batcher.IsSpanBreakerOpen() {
			return 1
		}
		return 0
	})
	selfobs.RegisterGaugeFunc("amber_segments_total", "Number of segments tracked by a manager.", func() float64 {
		return float64(stack.LogManager.SegmentCount() + stack.SpanManager.SegmentCount())
	})
	readOTLPReplayMetrics := registerOTLPReplayMetrics(stack.OTLPJournal)
	selfobs.RegisterCounterFunc("amber_wal_corrupt_records_total", "Malformed WAL records observed during replay.", func() float64 {
		return float64(stack.LogManager.WALCorruptRecords()+stack.SpanManager.WALCorruptRecords()) + readOTLPReplayMetrics().walRepairEvents
	})
	registerCheckpointMetrics(stack.Status, time.Now)

	if stack.MetricStore != nil {
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
		// One CacheStats snapshot per scrape: the gauges below are pulled in
		// sequence, so a short TTL coalesces them onto a single consistent
		// instant instead of taking the store's RLock once per metric
		// (mirrors selfobs.readMemStats).
		var (
			cacheStatsMu   sync.Mutex
			cacheStatsAt   time.Time
			cacheStatsSnap mestore.CacheStats
		)
		cacheStats := func() mestore.CacheStats {
			cacheStatsMu.Lock()
			defer cacheStatsMu.Unlock()
			if !cacheStatsAt.IsZero() && time.Since(cacheStatsAt) < 100*time.Millisecond {
				return cacheStatsSnap
			}
			cacheStatsSnap = stack.MetricStore.CacheStats()
			cacheStatsAt = time.Now()
			return cacheStatsSnap
		}
		registerCacheGauge := func(name, help string, read func(mestore.CacheStats) int64) {
			selfobs.RegisterGaugeFunc(name, help, func() float64 { return float64(read(cacheStats())) })
		}
		// Cumulative counters go through RegisterCounterFunc so /metrics types
		// them as counters (matching amber_wal_corrupt_records_total above);
		// registering a monotonic _total as a gauge breaks rate()/increase()
		// reset handling and OpenMetrics type validation downstream.
		registerCacheCounter := func(name, help string, read func(mestore.CacheStats) int64) {
			selfobs.RegisterCounterFunc(name, help, func() float64 { return float64(read(cacheStats())) })
		}
		registerCacheGauge("amber_metrics_dircache_bytes", "Bytes held by the decoded-directory cache.", func(cs mestore.CacheStats) int64 { return cs.DirBytes })
		registerCacheGauge("amber_metrics_dircache_budget_bytes", "Byte budget of the decoded-directory cache.", func(cs mestore.CacheStats) int64 { return cs.DirBudget })
		registerCacheCounter("amber_metrics_dircache_hits_total", "Directory cache hits since start.", func(cs mestore.CacheStats) int64 { return cs.DirHits })
		registerCacheCounter("amber_metrics_dircache_misses_total", "Directory cache misses since start.", func(cs mestore.CacheStats) int64 { return cs.DirMisses })
		registerCacheCounter("amber_metrics_dircache_evictions_total", "Directory cache evictions since start.", func(cs mestore.CacheStats) int64 { return cs.DirEvictions })
		registerCacheGauge("amber_metrics_residentcache_bytes", "Bytes held by the resident-index cache.", func(cs mestore.CacheStats) int64 { return cs.ResidentBytes })
		registerCacheGauge("amber_metrics_residentcache_budget_bytes", "Byte budget of the resident-index cache.", func(cs mestore.CacheStats) int64 { return cs.ResidentBudget })
		registerCacheCounter("amber_metrics_residentcache_hits_total", "Resident cache hits since start.", func(cs mestore.CacheStats) int64 { return cs.ResidentHits })
		registerCacheCounter("amber_metrics_residentcache_misses_total", "Resident cache misses since start.", func(cs mestore.CacheStats) int64 { return cs.ResidentMisses })
		registerCacheCounter("amber_metrics_residentcache_evictions_total", "Resident cache evictions since start.", func(cs mestore.CacheStats) int64 { return cs.ResidentEvictions })
	}

	if cfg.API.GRPCAddr != "" {
		grpcOpts, err := grpcreef.ServerOptions(&cfg.API.Security.TLS, authCfg)
		if err != nil {
			return fmt.Errorf("configure grpc reef: %w", err)
		}
		tlsconf.WarnIfPlaintext(log, "amber-grpc", cfg.API.Security.TLS.Enabled)
		grpcServer := ambergrpc.NewServerWithJournal(stack.Batcher, stack.MetricStore, stack.OTLPJournal, int(cfg.API.MaxRequestBytes), log, grpcOpts...)
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
		Batcher:     stack.Batcher,
		Executor:    stack.Executor,
		LogManager:  stack.LogManager,
		LogSparse:   stack.LogSparse,
		MetricStore: stack.MetricStore,
		OTLPJournal: stack.OTLPJournal,
		IsReady:     stack.IsReady,
		Status:      stack.Status,
		Logger:      log,
	}, amberhttp.RoutesConfig{
		MaxRequestBytes: cfg.API.MaxRequestBytes,
	})

	httpServer := &http.Server{
		Addr:              cfg.API.HTTPAddr,
		Handler:           httpAuth(mux),
		ReadTimeout:       cfg.API.ReadTimeout,
		ReadHeaderTimeout: cfg.API.ReadHeaderTimeout,
		WriteTimeout:      cfg.API.WriteTimeout,
		IdleTimeout:       cfg.API.IdleTimeout,
		TLSConfig:         httpTLS,
	}
	go func() {
		log.Info("http server listening", "addr", httpServer.Addr)
		var err error
		if httpTLS != nil {
			err = httpServer.ListenAndServeTLS("", "")
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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
