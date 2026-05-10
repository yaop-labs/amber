package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	ambergrpc "github.com/hnlbs/amber/internal/api/grpc"
	amberhttp "github.com/hnlbs/amber/internal/api/http"
	"github.com/hnlbs/amber/internal/bootstrap"
	"github.com/hnlbs/amber/internal/config"
	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/metrics"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/retention"
	"github.com/hnlbs/amber/internal/storage"
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

	rotationPolicy := storage.RotationPolicy{
		MaxRecords: cfg.Storage.SegmentMaxRecords,
		MaxBytes:   cfg.Storage.SegmentMaxBytes,
	}

	logDir := filepath.Join(cfg.Storage.DataDir, "logs")
	spanDir := filepath.Join(cfg.Storage.DataDir, "spans")

	logManager, err := storage.OpenSegmentManager(logDir, rotationPolicy)
	if err != nil {
		return fmt.Errorf("failed to open log segment manager: %w", err)
	}
	defer func() { _ = logManager.Close() }()

	spanManager, err := storage.OpenSegmentManager(spanDir, rotationPolicy)
	if err != nil {
		return fmt.Errorf("failed to open span segment manager: %w", err)
	}
	defer func() { _ = spanManager.Close() }()

	log.Info("storage opened")

	logSparse, err := index.LoadSparseIndex(logDir)
	if err != nil {
		return fmt.Errorf("failed to load log sparse index: %w", err)
	}

	spanSparse, err := index.LoadSparseIndex(spanDir)
	if err != nil {
		return fmt.Errorf("failed to load span sparse index: %w", err)
	}

	exec := query.NewExecutorWithCache(
		logManager, spanManager, logSparse, spanSparse,
		logDir, spanDir, cfg.Storage.IndexCacheSize,
	)

	bootstrap.SetupSealCallbacks(exec, logManager, spanManager, logDir, spanDir, log)

	var ready atomic.Bool
	go func() {
		bootstrap.LoadSealedIndexes(exec, logManager, spanManager, logDir, spanDir, log)
		ready.Store(true)
		log.Info("sealed indexes loaded")
	}()

	batcher := ingest.NewBatcher(
		logManager,
		spanManager,
		logSparse,
		spanSparse,
		exec,
		cfg.Ingest.BatchSize,
		cfg.Ingest.BatchTimeout,
		cfg.Ingest.QueueSize,
		cfg.Ingest.BreakerThreshold,
		log,
	)
	batcher.Start(ctx)

	metrics.RegisterGaugeFunc("amber_ingest_queue_length", "Items currently buffered in the ingest queue.", func() float64 {
		return float64(batcher.QueueLen())
	})
	metrics.RegisterGaugeFunc("amber_ingest_breaker_open", "1 if the ingest circuit breaker is currently open.", func() float64 {
		if batcher.IsBreakerOpen() {
			return 1
		}
		return 0
	})
	metrics.RegisterGaugeFunc("amber_segments_total", "Number of segments tracked by a manager.", func() float64 {
		return float64(logManager.SegmentCount() + spanManager.SegmentCount())
	})
	metrics.RegisterCounterFunc("amber_wal_corrupt_records_total", "Malformed WAL records observed during replay.", func() float64 {
		return float64(logManager.WALCorruptRecords() + spanManager.WALCorruptRecords())
	})

	if cfg.Retention.MaxAge > 0 || cfg.Retention.MaxBytes > 0 || cfg.Retention.MaxSegments > 0 {
		policy := retention.Policy{
			MaxAge:        cfg.Retention.MaxAge,
			MaxTotalBytes: cfg.Retention.MaxBytes,
			MaxSegments:   cfg.Retention.MaxSegments,
		}
		interval := cfg.Retention.Interval
		if interval == 0 {
			interval = time.Hour
		}
		logCleaner := retention.NewCleaner(logManager, logSparse, policy, logDir, log)
		spanCleaner := retention.NewCleaner(spanManager, spanSparse, policy, spanDir, log)
		logCleaner.SetOnDelete(exec.InvalidateLogSegment)
		spanCleaner.SetOnDelete(exec.InvalidateSpanSegment)
		go logCleaner.StartLoop(interval, ctx.Done())
		go spanCleaner.StartLoop(interval, ctx.Done())
		log.Info("retention enabled", "max_age", cfg.Retention.MaxAge, "max_bytes", cfg.Retention.MaxBytes, "interval", interval)
	}

	if cfg.API.GRPCAddr != "" {
		grpcServer := ambergrpc.NewServer(batcher, int(cfg.API.MaxRequestBytes), log)
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
	mux.Handle("GET /metrics", metrics.Handler())
	amberhttp.RegisterRoutes(mux, batcher, exec, logManager, logSparse, cfg.API.APIKey, cfg.API.MaxRequestBytes, &ready, log)

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
	done := make(chan struct{})
	go func() {
		batcher.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(batcherTimeout):
		log.Error("batcher shutdown timed out, abandoning in-flight items", "timeout", batcherTimeout)
	}

	if err := logSparse.Save(logDir); err != nil {
		log.Error("failed to save log sparse index", "err", err)
	}
	if err := spanSparse.Save(spanDir); err != nil {
		log.Error("failed to save span sparse index", "err", err)
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
