package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	ambergrpc "github.com/yaop-labs/amber/internal/api/grpc"
	amberhttp "github.com/yaop-labs/amber/internal/api/http"
	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/metrics"
	"github.com/yaop-labs/amber/internal/retention"
	"github.com/yaop-labs/amber/internal/runtime"
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
			SegmentMaxRecords: cfg.Storage.SegmentMaxRecords,
			SegmentMaxBytes:   cfg.Storage.SegmentMaxBytes,
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
	})
	if err != nil {
		return err
	}

	metrics.RegisterGaugeFunc("amber_ingest_queue_length", "Items currently buffered in the ingest queue.", func() float64 {
		return float64(stack.Batcher.QueueLen())
	})
	metrics.RegisterGaugeFunc("amber_ingest_breaker_open", "1 if the ingest circuit breaker is currently open.", func() float64 {
		if stack.Batcher.IsBreakerOpen() {
			return 1
		}
		return 0
	})
	metrics.RegisterGaugeFunc("amber_segments_total", "Number of segments tracked by a manager.", func() float64 {
		return float64(stack.LogManager.SegmentCount() + stack.SpanManager.SegmentCount())
	})
	metrics.RegisterCounterFunc("amber_wal_corrupt_records_total", "Malformed WAL records observed during replay.", func() float64 {
		return float64(stack.LogManager.WALCorruptRecords() + stack.SpanManager.WALCorruptRecords())
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
		logCleaner := retention.NewCleaner(stack.LogManager, stack.LogSparse, policy, stack.LogDir, log)
		spanCleaner := retention.NewCleaner(stack.SpanManager, stack.SpanSparse, policy, stack.SpanDir, log)
		logCleaner.SetOnDelete(stack.Executor.InvalidateLogSegment)
		spanCleaner.SetOnDelete(stack.Executor.InvalidateSpanSegment)
		if cfg.Storage.S3.Bucket != "" {
			// With a remote backend, only delete segments that the background
			// uploader has confirmed durable in S3. Without this guard a
			// transient outage could destroy the only copy of a segment.
			logCleaner.RequireUploaded(true)
			spanCleaner.RequireUploaded(true)
		}
		go logCleaner.StartLoop(interval, ctx.Done())
		go spanCleaner.StartLoop(interval, ctx.Done())
		log.Info("retention enabled", "max_age", cfg.Retention.MaxAge, "max_bytes", cfg.Retention.MaxBytes, "interval", interval)
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
	amberhttp.RegisterRoutes(mux, amberhttp.RoutesDeps{
		Batcher:    stack.Batcher,
		Executor:   stack.Executor,
		LogManager: stack.LogManager,
		LogSparse:  stack.LogSparse,
		IsReady:    stack.IsReady,
		Logger:     log,
	}, amberhttp.RoutesConfig{
		APIKey:          cfg.API.APIKey,
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
