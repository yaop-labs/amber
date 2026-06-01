// Package http serves the JSON, OTLP HTTP, and admin endpoints.
package http

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

type RoutesDeps struct {
	Batcher    *ingest.Batcher
	Executor   *query.Executor
	LogManager *storage.SegmentManager
	LogSparse  *index.SparseIndex
	// MetricStore is the embedded metricsengine store. nil disables the
	// /v1/metrics OTLP route (it returns 503).
	MetricStore *metricsengine.Store
	IsReady     func() bool
	Logger      *slog.Logger
}

type RoutesConfig struct {
	// APIKeys, when non-empty, gates every non-health route. Empty disables
	// auth (single-node / dev). Use config.APIConfig.ResolvedAPIKeys() at
	// wire-up time to merge legacy api_key with the named-list form.
	APIKeys         []config.NamedAPIKey
	MaxRequestBytes int64
}

func RegisterRoutes(mux *http.ServeMux, deps RoutesDeps, cfg RoutesConfig) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.Handle("GET /readyz", ReadyHandler(deps.IsReady))

	access := func(h http.Handler) http.Handler {
		return AccessLogMiddleware(deps.Logger, h)
	}
	auth := func(h http.Handler) http.Handler {
		return APIKeyMiddleware(cfg.APIKeys, access(h))
	}
	authPost := func(h http.Handler) http.Handler {
		return APIKeyMiddleware(cfg.APIKeys, access(MaxBytesMiddleware(cfg.MaxRequestBytes, h)))
	}

	mux.Handle("POST /api/v1/logs", authPost(NewIngestHandler(deps.Batcher, deps.Logger)))
	mux.Handle("GET /api/v1/logs", auth(NewQueryHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/traces/", auth(NewTraceHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/traces", auth(NewTracesHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/services", auth(NewServicesHandler(deps.Executor, deps.Logger)))

	otlpH := NewOTLPHandler(deps.Batcher, deps.MetricStore, deps.Logger)
	mux.Handle("POST /v1/logs", authPost(otlpH))
	mux.Handle("POST /v1/traces", authPost(otlpH))
	mux.Handle("POST /v1/metrics", authPost(otlpH))

	mux.Handle("GET /api/v1/metrics/rate", auth(NewMetricsQueryHandler(deps.MetricStore, deps.Logger)))

	adminH := NewAdminHandler(deps.LogManager, deps.LogSparse, deps.Logger)
	mux.Handle("GET /api/v1/admin/stats", auth(http.HandlerFunc(adminH.Stats)))
	mux.Handle("GET /api/v1/admin/segments", auth(http.HandlerFunc(adminH.Segments)))
}
