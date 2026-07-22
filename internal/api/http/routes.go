// Package http serves the JSON, OTLP HTTP, and admin endpoints.
package http

import (
	"log/slog"
	"net/http"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

// RoutesDeps are the runtime collaborators the HTTP handlers need.
type RoutesDeps struct {
	Batcher    *ingest.Batcher
	Executor   *query.Executor
	LogManager *storage.SegmentManager
	LogSparse  *index.SparseIndex
	// MetricStore is the embedded metricsengine store for all metric shapes
	// (counters, gauges, histograms). nil disables /v1/metrics ingest and the
	// metric query endpoints.
	MetricStore *metricsengine.Store
	OTLPJournal *otlpv4.Journal
	IsReady     func() bool
	Status      func() runtime.Status
	Logger      *slog.Logger
}

// RoutesConfig holds request-level policy: API-key auth and the body-size cap.
type RoutesConfig struct {
	// APIKeys, when non-empty, gates every non-health route. Empty disables
	// auth (single-node / dev). Use config.APIConfig.ResolvedAPIKeys() at
	// wire-up time to merge legacy api_key with the named-list form.
	APIKeys         []config.NamedAPIKey
	MaxRequestBytes int64
}

// RegisterRoutes wires every HTTP endpoint (ingest, query, trace, metrics,
// admin, health) onto mux, applying API-key auth and the body-size limit.
func RegisterRoutes(mux *http.ServeMux, deps RoutesDeps, cfg RoutesConfig) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if deps.Status == nil {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
			return
		}
		status := deps.Status()
		health := "ok"
		if status.Degraded {
			health = "degraded"
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"status": health, "ready": status.Ready, "degraded": status.Degraded, "reasons": status.Reasons,
		})
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
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

	otlpH := NewOTLPHandler(deps.Batcher, deps.MetricStore, deps.Logger, cfg.MaxRequestBytes)
	otlpH.journal = deps.OTLPJournal
	mux.Handle("POST /v1/logs", authPost(otlpH))
	mux.Handle("POST /v1/traces", authPost(otlpH))
	mux.Handle("POST /v1/metrics", authPost(otlpH))

	mux.Handle("GET /api/v1/metrics", auth(NewMetricsListHandler(deps.MetricStore, deps.Logger)))
	mux.Handle("GET /api/v1/metrics/rate", auth(NewMetricsQueryHandler(deps.MetricStore, deps.Logger)))
	mux.Handle("GET /api/v1/metrics/rate_range", auth(NewMetricsRateRangeHandler(deps.MetricStore, deps.Logger)))
	mux.Handle("GET /api/v1/metrics/stats", auth(NewMetricsStatsHandler(deps.MetricStore, deps.Logger)))
	mux.Handle("GET /api/v1/metrics/quantile", auth(NewMetricsQuantileHandler(deps.MetricStore, deps.Logger)))

	adminH := NewAdminHandler(deps.LogManager, deps.LogSparse, deps.Batcher, deps.Logger)
	if deps.Status != nil {
		mux.Handle("GET /api/v1/admin/status", auth(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(w, http.StatusOK, deps.Status())
		})))
	}
	mux.Handle("GET /api/v1/admin/stats", auth(http.HandlerFunc(adminH.Stats)))
	mux.Handle("GET /api/v1/admin/segments", auth(http.HandlerFunc(adminH.Segments)))
}
