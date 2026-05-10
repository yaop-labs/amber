package http

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/ingest"
	"github.com/hnlbs/amber/internal/query"
	"github.com/hnlbs/amber/internal/storage"
	"github.com/hnlbs/amber/internal/ui"
)

type RoutesDeps struct {
	Batcher    *ingest.Batcher
	Executor   *query.Executor
	LogManager *storage.SegmentManager
	LogSparse  *index.SparseIndex
	Ready      *atomic.Bool
	Logger     *slog.Logger
}

type RoutesConfig struct {
	APIKey          string
	MaxRequestBytes int64
}

func RegisterRoutes(mux *http.ServeMux, deps RoutesDeps, cfg RoutesConfig) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	mux.Handle("GET /readyz", ReadyHandler(deps.Ready))

	auth := func(h http.Handler) http.Handler {
		return APIKeyMiddleware(cfg.APIKey, h)
	}
	authPost := func(h http.Handler) http.Handler {
		return APIKeyMiddleware(cfg.APIKey, MaxBytesMiddleware(cfg.MaxRequestBytes, h))
	}

	mux.Handle("POST /api/v1/logs", authPost(NewIngestHandler(deps.Batcher, deps.Logger)))
	mux.Handle("GET /api/v1/logs", auth(NewQueryHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/traces/", auth(NewTraceHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/traces", auth(NewTracesHandler(deps.Executor, deps.Logger)))
	mux.Handle("GET /api/v1/services", auth(NewServicesHandler(deps.Executor, deps.Logger)))

	otlpH := NewOTLPHandler(deps.Batcher, deps.Logger)
	mux.Handle("POST /v1/logs", authPost(otlpH))
	mux.Handle("POST /v1/traces", authPost(otlpH))

	adminH := NewAdminHandler(deps.LogManager, deps.LogSparse, deps.Logger)
	mux.Handle("GET /api/v1/admin/stats", auth(http.HandlerFunc(adminH.Stats)))
	mux.Handle("GET /api/v1/admin/segments", auth(http.HandlerFunc(adminH.Segments)))

	mux.Handle("/", ui.Handler())
}
