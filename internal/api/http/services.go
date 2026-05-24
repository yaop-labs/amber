package http

import (
	"log/slog"
	"net/http"
	"sort"

	"github.com/yaop-labs/amber/internal/query"
)

type ServicesHandler struct {
	exec *query.Executor
	log  *slog.Logger
}

func NewServicesHandler(exec *query.Executor, log *slog.Logger) *ServicesHandler {
	return &ServicesHandler{exec: exec, log: log}
}

func (h *ServicesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	services := h.exec.Services()
	sort.Strings(services)
	writeJSON(w, http.StatusOK, map[string]any{"services": services})
}
