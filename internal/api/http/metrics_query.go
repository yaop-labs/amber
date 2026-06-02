package http

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/metricsengine"
)

type MetricsQueryHandler struct {
	store *metricsengine.Store
	log   *slog.Logger
}

func NewMetricsQueryHandler(store *metricsengine.Store, log *slog.Logger) *MetricsQueryHandler {
	return &MetricsQueryHandler{store: store, log: log}
}

// MetricsListHandler serves GET /api/v1/metrics — returns all metric names
// currently visible in the head index.
type MetricsListHandler struct {
	store *metricsengine.Store
}

func NewMetricsListHandler(store *metricsengine.Store) *MetricsListHandler {
	return &MetricsListHandler{store: store}
}

func (h *MetricsListHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	names := h.store.MetricNames()
	if names == nil {
		names = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"metrics": names})
}

// rateResponse is the JSON shape returned by GET /api/v1/metrics/rate.
// EndMillis echoes the evaluation point the server actually used (useful when
// the client passed no end= and wants to know how far ahead "now" was).
type rateResponse struct {
	Metric    string             `json:"metric"`
	WindowMS  int64              `json:"window_ms"`
	EndMillis int64              `json:"end_ms"`
	By        string             `json:"by,omitempty"`
	Rates     map[string]float64 `json:"rates"`
}

func (h *MetricsQueryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	start := time.Now()
	var ok bool
	defer func() {
		selfobs.MetricsQueryDuration.WithLabelValues("rate").Observe(time.Since(start).Seconds())
		if !ok {
			selfobs.MetricsQueryErrors.WithLabelValues("rate").Inc()
			return
		}
		selfobs.MetricsQueryTotal.WithLabelValues("rate").Inc()
	}()
	q := r.URL.Query()
	metric := strings.TrimSpace(q.Get("metric"))
	if metric == "" {
		writeError(w, http.StatusBadRequest, "metric is required")
		return
	}
	windowStr := strings.TrimSpace(q.Get("window"))
	if windowStr == "" {
		writeError(w, http.StatusBadRequest, "window is required (e.g. window=5m)")
		return
	}
	window, err := time.ParseDuration(windowStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "window: "+err.Error())
		return
	}
	if window <= 0 {
		writeError(w, http.StatusBadRequest, "window must be positive")
		return
	}

	endMillis := time.Now().UnixMilli()
	if raw := strings.TrimSpace(q.Get("end")); raw != "" {
		parsed, err := parseEndParam(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "end: "+err.Error())
			return
		}
		endMillis = parsed
	}

	by := strings.TrimSpace(q.Get("by"))

	matchers := []metricsengine.Matcher{metricsengine.MetricName(metric)}
	for _, raw := range q["selector"] {
		k, v, ok := strings.Cut(raw, "=")
		if !ok || k == "" {
			writeError(w, http.StatusBadRequest, "selector "+strconv.Quote(raw)+": want key=value")
			return
		}
		matchers = append(matchers, metricsengine.LabelEqual(k, v))
	}

	rs := metricsengine.RangeSelector{
		Selector: metricsengine.NewSelector(matchers...),
		Window:   window,
	}
	rates, err := h.store.RateByLabelRange(rs, endMillis, by)
	if err != nil {
		// ErrNoSamples is not user-facing — the natural representation is an
		// empty result map, which is also what callers get when nothing
		// matched the selector.
		if !errors.Is(err, metricsengine.ErrNoSamples) {
			h.log.Warn("metrics rate query failed", "metric", metric, "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		rates = map[string]float64{}
	}

	writeJSON(w, http.StatusOK, rateResponse{
		Metric:    metric,
		WindowMS:  window.Milliseconds(),
		EndMillis: endMillis,
		By:        by,
		Rates:     rates,
	})
	ok = true
}

// parseEndParam accepts either RFC3339 ("2026-06-01T10:00:00Z") or raw unix
// milliseconds. Other formats stay client-side: time.ParseDuration covers
// relative ranges via the CLI, the server only ever sees an absolute end.
func parseEndParam(raw string) (int64, error) {
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return n, nil
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}
