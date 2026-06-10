package http

import (
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
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

// MetricsListHandler serves GET /api/v1/metrics.
// It returns scalar metric names and histogram names as separate arrays.
type MetricsListHandler struct {
	scalar *metricsengine.Store
	hist   *histogram.Store
	log    *slog.Logger
}

func NewMetricsListHandler(scalar *metricsengine.Store, hist *histogram.Store, log *slog.Logger) *MetricsListHandler {
	return &MetricsListHandler{scalar: scalar, hist: hist, log: log}
}

type metricsListResponse struct {
	// Metrics is the scalar (counter/gauge) set; backwards-compatible with the
	// initial version of this endpoint.
	Metrics []string `json:"metrics"`
	// Histograms is the histogram-store set. Empty (not omitted) when the
	// histogram store is disabled or empty, so the field is always present.
	Histograms []string `json:"histograms"`
}

func (h *MetricsListHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if h.scalar == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	scalar := h.scalar.MetricNames()
	if scalar == nil {
		scalar = []string{}
	}
	hists := []string{}
	if h.hist != nil {
		names, err := h.hist.MetricNames()
		if err != nil {
			h.log.Warn("histogram metric-names failed", "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if names != nil {
			hists = names
		}
	}
	writeJSON(w, http.StatusOK, metricsListResponse{Metrics: scalar, Histograms: hists})
}

// MetricsStatsHandler serves GET /api/v1/metrics/stats.
// It returns storage counters for scalar and histogram stores.
type MetricsStatsHandler struct {
	scalar *metricsengine.Store
	hist   *histogram.Store
	log    *slog.Logger
}

func NewMetricsStatsHandler(scalar *metricsengine.Store, hist *histogram.Store, log *slog.Logger) *MetricsStatsHandler {
	return &MetricsStatsHandler{scalar: scalar, hist: hist, log: log}
}

// metricsStatsResponse is the wire shape for GET /api/v1/metrics/stats.
type metricsStatsResponse struct {
	Blocks          int                       `json:"blocks"`
	Series          int                       `json:"series"`
	Samples         int                       `json:"samples"`
	Bytes           int64                     `json:"bytes"`
	MinTimeMS       *int64                    `json:"min_time_ms,omitempty"`
	MaxTimeMS       *int64                    `json:"max_time_ms,omitempty"`
	BufferedSeries  int                       `json:"buffered_series"`
	BufferedSamples int                       `json:"buffered_samples"`
	Histogram       histogramStatsSubResponse `json:"histogram"`
}

type histogramStatsSubResponse struct {
	Blocks    int    `json:"blocks"`
	Series    int    `json:"series"`
	Bytes     int64  `json:"bytes"`
	MinTimeMS *int64 `json:"min_time_ms,omitempty"`
	MaxTimeMS *int64 `json:"max_time_ms,omitempty"`
}

func (h *MetricsStatsHandler) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	if h.scalar == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	stats, err := h.scalar.Stats()
	if err != nil {
		h.log.Warn("metrics stats failed", "err", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	resp := metricsStatsResponse{
		Blocks:          stats.Blocks,
		Series:          stats.Series,
		Samples:         stats.Samples,
		Bytes:           stats.Bytes,
		BufferedSeries:  stats.BufferedSeries,
		BufferedSamples: stats.BufferedSamples,
	}
	if stats.HasTime {
		minT, maxT := stats.MinTime, stats.MaxTime
		resp.MinTimeMS = &minT
		resp.MaxTimeMS = &maxT
	}
	if h.hist != nil {
		hs, err := h.hist.Stats()
		if err != nil {
			h.log.Warn("histogram stats failed", "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		resp.Histogram = histogramStatsSubResponse{
			Blocks: hs.Blocks,
			Series: hs.Series,
			Bytes:  hs.Bytes,
		}
		if hs.HasTime {
			minT, maxT := hs.MinTime, hs.MaxTime
			resp.Histogram.MinTimeMS = &minT
			resp.Histogram.MaxTimeMS = &maxT
		}
	}
	writeJSON(w, http.StatusOK, resp)
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
		// Treat no samples as an empty result map.
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

// MetricsQuantileHandler serves GET /api/v1/metrics/quantile.
// It answers one quantile over matching exponential histograms.
type MetricsQuantileHandler struct {
	store *histogram.Store
	log   *slog.Logger
}

func NewMetricsQuantileHandler(store *histogram.Store, log *slog.Logger) *MetricsQuantileHandler {
	return &MetricsQuantileHandler{store: store, log: log}
}

// quantileResponse is the wire shape for GET /api/v1/metrics/quantile.
type quantileResponse struct {
	Metric    string             `json:"metric"`
	Quantile  float64            `json:"quantile"`
	WindowMS  int64              `json:"window_ms,omitempty"`
	EndMillis int64              `json:"end_ms,omitempty"`
	By        string             `json:"by,omitempty"`
	Quantiles map[string]float64 `json:"quantiles"`
}

func (h *MetricsQuantileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "histogram store disabled")
		return
	}
	start := time.Now()
	var ok bool
	defer func() {
		selfobs.MetricsQueryDuration.WithLabelValues("quantile").Observe(time.Since(start).Seconds())
		if !ok {
			selfobs.MetricsQueryErrors.WithLabelValues("quantile").Inc()
			return
		}
		selfobs.MetricsQueryTotal.WithLabelValues("quantile").Inc()
	}()
	q := r.URL.Query()
	metric := strings.TrimSpace(q.Get("metric"))
	if metric == "" {
		writeError(w, http.StatusBadRequest, "metric is required")
		return
	}
	qStr := strings.TrimSpace(q.Get("q"))
	if qStr == "" {
		writeError(w, http.StatusBadRequest, "q is required (e.g. q=0.95)")
		return
	}
	quantile, err := strconv.ParseFloat(qStr, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "q: "+err.Error())
		return
	}
	if quantile < 0 || quantile > 1 {
		writeError(w, http.StatusBadRequest, "q must be in [0, 1]")
		return
	}

	// Window resolution: if window is set, [end-window, end] where end
	// defaults to now. If window is absent, the range is unbounded and
	// covers every sealed block. The unbounded case is the "give me the
	// quantile over everything" path used by ad-hoc CLI calls.
	tr := histogram.TimeRange{Start: math.MinInt64, End: math.MaxInt64}
	endMillis := int64(0)
	windowMS := int64(0)
	if windowStr := strings.TrimSpace(q.Get("window")); windowStr != "" {
		window, err := time.ParseDuration(windowStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "window: "+err.Error())
			return
		}
		if window <= 0 {
			writeError(w, http.StatusBadRequest, "window must be positive")
			return
		}
		end := time.Now().UnixMilli()
		if raw := strings.TrimSpace(q.Get("end")); raw != "" {
			parsed, err := parseEndParam(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "end: "+err.Error())
				return
			}
			end = parsed
		}
		tr = histogram.TimeRange{Start: end - window.Milliseconds(), End: end}
		endMillis = end
		windowMS = window.Milliseconds()
	}

	by := strings.TrimSpace(q.Get("by"))
	matchers := []metricsengine.Matcher{metricsengine.MetricName(metric)}
	for _, raw := range q["selector"] {
		k, v, found := strings.Cut(raw, "=")
		if !found || k == "" {
			writeError(w, http.StatusBadRequest, "selector "+strconv.Quote(raw)+": want key=value")
			return
		}
		matchers = append(matchers, metricsengine.LabelEqual(k, v))
	}
	selector := metricsengine.NewSelector(matchers...)

	result := make(map[string]float64)
	if by == "" {
		v, err := h.store.HistogramQuantile(selector, quantile, tr)
		if err != nil {
			h.log.Warn("histogram quantile failed", "metric", metric, "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !math.IsNaN(v) {
			result[""] = v
		}
	} else {
		// Single-label grouping only, mirroring /metrics/rate's contract.
		// Multi-label-by is a deliberate future extension; supporting it
		// today would force a different key format on the client.
		if strings.Contains(by, ",") {
			writeError(w, http.StatusBadRequest, "by must be a single label (multi-label grouping not supported yet)")
			return
		}
		grouped, err := h.store.HistogramQuantileBy(selector, quantile, tr, []string{by})
		if err != nil {
			h.log.Warn("histogram quantile-by failed", "metric", metric, "err", err)
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		for k, v := range grouped {
			if math.IsNaN(v) {
				continue
			}
			// Collapse the canonical single-label group key to just the value.
			result[unquoteSingleGroupKey(k, by)] = v
		}
	}

	writeJSON(w, http.StatusOK, quantileResponse{
		Metric:    metric,
		Quantile:  quantile,
		WindowMS:  windowMS,
		EndMillis: endMillis,
		By:        by,
		Quantiles: result,
	})
	ok = true
}

// unquoteSingleGroupKey converts a canonical single-label group key to its
// value. Invalid shapes are returned unchanged.
func unquoteSingleGroupKey(key, label string) string {
	prefix := strconv.Quote(label) + "="
	if !strings.HasPrefix(key, prefix) {
		return key
	}
	unq, err := strconv.Unquote(key[len(prefix):])
	if err != nil {
		return key
	}
	return unq
}
