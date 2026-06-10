package http

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/metricsengine"
)

type OTLPHandler struct {
	batcher     *ingest.Batcher
	metricStore *metricsengine.Store // nil when metrics are disabled
	histStore   *histogram.Store     // nil when metrics are disabled
	log         *slog.Logger
}

func NewOTLPHandler(batcher *ingest.Batcher, metricStore *metricsengine.Store, histStore *histogram.Store, log *slog.Logger) *OTLPHandler {
	return &OTLPHandler{batcher: batcher, metricStore: metricStore, histStore: histStore, log: log}
}

func (h *OTLPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	switch r.URL.Path {
	case "/v1/logs":
		if h.logIngestUnavailable(w) {
			return
		}
		h.handleLogs(w, r)
	case "/v1/traces":
		if h.spanIngestUnavailable(w) {
			return
		}
		h.handleTraces(w, r)
	case "/v1/metrics":
		h.handleMetrics(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *OTLPHandler) logIngestUnavailable(w http.ResponseWriter) bool {
	if h.batcher == nil || !h.batcher.IsLogBreakerOpen() {
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "ingest temporarily unavailable")
	return true
}

func (h *OTLPHandler) spanIngestUnavailable(w http.ResponseWriter) bool {
	if h.batcher == nil || !h.batcher.IsSpanBreakerOpen() {
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "ingest temporarily unavailable")
	return true
}

func (h *OTLPHandler) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectorlogs.ExportLogsServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		writeError(w, http.StatusBadRequest, "decode failed: "+err.Error())
		return
	}

	var accepted, rejected int
	for _, rl := range req.ResourceLogs {
		service, host := ingest.ExtractResource(rl.Resource.GetAttributes())
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				entry, err := ingest.OTLPLogToEntry(lr, service, host)
				if err != nil {
					rejected++
					continue
				}
				if err := h.batcher.SendLog(entry); err != nil {
					rejected++
					if errors.Is(err, ingest.ErrQueueFull) {
						h.log.Warn("otlp log dropped due to full queue", "service", service)
					}
					continue
				}
				accepted++
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	})
}

func (h *OTLPHandler) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectortrace.ExportTraceServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		writeError(w, http.StatusBadRequest, "decode failed: "+err.Error())
		return
	}

	var accepted, rejected int
	for _, rs := range req.ResourceSpans {
		service, _ := ingest.ExtractResource(rs.Resource.GetAttributes())
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				span, err := ingest.OTLPSpanToEntry(sp, service)
				if err != nil {
					rejected++
					continue
				}
				if err := h.batcher.SendSpan(span); err != nil {
					rejected++
					if errors.Is(err, ingest.ErrQueueFull) {
						h.log.Warn("otlp span dropped due to full queue", "service", service)
					}
					continue
				}
				accepted++
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	})
}

func unmarshalOTLP(contentType string, body []byte, msg proto.Message) error {
	if strings.Contains(contentType, "application/x-protobuf") {
		return proto.Unmarshal(body, msg)
	}
	return protojson.Unmarshal(body, msg)
}
