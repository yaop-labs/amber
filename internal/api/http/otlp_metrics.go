package http

import (
	"io"
	"net/http"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/amber/internal/otlpmetrics"
	"github.com/yaop-labs/amber/internal/otlpv4"
)

func (h *OTLPHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if h.metricStore == nil {
		writeError(w, http.StatusServiceUnavailable, "metrics store disabled")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectormetrics.ExportMetricsServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		writeError(w, http.StatusBadRequest, "decode failed: "+err.Error())
		return
	}

	res, err := otlpmetrics.Ingest(h.metricStore, req, h.log)
	if h.journal != nil && res.AcceptedRequest != nil {
		if journalErr := h.journal.AppendRequest(otlpv4.SignalMetrics, res.AcceptedRequest, time.Now()); journalErr != nil {
			writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "metrics replay journal failed; retry request")
			return
		}
	}
	if err != nil {
		writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "metrics durable ingest failed; retry request")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"accepted":    res.Accepted,
		"rejected":    res.Rejected,
		"unsupported": res.Unsupported,
	})
}
