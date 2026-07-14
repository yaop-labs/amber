package http

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strings"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/metricsengine"
)

// OTLPHandler serves the OTLP/HTTP endpoint for logs, traces, and metrics,
// routing logs/spans to the batcher and metrics to the metric store.
type OTLPHandler struct {
	batcher        *ingest.Batcher
	metricStore    *metricsengine.Store // nil when metrics are disabled
	log            *slog.Logger
	maxRequestSize int64
}

// NewOTLPHandler builds the OTLP handler. A nil metricStore disables metric ingest.
func NewOTLPHandler(batcher *ingest.Batcher, metricStore *metricsengine.Store, log *slog.Logger, maxRequestSize ...int64) *OTLPHandler {
	limit := int64(32 << 20)
	if len(maxRequestSize) > 0 {
		limit = maxRequestSize[0]
	}
	return &OTLPHandler{batcher: batcher, metricStore: metricStore, log: log, maxRequestSize: limit}
}

func (h *OTLPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	closeCompression, err := h.prepareRequestBody(w, r)
	if err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errUnsupportedOTLPContentEncoding) {
			code = http.StatusUnsupportedMediaType
		}
		writeOTLPError(w, r, code, 3, err.Error())
		return
	}
	if closeCompression != nil {
		defer closeCompression()
	}
	switch r.URL.Path {
	case "/v1/logs":
		if h.logIngestUnavailable() {
			writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "ingest temporarily unavailable")
			return
		}
		h.handleLogs(w, r)
	case "/v1/traces":
		if h.spanIngestUnavailable() {
			writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "ingest temporarily unavailable")
			return
		}
		h.handleTraces(w, r)
	case "/v1/metrics":
		h.handleMetrics(w, r)
	default:
		writeError(w, http.StatusNotFound, "not found")
	}
}

func (h *OTLPHandler) prepareRequestBody(w http.ResponseWriter, r *http.Request) (func() error, error) {
	encoding := strings.TrimSpace(strings.ToLower(r.Header.Get("Content-Encoding")))
	switch encoding {
	case "", "identity":
		return nil, nil
	case "gzip":
		zr, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("invalid gzip body: %w", err)
		}
		var body io.ReadCloser = io.NopCloser(zr)
		if h.maxRequestSize > 0 {
			body = http.MaxBytesReader(w, body, h.maxRequestSize)
		}
		r.Body = body
		return zr.Close, nil
	default:
		return nil, fmt.Errorf("%w: %q", errUnsupportedOTLPContentEncoding, encoding)
	}
}

func (h *OTLPHandler) logIngestUnavailable() bool {
	return h.batcher == nil || h.batcher.IsLogBreakerOpen()
}

func (h *OTLPHandler) spanIngestUnavailable() bool {
	return h.batcher == nil || h.batcher.IsSpanBreakerOpen()
}

func (h *OTLPHandler) handleLogs(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectorlogs.ExportLogsServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		if errors.Is(err, errUnsupportedOTLPMediaType) {
			writeOTLPError(w, r, http.StatusUnsupportedMediaType, 3, err.Error())
			return
		}
		writeOTLPError(w, r, http.StatusBadRequest, 3, "decode failed: "+err.Error())
		return
	}

	var rejected int64
	var transient error
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
					if retryableHTTPIngestError(err) {
						transient = err
					} else {
						rejected++
					}
					if errors.Is(err, ingest.ErrQueueFull) {
						h.log.Warn("otlp log dropped due to full queue", "service", service)
					}
					continue
				}
			}
		}
	}

	if err := h.batcher.FlushLogs(r.Context()); err != nil && transient == nil {
		transient = err
	}
	if transient != nil {
		writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "durable ingest handoff failed; retry request")
		return
	}
	resp := &collectorlogs.ExportLogsServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectorlogs.ExportLogsPartialSuccess{
			RejectedLogRecords: rejected,
			ErrorMessage:       fmt.Sprintf("rejected %d invalid log record(s)", rejected),
		}
	}
	writeOTLPMessage(w, r.Header.Get("Content-Type"), http.StatusOK, resp)
}

func (h *OTLPHandler) handleTraces(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	req := &collectortrace.ExportTraceServiceRequest{}
	if err := unmarshalOTLP(r.Header.Get("Content-Type"), body, req); err != nil {
		if errors.Is(err, errUnsupportedOTLPMediaType) {
			writeOTLPError(w, r, http.StatusUnsupportedMediaType, 3, err.Error())
			return
		}
		writeOTLPError(w, r, http.StatusBadRequest, 3, "decode failed: "+err.Error())
		return
	}

	var rejected int64
	var transient error
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
					if retryableHTTPIngestError(err) {
						transient = err
					} else {
						rejected++
					}
					if errors.Is(err, ingest.ErrQueueFull) {
						h.log.Warn("otlp span dropped due to full queue", "service", service)
					}
					continue
				}
			}
		}
	}

	if err := h.batcher.FlushSpans(r.Context()); err != nil && transient == nil {
		transient = err
	}
	if transient != nil {
		writeOTLPError(w, r, http.StatusServiceUnavailable, 14, "durable ingest handoff failed; retry request")
		return
	}
	resp := &collectortrace.ExportTraceServiceResponse{}
	if rejected > 0 {
		resp.PartialSuccess = &collectortrace.ExportTracePartialSuccess{
			RejectedSpans: rejected,
			ErrorMessage:  fmt.Sprintf("rejected %d invalid span(s)", rejected),
		}
	}
	writeOTLPMessage(w, r.Header.Get("Content-Type"), http.StatusOK, resp)
}

func retryableHTTPIngestError(err error) bool {
	return errors.Is(err, ingest.ErrQueueFull) || errors.Is(err, ingest.ErrBreakerOpen) || errors.Is(err, ingest.ErrClosed)
}

func writeOTLPError(w http.ResponseWriter, r *http.Request, httpCode int, rpcCode int32, message string) {
	writeOTLPMessage(w, r.Header.Get("Content-Type"), httpCode, &statuspb.Status{Code: rpcCode, Message: message})
}

func writeOTLPMessage(w http.ResponseWriter, contentType string, statusCode int, msg proto.Message) {
	var (
		body []byte
		err  error
	)
	if strings.Contains(contentType, "application/x-protobuf") {
		body, err = proto.Marshal(msg)
		w.Header().Set("Content-Type", "application/x-protobuf")
	} else {
		body, err = protojson.Marshal(msg)
		w.Header().Set("Content-Type", "application/json")
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "encode OTLP response failed")
		return
	}
	w.WriteHeader(statusCode)
	_, _ = w.Write(body)
}

func unmarshalOTLP(contentType string, body []byte, msg proto.Message) error {
	mediaType, _, err := mime.ParseMediaType(contentType)
	if err != nil {
		return fmt.Errorf("%w: %q", errUnsupportedOTLPMediaType, contentType)
	}
	switch strings.ToLower(mediaType) {
	case "application/x-protobuf":
		return proto.Unmarshal(body, msg)
	case "application/json":
		return protojson.Unmarshal(body, msg)
	default:
		return fmt.Errorf("%w: %q", errUnsupportedOTLPMediaType, mediaType)
	}
}

var errUnsupportedOTLPMediaType = errors.New("unsupported OTLP media type")
var errUnsupportedOTLPContentEncoding = errors.New("unsupported OTLP content encoding")
