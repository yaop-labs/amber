package http

import (
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/model"
)

// IngestHandler serves the native JSON log/span ingest endpoint, enqueuing
// parsed entries on the batcher. A partial queue-full reject returns 503 with
// accepted/rejected counts in the body.
type IngestHandler struct {
	batcher *ingest.Batcher
	log     *slog.Logger
}

// NewIngestHandler builds the native ingest handler.
func NewIngestHandler(batcher *ingest.Batcher, log *slog.Logger) *IngestHandler {
	return &IngestHandler{batcher: batcher, log: log}
}

type ingestRequest struct {
	Level   string            `json:"level"`
	Service string            `json:"service"`
	Host    string            `json:"host"`
	Body    string            `json:"body"`
	TraceID string            `json:"trace_id,omitempty"`
	SpanID  string            `json:"span_id,omitempty"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

func (h *IngestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.batcher.IsLogBreakerOpen() {
		writeError(w, http.StatusServiceUnavailable, "ingest temporarily unavailable")
		return
	}
	var req []ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}

	if len(req) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	var accepted, rejected int
	for _, item := range req {
		level, err := model.LevelFromString(item.Level)
		if err != nil {
			rejected++
			continue
		}

		attrs := make([]model.Attr, 0, len(item.Attrs))
		for k, v := range item.Attrs {
			attrs = append(attrs, model.Attr{Key: k, Value: v})
		}

		entry, err := model.NewLogEntry(level, item.Service, item.Host, item.Body, attrs...)
		if err != nil {
			h.log.Error("create log entry", "err", err)
			rejected++
			continue
		}

		if item.TraceID != "" {
			b, err := hex.DecodeString(item.TraceID)
			if err != nil || len(b) != 16 {
				rejected++
				continue
			}
			copy(entry.TraceID[:], b)
		}
		if item.SpanID != "" {
			b, err := hex.DecodeString(item.SpanID)
			if err != nil || len(b) != 8 {
				rejected++
				continue
			}
			copy(entry.SpanID[:], b)
		}

		if err := h.batcher.SendLog(entry); err != nil {
			h.log.Warn("send log failed", "err", err)
			rejected++
			continue
		}
		accepted++
	}

	if rejected > 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"accepted": accepted,
			"rejected": rejected,
		})
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"accepted": accepted,
		"rejected": rejected,
	})
}
