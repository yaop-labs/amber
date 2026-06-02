package client

import "time"

// Attr is a single key/value attribute on a log or span. The server encodes
// model.Attr with default (capitalised) field names, so the tags match the
// wire form exactly.
type Attr struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// LogEntry mirrors the JSON shape the server emits for model.LogEntry. The
// model type has no json tags and custom marshalers (hex IDs, string level),
// so the client keeps its own string-typed DTO rather than depend on
// unmarshal symmetry that the model does not provide.
type LogEntry struct {
	ID        string    `json:"ID"`
	Timestamp time.Time `json:"Timestamp"`
	Level     string    `json:"Level"`
	Service   string    `json:"Service"`
	Host      string    `json:"Host"`
	TraceID   string    `json:"TraceID"`
	SpanID    string    `json:"SpanID"`
	Body      string    `json:"Body"`
	Attrs     []Attr    `json:"Attrs"`
}

// LogResult is the decoded GET /api/v1/logs response.
type LogResult struct {
	Entries    []LogEntry `json:"entries"`
	TotalHits  int        `json:"total_hits"`
	Truncated  bool       `json:"truncated"`
	NextCursor string     `json:"next_cursor"`
	TookMs     int64      `json:"took_ms"`
	SegTotal   int        `json:"seg_total"`
	SegScanned int        `json:"seg_scanned"`
	CacheHit   bool       `json:"cache_hit"`
}

// TraceSummary is one row of GET /api/v1/traces.
type TraceSummary struct {
	TraceID    string    `json:"trace_id"`
	Service    string    `json:"service"`
	Operation  string    `json:"operation"`
	StartTime  time.Time `json:"start_time"`
	DurationMs int64     `json:"duration_ms"`
	SpanCount  int       `json:"span_count"`
	HasErrors  bool      `json:"has_errors"`
}

// TraceList is the decoded GET /api/v1/traces response.
type TraceList struct {
	Traces []TraceSummary `json:"traces"`
	Total  int            `json:"total"`
}

// Span mirrors model.SpanEntry on the wire. Status is the raw enum value
// (0 unset, 1 ok, 2 error) since model.SpanStatus marshals as a number.
type Span struct {
	ID        string    `json:"ID"`
	TraceID   string    `json:"TraceID"`
	SpanID    string    `json:"SpanID"`
	ParentID  string    `json:"ParentID"`
	Service   string    `json:"Service"`
	Operation string    `json:"Operation"`
	StartTime time.Time `json:"StartTime"`
	EndTime   time.Time `json:"EndTime"`
	Status    int       `json:"Status"`
	Attrs     []Attr    `json:"Attrs"`
}

// SpanStatusError is the enum value the server assigns to failed spans.
const SpanStatusError = 2

// SpanNode is one node of the trace waterfall: a span with its attached logs
// and child spans.
type SpanNode struct {
	Span     Span        `json:"span"`
	Logs     []LogEntry  `json:"logs"`
	Children []*SpanNode `json:"children"`
}

// Trace is the decoded GET /api/v1/traces/{id} response.
type Trace struct {
	TraceID   string      `json:"trace_id"`
	SpanCount int         `json:"span_count"`
	LogCount  int         `json:"log_count"`
	Tree      []*SpanNode `json:"tree"`
	TookMs    int64       `json:"took_ms"`
}

// MetricQuantileResult is the decoded GET /api/v1/metrics/quantile response.
// Quantiles is label-value → quantile-value. The key is "" when the query had
// no `by` grouping. WindowMS/EndMillis are zero for an unbounded query (no
// window= was passed). Quantile echoes the q the server actually applied so
// the caller can render without re-parsing the request.
type MetricQuantileResult struct {
	Metric    string             `json:"metric"`
	Quantile  float64            `json:"quantile"`
	WindowMS  int64              `json:"window_ms,omitempty"`
	EndMillis int64              `json:"end_ms,omitempty"`
	By        string             `json:"by,omitempty"`
	Quantiles map[string]float64 `json:"quantiles"`
}

// MetricRateResult is the decoded GET /api/v1/metrics/rate response. Rates is
// label-value → samples-per-second; the key is empty when the query had no
// `by` grouping. EndMillis echoes the evaluation point the server applied so
// the caller can render a deterministic timestamp.
type MetricRateResult struct {
	Metric    string             `json:"metric"`
	WindowMS  int64              `json:"window_ms"`
	EndMillis int64              `json:"end_ms"`
	By        string             `json:"by,omitempty"`
	Rates     map[string]float64 `json:"rates"`
}

// MetricStoreStats is the decoded GET /api/v1/metrics/stats response.
// MinTimeMS/MaxTimeMS are nil when the store is empty — that lets the CLI
// distinguish "no data" from "data at epoch 0".
type MetricStoreStats struct {
	Blocks          int    `json:"blocks"`
	Series          int    `json:"series"`
	Samples         int    `json:"samples"`
	Bytes           int64  `json:"bytes"`
	MinTimeMS       *int64 `json:"min_time_ms,omitempty"`
	MaxTimeMS       *int64 `json:"max_time_ms,omitempty"`
	BufferedSeries  int    `json:"buffered_series"`
	BufferedSamples int    `json:"buffered_samples"`
}

// Stats mirrors GET /api/v1/admin/stats.
type Stats struct {
	Segments struct {
		SealedCount  int   `json:"sealed_count"`
		TotalRecords int64 `json:"total_records"`
		TotalBytes   int64 `json:"total_bytes"`
		TotalMB      int64 `json:"total_mb"`
		Active       struct {
			Exists      bool   `json:"exists"`
			File        string `json:"file"`
			ID          uint32 `json:"id"`
			RecordCount int64  `json:"record_count"`
		} `json:"active"`
	} `json:"segments"`
	SparseIndex struct {
		Segments int `json:"segments"`
	} `json:"sparse_index"`
	Memory struct {
		HeapAllocMB  int64  `json:"heap_alloc_mb"`
		HeapInuseMB  int64  `json:"heap_inuse_mb"`
		HeapObjects  uint64 `json:"heap_objects"`
		TotalAllocMB int64  `json:"total_alloc_mb"`
	} `json:"memory"`
}
