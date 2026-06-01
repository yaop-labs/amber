// Package client is a thin HTTP client for amber's read API. The CLI and TUI
// both sit on top of it; it is the only place that knows the wire format of
// the /api/v1 endpoints. There is deliberately no in-process path: amber is a
// network store, so even a local instance is reached over HTTP on localhost.
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultAddr is the base URL used when none is configured.
const DefaultAddr = "http://localhost:8080"

// Client talks to a single amber instance.
type Client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the bearer token sent on every request. An empty key leaves
// the Authorization header off, which matches amber's dev mode (no auth).
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient overrides the underlying *http.Client (timeouts, transport).
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.http = h }
}

// New returns a Client for addr (e.g. "http://localhost:8080"). A trailing
// slash on addr is tolerated.
func New(addr string, opts ...Option) *Client {
	if addr == "" {
		addr = DefaultAddr
	}
	c := &Client{
		baseURL: strings.TrimRight(addr, "/"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// LogQuery are the filters accepted by GET /api/v1/logs.
type LogQuery struct {
	Services []string
	Levels   []string
	Hosts    []string
	FullText string
	From     time.Time
	To       time.Time
	Limit    int
	Cursor   string
	Attrs    map[string]string
}

func (q LogQuery) values() url.Values {
	v := url.Values{}
	if len(q.Services) > 0 {
		v.Set("service", strings.Join(q.Services, ","))
	}
	if len(q.Levels) > 0 {
		v.Set("level", strings.Join(q.Levels, ","))
	}
	if len(q.Hosts) > 0 {
		v.Set("host", strings.Join(q.Hosts, ","))
	}
	if q.FullText != "" {
		v.Set("q", q.FullText)
	}
	if !q.From.IsZero() {
		v.Set("from", q.From.Format(time.RFC3339))
	}
	if !q.To.IsZero() {
		v.Set("to", q.To.Format(time.RFC3339))
	}
	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Cursor != "" {
		v.Set("cursor", q.Cursor)
	}
	for k, val := range q.Attrs {
		v.Set("attr."+k, val)
	}
	return v
}

// TraceQuery are the filters accepted by GET /api/v1/traces.
type TraceQuery struct {
	Services []string
	From     time.Time
	To       time.Time
	Limit    int
	Offset   int
}

func (q TraceQuery) values() url.Values {
	v := url.Values{}
	if len(q.Services) > 0 {
		v.Set("service", strings.Join(q.Services, ","))
	}
	if !q.From.IsZero() {
		v.Set("from", q.From.Format(time.RFC3339))
	}
	if !q.To.IsZero() {
		v.Set("to", q.To.Format(time.RFC3339))
	}
	if q.Limit > 0 {
		v.Set("limit", strconv.Itoa(q.Limit))
	}
	if q.Offset > 0 {
		v.Set("offset", strconv.Itoa(q.Offset))
	}
	return v
}

// Services returns the known service names, sorted by the server.
func (c *Client) Services(ctx context.Context) ([]string, error) {
	var resp struct {
		Services []string `json:"services"`
	}
	if err := c.get(ctx, "/api/v1/services", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Services, nil
}

// Logs runs a log query.
func (c *Client) Logs(ctx context.Context, q LogQuery) (*LogResult, error) {
	var resp LogResult
	if err := c.get(ctx, "/api/v1/logs", q.values(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Traces lists trace summaries.
func (c *Client) Traces(ctx context.Context, q TraceQuery) (*TraceList, error) {
	var resp TraceList
	if err := c.get(ctx, "/api/v1/traces", q.values(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Trace fetches one trace's span tree (with attached logs) by hex ID.
func (c *Client) Trace(ctx context.Context, id string) (*Trace, error) {
	var resp Trace
	if err := c.get(ctx, "/api/v1/traces/"+id, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// MetricRateQuery is the input to GET /api/v1/metrics/rate. Window is
// mandatory; End defaults server-side to "now" when zero. Selector adds extra
// label= matchers on top of the implicit `__name__=Metric` one. By empty
// means "no grouping" — the server returns a single bucket keyed by "".
type MetricRateQuery struct {
	Metric   string
	Window   time.Duration
	End      time.Time
	By       string
	Selector map[string]string
}

func (q MetricRateQuery) values() url.Values {
	v := url.Values{}
	v.Set("metric", q.Metric)
	v.Set("window", q.Window.String())
	if !q.End.IsZero() {
		v.Set("end", strconv.FormatInt(q.End.UnixMilli(), 10))
	}
	if q.By != "" {
		v.Set("by", q.By)
	}
	for k, val := range q.Selector {
		v.Add("selector", k+"="+val)
	}
	return v
}

// MetricRate runs a rate query against the embedded metrics store.
func (c *Client) MetricRate(ctx context.Context, q MetricRateQuery) (*MetricRateResult, error) {
	var resp MetricRateResult
	if err := c.get(ctx, "/api/v1/metrics/rate", q.values(), &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Stats fetches admin storage/memory statistics.
func (c *Client) Stats(ctx context.Context) (*Stats, error) {
	var resp Stats
	if err := c.get(ctx, "/api/v1/admin/stats", nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("client: build request: %w", err)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("client: %s: %w", path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("client: %s: %s", path, decodeError(resp))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("client: decode %s: %w", path, err)
	}
	return nil
}

// decodeError turns a non-200 response into a readable message. The server
// writes errors as {"error": "..."}; fall back to the raw body otherwise.
func decodeError(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return fmt.Sprintf("%s (%d)", e.Error, resp.StatusCode)
	}
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(resp.StatusCode)
	}
	return fmt.Sprintf("%s (%d)", msg, resp.StatusCode)
}
