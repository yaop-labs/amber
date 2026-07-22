package http

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/reef/bearer"
)

// TestAccessLogMiddleware_RecordsKeyName threads the full auth + access-log
// stack and verifies the matched key name lands in the structured log line.
// Slog output is parsed as text - the exact field formatting comes from the
// stdlib handler and any drift here is a signal that the audit contract
// shifted, which downstream log shippers care about.
func TestAccessLogMiddleware_RecordsKeyName(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	keys := []config.NamedAPIKey{{Name: "billing", Key: "secret-billing"}}
	handler := APIKeyMiddleware(keys, AccessLogMiddleware(log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})))

	req := httptest.NewRequest("GET", "/api/v1/logs", nil)
	req.Header.Set("Authorization", "Bearer secret-billing")
	req.RemoteAddr = "10.0.0.5:1234"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d", rec.Code)
	}

	out := buf.String()
	wantFields := []string{
		`api_key_name=billing`,
		`method=GET`,
		`path=/api/v1/logs`,
		`status=200`,
		`remote=10.0.0.5`,
		`bytes_out=2`,
	}
	for _, want := range wantFields {
		if !strings.Contains(out, want) {
			t.Errorf("access log missing %q in:\n%s", want, out)
		}
	}
}

// TestAccessLogMiddleware_AnonymousWhenAuthDisabled confirms the key field
// is logged empty (not omitted) when no auth is configured, so log
// dashboards can distinguish "auth disabled" from a logging bug.
func TestAccessLogMiddleware_AnonymousWhenAuthDisabled(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	handler := AccessLogMiddleware(log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("GET", "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	out := buf.String()
	if !strings.Contains(out, `api_key_name=""`) {
		t.Errorf("expected empty api_key_name field in: %s", out)
	}
}

func TestAccessLogMiddleware_RecordsReefPrincipal(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	handler := AccessLogMiddleware(log, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest("GET", "/api/v1/status", nil)
	req = req.WithContext(bearer.ContextWithPrincipal(req.Context(), "collector"))
	handler.ServeHTTP(httptest.NewRecorder(), req)
	if !strings.Contains(buf.String(), `api_key_name=collector`) {
		t.Fatalf("access log did not record Reef principal: %s", buf.String())
	}
}

func TestAPIKeyNameFromContext_AbsentReturnsEmpty(t *testing.T) {
	name, ok := APIKeyNameFromContext(context.Background())
	if ok || name != "" {
		t.Errorf("absent ctx value: got (%q, %v), want (\"\", false)", name, ok)
	}
}

func TestClientIP_XForwardedFor(t *testing.T) {
	cases := []struct {
		name   string
		xff    string
		remote string
		want   string
	}{
		{"single XFF", "1.2.3.4", "10.0.0.1:5000", "1.2.3.4"},
		{"chained XFF picks first", "1.2.3.4, 5.6.7.8", "10.0.0.1:5000", "1.2.3.4"},
		{"no XFF falls back to RemoteAddr host", "", "10.0.0.1:5000", "10.0.0.1"},
		{"no port in RemoteAddr returned verbatim", "", "127.0.0.1", "127.0.0.1"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			if c.xff != "" {
				req.Header.Set("X-Forwarded-For", c.xff)
			}
			req.RemoteAddr = c.remote
			if got := clientIP(req); got != c.want {
				t.Errorf("clientIP: got %q, want %q", got, c.want)
			}
		})
	}
}
