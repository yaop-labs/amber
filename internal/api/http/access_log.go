package http

import (
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
)

// AccessLogMiddleware emits one structured log line per request once the
// response is written. Used for audit: every authenticated request is
// attributable to a named key. Skips noisy infra endpoints (/health,
// /readyz) so log volume stays tied to real client traffic.
//
// Fields:
//   - api_key_name: matched key (empty when auth is disabled)
//   - method, path, status
//   - dur_ms: server-side handling time
//   - remote: best-effort client IP (XFF first, then RemoteAddr)
//   - bytes_in / bytes_out: request body bytes read, response bytes written
func AccessLogMiddleware(log *slog.Logger, next http.Handler) http.Handler {
	if log == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		cr := &countingBody{ReadCloser: r.Body}
		r.Body = cr
		rw := &countingResponseWriter{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rw, r)

		keyName, _ := APIKeyNameFromContext(r.Context())
		log.Info("http request",
			"api_key_name", keyName,
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"dur_ms", time.Since(start).Milliseconds(),
			"remote", clientIP(r),
			"bytes_in", cr.n,
			"bytes_out", rw.bytes,
		)
	})
}

// clientIP returns the first X-Forwarded-For IP, or the RemoteAddr host.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type countingBody struct {
	ReadCloser interface {
		Read(p []byte) (int, error)
		Close() error
	}
	n int64
}

func (c *countingBody) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingBody) Close() error { return c.ReadCloser.Close() }

type countingResponseWriter struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *countingResponseWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.status = code
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *countingResponseWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.wroteHeader = true
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}
