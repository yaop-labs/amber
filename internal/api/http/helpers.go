package http

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/yaop-labs/amber/internal/config"
)

// apiKeyCtxKey identifies the matched API key name in request context.
// Private type to avoid cross-package context collisions.
type apiKeyCtxKey struct{}

// APIKeyNameFromContext returns the matched API key name and true if the
// request was authenticated, or ("", false) when auth was disabled.
func APIKeyNameFromContext(ctx context.Context) (string, bool) {
	v, ok := ctx.Value(apiKeyCtxKey{}).(string)
	return v, ok
}

func ReadyHandler(isReady func() bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isReady != nil && !isReady() {
			writeError(w, http.StatusServiceUnavailable, "indexes loading")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
}

func MaxBytesMiddleware(limit int64, next http.Handler) http.Handler {
	if limit <= 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, limit)
		next.ServeHTTP(w, r)
	})
}

// APIKeyMiddleware checks the Bearer token against the configured named
// keys. On a match the key name is stored in the request context so
// downstream handlers and the access log can attribute the call. An empty
// keys list disables auth entirely (single-node / dev mode).
//
// Comparison is constant-time per candidate key and does not short-circuit on
// the first match.
func APIKeyMiddleware(keys []config.NamedAPIKey, next http.Handler) http.Handler {
	if len(keys) == 0 {
		return next
	}
	// Precompute byte slices once; ConstantTimeCompare wants []byte.
	expected := make([][]byte, len(keys))
	for i, k := range keys {
		expected[i] = []byte(k.Key)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		got := []byte(auth[len("Bearer "):])

		// Match against every key so total work is independent of key position.
		matchedName := ""
		for i, exp := range expected {
			if subtle.ConstantTimeCompare(got, exp) == 1 && matchedName == "" {
				matchedName = keys[i].Name
			}
		}
		if matchedName == "" {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		ctx := context.WithValue(r.Context(), apiKeyCtxKey{}, matchedName)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

var jsonBufPool = sync.Pool{
	New: func() any { return bytes.NewBuffer(make([]byte, 0, 8192)) },
}

const jsonBufMaxRetain = 1 << 20

func writeJSON(w http.ResponseWriter, status int, v any) {
	buf := jsonBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		if buf.Cap() <= jsonBufMaxRetain {
			jsonBufPool.Put(buf)
		}
		http.Error(w, "encode error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(buf.Bytes())
	if buf.Cap() <= jsonBufMaxRetain {
		jsonBufPool.Put(buf)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			result = append(result, t)
		}
	}
	return result
}
