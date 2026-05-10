package http

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

func APIKeyMiddleware(apiKey string, next http.Handler) http.Handler {
	if apiKey == "" {
		return next
	}
	expected := []byte(apiKey)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		got := []byte(auth[len("Bearer "):])
		if subtle.ConstantTimeCompare(got, expected) != 1 {
			writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
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
