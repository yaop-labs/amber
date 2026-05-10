package http

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMaxBytesMiddleware_RejectsOversize(t *testing.T) {
	var readErr error
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, readErr = io.ReadAll(r.Body)
	})
	h := MaxBytesMiddleware(16, next)

	req := httptest.NewRequest("POST", "/", bytes.NewReader(make([]byte, 1024)))
	h.ServeHTTP(httptest.NewRecorder(), req)

	var maxErr *http.MaxBytesError
	if !errors.As(readErr, &maxErr) {
		t.Fatalf("expected *http.MaxBytesError, got %T: %v", readErr, readErr)
	}
}

func TestMaxBytesMiddleware_PassesUnderLimit(t *testing.T) {
	var got []byte
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
	})
	h := MaxBytesMiddleware(64, next)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", strings.NewReader("hello")))

	if string(got) != "hello" {
		t.Fatalf("body=%q, want hello", got)
	}
}

func TestMaxBytesMiddleware_ZeroLimitDisablesGuard(t *testing.T) {
	var got []byte
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
	})
	h := MaxBytesMiddleware(0, next)

	body := make([]byte, 1<<16)
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", "/", bytes.NewReader(body)))

	if len(got) != len(body) {
		t.Fatalf("read %d bytes, want %d", len(got), len(body))
	}
}

func TestReadyHandler(t *testing.T) {
	var ready atomic.Bool
	h := ReadyHandler(ready.Load)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("not ready: code=%d, want 503", rec.Code)
	}

	ready.Store(true)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("ready: code=%d, want 200", rec.Code)
	}
}
