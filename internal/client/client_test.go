package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLogsDecodesAndSendsFilters(t *testing.T) {
	var gotQuery string
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"entries":[{"ID":"01","Level":"ERROR","Service":"api","Body":"boom","TraceID":"abcd","Attrs":[{"Key":"k","Value":"v"}]}],"total_hits":1,"took_ms":3}`))
	}))
	defer srv.Close()

	c := New(srv.URL, WithAPIKey("secret"))
	res, err := c.Logs(context.Background(), LogQuery{
		Services: []string{"api"},
		Levels:   []string{"ERROR"},
		FullText: "boom",
		Limit:    50,
		Attrs:    map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}

	if gotAuth != "Bearer secret" {
		t.Errorf("auth header = %q, want Bearer secret", gotAuth)
	}
	for _, want := range []string{"service=api", "level=ERROR", "q=boom", "limit=50", "attr.env=prod"} {
		if !contains(gotQuery, want) {
			t.Errorf("query %q missing %q", gotQuery, want)
		}
	}
	if res.TotalHits != 1 || len(res.Entries) != 1 {
		t.Fatalf("unexpected result: %+v", res)
	}
	e := res.Entries[0]
	if e.Level != "ERROR" || e.Service != "api" || e.TraceID != "abcd" {
		t.Errorf("entry decoded wrong: %+v", e)
	}
	if len(e.Attrs) != 1 || e.Attrs[0].Key != "k" || e.Attrs[0].Value != "v" {
		t.Errorf("attrs decoded wrong: %+v", e.Attrs)
	}
}

func TestErrorResponseSurfacesServerMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"error":"invalid 'from' time"}`))
	}))
	defer srv.Close()

	_, err := New(srv.URL).Logs(context.Background(), LogQuery{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !contains(err.Error(), "invalid 'from' time") || !contains(err.Error(), "400") {
		t.Errorf("error %q does not surface server message/status", err)
	}
}

func TestNoAPIKeyOmitsAuthHeader(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		w.Write([]byte(`{"services":["a","b"]}`))
	}))
	defer srv.Close()

	svcs, err := New(srv.URL).Services(context.Background())
	if err != nil {
		t.Fatalf("Services: %v", err)
	}
	if hadAuth {
		t.Error("Authorization header sent despite no API key")
	}
	if len(svcs) != 2 {
		t.Errorf("services = %v, want 2", svcs)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
