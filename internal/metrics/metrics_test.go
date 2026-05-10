package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCounterVec_Increment(t *testing.T) {
	cv := NewCounterVec("test_total", "test help", "kind")
	cv.WithLabelValues("a").Inc()
	cv.WithLabelValues("a").Add(4)
	cv.WithLabelValues("b").Inc()

	if got := cv.WithLabelValues("a").Get(); got != 5 {
		t.Fatalf("a: got %d, want 5", got)
	}
	if got := cv.WithLabelValues("b").Get(); got != 1 {
		t.Fatalf("b: got %d, want 1", got)
	}
}

func TestCounterVec_PanicOnLabelMismatch(t *testing.T) {
	cv := NewCounterVec("test_total", "h", "kind", "reason")
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on wrong arity")
		}
	}()
	cv.WithLabelValues("only-one")
}

func TestEscapeLabel(t *testing.T) {
	cases := map[string]string{
		"plain":         "plain",
		`with "quote"`:  `with \"quote\"`,
		"with\nnewline": `with\nnewline`,
		`back\slash`:    `back\\slash`,
		`mix"\` + "\nx": `mix\"\\\nx`,
		"":              "",
	}
	for in, want := range cases {
		if got := escapeLabel(in); got != want {
			t.Errorf("escapeLabel(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHandler_TextExposition(t *testing.T) {
	cv := NewCounterVec("amber_test_events_total", "test events", "kind", "reason")
	cv.WithLabelValues("log", "queue_full").Add(7)
	cv.WithLabelValues("span", "write_failed").Inc()
	RegisterCounterVec(cv)

	RegisterGaugeFunc("amber_test_queue_length", "current queue depth", func() float64 { return 42 })
	RegisterCounterFunc("amber_test_corrupt_records_total", "corrupt rec", func() float64 { return 3 })

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content-type=%q, want text/plain prefix", ct)
	}

	body := rec.Body.String()
	must := []string{
		`# HELP amber_test_events_total test events`,
		`# TYPE amber_test_events_total counter`,
		`amber_test_events_total{kind="log",reason="queue_full"} 7`,
		`amber_test_events_total{kind="span",reason="write_failed"} 1`,
		`# HELP amber_test_queue_length current queue depth`,
		`# TYPE amber_test_queue_length gauge`,
		`amber_test_queue_length 42`,
		`# HELP amber_test_corrupt_records_total corrupt rec`,
		`# TYPE amber_test_corrupt_records_total counter`,
		`amber_test_corrupt_records_total 3`,
	}
	for _, line := range must {
		if !strings.Contains(body, line) {
			t.Errorf("missing line: %q\nbody:\n%s", line, body)
		}
	}
}

func TestFormatFloat_IntegerCollapses(t *testing.T) {
	if got := formatFloat(42); got != "42" {
		t.Errorf("formatFloat(42) = %q, want 42", got)
	}
	if got := formatFloat(0); got != "0" {
		t.Errorf("formatFloat(0) = %q, want 0", got)
	}
	if got := formatFloat(1.5); got != "1.5" {
		t.Errorf("formatFloat(1.5) = %q, want 1.5", got)
	}
}
