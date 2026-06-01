package main

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

func TestParseDebugSelector(t *testing.T) {
	selector, err := parseDebugSelector(`http_requests_total{job="api",instance=~"web-.+",env!="dev",pod!~"canary-.+"}`)
	if err != nil {
		t.Fatal(err)
	}
	want := []index.Matcher{
		{Name: "__name__", Op: index.MatchEqual, Value: "http_requests_total"},
		{Name: "job", Op: index.MatchEqual, Value: "api"},
		{Name: "instance", Op: index.MatchRegexp, Value: "web-.+"},
		{Name: "env", Op: index.MatchNotEqual, Value: "dev"},
		{Name: "pod", Op: index.MatchNotRegexp, Value: "canary-.+"},
	}
	if len(selector.Matchers) != len(want) {
		t.Fatalf("matchers len = %d, want %d", len(selector.Matchers), len(want))
	}
	for i := range want {
		if selector.Matchers[i] != want[i] {
			t.Fatalf("matcher[%d] = %+v, want %+v", i, selector.Matchers[i], want[i])
		}
	}
}

func TestParseDebugRangeSelector(t *testing.T) {
	rangeSelector, err := parseDebugRangeSelector(`http_requests_total{job="api"}[5m]`)
	if err != nil {
		t.Fatal(err)
	}
	if rangeSelector.Window != 5*time.Minute {
		t.Fatalf("window = %s, want 5m", rangeSelector.Window)
	}
	want := []index.Matcher{
		{Name: "__name__", Op: index.MatchEqual, Value: "http_requests_total"},
		{Name: "job", Op: index.MatchEqual, Value: "api"},
	}
	if len(rangeSelector.Selector.Matchers) != len(want) {
		t.Fatalf("matchers len = %d, want %d", len(rangeSelector.Selector.Matchers), len(want))
	}
	for i := range want {
		if rangeSelector.Selector.Matchers[i] != want[i] {
			t.Fatalf("matcher[%d] = %+v, want %+v", i, rangeSelector.Selector.Matchers[i], want[i])
		}
	}
}

func TestParseDebugSelectorRejectsInvalidRegexp(t *testing.T) {
	if _, err := parseDebugSelector(`{job=~"["}`); err == nil {
		t.Fatal("expected invalid regexp error")
	}
}

func TestParseDebugSelectorRejectsBadMetricName(t *testing.T) {
	if _, err := parseDebugSelector(`9metric{job="api"}`); err == nil {
		t.Fatal("expected invalid metric name error")
	}
}
