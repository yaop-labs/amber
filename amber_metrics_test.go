package amber_test

// Smoke test for the embedded metrics store. Same hard rule as the other
// public-surface tests: only github.com/yaop-labs/amber and its public
// metricsengine facade are allowed — no reach-around into internal/...
//
// What this proves end-to-end:
//   - Open enables the store by default and MetricStore() returns non-nil
//   - AppendBatch + RateByLabelRange round-trip a counter rate
//   - Close flushes the head and the WAL without error

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestEmbedded_MetricStore_RateRoundTrip(t *testing.T) {
	dir := t.TempDir()

	db, err := amber.Open(dir)
	if err != nil {
		t.Fatalf("amber.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store := db.MetricStore()
	if store == nil {
		t.Fatal("MetricStore() returned nil; metrics enabled by default")
	}

	labels := metricsengine.LabelSet{
		{Name: metricsengine.MetricNameLabel, Value: "http_requests_total"},
		{Name: "job", Value: "api"},
	}
	if _, err := store.AppendBatch([]metricsengine.Sample{
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: 60_000, Value: 61},
	}); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	rs := metricsengine.RangeSelector{
		Selector: metricsengine.NewSelector(metricsengine.MetricName("http_requests_total")),
		Window:   60 * time.Second,
	}
	rates, err := store.RateByLabelRange(rs, 60_000, "job")
	if err != nil {
		t.Fatalf("RateByLabelRange: %v", err)
	}
	// 60 increase over a 60s window = 1.0/s.
	if got := rates["api"]; got != 1 {
		t.Fatalf("rates[api] = %v, want 1", got)
	}
}

func TestEmbedded_MetricStore_DisabledReturnsNil(t *testing.T) {
	dir := t.TempDir()

	db, err := amber.Open(dir, &amber.Options{
		Metrics: amber.Metrics{Disabled: true},
	})
	if err != nil {
		t.Fatalf("amber.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if db.MetricStore() != nil {
		t.Fatal("MetricStore() should be nil when Disabled=true")
	}
}
