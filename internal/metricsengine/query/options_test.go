package query

import (
	"testing"
	"time"
)

func TestOptionsHelpers(t *testing.T) {
	opts := TimeRange(1000, 2000).WithValueRange(10, 20)
	if opts.StartMillis == nil || *opts.StartMillis != 1000 {
		t.Fatalf("StartMillis = %v, want 1000", opts.StartMillis)
	}
	if opts.EndMillis == nil || *opts.EndMillis != 2000 {
		t.Fatalf("EndMillis = %v, want 2000", opts.EndMillis)
	}
	if opts.MinValue == nil || *opts.MinValue != 10 {
		t.Fatalf("MinValue = %v, want 10", opts.MinValue)
	}
	if opts.MaxValue == nil || *opts.MaxValue != 20 {
		t.Fatalf("MaxValue = %v, want 20", opts.MaxValue)
	}
}

func TestTimeWindow(t *testing.T) {
	opts := TimeWindow(10_000, 5*time.Second)
	if opts.StartMillis == nil || *opts.StartMillis != 5000 {
		t.Fatalf("StartMillis = %v, want 5000", opts.StartMillis)
	}
	if opts.EndMillis == nil || *opts.EndMillis != 10_000 {
		t.Fatalf("EndMillis = %v, want 10000", opts.EndMillis)
	}
}
