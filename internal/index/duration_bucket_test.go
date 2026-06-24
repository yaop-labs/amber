package index

import (
	"slices"
	"testing"
	"time"
)

func TestDurationBucketMonotonic(t *testing.T) {
	// Buckets must be non-decreasing in duration so a range query's label set is
	// contiguous.
	prev := -1
	for _, d := range []time.Duration{
		time.Nanosecond, time.Microsecond, time.Millisecond,
		10 * time.Millisecond, 100 * time.Millisecond, time.Second,
	} {
		b := durationBucketIndex(d)
		if b < prev {
			t.Fatalf("bucket for %v = %d < previous %d", d, b, prev)
		}
		prev = b
	}
}

func TestDurationBucketLabelsCoverDuration(t *testing.T) {
	// A span's own bucket must appear in the label set of any range that includes
	// its duration (so the bitmap never prunes a true match away).
	cases := []struct {
		d        time.Duration
		min, max time.Duration
		want     bool
	}{
		{50 * time.Millisecond, 10 * time.Millisecond, 0, true},   // min-only, above min
		{50 * time.Millisecond, 100 * time.Millisecond, 0, false}, // min-only, below min
		{50 * time.Millisecond, 0, 100 * time.Millisecond, true},  // max-only, below max
		{50 * time.Millisecond, 0, 10 * time.Millisecond, false},  // max-only, above max
		{50 * time.Millisecond, 10 * time.Millisecond, 100 * time.Millisecond, true},
	}
	for _, c := range cases {
		labels := DurationBucketLabels(c.min, c.max)
		got := slices.Contains(labels, DurationBucket(c.d))
		// For boundary buckets the span may share a bucket with min/max even when
		// just outside the window - so a true membership is only *required* when
		// the duration is comfortably inside. We assert the must-include cases and
		// allow the coarse boundary to over-include.
		if c.want && !got {
			t.Fatalf("d=%v in [%v,%v]: bucket %s not in labels %v", c.d, c.min, c.max, DurationBucket(c.d), labels)
		}
	}
}
