package index

import (
	"math/bits"
	"strconv"
	"time"
)

// Span durations are indexed into log2 buckets so the span .bidx can prune a
// duration range without a per-value bitmap (duration is effectively continuous).
// Bucket b covers nanosecond durations [2^b, 2^(b+1)); a query unions the buckets
// overlapping its [min,max] window, then the scan applies the exact duration
// filter — the same "coarse index + exact recheck" amber uses for zone maps.

func durationBucketIndex(d time.Duration) int {
	ns := int64(d)
	if ns < 1 {
		ns = 1
	}
	return bits.Len64(uint64(ns)) - 1 // floor(log2(ns))
}

// DurationBucket returns the bucket label for one span duration, added to the
// span bitmap under the "dur_bucket" field.
func DurationBucket(d time.Duration) string {
	return strconv.Itoa(durationBucketIndex(d))
}

// DurationBucketField is the span bitmap field holding duration buckets.
const DurationBucketField = "dur_bucket"

// DurationBucketLabels returns every bucket label overlapping [min, max] (a zero
// bound is open: min=0 ⇒ from the smallest bucket, max=0 ⇒ to the largest). The
// boundary buckets also hold spans just outside the window, which the scan's
// exact duration filter removes.
func DurationBucketLabels(min, max time.Duration) []string {
	lo := 0
	if min > 0 {
		lo = durationBucketIndex(min)
	}
	hi := 63
	if max > 0 {
		hi = durationBucketIndex(max)
	}
	if hi < lo {
		return nil
	}
	out := make([]string, 0, hi-lo+1)
	for b := lo; b <= hi; b++ {
		out = append(out, strconv.Itoa(b))
	}
	return out
}
