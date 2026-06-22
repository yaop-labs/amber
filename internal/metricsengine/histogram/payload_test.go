package histogram

import (
	"math"
	"math/rand"
	"testing"
)

// bucketValueAt returns the count at an absolute bucket index (0 outside range).
func bucketValueAt(b Buckets, idx int32) uint64 { return bucketValue(b, idx) }

// assertBucketsEqual compares two bucket lists by value at every absolute index
// in the union range, tolerating differing offsets / trimmed zero padding.
func assertBucketsEqual(t *testing.T, tick int, side string, want, got Buckets) {
	t.Helper()
	lo := want.Offset
	if got.Offset < lo {
		lo = got.Offset
	}
	hi := want.Offset + int32(len(want.Counts))
	if g := got.Offset + int32(len(got.Counts)); g > hi {
		hi = g
	}
	for idx := lo; idx < hi; idx++ {
		if w, g := bucketValueAt(want, idx), bucketValueAt(got, idx); w != g {
			t.Fatalf("tick %d %s bucket idx %d: want %d got %d", tick, side, idx, w, g)
		}
	}
}

func assertSketchEqual(t *testing.T, tick int, want, got *ExponentialHistogram) {
	t.Helper()
	if want.Scale != got.Scale {
		t.Fatalf("tick %d scale: want %d got %d", tick, want.Scale, got.Scale)
	}
	if want.Count != got.Count || want.ZeroCount != got.ZeroCount || want.Sum != got.Sum {
		t.Fatalf("tick %d scalars: want count=%d zero=%d sum=%v got count=%d zero=%d sum=%v",
			tick, want.Count, want.ZeroCount, want.Sum, got.Count, got.ZeroCount, got.Sum)
	}
	assertBucketsEqual(t, tick, "pos", want.Positive, got.Positive)
	assertBucketsEqual(t, tick, "neg", want.Negative, got.Negative)
}

func roundtrip(t *testing.T, sketches []*ExponentialHistogram) {
	t.Helper()
	s := ExpSeries{ID: 1, Labels: lbls("__name__", "lat")}
	for i, sk := range sketches {
		s.Timestamps = append(s.Timestamps, int64(i*1000))
		s.Sketches = append(s.Sketches, sk)
	}
	payload := encodeExpPayload(s)
	ts, decoded, err := decodeExpPayload(payload, len(sketches))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != len(sketches) {
		t.Fatalf("tick count: want %d got %d", len(sketches), len(decoded))
	}
	for i := range sketches {
		if ts[i] != int64(i*1000) {
			t.Fatalf("tick %d timestamp: want %d got %d", i, i*1000, ts[i])
		}
		assertSketchEqual(t, i, sketches[i], decoded[i])
	}
}

// TestExpPayloadDeltaRoundtrip exercises the map-free delta decode against
// adversarial bucket transitions across consecutive same-scale ticks: an
// unchanged tick (empty delta), a new bucket beyond the previous range (range
// extension), a bucket dropping to zero (negative delta + trim), and an offset
// shift.
func TestExpPayloadDeltaRoundtrip(t *testing.T) {
	mk := func(off int32, counts ...uint64) Buckets {
		return Buckets{Offset: off, Counts: append([]uint64(nil), counts...)}
	}
	sum := func(b Buckets) uint64 {
		var n uint64
		for _, c := range b.Counts {
			n += c
		}
		return n
	}
	sketch := func(scale int32, pos, neg Buckets, zero uint64) *ExponentialHistogram {
		return &ExponentialHistogram{
			Scale: scale, ZeroCount: zero, Count: sum(pos) + sum(neg) + zero,
			Sum: 1, Positive: pos, Negative: neg,
		}
	}

	sketches := []*ExponentialHistogram{
		// Full tick.
		sketch(3, mk(0, 1, 2, 3), mk(-2, 5), 1),
		// Identical buckets (empty delta), only scalar zero count differs.
		sketch(3, mk(0, 1, 2, 3), mk(-2, 5), 2),
		// New bucket beyond the previous positive range (range extension).
		sketch(3, mk(0, 1, 2, 3, 0, 7), mk(-2, 5), 2),
		// A previously-nonzero bucket drops to zero (negative delta + trim).
		sketch(3, mk(2, 4), mk(-2, 5), 2),
		// Offset shift to lower indices.
		sketch(3, mk(-5, 9, 0, 0, 1), mk(-2, 5), 2),
	}
	roundtrip(t, sketches)
}

// TestExpPayloadObserveRoundtrip round-trips Observe-built sketches (realistic
// bucket structure) at a fixed scale so consecutive ticks delta-encode.
func TestExpPayloadObserveRoundtrip(t *testing.T) {
	rng := rand.New(rand.NewSource(99))
	var sketches []*ExponentialHistogram
	for tk := 0; tk < 20; tk++ {
		h := NewExponential(4)
		for i := 0; i < 200; i++ {
			h.Observe(math.Exp(rng.NormFloat64()*1.5 + 2))
		}
		sketches = append(sketches, h)
	}
	roundtrip(t, sketches)
}
