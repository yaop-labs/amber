package codec

import (
	"math"
	"math/rand"
	"testing"
)

func roundTripBitPacked(t *testing.T, values []int64) {
	t.Helper()
	payload := EncodeBitPacked(values)
	got, err := DecodeBitPackedInto(payload, len(values), nil)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(values) {
		t.Fatalf("len = %d, want %d", len(got), len(values))
	}
	for i := range values {
		if got[i] != values[i] {
			t.Fatalf("value[%d] = %d, want %d", i, got[i], values[i])
		}
	}
}

func TestBitPackedRoundTrip(t *testing.T) {
	cases := map[string][]int64{
		"empty":       {},
		"zeros":       {0, 0, 0, 0},
		"small":       {1, -1, 2, -2, 3, 0},
		"mixed":       {127, -128, 1000, -999, 0, 1},
		"max_int64":   {math.MaxInt64, math.MinInt64, 0},
		"single":      {-42},
		"width_one":   {0, -1, 0, -1, -1},
		"big_uniform": {1 << 40, -(1 << 40), 1<<40 + 1},
	}
	for name, values := range cases {
		t.Run(name, func(t *testing.T) { roundTripBitPacked(t, values) })
	}
}

func TestBitPackedRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 50; trial++ {
		n := rng.Intn(500)
		width := uint(rng.Intn(62) + 1)
		values := make([]int64, n)
		for i := range values {
			values[i] = rng.Int63n(1<<width) - 1<<(width-1)
		}
		roundTripBitPacked(t, values)
	}
}

func TestBitPackedBeatsVarintOnSmallResiduals(t *testing.T) {
	// Residuals in [-2, 2]: zigzag fits 3 bits, varint floors at 1 byte.
	values := make([]int64, 1000)
	rng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = rng.Int63n(5) - 2
	}
	packed := EncodeBitPacked(values)
	varint := EncodeSignedVarints(values)
	if len(packed) >= len(varint) {
		t.Fatalf("packed %d bytes >= varint %d bytes on small residuals", len(packed), len(varint))
	}
	// 3 bits/value + width byte ≈ 376 bytes vs 1000.
	if len(packed) > 400 {
		t.Errorf("packed = %d bytes, expected ~376", len(packed))
	}
}

func TestRacePicksVarintOnOutliers(t *testing.T) {
	// One huge outlier forces packed width to 64 bits/value; varint stays ~1
	// byte for the rest. The race must not let the outlier poison the block.
	values := make([]int64, 1000)
	for i := range values {
		values[i] = int64(i % 3)
	}
	values[500] = math.MaxInt64 / 2
	enc := EncodeIntegerValues(values)
	if _, packed := valueBaseStrategy(enc.Strategy); packed {
		t.Fatalf("race picked packed strategy %s on outlier data", enc.Strategy)
	}
	decoded, err := DecodeIntegerValues(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if decoded[i] != values[i] {
			t.Fatalf("value[%d] mismatch", i)
		}
	}
}

func TestRacePicksPackedOnUniformResiduals(t *testing.T) {
	// A noisy gauge around a level: residual transform leaves small uniform
	// values where bit-packing beats the varint floor.
	rng := rand.New(rand.NewSource(2))
	values := make([]int64, 1000)
	for i := range values {
		values[i] = 5000 + rng.Int63n(7) - 3
	}
	enc := EncodeIntegerValues(values)
	if _, packed := valueBaseStrategy(enc.Strategy); !packed {
		t.Fatalf("race picked %s, expected a packed strategy on uniform residuals", enc.Strategy)
	}
	decoded, err := DecodeIntegerValues(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := range values {
		if decoded[i] != values[i] {
			t.Fatalf("value[%d] mismatch", i)
		}
	}
}

func TestTimestampPackedRoundTrip(t *testing.T) {
	// Jittered scrape grid: irregular (defeats Regular), small dod residuals
	// (packed should win), exact round-trip required.
	rng := rand.New(rand.NewSource(3))
	ts := make([]int64, 500)
	cur := int64(1_700_000_000_000)
	for i := range ts {
		cur += 10_000 + rng.Int63n(7) - 3
		ts[i] = cur
	}
	enc := EncodeTimestamps(ts)
	if enc.Strategy != TimestampStrategyDeltaOfDeltaPacked {
		t.Fatalf("strategy = %s, want delta_of_delta_packed", enc.Strategy)
	}
	decoded, err := DecodeTimestamps(enc)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ts {
		if decoded[i] != ts[i] {
			t.Fatalf("ts[%d] = %d, want %d", i, decoded[i], ts[i])
		}
	}
}

func TestDecodeBitPackedRejectsCorruptPayload(t *testing.T) {
	values := []int64{1, 2, 3, 4}
	payload := EncodeBitPacked(values)

	if _, err := DecodeBitPackedInto(nil, 4, nil); err == nil {
		t.Error("empty payload accepted")
	}
	if _, err := DecodeBitPackedInto([]byte{65}, 1, nil); err == nil {
		t.Error("width > 64 accepted")
	}
	if _, err := DecodeBitPackedInto(payload[:len(payload)-1], 4, nil); err == nil {
		t.Error("truncated payload accepted")
	}
	if _, err := DecodeBitPackedInto(append(payload, 0), 4, nil); err == nil {
		t.Error("oversized payload accepted")
	}
}
