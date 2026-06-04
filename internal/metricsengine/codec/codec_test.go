package codec

import "testing"

func TestTimestampRoundTrip(t *testing.T) {
	in := []int64{1000, 2000, 3000, 4003, 5007, 6010}
	out, err := DecodeTimestamps(EncodeTimestamps(in))
	if err != nil {
		t.Fatal(err)
	}
	for i := range in {
		if in[i] != out[i] {
			t.Fatalf("timestamp[%d] = %d, want %d", i, out[i], in[i])
		}
	}
}

func TestRegularTimestampEncoding(t *testing.T) {
	enc := EncodeTimestamps([]int64{1000, 2000, 3000, 4000})
	if enc.Strategy != TimestampStrategyRegular {
		t.Fatalf("strategy = %s, want regular", enc.Strategy)
	}
	if len(enc.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(enc.Payload))
	}
}

func TestIntegerValueRoundTrip(t *testing.T) {
	cases := [][]int64{
		{},
		{7},
		{10, 10, 10, 10},
		{1, 3, 6, 10, 15, 21},
		{50, 48, 55, 49, 51},
	}
	for _, in := range cases {
		out, err := DecodeIntegerValues(EncodeIntegerValues(in))
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(in) {
			t.Fatalf("len(out) = %d, want %d", len(out), len(in))
		}
		for i := range in {
			if in[i] != out[i] {
				t.Fatalf("value[%d] = %d, want %d", i, out[i], in[i])
			}
		}
	}
}

func TestDecodeIntegerValuesIntoReusesBuffer(t *testing.T) {
	in := []int64{1, 3, 6, 10}
	buf := make([]int64, 0, len(in))
	out, reused, err := DecodeIntegerValuesInto(EncodeIntegerValues(in), buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != len(in) {
		t.Fatalf("len(out) = %d, want %d", len(out), len(in))
	}
	if len(reused) != len(in) {
		t.Fatalf("len(reused) = %d, want %d", len(reused), len(in))
	}
	if len(out) > 0 && &out[0] != &reused[0] {
		t.Fatal("out and reused should share the same buffer")
	}
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("value[%d] = %d, want %d", i, out[i], in[i])
		}
	}
}

func TestConstantValueEncoding(t *testing.T) {
	enc := EncodeIntegerValues([]int64{7, 7, 7, 7})
	if enc.Strategy != ValueStrategyConstant {
		t.Fatalf("strategy = %s, want constant", enc.Strategy)
	}
	if len(enc.Payload) != 0 {
		t.Fatalf("payload len = %d, want 0", len(enc.Payload))
	}
}

// TestEncodeIntegerValuesDominatesEveryStrategy is the dominance
// property: encode-all-keep-min is provably never worse than any
// single strategy in the candidate set. Pinning it as a test makes
// the property a contract the codec must keep even if a future
// strategy is added — adding a strategy to candidates can only ever
// shrink the chosen payload, never grow it.
//
// The property:
//
//	for every input v,
//	  len(EncodeIntegerValues(v).Payload) <=
//	    min(
//	      len(encodeDirectResidual(v).Payload),
//	      len(encodeDelta(v).Payload),
//	      len(encodeDeltaOfDelta(v).Payload),
//	    )
//
// (Constant inputs are special-cased upstream so they encode to
// zero-byte payloads — covered by TestConstantValueEncoding above.)
//
// Inputs span the four real-world shapes the codec exists to absorb:
// monotonic counters (delta wins), gauges with low-frequency jitter
// (dod wins), random noise (direct wins), and pathological mixed
// signals where no single strategy is obviously best.
func TestEncodeIntegerValuesDominatesEveryStrategy(t *testing.T) {
	cases := map[string][]int64{
		"monotonic-counter": {
			100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
		},
		"slow-counter": {
			0, 1, 2, 4, 7, 11, 16, 22, 29, 37, 46, 56, 67, 79, 92,
		},
		"gauge-low-jitter": {
			500, 502, 499, 503, 501, 498, 504, 500, 502, 499,
		},
		"gauge-step": {
			100, 100, 100, 200, 200, 200, 300, 300, 300, 400,
		},
		"random-noise": {
			-42, 178, -913, 4501, -2, 999_999, -123_456, 7, 8888, -1,
		},
		"all-zero": {0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		"single-value":  {42},
		"two-values":    {1, 1_000_000_000},
		"pathological-mixed": {
			0, 1_000_000, 0, 1_000_000, 0, 1_000_000, 0, 1_000_000,
		},
	}

	for name, values := range cases {
		t.Run(name, func(t *testing.T) {
			// Constant inputs short-circuit to ValueStrategyConstant
			// upstream; skip them here because the strategy is then
			// not selected from the candidate set the test is about.
			if isConstant(values) {
				t.Skip("constant input — covered by TestConstantValueEncoding")
			}

			chosen := EncodeIntegerValues(values)
			candidates := map[string]int{
				"direct-residual": len(encodeDirectResidual(values).Payload),
				"delta":           len(encodeDelta(values).Payload),
				"delta-of-delta":  len(encodeDeltaOfDelta(values).Payload),
			}

			for cand, size := range candidates {
				if len(chosen.Payload) > size {
					t.Errorf("dominance violated: chosen (%s, %d bytes) > %s candidate (%d bytes); values=%v",
						chosen.Strategy, len(chosen.Payload),
						cand, size, values)
				}
			}

			// Round-trip property: every selected strategy must
			// decode back to the original input. Without this the
			// dominance check is meaningless — a strategy that
			// encodes to zero bytes by losing data would "win".
			decoded, err := DecodeIntegerValues(chosen)
			if err != nil {
				t.Fatalf("decode %s: %v", chosen.Strategy, err)
			}
			if len(decoded) != len(values) {
				t.Fatalf("decoded length %d != input %d", len(decoded), len(values))
			}
			for i := range values {
				if decoded[i] != values[i] {
					t.Fatalf("decoded[%d] = %d, want %d (full decoded=%v)",
						i, decoded[i], values[i], decoded)
				}
			}
		})
	}
}
