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
