package codec

import (
	"math"
	"math/rand"
	"testing"
)

func roundTripFloats(t *testing.T, values []float64) FloatEncoding {
	t.Helper()
	enc := EncodeFloatValues(values)
	got, err := DecodeFloatValues(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(got) != len(values) {
		t.Fatalf("len = %d, want %d", len(got), len(values))
	}
	for i := range values {
		if math.Float64bits(got[i]) != math.Float64bits(values[i]) {
			t.Fatalf("value[%d] = %v (bits %x), want %v (bits %x)",
				i, got[i], math.Float64bits(got[i]), values[i], math.Float64bits(values[i]))
		}
	}
	return enc
}

func TestFloatRoundTripDecimal(t *testing.T) {
	enc := roundTripFloats(t, []float64{0.25, 99.95, 1.5, 0, 1000, 0.001, -3.75})
	if len(enc.ExceptionPositions) != 0 {
		t.Errorf("decimal-only input produced %d exceptions", len(enc.ExceptionPositions))
	}
}

func TestFloatRoundTripIntegers(t *testing.T) {
	enc := roundTripFloats(t, []float64{1, 2, 3, 1000000, -42})
	if enc.DecimalExp != 0 {
		t.Errorf("integer floats chose exp %d, want 0", enc.DecimalExp)
	}
	if len(enc.ExceptionPositions) != 0 {
		t.Errorf("integer floats produced exceptions")
	}
}

func TestFloatSpecialValuesAreExceptions(t *testing.T) {
	values := []float64{1.5, math.NaN(), math.Inf(1), math.Inf(-1), math.Pi, math.Copysign(0, -1), 2.5}
	enc := roundTripFloats(t, values)
	// NaN, +Inf, -Inf, pi (binary, non-decimal), -0.0 -> 5 exceptions.
	if len(enc.ExceptionPositions) != 5 {
		t.Errorf("exceptions = %d (%v), want 5", len(enc.ExceptionPositions), enc.ExceptionPositions)
	}
}

func TestFloatAllExceptions(t *testing.T) {
	roundTripFloats(t, []float64{math.NaN(), math.Inf(1), math.Sqrt2})
}

func TestFloatEmptyAndSingle(t *testing.T) {
	roundTripFloats(t, nil)
	roundTripFloats(t, []float64{3.14159})
	roundTripFloats(t, []float64{0.5})
}

// The block exponent must not be dragged up by a single high-precision value
// when that makes everything else more expensive than one exception.
func TestFloatExponentCostTradeoff(t *testing.T) {
	values := make([]float64, 1000)
	for i := range values {
		values[i] = float64(i) + 0.5 // needs exp 1
	}
	values[500] = 0.12345678901234 // needs exp 14
	enc := roundTripFloats(t, values)
	if enc.DecimalExp != 1 {
		t.Errorf("chosen exp = %d, want 1 (outlier should be an exception)", enc.DecimalExp)
	}
	if len(enc.ExceptionPositions) != 1 {
		t.Errorf("exceptions = %d, want 1", len(enc.ExceptionPositions))
	}
}

func TestFloatRoundTripRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for trial := 0; trial < 30; trial++ {
		n := rng.Intn(300)
		values := make([]float64, n)
		for i := range values {
			switch rng.Intn(5) {
			case 0: // decimal with 0..4 digits
				digits := rng.Intn(5)
				values[i] = math.Round(rng.Float64()*10000*pow10[digits]) / pow10[digits]
			case 1: // integer
				values[i] = float64(rng.Int63n(1_000_000))
			case 2: // raw binary float
				values[i] = rng.NormFloat64() * 1e3
			case 3: // special
				values[i] = []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, math.Copysign(0, -1)}[rng.Intn(5)]
			case 4: // huge
				values[i] = rng.Float64() * 1e17
			}
		}
		roundTripFloats(t, values)
	}
}

func TestDecodeFloatValuesRejectsCorrupt(t *testing.T) {
	enc := EncodeFloatValues([]float64{1.5, math.NaN(), 2.5})

	bad := enc
	bad.ExceptionPositions = []int{99}
	if _, err := DecodeFloatValues(bad); err == nil {
		t.Error("out-of-range exception position accepted")
	}

	bad = enc
	bad.ExceptionPositions = []int{0, 0}
	bad.ExceptionValues = []uint64{1, 2}
	if _, err := DecodeFloatValues(bad); err == nil {
		t.Error("duplicate exception position accepted")
	}

	bad = enc
	bad.DecimalExp = maxDecimalExp + 1
	if _, err := DecodeFloatValues(bad); err == nil {
		t.Error("exponent out of range accepted")
	}

	bad = enc
	bad.ExceptionValues = bad.ExceptionValues[:0]
	if _, err := DecodeFloatValues(bad); err == nil {
		t.Error("positions/values mismatch accepted")
	}
}
