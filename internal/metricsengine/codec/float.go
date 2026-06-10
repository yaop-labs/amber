package codec

import (
	"errors"
	"math"
)

// ALP-style lossless float encoding (decimal decomposition).
//
// Most real-world gauge values are decimal-quantized at the source (sensor
// readings, SDK-rounded seconds, percentages): v == m / 10^e for a small
// integer mantissa m. EncodeFloatValues detects the cheapest block-wide
// exponent e, routes the mantissas through the existing int64 strategy race
// (EncodeIntegerValues — residual/delta/dod × varint/bitpacked), and stores
// the minority of non-conforming values (NaN, ±Inf, true binary floats,
// overflow at the chosen e) verbatim as positional exceptions.
//
// The round trip is exact for every input, including NaN bit patterns —
// conformance is verified with the same division the decoder performs, and
// non-conforming values bypass the decimal path entirely.
//
// This is the codec core for the float-gauge value model; wiring it through
// model/WAL/head/block is staged separately (see docs/metricsengine/ROADMAP.md).

// maxDecimalExp bounds the per-block exponent: 10^14 keeps mantissas of
// magnitudes up to ~9·10^4 comfortably inside both float64 integer exactness
// checks and int64.
const maxDecimalExp = 14

// pow10 holds exactly-representable powers of ten (every 10^k for k ≤ 22 is
// exact in float64).
var pow10 = [maxDecimalExp + 1]float64{
	1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14,
}

// FloatEncoding is the encoded form of one float series chunk.
type FloatEncoding struct {
	Count int
	// DecimalExp is the block exponent e: value = mantissa / 10^e.
	DecimalExp uint8
	// Mantissas encodes the conforming values' mantissas in order.
	Mantissas ValueEncoding
	// ExceptionPositions are the indices (ascending) whose values bypass the
	// decimal path; ExceptionValues are their exact bit patterns.
	ExceptionPositions []int
	ExceptionValues    []uint64
}

// EncodedSize estimates the on-disk byte cost (payload + exceptions); used by
// benchmarks and, later, by the block writer to race against alternatives.
func (e FloatEncoding) EncodedSize() int {
	size := len(e.Mantissas.Payload)
	// Each exception: ~2 bytes position varint + 8 bytes value.
	size += len(e.ExceptionPositions) * 10
	return size
}

const exceptionCostBytes = 10

// decimalExpFor returns the smallest exponent e (0..maxDecimalExp) whose
// mantissa round-trips v exactly, or -1 when no decimal representation fits.
func decimalExpFor(v float64) int {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return -1
	}
	// Negative zero compares equal to zero but its sign bit would not
	// survive the integer mantissa: keep it as an exception.
	if v == 0 && math.Signbit(v) {
		return -1
	}
	for e := 0; e <= maxDecimalExp; e++ {
		if conformsAt(v, e) {
			return e
		}
	}
	return -1
}

// conformsAt reports whether round(v*10^e)/10^e reproduces v exactly with an
// int64-representable mantissa. The check mirrors the decoder's arithmetic.
func conformsAt(v float64, e int) bool {
	m := math.Round(v * pow10[e])
	if math.IsNaN(m) || m >= maxScaledFloat || m <= -maxScaledFloat {
		return false
	}
	return m/pow10[e] == v
}

// maxScaledFloat mirrors the engine-side guard: float64(math.MaxInt64) == 2^63.
const maxScaledFloat = float64(math.MaxInt64)

// EncodeFloatValues picks the cost-optimal block exponent and encodes.
func EncodeFloatValues(values []float64) FloatEncoding {
	// Minimal exponent per value; -1 = never conforms.
	exps := make([]int, len(values))
	maxNeeded := 0
	for i, v := range values {
		exps[i] = decimalExpFor(v)
		if exps[i] > maxNeeded {
			maxNeeded = exps[i]
		}
	}

	// Evaluate every candidate exponent 0..maxNeeded: conforming values cost
	// their mantissa varint length (a cheap, codec-agnostic proxy for the
	// strategy race's input entropy), non-conforming ones a flat exception
	// cost. A value with minimal exponent ev conforms at e ≥ ev only if the
	// exact division still holds, so conformance is re-checked per candidate.
	bestExp, bestCost := 0, math.MaxInt64>>1
	for e := 0; e <= maxNeeded; e++ {
		cost := 0
		for i, v := range values {
			if exps[i] < 0 || exps[i] > e || !conformsAt(v, e) {
				cost += exceptionCostBytes
				continue
			}
			cost += varintLen(int64(math.Round(v * pow10[e])))
		}
		if cost < bestCost {
			bestExp, bestCost = e, cost
		}
	}

	mantissas := make([]int64, 0, len(values))
	var excPos []int
	var excVal []uint64
	for i, v := range values {
		if exps[i] >= 0 && exps[i] <= bestExp && conformsAt(v, bestExp) {
			mantissas = append(mantissas, int64(math.Round(v*pow10[bestExp])))
			continue
		}
		excPos = append(excPos, i)
		excVal = append(excVal, math.Float64bits(v))
	}

	return FloatEncoding{
		Count:              len(values),
		DecimalExp:         uint8(bestExp),
		Mantissas:          EncodeIntegerValues(mantissas),
		ExceptionPositions: excPos,
		ExceptionValues:    excVal,
	}
}

// DecodeFloatValues reconstructs the exact input, NaN bits included.
func DecodeFloatValues(enc FloatEncoding) ([]float64, error) {
	if int(enc.DecimalExp) > maxDecimalExp {
		return nil, errors.New("codec: float decimal exponent out of range")
	}
	if len(enc.ExceptionPositions) != len(enc.ExceptionValues) {
		return nil, errors.New("codec: float exception positions/values mismatch")
	}
	mantissaCount := enc.Count - len(enc.ExceptionPositions)
	if mantissaCount < 0 || enc.Mantissas.Count != mantissaCount {
		return nil, errors.New("codec: float mantissa count mismatch")
	}
	mantissas, err := DecodeIntegerValues(enc.Mantissas)
	if err != nil {
		return nil, err
	}

	out := make([]float64, enc.Count)
	isException := make([]bool, enc.Count)
	for i, pos := range enc.ExceptionPositions {
		if pos < 0 || pos >= enc.Count {
			return nil, errors.New("codec: float exception position out of range")
		}
		if isException[pos] {
			return nil, errors.New("codec: duplicate float exception position")
		}
		isException[pos] = true
		out[pos] = math.Float64frombits(enc.ExceptionValues[i])
	}
	scale := pow10[enc.DecimalExp]
	next := 0
	for i := range out {
		if isException[i] {
			continue
		}
		out[i] = float64(mantissas[next]) / scale
		next++
	}
	return out, nil
}

// varintLen returns the encoded length of one zigzag varint.
func varintLen(v int64) int {
	z := ZigZagEncode(v)
	n := 1
	for z >= 0x80 {
		z >>= 7
		n++
	}
	return n
}
