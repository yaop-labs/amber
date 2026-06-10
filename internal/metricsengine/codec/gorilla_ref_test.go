package codec

// Reference Gorilla encoder (Pelkonen et al., VLDB 2015) — test-only code for
// the bytes/sample comparison in codec_compare_test.go. Encode-only: we
// measure stream sizes, not round-trips. The bit layouts follow the paper:
// timestamp delta-of-delta bucket codes and XOR float compression with
// leading/trailing-zero windows; this is also the chunk format Prometheus
// uses, so "beats Gorilla" here ≈ "beats the Prometheus chunk codec".

import (
	"math"
	"math/bits"
)

type bitWriter struct {
	n int // bits written
}

func (w *bitWriter) writeBits(count int) { w.n += count }

// gorillaTimestampBits returns the encoded size of the timestamp stream.
func gorillaTimestampBits(timestamps []int64) int {
	var w bitWriter
	if len(timestamps) == 0 {
		return 0
	}
	w.writeBits(64) // block header timestamp
	if len(timestamps) == 1 {
		return w.n
	}
	w.writeBits(14) // first delta (paper: 14-bit seconds; kept as-is)
	prevDelta := timestamps[1] - timestamps[0]
	for i := 2; i < len(timestamps); i++ {
		delta := timestamps[i] - timestamps[i-1]
		dod := delta - prevDelta
		prevDelta = delta
		switch {
		case dod == 0:
			w.writeBits(1) // '0'
		case dod >= -63 && dod <= 64:
			w.writeBits(2 + 7) // '10' + 7
		case dod >= -255 && dod <= 256:
			w.writeBits(3 + 9) // '110' + 9
		case dod >= -2047 && dod <= 2048:
			w.writeBits(4 + 12) // '1110' + 12
		default:
			w.writeBits(4 + 32) // '1111' + 32
		}
	}
	return w.n
}

// gorillaValueBits returns the encoded size of the XOR-compressed value stream.
func gorillaValueBits(values []float64) int {
	var w bitWriter
	if len(values) == 0 {
		return 0
	}
	w.writeBits(64) // first value verbatim
	prev := math.Float64bits(values[0])
	prevLeading, prevTrailing := -1, -1
	for _, v := range values[1:] {
		cur := math.Float64bits(v)
		xor := prev ^ cur
		prev = cur
		if xor == 0 {
			w.writeBits(1) // '0'
			continue
		}
		leading := bits.LeadingZeros64(xor)
		trailing := bits.TrailingZeros64(xor)
		if leading > 31 {
			leading = 31 // 5-bit field cap, as in the paper
		}
		if prevLeading >= 0 && leading >= prevLeading && trailing >= prevTrailing {
			// '10': meaningful bits fit the previous window — reuse it.
			w.writeBits(2 + (64 - prevLeading - prevTrailing))
			continue
		}
		// '11' + 5 bits leading + 6 bits meaningful-length + bits.
		meaningful := 64 - leading - trailing
		w.writeBits(2 + 5 + 6 + meaningful)
		prevLeading, prevTrailing = leading, trailing
	}
	return w.n
}

// gorillaIntValueBits encodes int64 samples the way a Gorilla/Prometheus
// chunk would see them: as float64 values.
func gorillaIntValueBits(values []int64) int {
	floats := make([]float64, len(values))
	for i, v := range values {
		floats[i] = float64(v)
	}
	return gorillaValueBits(floats)
}
