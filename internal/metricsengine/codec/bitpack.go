package codec

import (
	"errors"
	"math/bits"
)

// Bit-packed payload: an alternative to the signed-varint payload for the
// same transformed streams (residual / delta / delta-of-delta).
//
// Layout: width(1 byte) | packed bits.
// width is the bit length of the largest zigzag-encoded value (0..64); every
// value is stored in exactly width bits, little-endian bit order within the
// stream. width == 0 means every value is zero and no packed bytes follow.
//
// Varint floors at 1 byte per non-zero value; packing goes below it whenever
// residuals are small and uniform (the common case for regular telemetry).
// A single outlier inflates width for the whole stream — the strategy race
// in EncodeIntegerValues keeps the varint candidate around for exactly that
// case, so packing never has to handle exceptions itself.

// EncodeBitPacked packs values at the fixed width of the widest one.
func EncodeBitPacked(values []int64) []byte {
	width := 0
	for _, v := range values {
		if b := bits.Len64(ZigZagEncode(v)); b > width {
			width = b
		}
	}
	if width == 0 {
		return []byte{0}
	}
	out := make([]byte, 1+(len(values)*width+7)/8)
	out[0] = byte(width)
	bitPos := 0
	for _, v := range values {
		z := ZigZagEncode(v)
		base := bitPos
		for b := 0; b < width; b++ {
			if z&(1<<uint(b)) != 0 {
				idx := base + b
				out[1+idx/8] |= 1 << uint(idx%8)
			}
		}
		bitPos += width
	}
	return out
}

// DecodeBitPackedInto unpacks count values into out (reused when capacity
// allows). The payload length must match exactly.
func DecodeBitPackedInto(payload []byte, count int, out []int64) ([]int64, error) {
	if len(payload) < 1 {
		return nil, errors.New("codec: empty bit-packed payload")
	}
	width := int(payload[0])
	if width > 64 {
		return nil, errors.New("codec: bit-packed width exceeds 64")
	}
	if cap(out) < count {
		out = make([]int64, count)
	}
	out = out[:count]
	if width == 0 {
		if len(payload) != 1 {
			return nil, errors.New("codec: trailing bytes after zero-width bit-packed payload")
		}
		for i := range out {
			out[i] = 0
		}
		return out, nil
	}
	need := 1 + (count*width+7)/8
	if len(payload) != need {
		return nil, errors.New("codec: bit-packed payload length mismatch")
	}
	data := payload[1:]
	bitPos := 0
	for i := 0; i < count; i++ {
		var z uint64
		base := bitPos
		for b := 0; b < width; b++ {
			idx := base + b
			if data[idx/8]&(1<<uint(idx%8)) != 0 {
				z |= 1 << uint(b)
			}
		}
		out[i] = ZigZagDecode(z)
		bitPos += width
	}
	return out, nil
}
