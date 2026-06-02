package histogram

import (
	"encoding/binary"
	"errors"
	"math"
)

// Sketch (de)serialization. The on-disk form of an ExponentialHistogram is the
// OTLP-native sketch — scale, zero_count, zero_threshold, positive/negative
// {offset, counts[]}, sum, count, min, max — encoded losslessly so a sketch
// round-trips byte-for-byte equal to the source OTLP data point.

const flagHasMinMax = 1 << 0

// AppendSketch serializes h, appending to dst and returning the extended slice.
func AppendSketch(dst []byte, h *ExponentialHistogram) []byte {
	dst = appendVarint(dst, int64(h.Scale))
	var flags byte
	if h.HasMinMax {
		flags |= flagHasMinMax
	}
	dst = append(dst, flags)
	dst = appendFloat(dst, h.ZeroThreshold)
	dst = appendUvarint(dst, h.ZeroCount)
	dst = appendUvarint(dst, h.Count)
	dst = appendFloat(dst, h.Sum)
	if h.HasMinMax {
		dst = appendFloat(dst, h.Min)
		dst = appendFloat(dst, h.Max)
	}
	dst = appendBuckets(dst, h.Positive)
	dst = appendBuckets(dst, h.Negative)
	return dst
}

// EncodeSketch serializes h into a fresh byte slice.
func EncodeSketch(h *ExponentialHistogram) []byte {
	return AppendSketch(nil, h)
}

// DecodeSketch parses a sketch and returns it along with the number of bytes
// consumed (so callers can decode a packed stream of sketches).
func DecodeSketch(b []byte) (*ExponentialHistogram, int, error) {
	r := reader{b: b}
	h := &ExponentialHistogram{}
	scale, err := r.varint()
	if err != nil {
		return nil, 0, err
	}
	h.Scale = int32(scale)
	flags, err := r.byteVal()
	if err != nil {
		return nil, 0, err
	}
	h.HasMinMax = flags&flagHasMinMax != 0
	if h.ZeroThreshold, err = r.float(); err != nil {
		return nil, 0, err
	}
	if h.ZeroCount, err = r.uvarint(); err != nil {
		return nil, 0, err
	}
	if h.Count, err = r.uvarint(); err != nil {
		return nil, 0, err
	}
	if h.Sum, err = r.float(); err != nil {
		return nil, 0, err
	}
	if h.HasMinMax {
		if h.Min, err = r.float(); err != nil {
			return nil, 0, err
		}
		if h.Max, err = r.float(); err != nil {
			return nil, 0, err
		}
	}
	if h.Positive, err = r.buckets(); err != nil {
		return nil, 0, err
	}
	if h.Negative, err = r.buckets(); err != nil {
		return nil, 0, err
	}
	return h, r.off, nil
}

func appendBuckets(dst []byte, b Buckets) []byte {
	// Trim trailing/leading zeros so empty edges cost nothing. Count is written
	// first so the empty case (n==0) is unambiguous and self-terminating.
	lo, hi := 0, len(b.Counts)
	for lo < hi && b.Counts[lo] == 0 {
		lo++
	}
	for hi > lo && b.Counts[hi-1] == 0 {
		hi--
	}
	n := hi - lo
	dst = appendUvarint(dst, uint64(n))
	if n == 0 {
		return dst
	}
	dst = appendVarint(dst, int64(b.Offset+int32(lo)))
	for _, c := range b.Counts[lo:hi] {
		dst = appendUvarint(dst, c)
	}
	return dst
}

type reader struct {
	b   []byte
	off int
}

func (r *reader) byteVal() (byte, error) {
	if r.off >= len(r.b) {
		return 0, errors.New("histogram: sketch truncated")
	}
	v := r.b[r.off]
	r.off++
	return v, nil
}

func (r *reader) uvarint() (uint64, error) {
	v, n := binary.Uvarint(r.b[r.off:])
	if n <= 0 {
		return 0, errors.New("histogram: invalid uvarint in sketch")
	}
	r.off += n
	return v, nil
}

func (r *reader) varint() (int64, error) {
	v, n := binary.Varint(r.b[r.off:])
	if n <= 0 {
		return 0, errors.New("histogram: invalid varint in sketch")
	}
	r.off += n
	return v, nil
}

func (r *reader) float() (float64, error) {
	if r.off+8 > len(r.b) {
		return 0, errors.New("histogram: sketch truncated (float)")
	}
	bits := binary.LittleEndian.Uint64(r.b[r.off:])
	r.off += 8
	return math.Float64frombits(bits), nil
}

func (r *reader) buckets() (Buckets, error) {
	n, err := r.uvarint()
	if err != nil {
		return Buckets{}, err
	}
	if n == 0 {
		return Buckets{}, nil
	}
	offset, err := r.varint()
	if err != nil {
		return Buckets{}, err
	}
	counts := make([]uint64, n)
	for i := range counts {
		if counts[i], err = r.uvarint(); err != nil {
			return Buckets{}, err
		}
	}
	return Buckets{Offset: int32(offset), Counts: counts}, nil
}

func appendUvarint(dst []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

func appendVarint(dst []byte, v int64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutVarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

func appendFloat(dst []byte, v float64) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
	return append(dst, tmp[:]...)
}
