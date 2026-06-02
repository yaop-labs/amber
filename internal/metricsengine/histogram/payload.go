package histogram

import (
	"errors"
	"sort"
)

// Per-series payload encoding for the sketch_section.
//
// Temporal layout (kept deliberately simple — validation showed temporal-delta
// only saves ~1.2x over storing full sketches; most savings come from sketch
// compactness + bucket sparsity): the first tick is stored as a FULL sketch,
// then each subsequent tick stores only the buckets whose count CHANGED, as
// signed integer deltas, plus the (small) scalar synopsis fields. If the scale
// or zero-threshold changes between ticks, that tick falls back to a full sketch.

const (
	tickFull  = 0x01
	tickDelta = 0x00
)

// --- exponential ---

func encodeExpPayload(s ExpSeries) []byte {
	var dst []byte
	dst = appendTimestamps(dst, s.Timestamps)
	var prev *ExponentialHistogram
	for _, sk := range s.Sketches {
		if sk == nil {
			sk = &ExponentialHistogram{}
		}
		if prev == nil || sk.Scale != prev.Scale || sk.ZeroThreshold != prev.ZeroThreshold {
			dst = append(dst, tickFull)
			dst = AppendSketch(dst, sk)
		} else {
			dst = append(dst, tickDelta)
			dst = appendVarint(dst, int64(sk.Scale)) // redundant but cheap; keeps decode uniform
			dst = appendExpScalars(dst, sk)
			dst = appendBucketDeltas(dst, prev.Positive, sk.Positive)
			dst = appendBucketDeltas(dst, prev.Negative, sk.Negative)
		}
		prev = sk
	}
	return dst
}

func decodeExpPayload(b []byte, tickCount int) ([]int64, []*ExponentialHistogram, error) {
	r := reader{b: b}
	ts, err := readTimestamps(&r, tickCount)
	if err != nil {
		return nil, nil, err
	}
	sketches := make([]*ExponentialHistogram, 0, tickCount)
	var prev *ExponentialHistogram
	for t := 0; t < tickCount; t++ {
		marker, err := r.byteVal()
		if err != nil {
			return nil, nil, err
		}
		switch marker {
		case tickFull:
			sk, n, err := DecodeSketch(r.b[r.off:])
			if err != nil {
				return nil, nil, err
			}
			r.off += n
			sketches = append(sketches, sk)
			prev = sk
		case tickDelta:
			if prev == nil {
				return nil, nil, errors.New("histogram: delta tick without predecessor")
			}
			scale, err := r.varint()
			if err != nil {
				return nil, nil, err
			}
			sk := &ExponentialHistogram{Scale: int32(scale), ZeroThreshold: prev.ZeroThreshold}
			if err := readExpScalars(&r, sk); err != nil {
				return nil, nil, err
			}
			pos, err := applyBucketDeltas(&r, prev.Positive)
			if err != nil {
				return nil, nil, err
			}
			neg, err := applyBucketDeltas(&r, prev.Negative)
			if err != nil {
				return nil, nil, err
			}
			sk.Positive, sk.Negative = pos, neg
			sketches = append(sketches, sk)
			prev = sk
		default:
			return nil, nil, errors.New("histogram: bad tick marker")
		}
	}
	return ts, sketches, nil
}

func appendExpScalars(dst []byte, sk *ExponentialHistogram) []byte {
	var flags byte
	if sk.HasMinMax {
		flags |= flagHasMinMax
	}
	dst = append(dst, flags)
	dst = appendUvarint(dst, sk.ZeroCount)
	dst = appendUvarint(dst, sk.Count)
	dst = appendFloat(dst, sk.Sum)
	if sk.HasMinMax {
		dst = appendFloat(dst, sk.Min)
		dst = appendFloat(dst, sk.Max)
	}
	return dst
}

func readExpScalars(r *reader, sk *ExponentialHistogram) error {
	flags, err := r.byteVal()
	if err != nil {
		return err
	}
	sk.HasMinMax = flags&flagHasMinMax != 0
	if sk.ZeroCount, err = r.uvarint(); err != nil {
		return err
	}
	if sk.Count, err = r.uvarint(); err != nil {
		return err
	}
	if sk.Sum, err = r.float(); err != nil {
		return err
	}
	if sk.HasMinMax {
		if sk.Min, err = r.float(); err != nil {
			return err
		}
		if sk.Max, err = r.float(); err != nil {
			return err
		}
	}
	return nil
}

// bucketValue returns the count at an absolute index (0 if outside the slice).
func bucketValue(b Buckets, index int32) uint64 {
	p := int(index - b.Offset)
	if p < 0 || p >= len(b.Counts) {
		return 0
	}
	return b.Counts[p]
}

// appendBucketDeltas writes the buckets whose count differs between prev and cur,
// as (indexDelta, countDelta) signed-varint pairs in ascending index order.
func appendBucketDeltas(dst []byte, prev, cur Buckets) []byte {
	idxSet := make(map[int32]struct{})
	for i, c := range prev.Counts {
		if c != 0 {
			idxSet[prev.Offset+int32(i)] = struct{}{}
		}
	}
	for i, c := range cur.Counts {
		if c != 0 {
			idxSet[cur.Offset+int32(i)] = struct{}{}
		}
	}
	indices := make([]int32, 0, len(idxSet))
	for idx := range idxSet {
		if bucketValue(prev, idx) != bucketValue(cur, idx) {
			indices = append(indices, idx)
		}
	}
	sort.Slice(indices, func(i, j int) bool { return indices[i] < indices[j] })

	dst = appendUvarint(dst, uint64(len(indices)))
	var lastIdx int32
	for k, idx := range indices {
		if k == 0 {
			dst = appendVarint(dst, int64(idx))
		} else {
			dst = appendVarint(dst, int64(idx-lastIdx))
		}
		lastIdx = idx
		delta := int64(bucketValue(cur, idx)) - int64(bucketValue(prev, idx))
		dst = appendVarint(dst, delta)
	}
	return dst
}

// applyBucketDeltas reconstructs cur buckets by applying stored deltas onto prev.
func applyBucketDeltas(r *reader, prev Buckets) (Buckets, error) {
	n, err := r.uvarint()
	if err != nil {
		return Buckets{}, err
	}
	// Start from a copy of prev's nonzero buckets.
	merged := make(map[int32]int64)
	for i, c := range prev.Counts {
		if c != 0 {
			merged[prev.Offset+int32(i)] = int64(c)
		}
	}
	var lastIdx int32
	for k := uint64(0); k < n; k++ {
		raw, err := r.varint()
		if err != nil {
			return Buckets{}, err
		}
		var idx int32
		if k == 0 {
			idx = int32(raw)
		} else {
			idx = lastIdx + int32(raw)
		}
		lastIdx = idx
		delta, err := r.varint()
		if err != nil {
			return Buckets{}, err
		}
		merged[idx] += delta
	}
	return bucketsFromCountMap(merged), nil
}

func bucketsFromCountMap(m map[int32]int64) Buckets {
	var minIdx, maxIdx int32
	first := true
	for idx, c := range m {
		if c == 0 {
			continue
		}
		if first {
			minIdx, maxIdx, first = idx, idx, false
		} else {
			if idx < minIdx {
				minIdx = idx
			}
			if idx > maxIdx {
				maxIdx = idx
			}
		}
	}
	if first {
		return Buckets{}
	}
	out := Buckets{Offset: minIdx, Counts: make([]uint64, maxIdx-minIdx+1)}
	for idx, c := range m {
		if c > 0 {
			out.Counts[idx-minIdx] = uint64(c)
		}
	}
	return out
}

// --- explicit ---

func encodeExplicitPayload(s ExplicitSeries) []byte {
	var dst []byte
	dst = appendTimestamps(dst, s.Timestamps)
	for _, b := range s.Buckets {
		if b == nil {
			b = &ExplicitBucketHistogram{}
		}
		dst = appendExplicitScalars(dst, b)
		dst = appendSparseCounts(dst, b.Counts)
	}
	return dst
}

func decodeExplicitPayload(b []byte, tickCount int, bounds []float64) ([]int64, []*ExplicitBucketHistogram, error) {
	r := reader{b: b}
	ts, err := readTimestamps(&r, tickCount)
	if err != nil {
		return nil, nil, err
	}
	width := len(bounds) + 1
	out := make([]*ExplicitBucketHistogram, 0, tickCount)
	for t := 0; t < tickCount; t++ {
		h := &ExplicitBucketHistogram{Bounds: bounds}
		if err := readExplicitScalars(&r, h); err != nil {
			return nil, nil, err
		}
		counts, err := readSparseCounts(&r, width)
		if err != nil {
			return nil, nil, err
		}
		h.Counts = counts
		out = append(out, h)
	}
	return ts, out, nil
}

func appendExplicitScalars(dst []byte, h *ExplicitBucketHistogram) []byte {
	var flags byte
	if h.HasMinMax {
		flags |= flagHasMinMax
	}
	dst = append(dst, flags)
	dst = appendUvarint(dst, h.Count)
	dst = appendFloat(dst, h.Sum)
	if h.HasMinMax {
		dst = appendFloat(dst, h.Min)
		dst = appendFloat(dst, h.Max)
	}
	return dst
}

func readExplicitScalars(r *reader, h *ExplicitBucketHistogram) error {
	flags, err := r.byteVal()
	if err != nil {
		return err
	}
	h.HasMinMax = flags&flagHasMinMax != 0
	if h.Count, err = r.uvarint(); err != nil {
		return err
	}
	if h.Sum, err = r.float(); err != nil {
		return err
	}
	if h.HasMinMax {
		if h.Min, err = r.float(); err != nil {
			return err
		}
		if h.Max, err = r.float(); err != nil {
			return err
		}
	}
	return nil
}

// appendSparseCounts writes a count vector exploiting zero-run sparsity: only
// nonzero buckets are emitted as (positionDelta, count) pairs.
func appendSparseCounts(dst []byte, counts []uint64) []byte {
	nonzero := 0
	for _, c := range counts {
		if c != 0 {
			nonzero++
		}
	}
	dst = appendUvarint(dst, uint64(nonzero))
	last := -1
	for i, c := range counts {
		if c == 0 {
			continue
		}
		dst = appendUvarint(dst, uint64(i-last))
		last = i
		dst = appendUvarint(dst, c)
	}
	return dst
}

func readSparseCounts(r *reader, width int) ([]uint64, error) {
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	counts := make([]uint64, width)
	last := -1
	for k := uint64(0); k < n; k++ {
		gap, err := r.uvarint()
		if err != nil {
			return nil, err
		}
		pos := last + int(gap)
		if pos < 0 || pos >= width {
			return nil, errors.New("histogram: sparse count position out of range")
		}
		last = pos
		if counts[pos], err = r.uvarint(); err != nil {
			return nil, err
		}
	}
	return counts, nil
}

// --- timestamps ---

func appendTimestamps(dst []byte, ts []int64) []byte {
	if len(ts) == 0 {
		return dst
	}
	dst = appendVarint(dst, ts[0])
	for i := 1; i < len(ts); i++ {
		dst = appendVarint(dst, ts[i]-ts[i-1])
	}
	return dst
}

func readTimestamps(r *reader, n int) ([]int64, error) {
	if n == 0 {
		return nil, nil
	}
	out := make([]int64, n)
	v, err := r.varint()
	if err != nil {
		return nil, err
	}
	out[0] = v
	for i := 1; i < n; i++ {
		d, err := r.varint()
		if err != nil {
			return nil, err
		}
		out[i] = out[i-1] + d
	}
	return out, nil
}
