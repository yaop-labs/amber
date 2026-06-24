package histogram

import (
	"errors"
	"sort"
)

// Per-series payload encoding for the sketch section.
// The first tick stores a full sketch. Later ticks store bucket deltas and
// scalar fields unless scale or zero threshold changes.

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
			dst = appendVarint(dst, int64(sk.Scale))
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
	var sc decodeScratch
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
			pos, err := applyBucketDeltas(&r, prev.Positive, &sc)
			if err != nil {
				return nil, nil, err
			}
			neg, err := applyBucketDeltas(&r, prev.Negative, &sc)
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

// decodeScratch holds reusable buffers for applyBucketDeltas so a series' tick
// decode loop allocates only the retained output bucket arrays, not the
// transient index/delta/work buffers each tick. Not safe for concurrent use; the
// tick loop calls applyBucketDeltas sequentially.
type decodeScratch struct {
	idxs   []int32
	deltas []int64
	work   []int64
}

func (s *decodeScratch) reset(n int) {
	if cap(s.idxs) < n {
		s.idxs = make([]int32, n)
		s.deltas = make([]int64, n)
	} else {
		s.idxs = s.idxs[:n]
		s.deltas = s.deltas[:n]
	}
}

func (s *decodeScratch) workspace(n int) []int64 {
	if cap(s.work) < n {
		s.work = make([]int64, n)
	} else {
		s.work = s.work[:n]
		clear(s.work)
	}
	return s.work
}

// applyBucketDeltas reconstructs cur buckets by applying stored deltas onto prev.
// The deltas are stored as (indexDelta, countDelta) pairs sorted by ascending
// absolute index, so the reconstruction copies prev's dense counts and applies
// the sparse deltas over the union index range - no per-tick count map (the old
// map build dominated the quantile decode's allocations and CPU). Transient
// buffers come from sc; only the returned Counts array is freshly allocated.
func applyBucketDeltas(r *reader, prev Buckets, sc *decodeScratch) (Buckets, error) {
	n64, err := r.uvarint()
	if err != nil {
		return Buckets{}, err
	}
	n := int(n64)
	sc.reset(n)
	idxs, deltas := sc.idxs, sc.deltas
	var lastIdx int32
	for k := 0; k < n; k++ {
		raw, err := r.varint()
		if err != nil {
			return Buckets{}, err
		}
		idx := int32(raw)
		if k != 0 {
			idx += lastIdx
		}
		lastIdx = idx
		delta, err := r.varint()
		if err != nil {
			return Buckets{}, err
		}
		idxs[k] = idx
		deltas[k] = delta
	}

	// Union index range of prev's dense array and the delta indices.
	lo, hi, has := int32(0), int32(0), false
	if len(prev.Counts) > 0 {
		lo, hi, has = prev.Offset, prev.Offset+int32(len(prev.Counts))-1, true
	}
	if n > 0 {
		if !has {
			lo, hi, has = idxs[0], idxs[n-1], true
		} else {
			if idxs[0] < lo {
				lo = idxs[0]
			}
			if idxs[n-1] > hi {
				hi = idxs[n-1]
			}
		}
	}
	if !has {
		return Buckets{}, nil
	}

	work := sc.workspace(int(hi - lo + 1))
	for i, c := range prev.Counts {
		work[prev.Offset+int32(i)-lo] = int64(c)
	}
	for k := range idxs {
		work[idxs[k]-lo] += deltas[k]
	}

	// Trim to the positive-count range; interior non-positive buckets become 0.
	start := 0
	for start < len(work) && work[start] <= 0 {
		start++
	}
	if start == len(work) {
		return Buckets{}, nil
	}
	end := len(work) - 1
	for work[end] <= 0 {
		end--
	}
	out := Buckets{Offset: lo + int32(start), Counts: make([]uint64, end-start+1)}
	for i := start; i <= end; i++ {
		if work[i] > 0 {
			out.Counts[i-start] = uint64(work[i])
		}
	}
	return out, nil
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

// EncodeExplicitTick encodes one explicit-bucket histogram self-contained
// (bounds included) for the WAL sketch record.
func EncodeExplicitTick(h *ExplicitBucketHistogram) []byte {
	dst := appendUvarint(nil, uint64(len(h.Bounds)))
	for _, b := range h.Bounds {
		dst = appendFloat(dst, b)
	}
	dst = appendExplicitScalars(dst, h)
	dst = appendSparseCounts(dst, h.Counts)
	return dst
}

// DecodeExplicitTick decodes one EncodeExplicitTick payload.
func DecodeExplicitTick(b []byte) (*ExplicitBucketHistogram, error) {
	r := reader{b: b}
	n, err := r.uvarint()
	if err != nil {
		return nil, err
	}
	if n > uint64(len(b)) {
		return nil, errors.New("histogram: explicit tick bounds count out of range")
	}
	bounds := make([]float64, n)
	for i := range bounds {
		bounds[i], err = r.float()
		if err != nil {
			return nil, err
		}
	}
	h := &ExplicitBucketHistogram{Bounds: bounds}
	if err := readExplicitScalars(&r, h); err != nil {
		return nil, err
	}
	counts, err := readSparseCounts(&r, len(bounds)+1)
	if err != nil {
		return nil, err
	}
	h.Counts = counts
	return h, nil
}
