// Package indexlab contains static interval-index experiments for Amber's
// segment time ranges. It deliberately lives outside the production query
// path: experiments compare alternatives without changing SparseIndex.
package indexlab

import (
	"math"
	"sort"
	"sync/atomic"
	"unsafe"

	"github.com/yaop-labs/amber/internal/index"
)

// Query is a closed event-time interval [From, To].
type Query struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

// LookupStats describes the amount of interval work before exact filtering.
// Scanned is the number of SegmentTimeRange values inspected. Overfetch is
// Scanned minus the number of true overlaps returned.
type LookupStats struct {
	Scanned   int `json:"scanned"`
	Overfetch int `json:"overfetch"`
}

// IntervalIndex is the shared contract for the experiment.
type IntervalIndex interface {
	Name() string
	Lookup(Query) ([]index.SegmentTimeRange, LookupStats)
	// AuxBytes excludes the common SegmentTimeRange array and counts only
	// algorithm-specific metadata.
	AuxBytes() int64
	// ModelCount is zero for non-learned indexes.
	ModelCount() int
}

// BaseBytes estimates the common in-memory range array. Generated experiment
// ranges use empty FileName strings, so there is no separately allocated
// string payload to count.
func BaseBytes(segmentCount int) int64 {
	return int64(segmentCount) * int64(unsafe.Sizeof(index.SegmentTimeRange{}))
}

// LinearIndex mirrors SparseIndex.Lookup's O(number of segments) scan and
// result ordering. It intentionally keeps input order and sorts every result.
type LinearIndex struct {
	ranges []index.SegmentTimeRange
}

func NewLinear(ranges []index.SegmentTimeRange) *LinearIndex {
	return &LinearIndex{ranges: append([]index.SegmentTimeRange(nil), ranges...)}
}

func (*LinearIndex) Name() string    { return "linear" }
func (*LinearIndex) AuxBytes() int64 { return 0 }
func (*LinearIndex) ModelCount() int { return 0 }

func (l *LinearIndex) Lookup(q Query) ([]index.SegmentTimeRange, LookupStats) {
	if q.From > q.To {
		return nil, LookupStats{}
	}
	result := make([]index.SegmentTimeRange, 0)
	for _, r := range l.ranges {
		if overlaps(r, q) {
			result = append(result, r)
		}
	}
	sortRanges(result)
	return result, LookupStats{
		Scanned:   len(l.ranges),
		Overfetch: len(l.ranges) - len(result),
	}
}

// BinaryIntervalIndex sorts intervals by MinTS and stores a monotonic prefix
// maximum of MaxTS. Two exact binary searches find a safe candidate window:
//
//   - first prefixMax >= query.From
//   - first MinTS > query.To
//
// Intervals inside the window still receive an exact overlap check. A very
// wide late-arrival interval can widen the candidate window, but cannot cause
// a false negative.
type BinaryIntervalIndex struct {
	ranges    []index.SegmentTimeRange
	minKeys   []int64
	prefixMax []int64
}

func NewBinary(ranges []index.SegmentTimeRange) *BinaryIntervalIndex {
	sorted := append([]index.SegmentTimeRange(nil), ranges...)
	sortRanges(sorted)
	minKeys := make([]int64, len(sorted))
	prefixMax := make([]int64, len(sorted))
	var maxTS int64
	for i, r := range sorted {
		minKeys[i] = r.MinTS
		if i == 0 || r.MaxTS > maxTS {
			maxTS = r.MaxTS
		}
		prefixMax[i] = maxTS
	}
	return &BinaryIntervalIndex{
		ranges:    sorted,
		minKeys:   minKeys,
		prefixMax: prefixMax,
	}
}

func (*BinaryIntervalIndex) Name() string    { return "binary_interval" }
func (*BinaryIntervalIndex) ModelCount() int { return 0 }
func (b *BinaryIntervalIndex) AuxBytes() int64 {
	return int64(len(b.minKeys)+len(b.prefixMax)) * int64(unsafe.Sizeof(int64(0)))
}

func (b *BinaryIntervalIndex) Lookup(q Query) ([]index.SegmentTimeRange, LookupStats) {
	if q.From > q.To || len(b.ranges) == 0 {
		return nil, LookupStats{}
	}
	lo := lowerBound(b.prefixMax, q.From)
	hi := upperBound(b.minKeys, q.To)
	return filterWindow(b.ranges, q, lo, hi)
}

// PiecewiseLinearIndex uses bounded piecewise-linear rank models for the same
// two lower-bound operations as BinaryIntervalIndex. It is PGM/RadixSpline
// inspired, not a reference implementation of either paper:
//
//   - models approximate the CDF rank within fixed-size rank partitions;
//   - construction measures a conservative error corridor over every key run;
//   - lookup searches only that corridor, then verifies the exact lower bound;
//   - an invariant check falls back to global binary search if the corridor is
//     ever insufficient, preserving the no-false-negative contract.
//
// The exact interval filter is identical to BinaryIntervalIndex.
type PiecewiseLinearIndex struct {
	ranges          []index.SegmentTimeRange
	minKeys         []int64
	prefixMax       []int64
	minRank         rankIndex
	prefixMaxRank   rankIndex
	fallbackCounter atomic.Uint64
}

func NewPiecewiseLinear(ranges []index.SegmentTimeRange, pointsPerModel int) *PiecewiseLinearIndex {
	if pointsPerModel <= 0 {
		pointsPerModel = 256
	}
	binary := NewBinary(ranges)
	return &PiecewiseLinearIndex{
		ranges:        binary.ranges,
		minKeys:       binary.minKeys,
		prefixMax:     binary.prefixMax,
		minRank:       newRankIndex(binary.minKeys, pointsPerModel),
		prefixMaxRank: newRankIndex(binary.prefixMax, pointsPerModel),
	}
}

func (*PiecewiseLinearIndex) Name() string { return "piecewise_linear" }
func (p *PiecewiseLinearIndex) ModelCount() int {
	return len(p.minRank.models) + len(p.prefixMaxRank.models)
}
func (p *PiecewiseLinearIndex) AuxBytes() int64 {
	keys := int64(len(p.minKeys)+len(p.prefixMax)) * int64(unsafe.Sizeof(int64(0)))
	models := int64(p.ModelCount()) * int64(unsafe.Sizeof(rankSegment{}))
	return keys + models
}

func (p *PiecewiseLinearIndex) Lookup(q Query) ([]index.SegmentTimeRange, LookupStats) {
	if q.From > q.To || len(p.ranges) == 0 {
		return nil, LookupStats{}
	}
	lo, fallback := p.prefixMaxRank.lowerBound(q.From)
	if fallback {
		p.fallbackCounter.Add(1)
	}
	hi, fallback := p.minRank.upperBound(q.To)
	if fallback {
		p.fallbackCounter.Add(1)
	}
	return filterWindow(p.ranges, q, lo, hi)
}

// Fallbacks reports how many learned corridors failed their invariant check.
// A correct build should report zero; the exact fallback exists as a safety
// boundary for future dataset shapes.
func (p *PiecewiseLinearIndex) Fallbacks() uint64 { return p.fallbackCounter.Load() }

type rankIndex struct {
	keys   []int64
	models []rankSegment
}

// rankSegment predicts lower_bound(key) as StartRank + Slope*(key-FirstKey).
// MinDelta/MaxDelta bound trueRank-prediction across the stepwise CDF.
type rankSegment struct {
	FirstKey int64
	LastKey  int64
	Start    int
	End      int
	Slope    float64
	MinDelta float64
	MaxDelta float64
}

func newRankIndex(keys []int64, pointsPerModel int) rankIndex {
	r := rankIndex{keys: keys}
	if len(keys) == 0 {
		return r
	}
	for start := 0; start < len(keys); {
		end := min(start+pointsPerModel, len(keys))
		// Never split a duplicate-key run across models. This makes the first
		// model whose LastKey >= x own the global lower_bound(x).
		for end < len(keys) && keys[end] == keys[end-1] {
			end++
		}
		r.models = append(r.models, fitRankSegment(keys, start, end))
		start = end
	}
	return r
}

func fitRankSegment(keys []int64, start, end int) rankSegment {
	firstKey := keys[start]
	lastKey := keys[end-1]
	lastRank := start + sort.Search(end-start, func(i int) bool {
		return keys[start+i] >= lastKey
	})

	slope := 0.0
	if lastKey != firstKey {
		slope = float64(lastRank-start) / (float64(lastKey) - float64(firstKey))
	}
	m := rankSegment{
		FirstKey: firstKey,
		LastKey:  lastKey,
		Start:    start,
		End:      end,
		Slope:    slope,
		MinDelta: math.Inf(1),
		MaxDelta: math.Inf(-1),
	}

	for groupStart := start; groupStart < end; {
		groupEnd := groupStart + 1
		for groupEnd < end && keys[groupEnd] == keys[groupStart] {
			groupEnd++
		}
		m.observe(keys[groupStart], groupStart)
		if groupEnd < end && keys[groupStart] < math.MaxInt64 {
			// For x immediately after this key, lower_bound jumps to the next
			// key run. The prediction is linear between keys, so observing the
			// two interval endpoints bounds the entire gap.
			m.observe(keys[groupStart]+1, groupEnd)
			if keys[groupEnd] > keys[groupStart]+1 {
				m.observe(keys[groupEnd]-1, groupEnd)
			}
		}
		groupStart = groupEnd
	}
	if math.IsInf(m.MinDelta, 1) {
		m.MinDelta, m.MaxDelta = 0, 0
	}
	return m
}

func (m *rankSegment) observe(key int64, trueRank int) {
	predicted := m.predict(key)
	delta := float64(trueRank) - predicted
	if delta < m.MinDelta {
		m.MinDelta = delta
	}
	if delta > m.MaxDelta {
		m.MaxDelta = delta
	}
}

func (m rankSegment) predict(key int64) float64 {
	return float64(m.Start) + m.Slope*(float64(key)-float64(m.FirstKey))
}

func (r rankIndex) lowerBound(key int64) (int, bool) {
	if len(r.keys) == 0 {
		return 0, false
	}
	modelPos := sort.Search(len(r.models), func(i int) bool {
		return r.models[i].LastKey >= key
	})
	if modelPos == len(r.models) {
		return len(r.keys), false
	}
	m := r.models[modelPos]
	if key < m.FirstKey {
		return m.Start, false
	}

	predicted := m.predict(key)
	lo := max(m.Start, int(math.Floor(predicted+m.MinDelta))-2)
	hi := max(min(m.End, int(math.Ceil(predicted+m.MaxDelta))+3), lo)
	pos := lo + sort.Search(hi-lo, func(i int) bool {
		return r.keys[lo+i] >= key
	})
	if validLowerBound(r.keys, pos, key) {
		return pos, false
	}
	return lowerBound(r.keys, key), true
}

func (r rankIndex) upperBound(key int64) (int, bool) {
	if key == math.MaxInt64 {
		return len(r.keys), false
	}
	return r.lowerBound(key + 1)
}

func validLowerBound(keys []int64, pos int, key int64) bool {
	if pos < 0 || pos > len(keys) {
		return false
	}
	if pos > 0 && keys[pos-1] >= key {
		return false
	}
	return pos == len(keys) || keys[pos] >= key
}

func lowerBound(keys []int64, key int64) int {
	return sort.Search(len(keys), func(i int) bool { return keys[i] >= key })
}

func upperBound(keys []int64, key int64) int {
	return sort.Search(len(keys), func(i int) bool { return keys[i] > key })
}

func filterWindow(ranges []index.SegmentTimeRange, q Query, lo, hi int) ([]index.SegmentTimeRange, LookupStats) {
	lo = max(0, min(lo, len(ranges)))
	hi = max(0, min(hi, len(ranges)))
	if lo > hi {
		lo = hi
	}
	result := make([]index.SegmentTimeRange, 0, hi-lo)
	for _, r := range ranges[lo:hi] {
		if overlaps(r, q) {
			result = append(result, r)
		}
	}
	return result, LookupStats{
		Scanned:   hi - lo,
		Overfetch: hi - lo - len(result),
	}
}

func overlaps(r index.SegmentTimeRange, q Query) bool {
	return r.MaxTS >= q.From && r.MinTS <= q.To
}

func sortRanges(ranges []index.SegmentTimeRange) {
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].MinTS == ranges[j].MinTS {
			return ranges[i].SegmentID < ranges[j].SegmentID
		}
		return ranges[i].MinTS < ranges[j].MinTS
	})
}
