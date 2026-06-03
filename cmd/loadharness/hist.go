package main

import (
	"math"
	"sort"
	"sync"
)

// latencyHist is a log-spaced histogram for nanosecond durations. We pick
// concrete bucket edges from 1 µs to 60 s, ~6 buckets/decade. That's coarse
// vs HDRHistogram but enough for p50/p99/p999/max reporting at the precision
// the load harness needs (a few percent on quantile values). The hot path
// inserts via a binary search on a sorted edges slice — no allocations.
//
// All operations are lock-free using atomics for the counts; the bucket
// search is purely arithmetic. We avoid contention by sharding internally:
// each call to Observe hashes by goroutine-id into one of N shards. Reads
// merge shards into a single snapshot.

const (
	// 1 µs to 60 s, exponential. Edges generated lazily on first use.
	latencyMinNs = 1_000     // 1 µs
	latencyMaxNs = 60_000_000_000 // 60 s
	bucketsPerDecade = 6
)

type latencyHist struct {
	edges    []int64
	shards   []*latencyShard
	shardMod uint64
}

type latencyShard struct {
	mu     sync.Mutex
	counts []uint64
	sumNs  uint64
	maxNs  uint64
	count  uint64
}

func newLatencyHist(shards int) *latencyHist {
	if shards <= 0 {
		shards = 1
	}
	// Compute power-of-two shard count for cheap modulo.
	n := 1
	for n < shards {
		n <<= 1
	}
	shards = n

	edges := makeLogEdges(latencyMinNs, latencyMaxNs, bucketsPerDecade)
	h := &latencyHist{
		edges:    edges,
		shardMod: uint64(shards - 1),
		shards:   make([]*latencyShard, shards),
	}
	for i := range h.shards {
		h.shards[i] = &latencyShard{counts: make([]uint64, len(edges)+1)}
	}
	return h
}

func makeLogEdges(minNs, maxNs int64, perDecade int) []int64 {
	logMin := math.Log10(float64(minNs))
	logMax := math.Log10(float64(maxNs))
	steps := int((logMax-logMin)*float64(perDecade) + 0.5)
	out := make([]int64, 0, steps+1)
	for i := 0; i <= steps; i++ {
		v := math.Pow(10, logMin+float64(i)/float64(perDecade))
		out = append(out, int64(v))
	}
	return out
}

// Observe records a latency in nanoseconds. shardKey distributes inserts
// across shards to reduce contention; pass anything stable per-goroutine
// (e.g. a counter the generator owns). The caller's responsibility to keep
// shardKey within range; we mask.
func (h *latencyHist) Observe(ns int64, shardKey uint64) {
	if ns < 0 {
		return
	}
	s := h.shards[shardKey&h.shardMod]
	idx := sort.Search(len(h.edges), func(i int) bool { return h.edges[i] > ns })
	s.mu.Lock()
	s.counts[idx]++
	s.count++
	s.sumNs += uint64(ns)
	if uint64(ns) > s.maxNs {
		s.maxNs = uint64(ns)
	}
	s.mu.Unlock()
}

// Snapshot returns a merged copy of all shards. Safe to call while inserts
// continue (snapshot reflects state at the moment each shard was read).
func (h *latencyHist) Snapshot() latencySnapshot {
	merged := make([]uint64, len(h.edges)+1)
	var total, sum, max uint64
	for _, s := range h.shards {
		s.mu.Lock()
		for i, c := range s.counts {
			merged[i] += c
		}
		total += s.count
		sum += s.sumNs
		if s.maxNs > max {
			max = s.maxNs
		}
		s.mu.Unlock()
	}
	return latencySnapshot{
		edges:  h.edges,
		counts: merged,
		total:  total,
		sumNs:  sum,
		maxNs:  max,
	}
}

type latencySnapshot struct {
	edges  []int64
	counts []uint64
	total  uint64
	sumNs  uint64
	maxNs  uint64
}

// Quantile returns the nanosecond value at quantile q in [0,1]. Linear-
// interpolated within the bucket; bucket boundaries are exact, intra-bucket
// is uniformly assumed.
func (s latencySnapshot) Quantile(q float64) int64 {
	if s.total == 0 {
		return 0
	}
	if q <= 0 {
		return 0
	}
	if q >= 1 {
		return int64(s.maxNs)
	}
	target := uint64(float64(s.total) * q)
	var cum uint64
	for i, c := range s.counts {
		if cum+c >= target {
			lo := int64(0)
			if i > 0 {
				lo = s.edges[i-1]
			}
			hi := int64(s.maxNs)
			if i < len(s.edges) {
				hi = s.edges[i]
			}
			// Linear interpolation inside the bucket.
			into := float64(target-cum) / float64(c)
			return lo + int64(float64(hi-lo)*into)
		}
		cum += c
	}
	return int64(s.maxNs)
}

func (s latencySnapshot) Count() uint64 { return s.total }
func (s latencySnapshot) Mean() float64 {
	if s.total == 0 {
		return 0
	}
	return float64(s.sumNs) / float64(s.total)
}
func (s latencySnapshot) Max() int64 { return int64(s.maxNs) }
