package indexlab

import (
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/yaop-labs/amber/internal/index"
)

const (
	DatasetMonotonic  = "monotonic"
	DatasetBursty     = "bursty"
	DatasetGaps       = "gaps"
	DatasetLate       = "late_arrivals"
	DatasetOutOfOrder = "out_of_order"
)

var DatasetNames = []string{
	DatasetMonotonic,
	DatasetBursty,
	DatasetGaps,
	DatasetLate,
	DatasetOutOfOrder,
}

const (
	datasetBase = int64(1_700_000_000_000_000_000)
	stepNS      = int64(1_000_000) // one millisecond
)

// GenerateDataset returns deterministic segment ranges in their insertion
// order. Static indexes may sort their own copy during construction.
func GenerateDataset(name string, count int, seed uint64) ([]index.SegmentTimeRange, error) {
	if count < 0 {
		return nil, fmt.Errorf("indexlab: negative segment count %d", count)
	}
	// #nosec G404
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	ranges := make([]index.SegmentTimeRange, count)

	switch name {
	case DatasetMonotonic:
		for i := range ranges {
			minTS := datasetBase + int64(i)*stepNS
			ranges[i] = makeRange(i, minTS, minTS+stepNS-1)
		}

	case DatasetBursty:
		cursor := datasetBase
		for i := range ranges {
			if i > 0 {
				switch {
				case i%2048 == 0:
					cursor += 5_000 * stepNS
				case i%64 == 0:
					cursor += 100 * stepNS
				default:
					cursor += int64(1+rng.IntN(4)) * (stepNS / 16)
				}
			}
			width := int64(1+rng.IntN(32)) * (stepNS / 8)
			ranges[i] = makeRange(i, cursor, cursor+width)
		}

	case DatasetGaps:
		cursor := datasetBase
		for i := range ranges {
			if i > 0 {
				cursor += stepNS
				if i%512 == 0 {
					cursor += 30_000 * stepNS
				}
			}
			ranges[i] = makeRange(i, cursor, cursor+stepNS-1)
		}

	case DatasetLate:
		for i := range ranges {
			now := datasetBase + int64(i)*stepNS
			minTS := now
			maxTS := now + stepNS - 1
			if i > 0 && i%97 == 0 {
				delay := int64(1_000+i%25_000) * stepNS
				minTS -= delay
				// A late segment spans old event time through current ingest
				// time, producing interval overlap and prefix-max widening.
				maxTS += int64(1+rng.IntN(8)) * stepNS
			}
			ranges[i] = makeRange(i, minTS, maxTS)
		}

	case DatasetOutOfOrder:
		for i := range ranges {
			minTS := datasetBase + int64(i)*stepNS
			ranges[i] = makeRange(i, minTS, minTS+stepNS-1)
		}
		rng.Shuffle(len(ranges), func(i, j int) {
			ranges[i], ranges[j] = ranges[j], ranges[i]
		})

	default:
		return nil, fmt.Errorf("indexlab: unknown dataset %q", name)
	}
	return ranges, nil
}

func makeRange(i int, minTS, maxTS int64) index.SegmentTimeRange {
	return index.SegmentTimeRange{
		SegmentID: uint32(i + 1),
		MinTS:     minTS,
		MaxTS:     maxTS,
	}
}

// GenerateQueries creates a deterministic mix:
//
//   - 60% point/narrow hit queries;
//   - 25% wider range queries;
//   - 10% queries likely to land between segment starts;
//   - 5% misses outside the dataset.
func GenerateQueries(ranges []index.SegmentTimeRange, count int, seed uint64) []Query {
	if count <= 0 || len(ranges) == 0 {
		return nil
	}
	sorted := append([]index.SegmentTimeRange(nil), ranges...)
	sortRanges(sorted)
	// #nosec G404
	rng := rand.New(rand.NewPCG(seed, seed^0xd1b54a32d192ed03))
	queries := make([]Query, count)

	for i := range queries {
		switch bucket := i % 20; {
		case bucket < 12:
			r := sorted[rng.IntN(len(sorted))]
			center := r.MinTS + (r.MaxTS-r.MinTS)/2
			queries[i] = Query{From: center, To: center}

		case bucket < 17:
			start := rng.IntN(len(sorted))
			end := min(start+1+rng.IntN(32), len(sorted)-1)
			queries[i] = Query{
				From: sorted[start].MinTS,
				To:   sorted[end].MaxTS,
			}

		case bucket < 19:
			pos := rng.IntN(len(sorted))
			from := sorted[pos].MaxTS + 1
			queries[i] = Query{From: from, To: from + stepNS/2}

		default:
			if i%2 == 0 {
				queries[i] = Query{
					From: sorted[0].MinTS - 10_000*stepNS,
					To:   sorted[0].MinTS - 9_000*stepNS,
				}
			} else {
				last := sorted[len(sorted)-1]
				queries[i] = Query{
					From: last.MaxTS + 9_000*stepNS,
					To:   last.MaxTS + 10_000*stepNS,
				}
			}
		}
	}
	return queries
}

// SortedSegmentIDs returns a canonical result identity for correctness gates.
func SortedSegmentIDs(ranges []index.SegmentTimeRange) []uint32 {
	ids := make([]uint32, len(ranges))
	for i, r := range ranges {
		ids[i] = r.SegmentID
	}
	slices.Sort(ids)
	return ids
}
