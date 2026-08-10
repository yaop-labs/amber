package indexlab

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/yaop-labs/amber/internal/index"
)

func TestIndexesMatchLinearOracle(t *testing.T) {
	t.Parallel()

	for _, dataset := range DatasetNames {
		dataset := dataset
		t.Run(dataset, func(t *testing.T) {
			t.Parallel()

			ranges, err := GenerateDataset(dataset, 10_000, 42)
			if err != nil {
				t.Fatal(err)
			}
			queries := GenerateQueries(ranges, 4096, 84)
			assertIndexesMatch(t, ranges, queries)
		})
	}
}

func TestIndexesMatchOracleWithDuplicateStarts(t *testing.T) {
	t.Parallel()

	ranges := []index.SegmentTimeRange{
		{SegmentID: 1, MinTS: 10, MaxTS: 20},
		{SegmentID: 2, MinTS: 10, MaxTS: 12},
		{SegmentID: 3, MinTS: 10, MaxTS: 40},
		{SegmentID: 4, MinTS: 20, MaxTS: 30},
		{SegmentID: 5, MinTS: 50, MaxTS: 60},
		{SegmentID: 6, MinTS: 50, MaxTS: 55},
	}
	queries := []Query{
		{From: 9, To: 9},
		{From: 10, To: 10},
		{From: 13, To: 19},
		{From: 20, To: 20},
		{From: 31, To: 39},
		{From: 40, To: 50},
		{From: 61, To: 61},
	}
	assertIndexesMatch(t, ranges, queries)
}

func TestIndexesMatchOracleAtInt64Boundaries(t *testing.T) {
	t.Parallel()

	ranges := []index.SegmentTimeRange{
		{SegmentID: 1, MinTS: math.MinInt64, MaxTS: math.MinInt64 + 10},
		{SegmentID: 2, MinTS: -1, MaxTS: 1},
		{SegmentID: 3, MinTS: math.MaxInt64 - 10, MaxTS: math.MaxInt64},
	}
	queries := []Query{
		{From: math.MinInt64, To: math.MinInt64},
		{From: 0, To: 0},
		{From: math.MaxInt64, To: math.MaxInt64},
		{From: math.MinInt64, To: math.MaxInt64},
		{From: 1, To: -1},
	}
	assertIndexesMatch(t, ranges, queries)
}

func TestRankIndexMatchesExactSearch(t *testing.T) {
	t.Parallel()

	for _, dataset := range DatasetNames {
		t.Run(dataset, func(t *testing.T) {
			t.Parallel()

			ranges, err := GenerateDataset(dataset, 20_000, 123)
			if err != nil {
				t.Fatal(err)
			}
			binary := NewBinary(ranges)
			assertRankIndex(t, binary.minKeys)
			assertRankIndex(t, binary.prefixMax)
		})
	}
}

func TestRandomIntervalsMatchLinearOracle(t *testing.T) {
	t.Parallel()

	for seed := range uint64(32) {
		// #nosec G404
		rng := rand.New(rand.NewPCG(seed, seed^0xa0761d6478bd642f))
		ranges := make([]index.SegmentTimeRange, 500)
		for i := range ranges {
			minTS := int64(rng.IntN(10_000) - 5000)
			width := int64(rng.IntN(1000))
			ranges[i] = index.SegmentTimeRange{
				SegmentID: uint32(i + 1),
				MinTS:     minTS,
				MaxTS:     minTS + width,
			}
		}
		queries := make([]Query, 500)
		for i := range queries {
			from := int64(rng.IntN(12_000) - 6000)
			queries[i] = Query{
				From: from,
				To:   from + int64(rng.IntN(2000)),
			}
		}
		assertIndexesMatch(t, ranges, queries)
	}
}

func TestGenerateDatasetRejectsUnknownName(t *testing.T) {
	t.Parallel()

	if _, err := GenerateDataset("unknown", 10, 1); err == nil {
		t.Fatal("GenerateDataset() error = nil, want non-nil")
	}
}

func FuzzIntervalIndexesMatchLinear(f *testing.F) {
	f.Add([]byte{0, 0, 5, 1, 0, 0, 10, 2, 0, 0, 20, 3})
	f.Add([]byte{255, 255, 255, 0, 0, 0, 1, 255, 127, 16, 0, 0})
	f.Add([]byte("amber learned interval index"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 4 {
			t.Skip()
		}
		rangeCount := min(128, len(data)/4)
		ranges := make([]index.SegmentTimeRange, rangeCount)
		for i := range ranges {
			offset := i * 4
			minTS := int64(int16(binary.LittleEndian.Uint16(data[offset:])))
			width := int64(binary.LittleEndian.Uint16(data[offset+2:]) % 1024)
			ranges[i] = index.SegmentTimeRange{
				SegmentID: uint32(i + 1),
				MinTS:     minTS,
				MaxTS:     minTS + width,
			}
		}

		queryCount := min(64, len(data))
		queries := make([]Query, queryCount)
		for i := range queries {
			from := int64(int8(data[i])) * 256
			width := int64(data[len(data)-1-i%len(data)])
			queries[i] = Query{From: from, To: from + width}
		}

		linear := NewLinear(ranges)
		candidates := []IntervalIndex{
			NewBinary(ranges),
			NewPiecewiseLinear(ranges, 8),
		}
		for queryNumber, query := range queries {
			want, _ := linear.Lookup(query)
			wantIDs := SortedSegmentIDs(want)
			for _, candidate := range candidates {
				got, _ := candidate.Lookup(query)
				gotIDs := SortedSegmentIDs(got)
				if !slices.Equal(gotIDs, wantIDs) {
					t.Fatalf(
						"query %d %+v: %s IDs = %v, want %v",
						queryNumber,
						query,
						candidate.Name(),
						gotIDs,
						wantIDs,
					)
				}
			}
		}
	})
}

func assertIndexesMatch(t *testing.T, ranges []index.SegmentTimeRange, queries []Query) {
	t.Helper()

	linear := NewLinear(ranges)
	candidates := []IntervalIndex{
		NewBinary(ranges),
		NewPiecewiseLinear(ranges, 256),
	}

	for queryNumber, query := range queries {
		want, _ := linear.Lookup(query)
		wantIDs := SortedSegmentIDs(want)
		for _, candidate := range candidates {
			got, stats := candidate.Lookup(query)
			gotIDs := SortedSegmentIDs(got)
			if !slices.Equal(gotIDs, wantIDs) {
				t.Fatalf(
					"query %d %+v: %s IDs = %v, want %v",
					queryNumber,
					query,
					candidate.Name(),
					gotIDs,
					wantIDs,
				)
			}
			if stats.Scanned < len(got) {
				t.Fatalf(
					"query %d %+v: %s scanned %d ranges but returned %d",
					queryNumber,
					query,
					candidate.Name(),
					stats.Scanned,
					len(got),
				)
			}
		}
	}

	learned := candidates[1].(*PiecewiseLinearIndex)
	if learned.Fallbacks() != 0 {
		t.Fatalf("piecewise learned search used %d global fallbacks", learned.Fallbacks())
	}
}

func assertRankIndex(t *testing.T, keys []int64) {
	t.Helper()

	rank := newRankIndex(keys, 256)
	probes := make([]int64, 0, len(keys)*2+2)
	if len(keys) > 0 {
		probes = append(probes, keys[0], keys[len(keys)-1])
	}
	for i, key := range keys {
		probes = append(probes, key)
		if i+1 < len(keys) && key < keys[i+1] && key < math.MaxInt64 {
			probes = append(probes, key+1)
		}
	}
	slices.Sort(probes)

	for _, probe := range probes {
		gotLower, fallback := rank.lowerBound(probe)
		wantLower := lowerBound(keys, probe)
		if fallback || gotLower != wantLower {
			t.Fatalf(
				"lowerBound(%d) = (%d, fallback=%t), want (%d, false)",
				probe,
				gotLower,
				fallback,
				wantLower,
			)
		}
		gotUpper, fallback := rank.upperBound(probe)
		wantUpper := upperBound(keys, probe)
		if fallback || gotUpper != wantUpper {
			t.Fatalf(
				"upperBound(%d) = (%d, fallback=%t), want (%d, false)",
				probe,
				gotUpper,
				fallback,
				wantUpper,
			)
		}
	}
}
