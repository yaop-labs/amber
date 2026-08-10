package indexlab

import (
	"fmt"
	"testing"

	"github.com/yaop-labs/amber/internal/index"
)

var (
	benchmarkRanges []index.SegmentTimeRange
	benchmarkStats  LookupStats
	benchmarkRank   int
	benchmarkIndex  IntervalIndex
)

func BenchmarkLookup(b *testing.B) {
	for _, dataset := range DatasetNames {
		for _, size := range []int{10_000, 100_000, 1_000_000} {
			ranges, err := GenerateDataset(dataset, size, 42)
			if err != nil {
				b.Fatal(err)
			}
			queries := GenerateQueries(ranges, 2048, 84)
			indexes := []IntervalIndex{
				NewLinear(ranges),
				NewBinary(ranges),
				NewPiecewiseLinear(ranges, 256),
			}
			var totalMatches int64
			for _, query := range queries {
				result, _ := indexes[1].Lookup(query)
				totalMatches += int64(len(result))
			}

			for _, candidate := range indexes {
				name := fmt.Sprintf("%s/%d/%s", dataset, size, candidate.Name())
				b.Run(name, func(b *testing.B) {
					var totalScanned, totalOverfetch int64
					if candidate.Name() == "linear" {
						totalScanned = int64(size) * int64(len(queries))
						totalOverfetch = totalScanned - totalMatches
					} else {
						for _, query := range queries {
							_, stats := candidate.Lookup(query)
							totalScanned += int64(stats.Scanned)
							totalOverfetch += int64(stats.Overfetch)
						}
					}

					b.ReportAllocs()
					b.ReportMetric(
						float64(totalScanned)/float64(len(queries)),
						"ranges_scanned/op",
					)
					b.ReportMetric(
						float64(totalOverfetch)/float64(len(queries)),
						"overfetch/op",
					)
					b.ReportMetric(
						float64(candidate.AuxBytes())/float64(size),
						"aux_B/segment",
					)
					b.ResetTimer()

					var result []index.SegmentTimeRange
					var stats LookupStats
					for i := 0; i < b.N; i++ {
						result, stats = candidate.Lookup(queries[i%len(queries)])
					}
					benchmarkRanges = result
					benchmarkStats = stats
				})
			}
		}
	}
}

func BenchmarkBuild(b *testing.B) {
	for _, dataset := range DatasetNames {
		for _, size := range []int{10_000, 100_000, 1_000_000} {
			ranges, err := GenerateDataset(dataset, size, 42)
			if err != nil {
				b.Fatal(err)
			}
			builders := []struct {
				name  string
				build func([]index.SegmentTimeRange) IntervalIndex
			}{
				{name: "linear", build: func(ranges []index.SegmentTimeRange) IntervalIndex {
					return NewLinear(ranges)
				}},
				{name: "binary_interval", build: func(ranges []index.SegmentTimeRange) IntervalIndex {
					return NewBinary(ranges)
				}},
				{name: "piecewise_linear", build: func(ranges []index.SegmentTimeRange) IntervalIndex {
					return NewPiecewiseLinear(ranges, 256)
				}},
			}

			for _, builder := range builders {
				name := fmt.Sprintf("%s/%d/%s", dataset, size, builder.name)
				b.Run(name, func(b *testing.B) {
					b.ReportAllocs()
					for i := 0; i < b.N; i++ {
						benchmarkIndex = builder.build(ranges)
					}
				})
			}
		}
	}
}

func BenchmarkBoundarySearch(b *testing.B) {
	for _, dataset := range DatasetNames {
		ranges, err := GenerateDataset(dataset, 1_000_000, 42)
		if err != nil {
			b.Fatal(err)
		}
		binary := NewBinary(ranges)
		learned := newRankIndex(binary.minKeys, 256)
		queries := GenerateQueries(ranges, 2048, 84)

		b.Run(dataset+"/binary", func(b *testing.B) {
			var position int
			for i := 0; i < b.N; i++ {
				position = lowerBound(binary.minKeys, queries[i%len(queries)].From)
			}
			benchmarkRank = position
		})

		b.Run(dataset+"/piecewise_linear", func(b *testing.B) {
			var position int
			for i := 0; i < b.N; i++ {
				position, _ = learned.lowerBound(queries[i%len(queries)].From)
			}
			benchmarkRank = position
		})
	}
}
