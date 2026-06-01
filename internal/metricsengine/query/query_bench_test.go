package query

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func BenchmarkSumByLabelInBlockDirectory(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.meb")
	var series []block.Series
	for i := 0; i < 1000; i++ {
		series = append(series, block.Series{
			ID:         uint64(i + 1),
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{0, 1000, 2000, 3000},
			Values:     []int64{1, 2, 3, 4},
		})
	}
	if err := block.WriteFile(path, series); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := SumByLabelInBlock(path, index.Selector{}, Options{}, "job"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggregateByLabelInBlockBucketTimeRange(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.meb")
	var series []block.Series
	for i := 0; i < 1000; i++ {
		timestamps := make([]int64, 64)
		values := make([]int64, 64)
		for sample := range values {
			timestamps[sample] = int64(sample) * 1000
			values[sample] = int64(sample + 1)
		}
		series = append(series, block.Series{
			ID:         uint64(i + 1),
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: timestamps,
			Values:     values,
		})
	}
	if err := block.WriteFile(path, series); err != nil {
		b.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		b.Fatal(err)
	}
	start, end := int64(0), int64(63_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg, err := AggregateByLabelInBlockWithDirectory(path, dir, index.Selector{}, Options{StartMillis: &start, EndMillis: &end}, "job")
		if err != nil {
			b.Fatal(err)
		}
		if agg["api"].Count == 0 {
			b.Fatal("expected aggregate")
		}
	}
}

func BenchmarkAggregateByLabelInBlockBucketHybridPartial(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.meb")
	var series []block.Series
	for i := 0; i < 1000; i++ {
		timestamps := make([]int64, 64)
		values := make([]int64, 64)
		offset := int64(1000)
		if i%10 == 0 {
			offset = 0
		}
		for sample := range values {
			timestamps[sample] = offset + int64(sample)*1000
			values[sample] = int64(sample + 1)
		}
		series = append(series, block.Series{
			ID:         uint64(i + 1),
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: timestamps,
			Values:     values,
		})
	}
	if err := block.WriteFile(path, series); err != nil {
		b.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		b.Fatal(err)
	}
	start, end := int64(1000), int64(64_000)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg, err := AggregateByLabelInBlockWithDirectory(path, dir, index.Selector{}, Options{StartMillis: &start, EndMillis: &end}, "job")
		if err != nil {
			b.Fatal(err)
		}
		if agg["api"].Count == 0 {
			b.Fatal("expected aggregate")
		}
	}
}

func BenchmarkAggregateByLabelStepsInBlockBucketAligned(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.meb")
	var series []block.Series
	for i := 0; i < 1000; i++ {
		timestamps := make([]int64, 128)
		values := make([]int64, 128)
		for sample := range values {
			timestamps[sample] = int64(sample) * 1000
			values[sample] = int64(sample + 1)
		}
		series = append(series, block.Series{
			ID:         uint64(i + 1),
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: timestamps,
			Values:     values,
		})
	}
	if err := block.WriteFile(path, series); err != nil {
		b.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := AggregateByLabelStepsInBlockWithDirectory(path, dir, index.Selector{}, "job", 63_000, 127_000, 64*time.Second, 63*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) != 2 || steps[0].Values["api"].Count == 0 {
			b.Fatal("expected aggregate steps")
		}
	}
}

func BenchmarkAggregateByLabelStepsInBlockBucketHybridPartial(b *testing.B) {
	path := filepath.Join(b.TempDir(), "bench.meb")
	var series []block.Series
	for i := 0; i < 1000; i++ {
		timestamps := make([]int64, 64)
		values := make([]int64, 64)
		offset := int64(1000)
		if i%10 == 0 {
			offset = 0
		}
		for sample := range values {
			timestamps[sample] = offset + int64(sample)*1000
			values[sample] = int64(sample + 1)
		}
		series = append(series, block.Series{
			ID:         uint64(i + 1),
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: timestamps,
			Values:     values,
		})
	}
	if err := block.WriteFile(path, series); err != nil {
		b.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		steps, err := AggregateByLabelStepsInBlockWithDirectory(path, dir, index.Selector{}, "job", 64_000, 64_000, time.Second, 63*time.Second)
		if err != nil {
			b.Fatal(err)
		}
		if len(steps) != 1 || steps[0].Values["api"].Count == 0 {
			b.Fatal("expected aggregate steps")
		}
	}
}
