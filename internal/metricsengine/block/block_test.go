package block

import (
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestWriteReadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.meb")
	in := []Series{{
		ID:     1,
		Type:   model.MetricTypeCounter,
		Labels: model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{
			1000, 2000, 3000, 4000,
		},
		Values: []int64{10, 20, 31, 43},
	}}
	if err := WriteFile(path, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("len(out) = %d, want 1", len(out))
	}
	for i := range in[0].Values {
		if out[0].Values[i] != in[0].Values[i] {
			t.Fatalf("value[%d] = %d, want %d", i, out[0].Values[i], in[0].Values[i])
		}
	}
	if !out[0].Entry.ZoneMap.Monotonic {
		t.Fatal("expected monotonic zone map")
	}
}

func TestWriteFileSharesIdenticalTimestampPayloads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.meb")
	timestamps := []int64{1000, 2001, 3003}
	in := []Series{
		{ID: 1, Type: model.MetricTypeGauge, Labels: model.LabelSet{{Name: "job", Value: "a"}}, Timestamps: timestamps, Values: []int64{1, 2, 3}},
		{ID: 2, Type: model.MetricTypeGauge, Labels: model.LabelSet{{Name: "job", Value: "b"}}, Timestamps: timestamps, Values: []int64{4, 5, 6}},
	}
	if err := WriteFile(path, in); err != nil {
		t.Fatal(err)
	}
	dir, err := ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(dir.Series) != 2 {
		t.Fatalf("len(series) = %d, want 2", len(dir.Series))
	}
	if dir.Series[0].TimestampOff != dir.Series[1].TimestampOff {
		t.Fatalf("timestamp offsets differ: %d vs %d", dir.Series[0].TimestampOff, dir.Series[1].TimestampOff)
	}
}

func TestGaugeDecreaseIsNotCounterReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.meb")
	if err := WriteFile(path, []Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{1000, 2000},
		Values:     []int64{10, 5},
	}}); err != nil {
		t.Fatal(err)
	}
	dir, err := ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	if dir.Series[0].ZoneMap.HasReset {
		t.Fatal("gauge decrease should not be marked as counter reset")
	}
	if dir.Series[0].ZoneMap.Monotonic {
		t.Fatal("decreasing gauge should still be non-monotonic")
	}
}

func TestWriteFileSortsSamplesByTimestamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.meb")
	if err := WriteFile(path, []Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: []int64{2000, 1000},
		Values:     []int64{20, 10},
	}}); err != nil {
		t.Fatal(err)
	}
	series, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if series[0].Timestamps[0] != 1000 || series[0].Values[0] != 10 {
		t.Fatalf("series was not sorted: %+v", series[0])
	}
}

func TestWriteFileStoresAggregateBuckets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.meb")
	timestamps := make([]int64, 65)
	values := make([]int64, 65)
	for i := range values {
		timestamps[i] = int64(i) * 1000
		values[i] = int64(i + 1)
	}
	if err := WriteFile(path, []Series{{
		ID:         1,
		Type:       model.MetricTypeGauge,
		Labels:     model.LabelSet{{Name: "job", Value: "api"}},
		Timestamps: timestamps,
		Values:     values,
	}}); err != nil {
		t.Fatal(err)
	}
	dir, err := ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	buckets := dir.Series[0].AggregateBuckets
	if len(buckets) != 2 {
		t.Fatalf("len(buckets) = %d, want 2", len(buckets))
	}
	if buckets[0].TimeMin != 0 || buckets[0].TimeMax != 63_000 || buckets[0].Count != 64 || buckets[0].Sum != 2080 || buckets[0].Min != 1 || buckets[0].Max != 64 {
		t.Fatalf("bucket[0] = %+v, want first 64-value summary", buckets[0])
	}
	if buckets[1].TimeMin != 64_000 || buckets[1].TimeMax != 64_000 || buckets[1].Count != 1 || buckets[1].Sum != 65 {
		t.Fatalf("bucket[1] = %+v, want final single-value summary", buckets[1])
	}
}

func TestBuildAggregateBucketsRejectsInvalidInput(t *testing.T) {
	if buckets := BuildAggregateBuckets([]int64{1}, []int64{1}, 0); buckets != nil {
		t.Fatalf("buckets = %+v, want nil for invalid bucket size", buckets)
	}
	if buckets := BuildAggregateBuckets([]int64{1}, nil, 64); buckets != nil {
		t.Fatalf("buckets = %+v, want nil for mismatched lengths", buckets)
	}
}
