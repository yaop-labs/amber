package store

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// benchLabelSets builds n distinct series in the obsbench I1 shape: five
// labels, ~1k-cardinality route, 10 services.
func benchLabelSets(n int) []model.LabelSet {
	out := make([]model.LabelSet, n)
	for i := range out {
		out[i] = model.LabelSet{
			{Name: model.MetricNameLabel, Value: "http_requests_total"},
			{Name: "host", Value: fmt.Sprintf("node-%02d", i%6)},
			{Name: "route", Value: fmt.Sprintf("route-%04d", i%1000)},
			{Name: "service", Value: fmt.Sprintf("svc-%02d", i%10)},
			{Name: "shard", Value: fmt.Sprintf("shard-%03d", i/1000)},
		}
	}
	return out
}

// BenchmarkStats_I1Shape measures Stats() over a store shaped like a metrics
// campaign run: 100k-series blocks sealed every few ticks. The campaign's
// verify step calls this through /api/v1/metrics/stats with a 120s client
// timeout.
func BenchmarkStats_I1Shape(b *testing.B) {
	const (
		seriesN = 100_000
		ticks   = 12
	)
	st, err := OpenWithOptions(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	labels := benchLabelSets(seriesN)
	ts := time.Now().UnixMilli()
	for tick := 0; tick < ticks; tick++ {
		for lo := 0; lo < seriesN; lo += 2000 {
			batch := make([]model.Sample, 0, 2000)
			for j := lo; j < lo+2000 && j < seriesN; j++ {
				batch = append(batch, model.Sample{
					Labels: labels[j], Type: model.MetricTypeCounter,
					Timestamp: ts + int64(tick)*10_000, Value: int64(tick),
				})
			}
			if _, err := st.AppendBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
		if tick%3 == 2 {
			if _, err := st.Flush(); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.Stats(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendBatch_I1Shape measures the OTLP ingest write path the way
// the metrics benchmark drives it: 1000-sample batches over a fixed 100k
// series population, 4 concurrent appenders. b.N counts samples.
func BenchmarkAppendBatch_I1Shape(b *testing.B) {
	const (
		seriesN = 100_000
		batchN  = 1000
		workers = 4
	)
	st, err := OpenWithOptions(b.TempDir(), Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	labels := benchLabelSets(seriesN)

	var mu sync.Mutex
	next := 0
	ts := time.Now().UnixMilli()

	b.ResetTimer()
	b.SetParallelism(workers)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			lo := next % seriesN
			next += batchN
			tick := int64(next / seriesN)
			mu.Unlock()

			batch := make([]model.Sample, 0, batchN)
			for j := 0; j < batchN; j++ {
				batch = append(batch, model.Sample{
					Labels:    labels[(lo+j)%seriesN],
					Type:      model.MetricTypeCounter,
					Timestamp: ts + tick*10_000,
					Value:     int64(tick * 10),
				})
			}
			if _, err := st.AppendBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.StopTimer()
	b.ReportMetric(float64(b.N*batchN)/b.Elapsed().Seconds(), "samples/s")
}
