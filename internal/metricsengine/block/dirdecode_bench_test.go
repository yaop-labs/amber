package block

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// dirdecode_bench_test.go isolates the per-block directory decode cost. The
// metrics-campaign query wall is dominated by json.Unmarshal of 100k-entry
// directories (see store/rate_query_bench_test.go). This bench measures one
// directory decode and reports the on-disk directory size + in-memory size.
func BenchmarkReadDirectory100k(b *testing.B) {
	const seriesN = 100_000
	dir := b.TempDir()
	path := filepath.Join(dir, "block.meb")

	series := make([]Series, seriesN)
	for i := range series {
		series[i] = Series{
			ID:   uint64(i),
			Type: model.MetricTypeCounter,
			Labels: model.LabelSet{
				{Name: model.MetricNameLabel, Value: "http_requests_total"},
				{Name: "host", Value: fmt.Sprintf("node-%02d", i%6)},
				{Name: "route", Value: fmt.Sprintf("route-%04d", i%1000)},
				{Name: "service", Value: fmt.Sprintf("svc-%02d", i%10)},
				{Name: "shard", Value: fmt.Sprintf("shard-%03d", i/1000)},
			},
			Timestamps: []int64{1, 2, 3, 4, 5, 6},
			Values:     []int64{0, 10, 20, 30, 40, 50},
		}
	}
	if err := WriteFile(path, series); err != nil {
		b.Fatal(err)
	}
	info, _ := os.Stat(path)
	d, err := ReadDirectory(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Logf("block file size=%d bytes, directory entries=%d", info.Size(), len(d.Series))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dd, err := ReadDirectory(path)
		if err != nil {
			b.Fatal(err)
		}
		if len(dd.Series) != seriesN {
			b.Fatalf("got %d entries", len(dd.Series))
		}
	}
}
