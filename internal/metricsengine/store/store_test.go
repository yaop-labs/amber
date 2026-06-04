package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

func TestStoreFlushAndSelect(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeCounter, 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeCounter, 1000, 20); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	series, err := st.Select(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchEqual, Value: "api",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
}

func TestStoreSelectIncludesHead(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeCounter, 0, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 1000, 20); err != nil {
		t.Fatal(err)
	}
	series, err := st.Select(index.NewSelector(index.LabelEqual("job", "api")), query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 {
		t.Fatalf("series = %+v, want one live head series with two samples", series)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	series, err = st.Select(index.NewSelector(index.LabelEqual("job", "api")), query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 {
		t.Fatalf("series after flush = %+v, want one persisted series without duplicate head data", series)
	}
}

func TestStoreCatalogPersistsSeriesID(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	id1, err := st.Append(labels, model.MetricTypeGauge, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	id2, err := reopened.Append(labels, model.MetricTypeGauge, 1000, 2)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("ids across reopen = %d/%d, want stable id", id1, id2)
	}
}

func TestStoreRebuildsMissingCatalog(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	// Remove every catalog persistence file so the reopen has to
	// rebuild from blocks. As of INDEX_EVICTION_SPEC_v0 §2 the source
	// of truth is the append-only log (catalog.log + catalog.snapshot);
	// the legacy JSON catalog (catalogFileName) is kept as a fallback
	// but is no longer the primary write target. Remove all of them.
	for _, name := range []string{
		catalogFileName,
		catalogLogFileName,
		catalogLogOldFileName,
		catalogSnapshotFileName,
		catalogSnapshotTmpFileName,
	} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if len(reopened.catalog.Series) != 1 {
		t.Fatalf("catalog series = %d, want 1", len(reopened.catalog.Series))
	}
}

func TestStoreEmptyFlush(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Flush(); !errors.Is(err, ErrNoSamples) {
		t.Fatalf("err = %v, want ErrNoSamples", err)
	}
}

func TestStoreFlushIfNeeded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if path, flushed, err := st.FlushIfNeeded(1); err != nil || flushed || path != "" {
		t.Fatalf("FlushIfNeeded empty = path=%q flushed=%v err=%v", path, flushed, err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	path, flushed, err := st.FlushIfNeeded(1)
	if err != nil {
		t.Fatal(err)
	}
	if !flushed || path == "" {
		t.Fatalf("FlushIfNeeded = path=%q flushed=%v, want flushed path", path, flushed)
	}
}

func TestStoreFlushIfNeededBySamples(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	path, flushed, err := st.FlushIfNeededBy(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !flushed || path == "" {
		t.Fatalf("FlushIfNeededBy = path=%q flushed=%v, want flushed path", path, flushed)
	}
}

func TestStoreThresholdFlushAfterAppend(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{MaxBufferedSamples: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blocks != 1 || stats.BufferedSamples != 0 {
		t.Fatalf("stats = %+v, want one flushed block and empty head", stats)
	}
}

func TestStoreRejectsInvalidLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_, err = st.Append(model.LabelSet{{Name: "job", Value: "api"}, {Name: "job", Value: "worker"}}, model.MetricTypeGauge, 0, 1)
	if !errors.Is(err, ErrInvalidLabels) {
		t.Fatalf("err = %v, want ErrInvalidLabels", err)
	}
}

func TestStoreEnforcesLabelLimits(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{MaxLabelsPerSeries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_, err = st.Append(model.LabelSet{{Name: "job", Value: "api"}, {Name: "instance", Value: "a"}}, model.MetricTypeGauge, 0, 1)
	if !errors.Is(err, ErrLabelLimitExceeded) {
		t.Fatalf("err = %v, want ErrLabelLimitExceeded", err)
	}
}

func TestStoreEnforcesActiveSeriesLimit(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{MaxActiveSeries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	_, err = st.Append(model.LabelSet{{Name: "job", Value: "worker"}}, model.MetricTypeGauge, 0, 1)
	if !errors.Is(err, ErrActiveSeriesLimitExceeded) {
		t.Fatalf("err = %v, want ErrActiveSeriesLimitExceeded", err)
	}
}

func TestStoreAppendBatch(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCloseFlushesHead(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	blocks, err := reopened.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want flushed block after close", len(blocks))
	}
	series, err := reopened.Select(index.NewSelector(index.LabelEqual("job", "api")), query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 1 {
		t.Fatalf("series = %+v, want persisted close-flushed sample", series)
	}
}

func TestStoreSumByLabel(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 2},
		{Labels: model.LabelSet{{Name: "job", Value: "worker"}}, Type: model.MetricTypeGauge, Timestamp: 0, Value: 10},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	sum, err := st.SumByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if sum["api"] != 3 || sum["worker"] != 10 {
		t.Fatalf("sum = %+v, want api=3 worker=10", sum)
	}
	agg, err := st.AggregateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if agg["api"].Count != 2 || agg["api"].Avg() != 1.5 {
		t.Fatalf("agg[api] = %+v, want count=2 avg=1.5", agg["api"])
	}
}

func TestStoreSumByLabelIncludesHead(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	sum, err := st.SumByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if sum["api"] != 3 {
		t.Fatalf("sum[api] = %d, want live head sum 3", sum["api"])
	}
}

func TestStoreAggregateByLabelWithPointFilters(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 1},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 2000, Value: 2},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 3000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	start, end := int64(2000), int64(3000)
	agg, err := st.AggregateByLabel(index.Selector{}, query.Options{StartMillis: &start, EndMillis: &end}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if agg["api"].Sum != 5 || agg["api"].Count != 2 {
		t.Fatalf("agg[api] = %+v, want sum=5 count=2", agg["api"])
	}
}

func TestStoreRateByLabel(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	rates, err := st.RateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if rates["api"] != 2 {
		t.Fatalf("rates[api] = %f, want 2", rates["api"])
	}
	increases, err := st.IncreaseByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if increases["api"] != 2 {
		t.Fatalf("increases[api] = %d, want 2", increases["api"])
	}
}

func TestStoreRateByLabelMergesSeriesAcrossBlocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	rates, err := st.RateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	want := float64(4) / 3
	if rates["api"] != want {
		t.Fatalf("rates[api] = %f, want %f", rates["api"], want)
	}
	increases, err := st.IncreaseByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if increases["api"] != 4 {
		t.Fatalf("increases[api] = %d, want 4", increases["api"])
	}
}

func TestStoreRateByLabelMergesOutOfOrderBlocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	rates, err := st.RateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	want := float64(4) / 3
	if rates["api"] != want {
		t.Fatalf("rates[api] = %f, want %f", rates["api"], want)
	}
	increases, err := st.IncreaseByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if increases["api"] != 4 {
		t.Fatalf("increases[api] = %d, want 4", increases["api"])
	}
}

func TestStoreRateByLabelFallsBackForOverlappingBlocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	rates, err := st.RateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	want := float64(4) / 3
	if rates["api"] != want {
		t.Fatalf("rates[api] = %f, want %f", rates["api"], want)
	}
	increases, err := st.IncreaseByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if increases["api"] != 4 {
		t.Fatalf("increases[api] = %d, want 4", increases["api"])
	}
}

func TestStoreRateByLabelMergesBlocksAndHead(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	rates, err := st.RateByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	want := float64(4) / 3
	if rates["api"] != want {
		t.Fatalf("rates[api] = %f, want %f", rates["api"], want)
	}
	increases, err := st.IncreaseByLabel(index.Selector{}, query.Options{}, "job")
	if err != nil {
		t.Fatal(err)
	}
	if increases["api"] != 4 {
		t.Fatalf("increases[api] = %d, want 4", increases["api"])
	}
}

func TestStoreRateByLabelRange(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "job", Value: "api"}}, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "job", Value: "api"}}, Type: model.MetricTypeCounter, Timestamp: 60_000, Value: 61},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	rs := query.RangeSelector{
		Selector: index.Selector{Matchers: []index.Matcher{
			{Name: "__name__", Op: index.MatchEqual, Value: "http_requests_total"},
			{Name: "job", Op: index.MatchEqual, Value: "api"},
		}},
		Window: time.Minute,
	}
	rates, err := st.RateByLabelRange(rs, 60_000, "job")
	if err != nil {
		t.Fatal(err)
	}
	if rates["api"] != 1 {
		t.Fatalf("rates[api] = %f, want 1", rates["api"])
	}
}

func TestStoreRateByLabelRangeSteps(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}

	steps, err := st.RateByLabelRangeSteps(query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("http_requests_total")),
		Window:   2 * time.Second,
	}, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("len(steps) = %d, want 2", len(steps))
	}
	if steps[0].Values["api"] != 1.5 || steps[1].Values["api"] != 1.5 {
		t.Fatalf("steps = %+v, want api=1.5 at both steps", steps)
	}
	increaseSteps, err := st.IncreaseByLabelRangeSteps(query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("http_requests_total")),
		Window:   2 * time.Second,
	}, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(increaseSteps) != 2 {
		t.Fatalf("len(increaseSteps) = %d, want 2", len(increaseSteps))
	}
	if increaseSteps[0].Values["api"] != 3 || increaseSteps[1].Values["api"] != 3 {
		t.Fatalf("increaseSteps = %+v, want api=3 at both steps", increaseSteps)
	}
}

func TestStoreRateByLabelRangeStepsAcrossBlocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 4},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("http_requests_total")),
		Window:   2 * time.Second,
	}
	steps, err := st.RateByLabelRangeSteps(rangeSelector, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 || steps[0].Values["api"] != 1.5 || steps[1].Values["api"] != 1.5 {
		t.Fatalf("steps = %+v, want api=1.5 at both steps", steps)
	}
	increaseSteps, err := st.IncreaseByLabelRangeSteps(rangeSelector, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(increaseSteps) != 2 || increaseSteps[0].Values["api"] != 3 || increaseSteps[1].Values["api"] != 3 {
		t.Fatalf("increaseSteps = %+v, want api=3 at both steps", increaseSteps)
	}
}

func TestStoreRateByLabelRangeStepsHonorsMaxSampleGap(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 5000, Value: 8},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 6000, Value: 9},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	rangeSelector := query.RangeSelector{
		Selector:     index.NewSelector(index.MetricName("http_requests_total")),
		Window:       6 * time.Second,
		MaxSampleGap: 2 * time.Second,
	}
	steps, err := st.RateByLabelRangeSteps(rangeSelector, 6000, 6000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || len(steps[0].Values) != 0 {
		t.Fatalf("steps = %+v, want stale window suppressed", steps)
	}
}

func TestStoreRateByLabelRangeHonorsMaxSampleGapAcrossBlocks(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "http_requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 5000, Value: 8},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 6000, Value: 9},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	values, err := st.RateByLabelRange(query.RangeSelector{
		Selector:     index.NewSelector(index.MetricName("http_requests_total")),
		Window:       6 * time.Second,
		MaxSampleGap: 2 * time.Second,
	}, 6000, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 0 {
		t.Fatalf("values = %+v, want stale window suppressed", values)
	}
}

func TestStoreAggregateByLabelRangeSteps(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 5},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 2000, Value: 2},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 3000, Value: 4},
	}); err != nil {
		t.Fatal(err)
	}

	rangeSelector := query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("cpu_usage")),
		Window:   2 * time.Second,
	}
	steps, err := st.AggregateByLabelRangeSteps(rangeSelector, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 2 {
		t.Fatalf("len(steps) = %d, want 2", len(steps))
	}
	if got := steps[0].Values["api"]; got.Sum != 8 || got.Count != 3 || got.Min != 1 || got.Max != 5 {
		t.Fatalf("steps[0].api = %+v, want sum=8 count=3 min=1 max=5", got)
	}
	if got := steps[1].Values["api"]; got.Sum != 11 || got.Count != 3 || got.Min != 2 || got.Max != 5 {
		t.Fatalf("steps[1].api = %+v, want sum=11 count=3 min=2 max=5", got)
	}
	sumSteps, err := st.SumByLabelRangeSteps(rangeSelector, 2000, 3000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if sumSteps[0].Values["api"] != 8 || sumSteps[1].Values["api"] != 11 {
		t.Fatalf("sumSteps = %+v, want 8 and 11", sumSteps)
	}
}

func TestStoreAggregateByLabelRangeStepsOverlappingBlocksDedupes(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 20},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 2000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	steps, err := st.AggregateByLabelRangeSteps(query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("cpu_usage")),
		Window:   2 * time.Second,
	}, 2000, 2000, time.Second, "job")
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("len(steps) = %d, want 1", len(steps))
	}
	if got := steps[0].Values["api"]; got.Sum != 24 || got.Count != 3 || got.Min != 1 || got.Max != 20 {
		t.Fatalf("steps[0].api = %+v, want exact deduped aggregate", got)
	}
}

func TestStoreExecuteTypedPlan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeCounter, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 1000, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 2000, 6); err != nil {
		t.Fatal(err)
	}

	result, err := st.Execute(query.Plan{
		Operation: query.OpRateByLabelRange,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   2 * time.Second,
		},
		EndMillis: 2000,
		ByLabel:   "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.FloatValues["api"] != 2.5 {
		t.Fatalf("result.FloatValues = %+v, want api=2.5", result.FloatValues)
	}

	stepResult, err := st.Execute(query.Plan{
		Operation: query.OpIncreaseByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   2 * time.Second,
		},
		StartMillis: 1000,
		EndMillis:   2000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(stepResult.IntSteps) != 2 || stepResult.IntSteps[0].Values["api"] != 2 || stepResult.IntSteps[1].Values["api"] != 5 {
		t.Fatalf("stepResult.IntSteps = %+v, want increases 2 and 5", stepResult.IntSteps)
	}

	aggregateResult, err := st.Execute(query.Plan{
		Operation: query.OpAggregateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   2 * time.Second,
		},
		StartMillis: 1000,
		EndMillis:   2000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(aggregateResult.AggregateSteps) != 2 || aggregateResult.AggregateSteps[0].Values["api"].Sum != 4 || aggregateResult.AggregateSteps[1].Values["api"].Sum != 10 {
		t.Fatalf("aggregateResult.AggregateSteps = %+v, want sums 4 and 10", aggregateResult.AggregateSteps)
	}
}

func TestStoreExplainTypedPlan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	execPlan, err := st.Explain(query.Plan{
		Operation: query.OpRateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   time.Second,
		},
		StartMillis: 1000,
		EndMillis:   1000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathSingleBlockStreaming || execPlan.Stats.BlockCount != 1 || execPlan.Stats.BlockSeries != 1 || execPlan.Stats.StepCount != 1 {
		t.Fatalf("execPlan = %+v, want one-block streaming stats", execPlan)
	}
}

func TestStoreExplainShortRateRangeStepsUsesSummaryPath(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "requests_total"}, {Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 6},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 3000, Value: 10},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	execPlan, err := st.Explain(query.Plan{
		Operation: query.OpRateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("requests_total")),
			Window:   time.Second,
		},
		StartMillis: 2000,
		EndMillis:   3000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathCoalescedSummaries || execPlan.Cost.EstimatedDecodedSamples != 4 {
		t.Fatalf("execPlan = %+v, want short summary path", execPlan)
	}
	execPlan, err = st.Explain(query.Plan{
		Operation: query.OpRateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector:     index.NewSelector(index.MetricName("requests_total")),
			Window:       time.Second,
			MaxSampleGap: time.Second,
		},
		StartMillis: 2000,
		EndMillis:   3000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathMultiBlockStreaming {
		t.Fatalf("execPlan = %+v, want max-gap query to avoid summary path", execPlan)
	}
}

func TestStoreExplainBucketAggregatePlan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}
	samples := make([]model.Sample, 0, 64)
	for i := 0; i < 64; i++ {
		samples = append(samples, model.Sample{Labels: labels, Type: model.MetricTypeGauge, Timestamp: int64(i) * 1000, Value: int64(i + 1)})
	}
	if _, err := st.AppendBatch(samples); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	start, end := int64(0), int64(63_000)
	execPlan, err := st.Explain(query.Plan{
		Operation: query.OpAggregateByLabel,
		Selector:  index.NewSelector(index.MetricName("cpu_usage")),
		Options:   query.Options{StartMillis: &start, EndMillis: &end},
		ByLabel:   "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathBucketAggregate || execPlan.Stats.BucketSamples != 64 || execPlan.Cost.EstimatedDecodedSamples != 0 {
		t.Fatalf("execPlan = %+v, want bucket aggregate with zero decoded samples", execPlan)
	}
	execPlan, err = st.Explain(query.Plan{
		Operation: query.OpAggregateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("cpu_usage")),
			Window:   63 * time.Second,
		},
		StartMillis: 63_000,
		EndMillis:   63_000,
		Step:        time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathBucketAggregate || execPlan.Stats.BucketSamples != 64 || execPlan.Cost.EstimatedDecodedSamples != 0 {
		t.Fatalf("range-step execPlan = %+v, want bucket aggregate with zero decoded samples", execPlan)
	}
}

func TestStoreExplainMultiBlockBucketAggregatePlan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}
	for blockIndex := 0; blockIndex < 2; blockIndex++ {
		samples := make([]model.Sample, 0, 64)
		for i := 0; i < 64; i++ {
			sampleIndex := blockIndex*64 + i
			samples = append(samples, model.Sample{Labels: labels, Type: model.MetricTypeGauge, Timestamp: int64(sampleIndex) * 1000, Value: int64(sampleIndex + 1)})
		}
		if _, err := st.AppendBatch(samples); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	execPlan, err := st.Explain(query.Plan{
		Operation: query.OpAggregateByLabelRangeSteps,
		RangeSelector: query.RangeSelector{
			Selector: index.NewSelector(index.MetricName("cpu_usage")),
			Window:   63 * time.Second,
		},
		StartMillis: 63_000,
		EndMillis:   127_000,
		Step:        64 * time.Second,
		ByLabel:     "job",
	})
	if err != nil {
		t.Fatal(err)
	}
	if execPlan.Path != query.PathBucketAggregate || execPlan.Stats.BlockCount != 2 || execPlan.Stats.BucketSamples != 128 || execPlan.Cost.EstimatedDecodedSamples != 0 {
		t.Fatalf("execPlan = %+v, want multi-block bucket aggregate with zero decoded samples", execPlan)
	}
}

func TestStoreExecuteRejectsInvalidPlan(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	_, err = st.Execute(query.Plan{Operation: query.OpRateByLabelRange})
	if err == nil {
		t.Fatal("expected invalid range plan error")
	}
}

func TestStoreSelectRange(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 0, Value: 10},
		{Labels: model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 60_000, Value: 20},
		{Labels: model.LabelSet{{Name: model.MetricNameLabel, Value: "cpu_usage"}, {Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 120_000, Value: 30},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	series, err := st.SelectRange(query.RangeSelector{
		Selector: index.NewSelector(index.MetricName("cpu_usage"), index.LabelEqual("job", "api")),
		Window:   time.Minute,
	}, 120_000)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 || series[0].Values[0] != 20 || series[0].Values[1] != 30 {
		t.Fatalf("series = %+v, want the last two samples", series)
	}
}

func TestStoreStats(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.AppendBatch([]model.Sample{
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 1},
		{Labels: model.LabelSet{{Name: "job", Value: "api"}}, Type: model.MetricTypeGauge, Timestamp: 2000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Blocks != 1 || stats.Series != 1 || stats.Samples != 2 || !stats.HasTime {
		t.Fatalf("stats = %+v, want 1 block/series and 2 samples with time", stats)
	}
}

func TestStoreStatsIncludesBufferedCounts(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 1},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 2000, Value: 2},
	}); err != nil {
		t.Fatal(err)
	}
	stats, err := st.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.BufferedSeries != 1 || stats.BufferedSamples != 2 {
		t.Fatalf("stats = %+v, want buffered series=1 samples=2", stats)
	}
}

func TestStoreBackgroundFlush(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats, err := st.Stats()
		if err != nil {
			t.Fatal(err)
		}
		if stats.Blocks == 1 && stats.BufferedSamples == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("background flush did not publish a block")
}

func TestStoreMaintenanceAppliesRetention(t *testing.T) {
	now := time.UnixMilli(10_000)
	st, err := OpenWithOptions(t.TempDir(), Options{
		Retention: 5 * time.Second,
		Clock: func() time.Time {
			return now
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "old"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.maintenance(); err != nil {
		t.Fatal(err)
	}
	blocks, err := st.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 0 {
		t.Fatalf("blocks = %v, want retention to delete old block", blocks)
	}
}

func TestStoreManifestPersistsBlocks(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	blockPath, err := st.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	blocks, err := reopened.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0] != blockPath {
		t.Fatalf("blocks = %v, want [%s]", blocks, blockPath)
	}
}

func TestStoreRebuildsMissingManifest(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	blockPath, err := st.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, manifestFileName)); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	blocks, err := reopened.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || blocks[0] != blockPath {
		t.Fatalf("blocks = %v, want [%s]", blocks, blockPath)
	}
	if len(reopened.manifest.Blocks[0].LabelValues["job"]) != 1 || reopened.manifest.Blocks[0].LabelValues["job"][0] != "api" {
		t.Fatalf("rebuilt label values = %+v, want job=api", reopened.manifest.Blocks[0].LabelValues)
	}
	if len(reopened.catalog.Series) == 0 {
		t.Fatal("expected catalog to persist at least one series")
	}
}

func TestStoreRecoversPreparedFlushWithoutManifest(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "block-prepared.meb")
	if err := st.engine.PrepareFlushBlock(path); err != nil {
		t.Fatal(err)
	}
	if err := st.engine.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	blocks, err := recovered.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 0 {
		t.Fatalf("blocks = %v, want no manifest recovery while WAL is non-empty", blocks)
	}
	if recovered.engine.BufferedSeries() != 1 {
		t.Fatalf("BufferedSeries = %d, want WAL replayed sample", recovered.engine.BufferedSeries())
	}
}

func TestStoreSelectPrunesBlocksByManifestTime(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "old"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "new"}}, model.MetricTypeGauge, 10_000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	start := int64(9000)
	series, err := st.Select(index.Selector{}, query.Options{StartMillis: &start})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
	if got, _ := series[0].Entry.Labels.Get("job"); got != "new" {
		t.Fatalf("job = %q, want new", got)
	}
}

func TestStoreSelectPrunesBlocksByManifestLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "worker"}}, model.MetricTypeGauge, 2000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	paths, err := st.blocksForQueryLocked(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchEqual, Value: "api",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1 after label pruning", len(paths))
	}
	series, err := st.Select(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchEqual, Value: "api",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
}

func TestStoreSelectPrunesBlocksByManifestMultipleLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}, {Name: "region", Value: "us"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}, {Name: "region", Value: "eu"}}, model.MetricTypeGauge, 2000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "worker"}, {Name: "region", Value: "us"}}, model.MetricTypeGauge, 3000, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	selector := index.NewSelector(index.LabelEqual("job", "api"), index.LabelEqual("region", "us"))
	paths, err := st.blocksForQueryLocked(selector, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1 after multi-label pruning", len(paths))
	}
	series, err := st.Select(selector, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
	if got, _ := series[0].Entry.Labels.Get("region"); got != "us" {
		t.Fatalf("region = %q, want us", got)
	}
}

func TestStoreSelectPrunesBlocksByManifestTimeAndLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 10_000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "worker"}}, model.MetricTypeGauge, 10_000, 3); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	start := int64(9000)
	paths, err := st.blocksForQueryLocked(index.NewSelector(index.LabelEqual("job", "api")), query.Options{StartMillis: &start})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1 after time+label pruning", len(paths))
	}
	series, err := st.Select(index.NewSelector(index.LabelEqual("job", "api")), query.Options{StartMillis: &start})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || series[0].Values[0] != 2 {
		t.Fatalf("series = %+v, want only new api block", series)
	}
}

func TestStoreSelectPrunesBlocksByManifestRegexLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "worker"}}, model.MetricTypeGauge, 2000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	paths, err := st.blocksForQueryLocked(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchRegexp, Value: "api|web",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1 after regex label pruning", len(paths))
	}
}

func TestStoreSelectRejectsInvalidSelector(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	_, err = st.Select(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchRegexp, Value: "[",
	}}}, query.Options{})
	if err == nil {
		t.Fatal("expected invalid selector error")
	}
}

func TestStoreSelectPrunesBlocksByManifestNegativeLabels(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "worker"}}, model.MetricTypeGauge, 2000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	paths, err := st.blocksForQueryLocked(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchNotEqual, Value: "api",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 {
		t.Fatalf("len(paths) = %d, want 1 after negative label pruning", len(paths))
	}
	series, err := st.Select(index.Selector{Matchers: []index.Matcher{{
		Name: "job", Op: index.MatchNotEqual, Value: "api",
	}}}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
}

func TestStoreDeleteBefore(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "old"}}, model.MetricTypeGauge, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(model.LabelSet{{Name: "job", Value: "new"}}, model.MetricTypeGauge, 10_000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	deleted, err := st.DeleteBefore(5000)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	blocks, err := st.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want 1", len(blocks))
	}
}

func TestStoreCompact(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeCounter, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 1000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	path, err := st.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path)[:8] != "compact-" {
		t.Fatalf("compact path = %s, want compact-*", path)
	}
	blocks, err := st.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("len(blocks) = %d, want 1", len(blocks))
	}
	series, err := st.Select(index.Selector{}, query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 {
		t.Fatalf("series = %+v, want one merged series with two values", series)
	}
}

func TestStoreCompactIfNeeded(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeCounter, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 1000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	if path, compacted, err := st.CompactIfNeeded(3); err != nil || compacted || path != "" {
		t.Fatalf("CompactIfNeeded(3) = path=%q compacted=%v err=%v, want skip", path, compacted, err)
	}
	path, compacted, err := st.CompactIfNeeded(2)
	if err != nil {
		t.Fatal(err)
	}
	if !compacted || filepath.Base(path)[:8] != "compact-" {
		t.Fatalf("CompactIfNeeded(2) = path=%q compacted=%v, want compacted path", path, compacted)
	}
}

func TestStoreMaintenanceCompactsWhenConfigured(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{CompactionMinBlocks: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeCounter, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(labels, model.MetricTypeCounter, 1000, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := st.maintenance(); err != nil {
		t.Fatal(err)
	}
	blocks, err := st.Blocks()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 || filepath.Base(blocks[0])[:8] != "compact-" {
		t.Fatalf("blocks = %+v, want one compact block", blocks)
	}
}

func TestStoreCompactDeduplicatesOverlappingSamples(t *testing.T) {
	st, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := st.Append(labels, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 0, Value: 2},
		{Labels: labels, Type: model.MetricTypeGauge, Timestamp: 1000, Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Flush(); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Compact(); err != nil {
		t.Fatal(err)
	}
	series, err := st.Select(index.NewSelector(index.LabelEqual("job", "api")), query.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 {
		t.Fatalf("len(series) = %d, want 1", len(series))
	}
	if len(series[0].Values) != 2 || series[0].Timestamps[0] != 0 || series[0].Values[0] != 2 || series[0].Timestamps[1] != 1000 || series[0].Values[1] != 3 {
		t.Fatalf("series = %+v, want duplicate timestamp to keep latest value", series[0])
	}
}
