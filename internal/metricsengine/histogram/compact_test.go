package histogram

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func expSeriesAt(name string, ts int64, values ...float64) ExpSeries {
	sk := FromValues(3, values)
	return ExpSeries{
		ID:         1,
		Labels:     model.LabelSet{{Name: model.MetricNameLabel, Value: name}, {Name: "job", Value: "api"}},
		Timestamps: []int64{ts},
		Sketches:   []*ExponentialHistogram{sk},
	}
}

func blockCount(t *testing.T, dir string) int {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "hblock-*.mhb"))
	if err != nil {
		t.Fatal(err)
	}
	return len(paths)
}

// noAutoCompact disables WriteBlock's inline compaction so tests control it.
var noAutoCompact = Options{CompactMinBlocks: -1}

func TestCompactMergesBlocksPreservingQuantiles(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}

	// Three POSTs → three blocks, same series.
	for i, vals := range [][]float64{{1, 2, 3}, {10, 20, 30}, {100, 200, 300}} {
		if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", int64(1000*(i+1)), vals...)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := blockCount(t, dir); got != 3 {
		t.Fatalf("blocks before compact = %d, want 3", got)
	}

	sel := index.NewSelector(index.MetricName("latency"))
	before, err := st.HistogramQuantile(sel, 0.5, fullRange())
	if err != nil {
		t.Fatal(err)
	}
	sumBefore, err := st.Summary(sel, fullRange())
	if err != nil {
		t.Fatal(err)
	}

	path, err := st.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if path == "" {
		t.Fatal("Compact returned empty path with live data")
	}
	if got := blockCount(t, dir); got != 1 {
		t.Fatalf("blocks after compact = %d, want 1", got)
	}
	if _, err := os.Stat(filepath.Join(dir, compactMarkerName)); !os.IsNotExist(err) {
		t.Fatalf("marker left behind: %v", err)
	}

	after, err := st.HistogramQuantile(sel, 0.5, fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if math.Float64bits(before) != math.Float64bits(after) {
		t.Errorf("quantile changed by compaction: %v -> %v", before, after)
	}
	sumAfter, err := st.Summary(sel, fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sumBefore != sumAfter {
		t.Errorf("summary changed by compaction: %+v -> %+v", sumBefore, sumAfter)
	}

	// Time-bounded query must still see per-tick granularity.
	q2, err := st.HistogramQuantile(sel, 0.5, TimeRange{Start: 2000, End: 2000})
	if err != nil {
		t.Fatal(err)
	}
	if q2 < 10 || q2 > 30 {
		t.Errorf("tick-bounded quantile = %v, want within [10,30]", q2)
	}
}

func TestCompactSeparatesLabelGroups(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}

	a := expSeriesAt("latency", 1000, 1, 2, 3)
	b := expSeriesAt("latency", 1000, 100, 200, 300)
	b.Labels = model.LabelSet{{Name: model.MetricNameLabel, Value: "latency"}, {Name: "job", Value: "worker"}}
	if _, err := st.WriteBlock([]ExpSeries{a}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{b}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Compact(); err != nil {
		t.Fatal(err)
	}

	byJob, err := st.HistogramQuantileBy(index.NewSelector(index.MetricName("latency")), 0.5, fullRange(), []string{"job"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byJob) != 2 {
		t.Fatalf("groups after compact = %d (%v), want 2", len(byJob), byJob)
	}
}

func TestCompactDropsExpiredTicks(t *testing.T) {
	now := time.UnixMilli(10_000_000)
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, Options{
		Retention:        time.Hour,
		CompactMinBlocks: -1,
		Clock:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	cutoff := now.Add(-time.Hour).UnixMilli()

	// One block holding both an expired and a fresh tick (whole-file
	// retention can't touch it), plus a fresh-only block.
	mixed := expSeriesAt("latency", cutoff-60_000, 1, 2, 3)
	mixed.Timestamps = append(mixed.Timestamps, cutoff+60_000)
	mixed.Sketches = append(mixed.Sketches, FromValues(3, []float64{4, 5, 6}))
	if _, err := st.WriteBlock([]ExpSeries{mixed}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", cutoff+120_000, 7, 8, 9)}, nil); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Compact(); err != nil {
		t.Fatal(err)
	}

	sum, err := st.Summary(index.NewSelector(index.MetricName("latency")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 6 {
		t.Errorf("count after compact = %d, want 6 (expired tick dropped)", sum.Count)
	}
}

func TestCompactAllExpiredRemovesEverything(t *testing.T) {
	now := time.UnixMilli(10_000_000)
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, Options{
		Retention:        time.Hour,
		CompactMinBlocks: -1,
		Clock:            func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * time.Hour).UnixMilli()
	// Two mixed-age blocks would be kept by whole-file retention if any tick
	// were fresh; here everything is old.
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", old, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	// Manually bypass the write-path retention by checking state before it runs.
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", old+1, 2)}, nil); err != nil {
		t.Fatal(err)
	}

	path, err := st.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if path != "" {
		t.Errorf("Compact wrote %q for fully-expired data", path)
	}
	if got := blockCount(t, dir); got != 0 {
		t.Errorf("blocks after all-expired compact = %d, want 0", got)
	}
}

func TestWriteBlockAutoCompacts(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, Options{CompactMinBlocks: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", int64(1000*(i+1)), float64(i+1))}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := blockCount(t, dir); got != 1 {
		t.Fatalf("blocks after auto-compact = %d, want 1", got)
	}
	sum, err := st.Summary(index.NewSelector(index.MetricName("latency")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 3 {
		t.Errorf("count = %d, want 3", sum.Count)
	}
}

// Crash after the marker was written but before the merged block landed:
// recovery must roll back, keeping the sources.
func TestCompactRecoveryRollsBackWithoutDest(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", 1000, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", 2000, 2)}, nil); err != nil {
		t.Fatal(err)
	}

	marker := compactMarker{Dest: "hblock-000099.mhb", Sources: []string{"hblock-000000.mhb", "hblock-000001.mhb"}}
	if err := writeCompactMarker(dir, marker); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatalf("reopen with pending marker: %v", err)
	}
	if got := blockCount(t, dir); got != 2 {
		t.Fatalf("blocks after rollback = %d, want 2 (sources intact)", got)
	}
	if _, err := os.Stat(filepath.Join(dir, compactMarkerName)); !os.IsNotExist(err) {
		t.Fatal("marker not removed by rollback")
	}
	sum, err := st2.Summary(index.NewSelector(index.MetricName("latency")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 2 {
		t.Errorf("count after rollback = %d, want 2", sum.Count)
	}
}

// Crash after the merged block landed but before the sources were deleted:
// recovery must finish the deletes — otherwise every sketch counts twice.
func TestCompactRecoveryFinishesWithDest(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", 1000, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", 2000, 2)}, nil); err != nil {
		t.Fatal(err)
	}

	// Build the merged block the way compactLocked would, then "crash"
	// before the source deletes.
	merged := expSeriesAt("latency", 1000, 1)
	merged.Timestamps = append(merged.Timestamps, 2000)
	merged.Sketches = append(merged.Sketches, FromValues(3, []float64{2}))
	dest := filepath.Join(dir, "hblock-000002.mhb")
	if err := WriteBlock(dest, []ExpSeries{merged}, nil); err != nil {
		t.Fatal(err)
	}
	if err := writeCompactMarker(dir, compactMarker{
		Dest:    "hblock-000002.mhb",
		Sources: []string{"hblock-000000.mhb", "hblock-000001.mhb"},
	}); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatalf("reopen with pending marker: %v", err)
	}
	if got := blockCount(t, dir); got != 1 {
		t.Fatalf("blocks after recovery = %d, want 1 (sources deleted)", got)
	}
	sum, err := st2.Summary(index.NewSelector(index.MetricName("latency")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 2 {
		t.Errorf("count after recovery = %d, want 2 (no double-count)", sum.Count)
	}
}

// A torn marker (unparseable JSON) must roll back, not fail the open.
func TestCompactRecoveryTornMarker(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock([]ExpSeries{expSeriesAt("latency", 1000, 1)}, nil); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, compactMarkerName), []byte(`{"dest":"hbl`), 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenStoreWithOptions(dir, noAutoCompact); err != nil {
		t.Fatalf("reopen with torn marker: %v", err)
	}
	if got := blockCount(t, dir); got != 1 {
		t.Fatalf("blocks = %d, want 1", got)
	}
}

func TestCompactExplicitBoundsKeptSeparate(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStoreWithOptions(dir, noAutoCompact)
	if err != nil {
		t.Fatal(err)
	}
	labels := model.LabelSet{{Name: model.MetricNameLabel, Value: "sizes"}}
	mk := func(bounds []float64, ts int64) ExplicitSeries {
		h := NewExplicit(bounds)
		h.Observe(1.5)
		return ExplicitSeries{ID: 1, Labels: labels, Timestamps: []int64{ts}, Buckets: []*ExplicitBucketHistogram{h}}
	}
	if _, err := st.WriteBlock(nil, []ExplicitSeries{mk([]float64{1, 2, 3}, 1000)}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.WriteBlock(nil, []ExplicitSeries{mk([]float64{10, 20}, 2000)}); err != nil {
		t.Fatal(err)
	}

	if _, err := st.Compact(); err != nil {
		t.Fatal(err)
	}
	dirData, err := ReadDirectory(filepath.Join(dir, mustSingleBlock(t, dir)))
	if err != nil {
		t.Fatal(err)
	}
	if len(dirData.Series) != 2 {
		t.Fatalf("series entries = %d, want 2 (different bounds stay separate)", len(dirData.Series))
	}
	sum, err := st.Summary(index.NewSelector(index.MetricName("sizes")), fullRange())
	if err != nil {
		t.Fatal(err)
	}
	if sum.Count != 2 {
		t.Errorf("count = %d, want 2", sum.Count)
	}
}

func mustSingleBlock(t *testing.T, dir string) string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "hblock-*.mhb"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("want exactly 1 block, got %v (err %v)", paths, err)
	}
	return filepath.Base(paths[0])
}

// The marker file itself must be valid JSON understood by recovery.
func TestCompactMarkerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	m := compactMarker{Dest: "hblock-000005.mhb", Sources: []string{"a", "b"}}
	if err := writeCompactMarker(dir, m); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, compactMarkerName))
	if err != nil {
		t.Fatal(err)
	}
	var got compactMarker
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Dest != m.Dest || len(got.Sources) != 2 {
		t.Fatalf("marker round-trip mismatch: %+v", got)
	}
}
