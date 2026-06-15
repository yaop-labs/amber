package store

// Histogram ingest, query, and compaction on the main store: sketches share
// the catalog, registry, WAL, flush lifecycle, manifest, retention, and
// eviction with scalar series.

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// AppendSketches ingests histogram ticks: catalog registration (REGISTER in
// the catalog log, cardinality limits) plus WAL-backed engine append.
func (s *Store) AppendSketches(samples []engine.SketchSample) ([]index.SeriesID, error) {
	labelSets := make([]model.LabelSet, 0, len(samples))
	for _, sample := range samples {
		labelSets = append(labelSets, sample.Labels)
	}
	if err := s.ensureCatalog(labelSets); err != nil {
		return nil, err
	}
	ids, err := s.engine.AppendSketches(samples)
	if err != nil {
		return nil, err
	}
	if err := s.flushAfterAppend(); err != nil {
		return ids, err
	}
	return ids, nil
}

// histBlockPathsLocked lists manifest histogram blocks overlapping tr and
// matching the selector via manifest label values.
func (s *Store) histBlockPathsLocked(selector index.Selector, tr histogram.TimeRange) []string {
	paths := make([]string, 0)
	for _, meta := range s.manifest.Blocks {
		if meta.Kind != BlockKindHistogram {
			continue
		}
		if !tr.Overlaps(meta.MinTime, meta.MaxTime) {
			continue
		}
		if !metaMatchesSelector(meta, selector) {
			continue
		}
		paths = append(paths, filepath.Join(s.dir, meta.Path))
	}
	return paths
}

func (s *Store) histBlockPaths(selector index.Selector, tr histogram.TimeRange) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.histBlockPathsLocked(selector, tr)
}

// headMatchedExp collects buffered exp sketches matching selector and tr,
// grouped by label fingerprint.
func (s *Store) headMatchedExp(selector index.Selector, tr histogram.TimeRange) map[uint64]*histogram.MatchedExp {
	exp, _ := s.engine.SketchSnapshot()
	out := make(map[uint64]*histogram.MatchedExp)
	for _, series := range exp {
		if !histogram.MatchLabels(series.Labels, selector) {
			continue
		}
		var picked []*histogram.ExponentialHistogram
		for i, ts := range series.Timestamps {
			if tr.Contains(ts) {
				picked = append(picked, series.Sketches[i])
			}
		}
		if len(picked) == 0 {
			continue
		}
		fp := series.Labels.Fingerprint()
		m := out[fp]
		if m == nil {
			m = &histogram.MatchedExp{Labels: series.Labels}
			out[fp] = m
		}
		m.Sketches = append(m.Sketches, picked...)
	}
	return out
}

func mergeMatched(dst, src map[uint64]*histogram.MatchedExp) {
	for fp, m := range src {
		if cur := dst[fp]; cur != nil {
			cur.Sketches = append(cur.Sketches, m.Sketches...)
		} else {
			dst[fp] = m
		}
	}
}

// HistogramQuantile merges every matching exp sketch (blocks + head) in the
// window and reads one quantile. NaN when nothing matched.
func (s *Store) HistogramQuantile(selector index.Selector, q float64, tr histogram.TimeRange) (float64, error) {
	matched, err := histogram.CollectExp(s.histBlockPaths(selector, tr), selector, tr)
	if err != nil {
		return 0, err
	}
	mergeMatched(matched, s.headMatchedExp(selector, tr))
	all := make([]*histogram.ExponentialHistogram, 0)
	for _, m := range matched {
		all = append(all, m.Sketches...)
	}
	merged := histogram.MergeAll(all)
	if merged == nil {
		return math.NaN(), nil
	}
	return merged.Quantile(q), nil
}

// HistogramQuantileBy groups matches by the given labels and answers one
// quantile per group.
func (s *Store) HistogramQuantileBy(selector index.Selector, q float64, tr histogram.TimeRange, by []string) (map[string]float64, error) {
	matched, err := histogram.CollectExp(s.histBlockPaths(selector, tr), selector, tr)
	if err != nil {
		return nil, err
	}
	mergeMatched(matched, s.headMatchedExp(selector, tr))
	groups := make(map[string][]*histogram.ExponentialHistogram)
	for _, m := range matched {
		key := histogram.GroupKey(m.Labels, by)
		groups[key] = append(groups[key], m.Sketches...)
	}
	out := make(map[string]float64, len(groups))
	for key, hists := range groups {
		merged := histogram.MergeAll(hists)
		if merged == nil {
			continue
		}
		out[key] = merged.Quantile(q)
	}
	return out, nil
}

// ExplicitQuantile merges matching explicit-bucket histograms (blocks + head;
// boundaries must agree) and answers the quantile via interpolation.
func (s *Store) ExplicitQuantile(selector index.Selector, q float64, tr histogram.TimeRange) (float64, error) {
	merged, err := histogram.ExplicitQuantileForPaths(s.histBlockPaths(selector, tr), selector, tr)
	if err != nil {
		return 0, err
	}
	_, explicit := s.engine.SketchSnapshot()
	for _, series := range explicit {
		if !histogram.MatchLabels(series.Labels, selector) {
			continue
		}
		for i, ts := range series.Timestamps {
			if !tr.Contains(ts) {
				continue
			}
			if merged == nil {
				merged = histogram.NewExplicit(series.Buckets[i].Bounds)
			}
			if !histogram.MergeExplicit(merged, series.Buckets[i]) {
				return 0, errExplicitBoundsMismatch
			}
		}
	}
	if merged == nil {
		return math.NaN(), nil
	}
	return merged.Quantile(q), nil
}

// HistogramSummary accumulates sum/count/min/max over matching ticks.
func (s *Store) HistogramSummary(selector index.Selector, tr histogram.TimeRange) (histogram.Synopsis, error) {
	acc, err := histogram.SummaryForPaths(s.histBlockPaths(selector, tr), selector, tr)
	if err != nil {
		return histogram.Synopsis{}, err
	}
	exp, explicit := s.engine.SketchSnapshot()
	for _, series := range exp {
		if !histogram.MatchLabels(series.Labels, selector) {
			continue
		}
		for i, ts := range series.Timestamps {
			if !tr.Contains(ts) {
				continue
			}
			sk := series.Sketches[i]
			syn := histogram.Synopsis{Sum: sk.Sum, Count: sk.Count}
			if sk.HasMinMax {
				syn.Min, syn.Max, syn.HasMinMax = sk.Min, sk.Max, true
			}
			acc = histogram.MergeSynopsis(acc, syn)
		}
	}
	for _, series := range explicit {
		if !histogram.MatchLabels(series.Labels, selector) {
			continue
		}
		for i, ts := range series.Timestamps {
			if !tr.Contains(ts) {
				continue
			}
			h := series.Buckets[i]
			syn := histogram.Synopsis{Sum: h.Sum, Count: h.Count}
			if h.HasMinMax {
				syn.Min, syn.Max, syn.HasMinMax = h.Min, h.Max, true
			}
			acc = histogram.MergeSynopsis(acc, syn)
		}
	}
	return acc, nil
}

// HistogramMetricNames returns sorted metric names across hist blocks + head.
func (s *Store) HistogramMetricNames() ([]string, error) {
	names, err := histogram.MetricNamesForPaths(s.histBlockPaths(index.Selector{}, histogram.FullRange()))
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(names))
	for _, n := range names {
		seen[n] = struct{}{}
	}
	exp, explicit := s.engine.SketchSnapshot()
	for _, series := range exp {
		if v, ok := series.Labels.Get(model.MetricNameLabel); ok {
			seen[v] = struct{}{}
		}
	}
	for _, series := range explicit {
		if v, ok := series.Labels.Get(model.MetricNameLabel); ok {
			seen[v] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// HistogramStats summarizes the histogram side of the store.
type HistogramStats struct {
	Blocks  int
	Series  int
	Bytes   int64
	MinTime int64
	MaxTime int64
	HasTime bool

	BufferedSeries  int
	BufferedSamples int
}

// HistStats returns histogram-block totals and the buffered sketch counts, the
// histogram counterpart of Stats.
func (s *Store) HistStats() (HistogramStats, error) {
	s.mu.RLock()
	metas := make([]BlockMeta, 0)
	for _, meta := range s.manifest.Blocks {
		if meta.Kind == BlockKindHistogram {
			metas = append(metas, meta)
		}
	}
	s.mu.RUnlock()

	st := HistogramStats{
		Blocks:          len(metas),
		BufferedSeries:  s.engine.BufferedSketchSeries(),
		BufferedSamples: s.engine.BufferedSketchSamples(),
	}
	for _, meta := range metas {
		if info, err := os.Stat(filepath.Join(s.dir, meta.Path)); err == nil {
			st.Bytes += info.Size()
		}
		st.Series += meta.SeriesCount
		if !st.HasTime {
			st.MinTime, st.MaxTime, st.HasTime = meta.MinTime, meta.MaxTime, true
		} else {
			if meta.MinTime < st.MinTime {
				st.MinTime = meta.MinTime
			}
			if meta.MaxTime > st.MaxTime {
				st.MaxTime = meta.MaxTime
			}
		}
	}
	return st, nil
}

// compactHistLocked merges all histogram blocks into one, dropping expired
// ticks. Caller holds s.mu. Returns the merged block path ("" if nothing to
// do or everything expired).
func (s *Store) compactHistLocked() (string, error) {
	var histMetas []BlockMeta
	var rest []BlockMeta
	for _, meta := range s.manifest.Blocks {
		if meta.Kind == BlockKindHistogram {
			histMetas = append(histMetas, meta)
		} else {
			rest = append(rest, meta)
		}
	}
	if len(histMetas) <= 1 {
		return "", nil
	}

	var cutoff int64
	if s.opts.Retention > 0 {
		cutoff = s.clock().Add(-s.opts.Retention).UnixMilli()
	}
	paths := make([]string, 0, len(histMetas))
	for _, meta := range histMetas {
		paths = append(paths, filepath.Join(s.dir, meta.Path))
	}
	exp, explicit, err := histogram.MergeBlocks(paths, cutoff)
	if err != nil {
		return "", err
	}

	if len(exp) == 0 && len(explicit) == 0 {
		s.manifest.Blocks = rest
		if err := saveManifest(s.dir, s.manifest); err != nil {
			return "", err
		}
		for _, path := range paths {
			_ = os.Remove(path)
		}
		return "", nil
	}

	dest := strings.TrimSuffix(s.nextBlockPathWithPrefix("hist-compact"), ".meb") + ".mhb"
	if err := histogram.WriteBlock(dest, exp, explicit); err != nil {
		return "", err
	}
	hdir, err := histogram.ReadDirectory(dest)
	if err != nil {
		return "", err
	}
	hmin, hmax, _ := hdir.TimeRange()
	s.manifest.Blocks = append(rest, BlockMeta{
		Path:        filepath.Base(dest),
		Kind:        BlockKindHistogram,
		MinTime:     hmin,
		MaxTime:     hmax,
		SeriesCount: len(hdir.Series),
		LabelValues: histLabelValues(hdir),
	})
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return "", err
	}
	for _, path := range paths {
		_ = os.Remove(path)
	}
	return dest, nil
}

// histLabelValues builds the manifest label-value pruning map for a
// histogram block directory.
func histLabelValues(dir histogram.Directory) map[string][]string {
	values := make(map[string]map[string]struct{})
	for _, e := range dir.Series {
		for _, label := range e.Labels {
			if values[label.Name] == nil {
				values[label.Name] = make(map[string]struct{})
			}
			values[label.Name][label.Value] = struct{}{}
		}
	}
	out := make(map[string][]string, len(values))
	for name, set := range values {
		list := make([]string, 0, len(set))
		for v := range set {
			list = append(list, v)
		}
		sort.Strings(list)
		out[name] = list
	}
	return out
}

var errExplicitBoundsMismatch = errors.New("store: explicit bounds mismatch across matched series")
