package histogram

// Path-based query and merge functions over histogram block files. Lifecycle
// (which blocks exist, retention, compaction scheduling) is owned by the main
// metrics store; this package only knows how to read, match, and merge.

import (
	"errors"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TimeRange bounds a query window.
type TimeRange struct {
	Start int64
	End   int64
}

// FullRange covers all time.
func FullRange() TimeRange { return TimeRange{Start: math.MinInt64, End: math.MaxInt64} }

func (tr TimeRange) Contains(ts int64) bool { return ts >= tr.Start && ts <= tr.End }

func (tr TimeRange) Overlaps(mn, mx int64) bool { return mn <= tr.End && mx >= tr.Start }

// MatchedExp is one label-distinct group of matched exp sketches, folded into a
// single accumulator as they arrive so the query never holds every in-window
// sketch at once (caps peak memory at one histogram per matched series).
type MatchedExp struct {
	Labels model.LabelSet
	Merged *ExponentialHistogram
}

// Add folds one in-window sketch into the accumulator. h is not retained or
// mutated (the first Add clones it).
func (m *MatchedExp) Add(h *ExponentialHistogram) {
	m.Merged = foldExp(m.Merged, h)
}

// AddMerged folds another series' accumulator into this one (e.g. head sketches
// into the block-collected map for the same fingerprint).
func (m *MatchedExp) AddMerged(other *MatchedExp) {
	if other != nil {
		m.Merged = foldExp(m.Merged, other.Merged)
	}
}

// CollectExp gathers, grouped by label fingerprint, every exp sketch in the
// given blocks whose ticks fall within tr and whose labels match the
// selector. Blocks that vanish mid-read (concurrent compaction) are skipped.
func CollectExp(paths []string, selector index.Selector, tr TimeRange) (map[uint64]*MatchedExp, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	out := make(map[uint64]*MatchedExp)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return nil, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.Overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if e.Kind != KindExponential {
				continue
			}
			if !MatchLabels(e.Labels, selector) || !tr.Overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			dec, err := DecodeExpSeries(data, e)
			if err != nil {
				return nil, err
			}
			fp := e.Labels.Fingerprint()
			m := out[fp]
			if m == nil {
				m = &MatchedExp{Labels: e.Labels}
				out[fp] = m
			}
			for i, ts := range dec.Timestamps {
				if tr.Contains(ts) {
					m.Add(dec.Sketches[i])
				}
			}
		}
	}
	return out, nil
}

// SummaryForPaths accumulates the synopsis (sum/count/min/max) of every
// matching series tick in the window across the given blocks.
func SummaryForPaths(paths []string, selector index.Selector, tr TimeRange) (Synopsis, error) {
	if err := selector.Validate(); err != nil {
		return Synopsis{}, err
	}
	unbounded := tr.Start == math.MinInt64 && tr.End == math.MaxInt64
	var acc Synopsis
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return Synopsis{}, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return Synopsis{}, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.Overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if !MatchLabels(e.Labels, selector) || !tr.Overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			if unbounded || (e.TimeMin >= tr.Start && e.TimeMax <= tr.End) {
				acc = MergeSynopsis(acc, e.Synopsis)
				continue
			}
			syn, err := decodeSeriesSynopsis(data, e, dir, tr)
			if err != nil {
				return Synopsis{}, err
			}
			acc = MergeSynopsis(acc, syn)
		}
	}
	return acc, nil
}

func decodeSeriesSynopsis(data []byte, e HistEntry, dir Directory, tr TimeRange) (Synopsis, error) {
	var acc Synopsis
	switch e.Kind {
	case KindExponential:
		dec, err := DecodeExpSeries(data, e)
		if err != nil {
			return Synopsis{}, err
		}
		for i, ts := range dec.Timestamps {
			if tr.Contains(ts) {
				acc = MergeSynopsis(acc, expSynopsis([]*ExponentialHistogram{dec.Sketches[i]}))
			}
		}
	case KindExplicit:
		bounds := dir.Bounds[e.BoundsRef]
		ts, bk, err := decodeExplicitPayload(data[e.PayloadOff:e.PayloadOff+e.PayloadLen], e.TickCount, bounds)
		if err != nil {
			return Synopsis{}, err
		}
		for i, t := range ts {
			if tr.Contains(t) {
				acc = MergeSynopsis(acc, explicitSynopsis([]*ExplicitBucketHistogram{bk[i]}))
			}
		}
	}
	return acc, nil
}

// ExplicitQuantileForPaths merges matching explicit-bucket histograms (which
// must share boundaries) over the window and answers the quantile.
func ExplicitQuantileForPaths(paths []string, selector index.Selector, tr TimeRange) (*ExplicitBucketHistogram, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	var merged *ExplicitBucketHistogram
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return nil, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.Overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if e.Kind != KindExplicit || !MatchLabels(e.Labels, selector) || !tr.Overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			bounds := dir.Bounds[e.BoundsRef]
			ts, bk, err := decodeExplicitPayload(data[e.PayloadOff:e.PayloadOff+e.PayloadLen], e.TickCount, bounds)
			if err != nil {
				return nil, err
			}
			if merged == nil {
				merged = NewExplicit(bounds)
			}
			for i, t := range ts {
				if tr.Contains(t) {
					if !MergeExplicit(merged, bk[i]) {
						return nil, errors.New("histogram: explicit bounds mismatch across matched series")
					}
				}
			}
		}
	}
	return merged, nil
}

// MetricNamesForPaths returns the sorted metric names across the blocks.
func MetricNamesForPaths(paths []string) ([]string, error) {
	seen := make(map[string]struct{})
	for _, path := range paths {
		dir, err := ReadDirectory(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range dir.Series {
			if v, ok := e.Labels.Get(model.MetricNameLabel); ok {
				seen[v] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// MergeBlocks reads the given blocks and merges their series: grouped by
// canonical labels (explicit series additionally by bounds - a series holds
// one bounds set), ticks sorted by timestamp, ticks older than cutoffMillis
// dropped (0 disables the cutoff). Series IDs keep the first-seen entry ID.
func MergeBlocks(paths []string, cutoffMillis int64) ([]ExpSeries, []ExplicitSeries, error) {
	type expGroup struct {
		id         uint64
		labels     model.LabelSet
		timestamps []int64
		sketches   []*ExponentialHistogram
	}
	type explicitGroup struct {
		id         uint64
		labels     model.LabelSet
		timestamps []int64
		buckets    []*ExplicitBucketHistogram
	}
	expGroups := make(map[string]*expGroup)
	explicitGroups := make(map[string]*explicitGroup)
	var expOrder, explicitOrder []string

	for _, path := range paths {
		exps, explicits, err := ReadBlock(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		for _, series := range exps {
			labels := series.Entry.Labels.Canonical()
			key := labels.Key()
			g := expGroups[key]
			if g == nil {
				g = &expGroup{id: series.Entry.SeriesID, labels: labels}
				expGroups[key] = g
				expOrder = append(expOrder, key)
			}
			for i, ts := range series.Timestamps {
				if cutoffMillis > 0 && ts < cutoffMillis {
					continue
				}
				g.timestamps = append(g.timestamps, ts)
				g.sketches = append(g.sketches, series.Sketches[i])
			}
		}
		for _, series := range explicits {
			labels := series.Entry.Labels.Canonical()
			key := labels.Key() + "\x00" + boundsKey(series.Bounds)
			g := explicitGroups[key]
			if g == nil {
				g = &explicitGroup{id: series.Entry.SeriesID, labels: labels}
				explicitGroups[key] = g
				explicitOrder = append(explicitOrder, key)
			}
			for i, ts := range series.Timestamps {
				if cutoffMillis > 0 && ts < cutoffMillis {
					continue
				}
				g.timestamps = append(g.timestamps, ts)
				g.buckets = append(g.buckets, series.Buckets[i])
			}
		}
	}

	exp := make([]ExpSeries, 0, len(expOrder))
	explicit := make([]ExplicitSeries, 0, len(explicitOrder))
	for _, key := range expOrder {
		g := expGroups[key]
		if len(g.timestamps) == 0 {
			continue
		}
		sortExpTicks(g.timestamps, g.sketches)
		exp = append(exp, ExpSeries{ID: g.id, Labels: g.labels, Timestamps: g.timestamps, Sketches: g.sketches})
	}
	for _, key := range explicitOrder {
		g := explicitGroups[key]
		if len(g.timestamps) == 0 {
			continue
		}
		sortExplicitTicks(g.timestamps, g.buckets)
		explicit = append(explicit, ExplicitSeries{ID: g.id, Labels: g.labels, Timestamps: g.timestamps, Buckets: g.buckets})
	}
	return exp, explicit, nil
}

func sortExpTicks(timestamps []int64, sketches []*ExponentialHistogram) {
	if sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		return
	}
	idx := sortedTickIndex(timestamps)
	tmpTS := make([]int64, len(timestamps))
	tmpSK := make([]*ExponentialHistogram, len(sketches))
	for i, j := range idx {
		tmpTS[i], tmpSK[i] = timestamps[j], sketches[j]
	}
	copy(timestamps, tmpTS)
	copy(sketches, tmpSK)
}

func sortExplicitTicks(timestamps []int64, buckets []*ExplicitBucketHistogram) {
	if sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		return
	}
	idx := sortedTickIndex(timestamps)
	tmpTS := make([]int64, len(timestamps))
	tmpBK := make([]*ExplicitBucketHistogram, len(buckets))
	for i, j := range idx {
		tmpTS[i], tmpBK[i] = timestamps[j], buckets[j]
	}
	copy(timestamps, tmpTS)
	copy(buckets, tmpBK)
}

func sortedTickIndex(timestamps []int64) []int {
	idx := make([]int, len(timestamps))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return timestamps[idx[a]] < timestamps[idx[b]] })
	return idx
}

// MergeSynopsis combines two synopses.
func MergeSynopsis(a, b Synopsis) Synopsis {
	a.Sum += b.Sum
	a.Count += b.Count
	if b.HasMinMax {
		if !a.HasMinMax {
			a.Min, a.Max, a.HasMinMax = b.Min, b.Max, true
		} else {
			if b.Min < a.Min {
				a.Min = b.Min
			}
			if b.Max > a.Max {
				a.Max = b.Max
			}
		}
	}
	return a
}

// GroupKey renders the "sum by" grouping key for a label set.
func GroupKey(labels model.LabelSet, by []string) string {
	if len(by) == 0 {
		return ""
	}
	var b strings.Builder
	for i, name := range by {
		if i > 0 {
			b.WriteByte(',')
		}
		v, _ := labels.Get(name)
		b.WriteString(strconv.Quote(name))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(v))
	}
	return b.String()
}

// MatchLabels reports whether a label set satisfies every matcher.
func MatchLabels(labels model.LabelSet, selector index.Selector) bool {
	for _, m := range selector.Matchers {
		got, ok := labels.Get(m.Name)
		if !ok {
			if m.Op == index.MatchNotEqual || m.Op == index.MatchNotRegexp {
				continue
			}
			return false
		}
		if !m.Matches(got) {
			return false
		}
	}
	return true
}

// QuantileForPaths merges every matching exp sketch in the window across the
// blocks and reads one quantile. Returns NaN when nothing matched.
func QuantileForPaths(paths []string, selector index.Selector, q float64, tr TimeRange) (float64, error) {
	matched, err := CollectExp(paths, selector, tr)
	if err != nil {
		return 0, err
	}
	all := make([]*ExponentialHistogram, 0, len(matched))
	for _, m := range matched {
		all = append(all, m.Merged)
	}
	merged := MergeAll(all)
	if merged == nil {
		return math.NaN(), nil
	}
	return merged.Quantile(q), nil
}

// QuantileByForPaths groups matches by the given labels and answers one
// quantile per group.
func QuantileByForPaths(paths []string, selector index.Selector, q float64, tr TimeRange, by []string) (map[string]float64, error) {
	matched, err := CollectExp(paths, selector, tr)
	if err != nil {
		return nil, err
	}
	groups := make(map[string][]*ExponentialHistogram)
	for _, m := range matched {
		key := GroupKey(m.Labels, by)
		groups[key] = append(groups[key], m.Merged)
	}
	out := make(map[string]float64, len(groups))
	for key, hists := range groups {
		merged := MergeAll(hists)
		if merged == nil {
			continue
		}
		out[key] = merged.Quantile(q)
	}
	return out, nil
}
