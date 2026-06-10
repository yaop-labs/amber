package histogram

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Store persists histogram blocks in a directory and answers histogram queries
// by merging sketches across blocks and label groupings in the compressed
// domain. It never decodes exp-histograms to raw points.
type Store struct {
	dir   string
	opts  Options
	clock func() time.Time
	mu    sync.Mutex
	seq   int
}

// Options configures histogram retention and cardinality limits.
type Options struct {
	Retention          time.Duration
	MaxActiveSeries    int
	MaxLabelsPerSeries int
	Clock              func() time.Time
}

// OpenStore opens a histogram store rooted at dir.
func OpenStore(dir string) (*Store, error) {
	return OpenStoreWithOptions(dir, Options{})
}

// OpenStoreWithOptions opens a histogram store with options.
func OpenStoreWithOptions(dir string, opts Options) (*Store, error) {
	if dir == "" {
		return nil, errors.New("histogram: dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = time.Now
	}
	s := &Store{dir: dir, opts: opts, clock: clock}
	paths, err := s.blockPaths()
	if err != nil {
		return nil, err
	}
	s.seq = nextBlockSeq(paths)
	if err := s.enforceRetentionLocked(paths); err != nil {
		return nil, err
	}
	return s, nil
}

// TimeRange bounds a query window. Zero value means unbounded.
type TimeRange struct {
	Start int64
	End   int64
}

func fullRange() TimeRange { return TimeRange{Start: math.MinInt64, End: math.MaxInt64} }

func (tr TimeRange) contains(ts int64) bool { return ts >= tr.Start && ts <= tr.End }

func (tr TimeRange) overlaps(mn, mx int64) bool { return mn <= tr.End && mx >= tr.Start }

// WriteBlock writes a new immutable block and returns its path.
func (s *Store) WriteBlock(exp []ExpSeries, explicit []ExplicitSeries) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.validateWriteLocked(exp, explicit); err != nil {
		return "", err
	}
	path := filepath.Join(s.dir, fmt.Sprintf("hblock-%06d.mhb", s.seq))
	if err := WriteBlock(path, exp, explicit); err != nil {
		return "", err
	}
	s.seq++
	if err := s.enforceRetentionLocked(nil); err != nil {
		return "", err
	}
	return path, nil
}

func (s *Store) blockPaths() ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(s.dir, "hblock-*.mhb"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func nextBlockSeq(paths []string) int {
	maxSeq := -1
	for _, path := range paths {
		base := filepath.Base(path)
		if !strings.HasPrefix(base, "hblock-") || !strings.HasSuffix(base, ".mhb") {
			continue
		}
		raw := strings.TrimSuffix(strings.TrimPrefix(base, "hblock-"), ".mhb")
		n, err := strconv.Atoi(raw)
		if err != nil {
			continue
		}
		if n > maxSeq {
			maxSeq = n
		}
	}
	return maxSeq + 1
}

func (s *Store) validateWriteLocked(exp []ExpSeries, explicit []ExplicitSeries) error {
	incoming := make(map[string]model.LabelSet)
	for _, series := range exp {
		if err := validateLabels(series.Labels, s.opts); err != nil {
			return err
		}
		labels := series.Labels.Canonical()
		incoming[labels.Key()] = labels
	}
	for _, series := range explicit {
		if err := validateLabels(series.Labels, s.opts); err != nil {
			return err
		}
		labels := series.Labels.Canonical()
		incoming[labels.Key()] = labels
	}
	if s.opts.MaxActiveSeries <= 0 || len(incoming) == 0 {
		return nil
	}

	existing, err := s.seriesKeysLocked()
	if err != nil {
		return err
	}
	newCount := 0
	for key := range incoming {
		if _, ok := existing[key]; !ok {
			newCount++
		}
	}
	if len(existing)+newCount > s.opts.MaxActiveSeries {
		return fmt.Errorf("histogram: active series limit exceeded: have %d new %d max %d", len(existing), newCount, s.opts.MaxActiveSeries)
	}
	return nil
}

func validateLabels(labels model.LabelSet, opts Options) error {
	if opts.MaxLabelsPerSeries > 0 && len(labels) > opts.MaxLabelsPerSeries {
		return fmt.Errorf("histogram: label limit exceeded: labels %d max %d", len(labels), opts.MaxLabelsPerSeries)
	}
	for _, label := range labels.Canonical() {
		if label.Name == "" {
			return errors.New("histogram: invalid labels: empty label name")
		}
	}
	return nil
}

func (s *Store) seriesKeysLocked() (map[string]struct{}, error) {
	paths, err := s.blockPaths()
	if err != nil {
		return nil, err
	}
	keys := make(map[string]struct{})
	for _, path := range paths {
		dir, err := ReadDirectory(path)
		if err != nil {
			return nil, err
		}
		for _, entry := range dir.Series {
			keys[entry.Labels.Canonical().Key()] = struct{}{}
		}
	}
	return keys, nil
}

func (s *Store) enforceRetentionLocked(paths []string) error {
	if s.opts.Retention <= 0 {
		return nil
	}
	if paths == nil {
		var err error
		paths, err = s.blockPaths()
		if err != nil {
			return err
		}
	}
	cutoff := s.clock().Add(-s.opts.Retention).UnixMilli()
	for _, path := range paths {
		dir, err := ReadDirectory(path)
		if err != nil {
			return err
		}
		_, maxTS, ok := dir.TimeRange()
		if ok && maxTS < cutoff {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("histogram: remove expired block %s: %w", filepath.Base(path), err)
			}
		}
	}
	return nil
}

// MetricNames returns the sorted metric names in histogram blocks.
func (s *Store) MetricNames() ([]string, error) {
	paths, err := s.blockPaths()
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	for _, path := range paths {
		dir, err := ReadDirectory(path)
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

// Stats summarizes the histogram store.
type Stats struct {
	Blocks  int
	Series  int
	Bytes   int64
	MinTime int64
	MaxTime int64
	HasTime bool
}

func (s *Store) Stats() (Stats, error) {
	paths, err := s.blockPaths()
	if err != nil {
		return Stats{}, err
	}
	st := Stats{Blocks: len(paths)}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return Stats{}, err
		}
		st.Bytes += info.Size()
		dir, err := ReadDirectory(path)
		if err != nil {
			return Stats{}, err
		}
		st.Series += len(dir.Series)
		if mn, mx, ok := dir.TimeRange(); ok {
			if !st.HasTime {
				st.MinTime, st.MaxTime = mn, mx
				st.HasTime = true
			} else {
				if mn < st.MinTime {
					st.MinTime = mn
				}
				if mx > st.MaxTime {
					st.MaxTime = mx
				}
			}
		}
	}
	return st, nil
}

// matchedExp collects, per series ID, all exp sketches across blocks whose ticks
// fall within tr and whose labels match the selector. Labels are carried for
// grouping.
type matchedExp struct {
	labels   model.LabelSet
	sketches []*ExponentialHistogram
}

func (s *Store) collectExp(selector index.Selector, tr TimeRange) (map[uint64]*matchedExp, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	paths, err := s.blockPaths()
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]*matchedExp)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return nil, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if e.Kind != KindExponential {
				continue
			}
			if !matchLabels(e.Labels, selector) || !tr.overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			dec, err := DecodeExpSeries(data, e)
			if err != nil {
				return nil, err
			}
			// Key by label fingerprint, not SeriesID: each block assigns its
			// own local IDs (1..N), so identical series across blocks would
			// collide and silently merge into one group otherwise. Two POSTs
			// of the same labels become two blocks with SeriesID=1 each,
			// which the read path must reunify.
			fp := e.Labels.Fingerprint()
			m := out[fp]
			if m == nil {
				m = &matchedExp{labels: e.Labels}
				out[fp] = m
			}
			for i, ts := range dec.Timestamps {
				if tr.contains(ts) {
					m.sketches = append(m.sketches, dec.Sketches[i])
				}
			}
		}
	}
	return out, nil
}

// HistogramQuantile resolves the selector, merges every matching exp-histogram
// sketch in the window into one sketch, and reads a single quantile off it.
func (s *Store) HistogramQuantile(selector index.Selector, q float64, tr TimeRange) (float64, error) {
	matched, err := s.collectExp(selector, tr)
	if err != nil {
		return 0, err
	}
	all := make([]*ExponentialHistogram, 0)
	for _, m := range matched {
		all = append(all, m.sketches...)
	}
	merged := MergeAll(all)
	if merged == nil {
		return math.NaN(), nil
	}
	return merged.Quantile(q), nil
}

// HistogramQuantileBy groups matching series by the given labels (the "sum by"
// grouping), merges the sketches within each group, and returns one quantile per
// group key.
func (s *Store) HistogramQuantileBy(selector index.Selector, q float64, tr TimeRange, by []string) (map[string]float64, error) {
	matched, err := s.collectExp(selector, tr)
	if err != nil {
		return nil, err
	}
	groups := make(map[string][]*ExponentialHistogram)
	for _, m := range matched {
		key := groupKey(m.labels, by)
		groups[key] = append(groups[key], m.sketches...)
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

// MergeExp resolves the selector and returns the single merged sketch for the
// window (exposed for callers that want the full distribution, e.g. benches).
func (s *Store) MergeExp(selector index.Selector, tr TimeRange) (*ExponentialHistogram, error) {
	matched, err := s.collectExp(selector, tr)
	if err != nil {
		return nil, err
	}
	all := make([]*ExponentialHistogram, 0)
	for _, m := range matched {
		all = append(all, m.sketches...)
	}
	return MergeAll(all), nil
}

// Summary returns sum, count, min, and max for matching histograms.
func (s *Store) Summary(selector index.Selector, tr TimeRange) (Synopsis, error) {
	if err := selector.Validate(); err != nil {
		return Synopsis{}, err
	}
	paths, err := s.blockPaths()
	if err != nil {
		return Synopsis{}, err
	}
	unbounded := tr.Start == math.MinInt64 && tr.End == math.MaxInt64
	var acc Synopsis
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return Synopsis{}, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return Synopsis{}, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if !matchLabels(e.Labels, selector) || !tr.overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			if unbounded || (e.TimeMin >= tr.Start && e.TimeMax <= tr.End) {
				acc = mergeSynopsis(acc, e.Synopsis)
				continue
			}
			// Partial overlap: decode and accumulate only in-window ticks.
			syn, err := decodeSeriesSynopsis(data, e, dir, tr)
			if err != nil {
				return Synopsis{}, err
			}
			acc = mergeSynopsis(acc, syn)
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
			if tr.contains(ts) {
				acc = mergeSynopsis(acc, expSynopsis([]*ExponentialHistogram{dec.Sketches[i]}))
			}
		}
	case KindExplicit:
		bounds := dir.Bounds[e.BoundsRef]
		ts, bk, err := decodeExplicitPayload(data[e.PayloadOff:e.PayloadOff+e.PayloadLen], e.TickCount, bounds)
		if err != nil {
			return Synopsis{}, err
		}
		for i, t := range ts {
			if tr.contains(t) {
				acc = mergeSynopsis(acc, explicitSynopsis([]*ExplicitBucketHistogram{bk[i]}))
			}
		}
	}
	return acc, nil
}

// ExplicitQuantile merges matching explicit-bucket histograms (which must share
// boundaries) over the window and answers the quantile via linear interpolation.
func (s *Store) ExplicitQuantile(selector index.Selector, q float64, tr TimeRange) (float64, error) {
	if err := selector.Validate(); err != nil {
		return 0, err
	}
	paths, err := s.blockPaths()
	if err != nil {
		return 0, err
	}
	var merged *ExplicitBucketHistogram
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return 0, err
		}
		dir, err := parseDirectory(data)
		if err != nil {
			return 0, err
		}
		if mn, mx, ok := dir.TimeRange(); ok && !tr.overlaps(mn, mx) {
			continue
		}
		for _, e := range dir.Series {
			if e.Kind != KindExplicit || !matchLabels(e.Labels, selector) || !tr.overlaps(e.TimeMin, e.TimeMax) {
				continue
			}
			bounds := dir.Bounds[e.BoundsRef]
			ts, bk, err := decodeExplicitPayload(data[e.PayloadOff:e.PayloadOff+e.PayloadLen], e.TickCount, bounds)
			if err != nil {
				return 0, err
			}
			if merged == nil {
				merged = NewExplicit(bounds)
			}
			for i, t := range ts {
				if tr.contains(t) {
					if !MergeExplicit(merged, bk[i]) {
						return 0, errors.New("histogram: explicit bounds mismatch across matched series")
					}
				}
			}
		}
	}
	if merged == nil {
		return math.NaN(), nil
	}
	return merged.Quantile(q), nil
}

func mergeSynopsis(a, b Synopsis) Synopsis {
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

func groupKey(labels model.LabelSet, by []string) string {
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

// matchLabels reports whether a label set satisfies every matcher in a selector.
func matchLabels(labels model.LabelSet, selector index.Selector) bool {
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
