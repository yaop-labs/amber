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

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Store persists histogram blocks in a directory and answers histogram queries
// by merging sketches across blocks and label groupings in the compressed
// domain. It never decodes exp-histograms to raw points.
type Store struct {
	dir string
	mu  sync.Mutex
	seq int
}

// OpenStore opens (creating if needed) a histogram store rooted at dir.
func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("histogram: dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	// Resume the block sequence past any existing blocks.
	paths, _ := s.blockPaths()
	s.seq = len(paths)
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
	path := filepath.Join(s.dir, fmt.Sprintf("hblock-%06d.mhb", s.seq))
	if err := WriteBlock(path, exp, explicit); err != nil {
		return "", err
	}
	s.seq++
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

// Summary answers sum/count/min/max over matching histograms. When the window is
// unbounded it is answered purely from the directory synopsis (zone-map) without
// decoding any sketch; otherwise it decodes only the ticks inside the window.
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
