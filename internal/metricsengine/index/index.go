package index

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

type SeriesID uint64

type MatchOp uint8

const (
	MatchEqual MatchOp = iota + 1
	MatchRegexp
	MatchNotEqual
	MatchNotRegexp
)

type Matcher struct {
	Name  string
	Op    MatchOp
	Value string
}

type Selector struct {
	Matchers []Matcher
}

var regexpCache sync.Map

func NewSelector(matchers ...Matcher) Selector {
	return Selector{Matchers: append([]Matcher(nil), matchers...)}
}

func MetricName(name string) Matcher {
	return LabelEqual(model.MetricNameLabel, name)
}

func LabelEqual(name string, value string) Matcher {
	return Matcher{Name: name, Op: MatchEqual, Value: value}
}

func LabelRegexp(name string, pattern string) Matcher {
	return Matcher{Name: name, Op: MatchRegexp, Value: pattern}
}

func LabelNotEqual(name string, value string) Matcher {
	return Matcher{Name: name, Op: MatchNotEqual, Value: value}
}

func LabelNotRegexp(name string, pattern string) Matcher {
	return Matcher{Name: name, Op: MatchNotRegexp, Value: pattern}
}

func (s Selector) Validate() error {
	for _, matcher := range s.Matchers {
		if err := matcher.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s Selector) Optimized() Selector {
	out := Selector{Matchers: append([]Matcher(nil), s.Matchers...)}
	sort.SliceStable(out.Matchers, func(i, j int) bool {
		left := matcherCost(out.Matchers[i])
		right := matcherCost(out.Matchers[j])
		if left != right {
			return left < right
		}
		if out.Matchers[i].Name != out.Matchers[j].Name {
			return out.Matchers[i].Name < out.Matchers[j].Name
		}
		return out.Matchers[i].Value < out.Matchers[j].Value
	})
	return out
}

type Registry struct {
	mu       sync.RWMutex
	next     SeriesID
	byFP     map[uint64][]seriesEntry
	labels   map[SeriesID]model.LabelSet
	postings map[labelPair]map[SeriesID]struct{}
}

type seriesEntry struct {
	id     SeriesID
	labels model.LabelSet
}

type labelPair struct {
	name  string
	value string
}

func NewRegistry() *Registry {
	return &Registry{
		next:     1,
		byFP:     make(map[uint64][]seriesEntry),
		labels:   make(map[SeriesID]model.LabelSet),
		postings: make(map[labelPair]map[SeriesID]struct{}),
	}
}

func (r *Registry) GetOrCreate(labels model.LabelSet) SeriesID {
	canonical := labels.Canonical()
	fp := canonical.Fingerprint()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, entry := range r.byFP[fp] {
		if entry.labels.Equal(canonical) {
			return entry.id
		}
	}

	id := r.next
	r.next++
	r.byFP[fp] = append(r.byFP[fp], seriesEntry{id: id, labels: canonical})
	r.labels[id] = canonical
	for _, label := range canonical {
		key := labelPair{name: label.Name, value: label.Value}
		if r.postings[key] == nil {
			r.postings[key] = make(map[SeriesID]struct{})
		}
		r.postings[key][id] = struct{}{}
	}
	return id
}

func (r *Registry) Import(id SeriesID, labels model.LabelSet) {
	canonical := labels.Canonical()
	fp := canonical.Fingerprint()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, entry := range r.byFP[fp] {
		if entry.id == id || entry.labels.Equal(canonical) {
			return
		}
	}
	r.byFP[fp] = append(r.byFP[fp], seriesEntry{id: id, labels: canonical})
	r.labels[id] = canonical
	for _, label := range canonical {
		key := labelPair{name: label.Name, value: label.Value}
		if r.postings[key] == nil {
			r.postings[key] = make(map[SeriesID]struct{})
		}
		r.postings[key][id] = struct{}{}
	}
	if id >= r.next {
		r.next = id + 1
	}
}

func (r *Registry) Labels(id SeriesID) (model.LabelSet, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	labels, ok := r.labels[id]
	return append(model.LabelSet(nil), labels...), ok
}

func (r *Registry) Match(selector Selector) ([]SeriesID, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(selector.Matchers) == 0 {
		ids := make([]SeriesID, 0, len(r.labels))
		for id := range r.labels {
			ids = append(ids, id)
		}
		sortSeriesIDs(ids)
		return ids, nil
	}

	matchers := append([]Matcher(nil), selector.Matchers...)
	sort.Slice(matchers, func(i, j int) bool {
		return len(r.postings[labelPair{name: matchers[i].Name, value: matchers[i].Value}]) <
			len(r.postings[labelPair{name: matchers[j].Name, value: matchers[j].Value}])
	})

	var current map[SeriesID]struct{}
	for i, matcher := range matchers {
		if matcher.Op != MatchEqual {
			return nil, errors.New("index: only equality matchers are implemented in registry")
		}
		posting := r.postings[labelPair{name: matcher.Name, value: matcher.Value}]
		if i == 0 {
			current = clonePosting(posting)
			continue
		}
		for id := range current {
			if _, ok := posting[id]; !ok {
				delete(current, id)
			}
		}
	}

	out := make([]SeriesID, 0, len(current))
	for id := range current {
		out = append(out, id)
	}
	sortSeriesIDs(out)
	return out, nil
}

func (m Matcher) Matches(value string) bool {
	switch m.Op {
	case MatchEqual:
		return value == m.Value
	case MatchRegexp:
		re, ok := cachedRegexp(m.Value)
		return ok && re.MatchString(value)
	case MatchNotEqual:
		return value != m.Value
	case MatchNotRegexp:
		re, ok := cachedRegexp(m.Value)
		return ok && !re.MatchString(value)
	default:
		return false
	}
}

func (m Matcher) Validate() error {
	if m.Name == "" {
		return errors.New("index: empty matcher name")
	}
	switch m.Op {
	case MatchEqual, MatchNotEqual:
		return nil
	case MatchRegexp, MatchNotRegexp:
		if _, err := regexp.Compile(m.Value); err != nil {
			return fmt.Errorf("index: invalid regexp matcher %q: %w", m.Name, err)
		}
		return nil
	default:
		return fmt.Errorf("index: unsupported matcher op %d", m.Op)
	}
}

func matcherCost(m Matcher) int {
	switch m.Op {
	case MatchEqual:
		return 0
	case MatchNotEqual:
		return 1
	case MatchRegexp:
		return 2
	case MatchNotRegexp:
		return 3
	default:
		return 4
	}
}

func cachedRegexp(pattern string) (*regexp.Regexp, bool) {
	if cached, ok := regexpCache.Load(pattern); ok {
		re, ok := cached.(*regexp.Regexp)
		return re, ok
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, false
	}
	actual, _ := regexpCache.LoadOrStore(pattern, re)
	re, ok := actual.(*regexp.Regexp)
	return re, ok
}

func clonePosting(in map[SeriesID]struct{}) map[SeriesID]struct{} {
	out := make(map[SeriesID]struct{}, len(in))
	for id := range in {
		out[id] = struct{}{}
	}
	return out
}

func sortSeriesIDs(ids []SeriesID) {
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
}
