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
	// lastTouch is the most recent ingest timestamp (Unix ms) per series.
	// Updated by GetOrCreateAt on every append; read by the eviction sweep
	// to decide whether a series is cold (now-lastTouch > retention).
	// INDEX_EVICTION_SPEC_v0.md §1: cold-criterion = last_touch + retention.
	lastTouch map[SeriesID]int64

	// Bucketed timing wheel for the eviction sweep. Each series is in one
	// bucket keyed by (last_touch + retention) / bucketGranularity.
	// bucketGranularity == 0 disables bucketing.
	bucketGranularity int64
	bucketRetention   int64
	buckets           map[int64]map[SeriesID]struct{}
	bucketOf          map[SeriesID]int64
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
		next:      1,
		byFP:      make(map[uint64][]seriesEntry),
		labels:    make(map[SeriesID]model.LabelSet),
		postings:  make(map[labelPair]map[SeriesID]struct{}),
		lastTouch: make(map[SeriesID]int64),
		buckets:   make(map[int64]map[SeriesID]struct{}),
		bucketOf:  make(map[SeriesID]int64),
	}
}

// SetEvictionBucketing enables bucketed eviction.
// retention and granularity are in milliseconds. Changing either value
// re-buckets all existing series.
func (r *Registry) SetEvictionBucketing(retention, granularity int64) {
	if granularity <= 0 || retention <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.bucketRetention == retention && r.bucketGranularity == granularity {
		return
	}
	r.bucketRetention = retention
	r.bucketGranularity = granularity
	r.buckets = make(map[int64]map[SeriesID]struct{})
	r.bucketOf = make(map[SeriesID]int64)
	for id, lt := range r.lastTouch {
		r.placeInBucketLocked(id, lt)
	}
}

// placeInBucketLocked inserts id into the bucket for lastTouch.
// The caller must hold r.mu for writing.
func (r *Registry) placeInBucketLocked(id SeriesID, lastTouch int64) {
	if r.bucketGranularity <= 0 {
		return
	}
	var epoch int64
	if lastTouch > 0 {
		epoch = (lastTouch + r.bucketRetention) / r.bucketGranularity
	}
	if r.buckets[epoch] == nil {
		r.buckets[epoch] = make(map[SeriesID]struct{})
	}
	r.buckets[epoch][id] = struct{}{}
	r.bucketOf[id] = epoch
}

// rebucketIfMovedLocked moves id when its last-touch bucket changes.
func (r *Registry) rebucketIfMovedLocked(id SeriesID, newLastTouch int64) {
	if r.bucketGranularity <= 0 {
		return
	}
	newEpoch := int64(0)
	if newLastTouch > 0 {
		newEpoch = (newLastTouch + r.bucketRetention) / r.bucketGranularity
	}
	oldEpoch, ok := r.bucketOf[id]
	if ok && oldEpoch == newEpoch {
		return
	}
	if ok {
		if b := r.buckets[oldEpoch]; b != nil {
			delete(b, id)
			if len(b) == 0 {
				delete(r.buckets, oldEpoch)
			}
		}
	}
	if r.buckets[newEpoch] == nil {
		r.buckets[newEpoch] = make(map[SeriesID]struct{})
	}
	r.buckets[newEpoch][id] = struct{}{}
	r.bucketOf[id] = newEpoch
}

// GetOrCreate looks up a series by canonical labels or assigns a new id.
// It does not update last-touch; ingest callers should use GetOrCreateAt.
func (r *Registry) GetOrCreate(labels model.LabelSet) SeriesID {
	return r.GetOrCreateAt(labels, 0)
}

// GetOrCreateAt is GetOrCreate plus a last-touch update.
// Pass ts=0 from non-ingest call sites. The sweep only treats non-zero
// last-touch values as observed activity.
func (r *Registry) GetOrCreateAt(labels model.LabelSet, ts int64) SeriesID {
	id, _ := r.getOrCreateAtInternal(labels, ts)
	return id
}

// GetOrCreateAtReportCreated is GetOrCreateAt plus a created flag.
func (r *Registry) GetOrCreateAtReportCreated(labels model.LabelSet, ts int64) (SeriesID, bool) {
	return r.getOrCreateAtInternal(labels, ts)
}

func (r *Registry) getOrCreateAtInternal(labels model.LabelSet, ts int64) (SeriesID, bool) {
	canonical := labels.Canonical()
	fp := canonical.Fingerprint()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, entry := range r.byFP[fp] {
		if entry.labels.Equal(canonical) {
			if ts > r.lastTouch[entry.id] {
				r.lastTouch[entry.id] = ts
				r.rebucketIfMovedLocked(entry.id, ts)
			}
			return entry.id, false
		}
	}

	id := r.next
	r.next++
	r.byFP[fp] = append(r.byFP[fp], seriesEntry{id: id, labels: canonical})
	r.labels[id] = canonical
	r.lastTouch[id] = ts
	for _, label := range canonical {
		key := labelPair{name: label.Name, value: label.Value}
		if r.postings[key] == nil {
			r.postings[key] = make(map[SeriesID]struct{})
		}
		r.postings[key][id] = struct{}{}
	}
	r.placeInBucketLocked(id, ts)
	return id, true
}

// Import re-registers a series at a specific ID during catalog recovery.
// It sets lastTouch to 0, which Sweep treats as unknown.
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
	r.lastTouch[id] = 0
	for _, label := range canonical {
		key := labelPair{name: label.Name, value: label.Value}
		if r.postings[key] == nil {
			r.postings[key] = make(map[SeriesID]struct{})
		}
		r.postings[key][id] = struct{}{}
	}
	r.placeInBucketLocked(id, 0)
	if id >= r.next {
		r.next = id + 1
	}
}

// SeriesCount returns the number of non-evicted series tracked by the registry.
func (r *Registry) SeriesCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.labels)
}

// LastTouch returns the most recent ingest timestamp for id.
// A zero timestamp means the series has not been reconciled with real activity.
func (r *Registry) LastTouch(id SeriesID) (int64, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ts, ok := r.lastTouch[id]
	return ts, ok
}

// UpdateLastTouch advances the last-touch timestamp for a known series.
// Out-of-order updates do not regress the value. It returns false for unknown
// series IDs.
func (r *Registry) UpdateLastTouch(id SeriesID, ts int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.labels[id]; !ok {
		return false
	}
	if ts > r.lastTouch[id] {
		r.lastTouch[id] = ts
		r.rebucketIfMovedLocked(id, ts)
	}
	return true
}

// Sweep evicts series whose last-touch is older than now-retention.
// Series with lastTouch=0 are skipped until reconciliation or ingest gives them
// a real timestamp. The caller persists returned evictions after Sweep releases
// the registry lock.
func (r *Registry) Sweep(now int64) []SeriesID {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.bucketGranularity <= 0 {
		// Fallback to full scan.
		return r.sweepFullLocked(now, r.bucketRetention)
	}
	threshold := now - r.bucketRetention
	if threshold <= 0 {
		return nil
	}
	expiredEpochCeil := now / r.bucketGranularity
	evicted := make([]SeriesID, 0, len(r.lastTouch))
	for epoch, bucket := range r.buckets {
		if epoch == 0 || epoch > expiredEpochCeil {
			continue
		}
		for id := range bucket {
			lt := r.lastTouch[id]
			if lt == 0 || lt > threshold {
				continue
			}
			r.evictLocked(id)
			evicted = append(evicted, id)
		}
	}
	return evicted
}

func (r *Registry) sweepFullLocked(now, retention int64) []SeriesID {
	if retention <= 0 {
		return nil
	}
	threshold := now - retention
	evicted := make([]SeriesID, 0, len(r.lastTouch))
	for id, lt := range r.lastTouch {
		if lt == 0 || lt > threshold {
			continue
		}
		r.evictLocked(id)
		evicted = append(evicted, id)
	}
	return evicted
}

// evictLocked removes a series from all index structures. Caller must
// hold r.mu (write).
func (r *Registry) evictLocked(id SeriesID) {
	labels, ok := r.labels[id]
	if !ok {
		return
	}
	delete(r.labels, id)
	delete(r.lastTouch, id)
	if epoch, ok := r.bucketOf[id]; ok {
		if b := r.buckets[epoch]; b != nil {
			delete(b, id)
			if len(b) == 0 {
				delete(r.buckets, epoch)
			}
		}
		delete(r.bucketOf, id)
	}
	// Remove from byFP.
	fp := labels.Fingerprint()
	entries := r.byFP[fp]
	for i, e := range entries {
		if e.id == id {
			r.byFP[fp] = append(entries[:i], entries[i+1:]...)
			break
		}
	}
	if len(r.byFP[fp]) == 0 {
		delete(r.byFP, fp)
	}
	// Remove from postings.
	for _, label := range labels {
		key := labelPair{name: label.Name, value: label.Value}
		if posting := r.postings[key]; posting != nil {
			delete(posting, id)
			if len(posting) == 0 {
				delete(r.postings, key)
			}
		}
	}
}

// LabelValues returns the sorted unique values for the given label name across
// all in-memory series. Used for cheap metric-name enumeration on the read path.
func (r *Registry) LabelValues(name string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for k := range r.postings {
		if k.name == name {
			out = append(out, k.value)
		}
	}
	sort.Strings(out)
	return out
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
