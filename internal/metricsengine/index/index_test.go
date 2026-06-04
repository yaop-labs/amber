package index

import (
	"strconv"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestRegistryMatch(t *testing.T) {
	reg := NewRegistry()
	api := reg.GetOrCreate(model.LabelSet{{Name: "job", Value: "api"}, {Name: "pod", Value: "a"}})
	reg.GetOrCreate(model.LabelSet{{Name: "job", Value: "worker"}, {Name: "pod", Value: "b"}})

	ids, err := reg.Match(Selector{Matchers: []Matcher{{Name: "job", Op: MatchEqual, Value: "api"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != api {
		t.Fatalf("ids = %v, want [%d]", ids, api)
	}
}

func TestRegistryMatchIsSortedAndOrderIndependent(t *testing.T) {
	reg := NewRegistry()
	reg.GetOrCreate(model.LabelSet{{Name: "job", Value: "api"}, {Name: "pod", Value: "a"}})
	want := reg.GetOrCreate(model.LabelSet{{Name: "job", Value: "api"}, {Name: "pod", Value: "b"}})
	reg.GetOrCreate(model.LabelSet{{Name: "job", Value: "worker"}, {Name: "pod", Value: "b"}})

	ids, err := reg.Match(Selector{Matchers: []Matcher{
		{Name: "job", Op: MatchEqual, Value: "api"},
		{Name: "pod", Op: MatchEqual, Value: "b"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != want {
		t.Fatalf("ids = %v, want [%d]", ids, want)
	}

	all, err := reg.Match(Selector{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(all); i++ {
		if all[i-1] > all[i] {
			t.Fatalf("ids are not sorted: %v", all)
		}
	}
}

func TestSelectorValidate(t *testing.T) {
	if err := (Selector{Matchers: []Matcher{{Name: "job", Op: MatchRegexp, Value: "api|worker"}}}).Validate(); err != nil {
		t.Fatal(err)
	}
	if err := (Selector{Matchers: []Matcher{{Name: "job", Op: MatchRegexp, Value: "["}}}).Validate(); err == nil {
		t.Fatal("expected invalid regexp error")
	}
	if err := (Selector{Matchers: []Matcher{{Name: "", Op: MatchEqual, Value: "api"}}}).Validate(); err == nil {
		t.Fatal("expected empty matcher name error")
	}
	if err := (Selector{Matchers: []Matcher{{Name: "job", Op: MatchOp(99), Value: "api"}}}).Validate(); err == nil {
		t.Fatal("expected unsupported matcher op error")
	}
}

func TestSelectorHelpers(t *testing.T) {
	selector := NewSelector(
		MetricName("http_requests_total"),
		LabelEqual("job", "api"),
		LabelRegexp("instance", "web-.+"),
		LabelNotEqual("env", "dev"),
		LabelNotRegexp("pod", "canary-.+"),
	)
	want := []Matcher{
		{Name: model.MetricNameLabel, Op: MatchEqual, Value: "http_requests_total"},
		{Name: "job", Op: MatchEqual, Value: "api"},
		{Name: "instance", Op: MatchRegexp, Value: "web-.+"},
		{Name: "env", Op: MatchNotEqual, Value: "dev"},
		{Name: "pod", Op: MatchNotRegexp, Value: "canary-.+"},
	}
	if len(selector.Matchers) != len(want) {
		t.Fatalf("matchers len = %d, want %d", len(selector.Matchers), len(want))
	}
	for i := range want {
		if selector.Matchers[i] != want[i] {
			t.Fatalf("matcher[%d] = %+v, want %+v", i, selector.Matchers[i], want[i])
		}
	}
}

func TestRegistryUpdateLastTouch(t *testing.T) {
	reg := NewRegistry()
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	id := reg.GetOrCreateAt(labels, 0) // imported-style: ts=0 sentinel

	// Reconcile path: block-derived UpdateLastTouch advances 0 -> 5000.
	if !reg.UpdateLastTouch(id, 5000) {
		t.Fatal("UpdateLastTouch on known id returned false")
	}
	if ts, _ := reg.LastTouch(id); ts != 5000 {
		t.Fatalf("after reconcile: ts=%d want 5000", ts)
	}
	// Out-of-order older does not regress.
	reg.UpdateLastTouch(id, 1000)
	if ts, _ := reg.LastTouch(id); ts != 5000 {
		t.Fatalf("after stale UpdateLastTouch: ts=%d want 5000 unchanged", ts)
	}
	// Unknown id returns false, no panic on missing labels map entry.
	if reg.UpdateLastTouch(SeriesID(99999), 100) {
		t.Fatal("UpdateLastTouch on unknown id returned true")
	}
	if ts, ok := reg.LastTouch(SeriesID(99999)); ok || ts != 0 {
		t.Fatalf("LastTouch on unknown id: ts=%d ok=%v want 0,false", ts, ok)
	}
}

func TestRegistryLastTouch(t *testing.T) {
	reg := NewRegistry()
	labels := model.LabelSet{{Name: "job", Value: "api"}}

	// New series via GetOrCreateAt records the timestamp.
	id := reg.GetOrCreateAt(labels, 100)
	if ts, ok := reg.LastTouch(id); !ok || ts != 100 {
		t.Fatalf("after first touch: ts=%d ok=%v, want ts=100 ok=true", ts, ok)
	}

	// A later touch advances the value.
	reg.GetOrCreateAt(labels, 200)
	if ts, _ := reg.LastTouch(id); ts != 200 {
		t.Fatalf("after later touch: ts=%d, want 200", ts)
	}

	// An out-of-order older touch does NOT regress the value — last-touch
	// tracks max-seen, so the sweep can age the series by its newest
	// activity even when collectors backfill late.
	reg.GetOrCreateAt(labels, 150)
	if ts, _ := reg.LastTouch(id); ts != 200 {
		t.Fatalf("after stale touch: ts=%d, want 200 (no regression)", ts)
	}

	// Bare GetOrCreate does not touch — preserves the existing value so
	// non-ingest call sites (tests, future query-path hits) cannot reset
	// the eviction clock.
	reg.GetOrCreate(labels)
	if ts, _ := reg.LastTouch(id); ts != 200 {
		t.Fatalf("after GetOrCreate (no ts): ts=%d, want 200 unchanged", ts)
	}

	// Import seeds last-touch=0 = "unknown" sentinel so the sweep will not
	// evict pre-step-2-catalog-log series until they're either re-touched
	// by ingest or the append-log recovery replaces this with a real ts.
	imported := SeriesID(999)
	reg.Import(imported, model.LabelSet{{Name: "job", Value: "imported"}})
	if ts, ok := reg.LastTouch(imported); !ok || ts != 0 {
		t.Fatalf("after Import: ts=%d ok=%v, want ts=0 ok=true", ts, ok)
	}

	// Unknown id reports (0,false).
	if ts, ok := reg.LastTouch(SeriesID(424242)); ok || ts != 0 {
		t.Fatalf("unknown id: ts=%d ok=%v, want ts=0 ok=false", ts, ok)
	}
}

func TestRegistrySweepEvictsColdSeries(t *testing.T) {
	reg := NewRegistry()
	reg.SetEvictionBucketing(60_000, 5_000) // retention 60s, bucket 5s

	// At t=1000, three series. All touched then.
	hot := reg.GetOrCreateAt(model.LabelSet{{Name: "job", Value: "api"}}, 1000)
	cold1 := reg.GetOrCreateAt(model.LabelSet{{Name: "job", Value: "worker"}}, 1000)
	cold2 := reg.GetOrCreateAt(model.LabelSet{{Name: "job", Value: "scheduler"}}, 1000)

	// At t=70_000, only `hot` was touched again. Sweep at t=70_000 with
	// retention 60_000 should evict cold1, cold2 (last touched at 1000,
	// threshold = 70000-60000 = 10000 > 1000) but keep hot.
	reg.GetOrCreateAt(model.LabelSet{{Name: "job", Value: "api"}}, 70_000)

	evicted := reg.Sweep(70_000)

	gotEvicted := map[SeriesID]bool{}
	for _, id := range evicted {
		gotEvicted[id] = true
	}
	if !gotEvicted[cold1] || !gotEvicted[cold2] {
		t.Fatalf("evicted=%v want cold1=%d cold2=%d", evicted, cold1, cold2)
	}
	if gotEvicted[hot] {
		t.Fatalf("hot series %d was evicted", hot)
	}
	if reg.SeriesCount() != 1 {
		t.Fatalf("SeriesCount=%d want 1", reg.SeriesCount())
	}
	// Evicted series must be gone from byFP / postings too — re-registering
	// should yield a NEW id (not the old one).
	newID := reg.GetOrCreateAt(model.LabelSet{{Name: "job", Value: "worker"}}, 70_000)
	if newID == cold1 {
		t.Fatalf("re-registered worker got recycled id %d", newID)
	}
}

func TestRegistrySweepIgnoresSentinelZero(t *testing.T) {
	// lastTouch=0 = the Import sentinel. Sweep must NEVER evict these:
	// they're series whose only evidence-of-life is the catalog log
	// recovery that hasn't been reconciled to a real timestamp yet
	// (the block-derived reconcile path).
	reg := NewRegistry()
	reg.SetEvictionBucketing(60_000, 5_000)

	imported := SeriesID(42)
	reg.Import(imported, model.LabelSet{{Name: "job", Value: "imported"}})

	// Even after a long time, sweep must not evict the lastTouch=0 entry.
	evicted := reg.Sweep(10_000_000)
	for _, id := range evicted {
		if id == imported {
			t.Fatalf("sweep evicted sentinel-0 series %d", id)
		}
	}
	if reg.SeriesCount() != 1 {
		t.Fatalf("SeriesCount=%d want 1", reg.SeriesCount())
	}
}

func TestRegistrySweepBucketingMatchesFullScan(t *testing.T) {
	// Same series + touches in two registries, one bucketed, one not.
	// Sweep must evict the same set.
	mk := func(bucketed bool) *Registry {
		reg := NewRegistry()
		if bucketed {
			reg.SetEvictionBucketing(60_000, 5_000)
		} else {
			// Set retention without bucketing — the full-scan path uses
			// bucketRetention as its threshold.
			reg.bucketRetention = 60_000
		}
		for i := 0; i < 50; i++ {
			ts := int64(1000 + i*1500) // staggered touches
			reg.GetOrCreateAt(model.LabelSet{{Name: "id", Value: strconv.Itoa(i)}}, ts)
		}
		return reg
	}
	bucketed := mk(true)
	full := mk(false)
	const now = 80_000
	bEvicted := toSet(bucketed.Sweep(now))
	fEvicted := toSet(full.Sweep(now))
	if len(bEvicted) != len(fEvicted) {
		t.Fatalf("bucketed evicted=%d full evicted=%d, must match", len(bEvicted), len(fEvicted))
	}
	for id := range bEvicted {
		if !fEvicted[id] {
			t.Fatalf("bucketed evicted %d that full did not", id)
		}
	}
}

func toSet(ids []SeriesID) map[SeriesID]bool {
	m := make(map[SeriesID]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func TestSelectorOptimizedOrdersCheapMatchersFirst(t *testing.T) {
	selector := NewSelector(
		LabelNotRegexp("pod", "canary-.+"),
		LabelRegexp("instance", "web-.+"),
		LabelEqual("job", "api"),
		LabelNotEqual("env", "dev"),
	)
	got := selector.Optimized()
	wantOps := []MatchOp{MatchEqual, MatchNotEqual, MatchRegexp, MatchNotRegexp}
	for i, op := range wantOps {
		if got.Matchers[i].Op != op {
			t.Fatalf("matcher[%d] = %+v, want op %d", i, got.Matchers[i], op)
		}
	}
	if selector.Matchers[0].Op != MatchNotRegexp {
		t.Fatalf("selector mutated: %+v", selector.Matchers)
	}
}
