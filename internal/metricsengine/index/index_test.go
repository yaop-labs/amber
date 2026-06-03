package index

import (
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
