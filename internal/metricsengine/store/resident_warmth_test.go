package store

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

// TestRangeStepsResidentCacheWarmth pins the resident cache contract: the
// second identical range-step query over sealed blocks must be served
// entirely from the cache. Every miss is a full directory decode, so a
// regression here re-introduces the per-query decode the resident form
// exists to eliminate.
func TestRangeStepsResidentCacheWarmth(t *testing.T) {
	d := makeEquivDataset("warmth", 500, 1, 60, 0, 7)
	ticks := d.samplesFor()
	st := buildEquivStore(t, ticks, 6) // 10 sealed blocks, empty head

	sel := index.NewSelector(index.MetricName("http_requests_total"))
	rs := query.RangeSelector{Selector: sel, Window: time.Minute}
	from := d.firstTS
	to := d.firstTS + (d.lastTS-d.firstTS)/2
	step := 45 * time.Second

	runQuery := func() {
		t.Helper()
		steps, err := st.RateByLabelRangeSteps(rs, from, to, step, "service")
		if err != nil {
			t.Fatal(err)
		}
		if len(steps) == 0 {
			t.Fatal("empty result")
		}
	}

	runQuery() // cold: populates the resident cache
	warm := st.CacheStats()

	runQuery() // must be fully served from the resident cache
	after := st.CacheStats()

	if evicted := after.ResidentEvictions - warm.ResidentEvictions; evicted != 0 {
		t.Fatalf("resident cache evicted %d entries between identical queries", evicted)
	}
	if misses := after.ResidentMisses - warm.ResidentMisses; misses != 0 {
		t.Errorf("second identical query missed the resident cache %d times (hits %d)",
			misses, after.ResidentHits-warm.ResidentHits)
	}
	if misses := after.DirMisses - warm.DirMisses; misses != 0 {
		t.Errorf("second identical query missed the directory cache %d times", misses)
	}
}
