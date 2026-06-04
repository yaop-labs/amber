package index

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestRegistryConcurrentSweepAndQuery exercises the §4 v0 invariant: under
// concurrent ingest + sweep + query, the RW-lock + monotonic-bucket scheme
// must yield no panics, no torn reads, and a series evicted by the sweep
// must be gone from a subsequent Match.
//
// Run with `-race` for the tearing/panic part (the test exits cleanly if
// the race detector finds anything). The Match-after-eviction check
// requires no race detector — it's a functional assertion.
//
// What this test does NOT cover (deferred per spec §4):
//   - The grace-period for in-flight queries spanning the sweep. The v0
//     invariant is that readers RLock for the duration of Match, so a
//     sweep cannot interleave. That's exercised here implicitly: any
//     Match call observes a consistent snapshot.
//   - RCU/epoch reclamation. Later iteration.
func TestRegistryConcurrentSweepAndQuery(t *testing.T) {
	reg := NewRegistry()
	const retentionMs = 200
	const granularityMs = 25
	reg.SetEvictionBucketing(retentionMs, granularityMs)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var ingestCount, queryCount, sweepCount atomic.Uint64
	var clockMs atomic.Int64
	clockMs.Store(time.Now().UnixMilli())

	// Ingest goroutines: register new series and touch existing ones.
	const ingestWorkers = 4
	for w := 0; w < ingestWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				// Half stable (shared across workers), half ephemeral
				// (per-worker). The ephemerals will be evicted; the
				// stable set must survive.
				var ls model.LabelSet
				if i%2 == 0 {
					ls = model.LabelSet{{Name: "kind", Value: "stable"}, {Name: "id", Value: strconv.Itoa(i % 16)}}
				} else {
					ls = model.LabelSet{{Name: "kind", Value: "ephemeral"}, {Name: "w", Value: strconv.Itoa(workerID)}, {Name: "n", Value: strconv.Itoa(i)}}
				}
				reg.GetOrCreateAt(ls, clockMs.Load())
				ingestCount.Add(1)
				i++
			}
		}(w)
	}

	// Query goroutines.
	const queryWorkers = 3
	for q := 0; q < queryWorkers; q++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sel := Selector{Matchers: []Matcher{{Name: "kind", Op: MatchEqual, Value: "stable"}}}
			for {
				select {
				case <-stop:
					return
				default:
				}
				ids, err := reg.Match(sel)
				if err != nil {
					t.Errorf("Match error: %v", err)
					return
				}
				_ = ids
				queryCount.Add(1)
			}
		}()
	}

	// Sweep goroutine. Advances the clock so ephemerals age out and get
	// evicted in batches.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				now := clockMs.Add(50)
				reg.Sweep(now)
				sweepCount.Add(1)
			}
		}
	}()

	// Run for 500ms then stop.
	time.Sleep(500 * time.Millisecond)
	close(stop)
	wg.Wait()

	if ingestCount.Load() < 100 {
		t.Fatalf("ingestCount=%d, expected substantial work", ingestCount.Load())
	}
	if queryCount.Load() < 100 {
		t.Fatalf("queryCount=%d, expected substantial work", queryCount.Load())
	}
	if sweepCount.Load() < 10 {
		t.Fatalf("sweepCount=%d, expected ~100", sweepCount.Load())
	}
	t.Logf("ingest=%d queries=%d sweeps=%d final_series=%d",
		ingestCount.Load(), queryCount.Load(), sweepCount.Load(), reg.SeriesCount())

	// After all the activity, stable series must still be findable.
	// (They were re-touched on every iteration of the ingest loops, so
	// they cannot have aged out.)
	sel := Selector{Matchers: []Matcher{{Name: "kind", Op: MatchEqual, Value: "stable"}}}
	ids, err := reg.Match(sel)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) == 0 {
		t.Fatal("stable series were all evicted — sweep took too much")
	}
}

// TestRegistryEvictedSeriesGoneFromSubsequentMatch is a focused assertion:
// once Sweep evicts a series, the very next Match must not return it.
func TestRegistryEvictedSeriesGoneFromSubsequentMatch(t *testing.T) {
	reg := NewRegistry()
	reg.SetEvictionBucketing(60_000, 5_000)

	ephID := reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "eph"}}, 1000)
	stableID := reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "stable"}}, 1000)

	// Touch stable again at t=70_000; do NOT touch eph.
	reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "stable"}}, 70_000)

	evicted := reg.Sweep(70_000)
	gotEph := false
	for _, id := range evicted {
		if id == ephID {
			gotEph = true
		}
	}
	if !gotEph {
		t.Fatalf("eph %d was not evicted: evicted=%v", ephID, evicted)
	}

	// Match for kind=eph must return nothing now.
	ids, err := reg.Match(Selector{Matchers: []Matcher{{Name: "kind", Op: MatchEqual, Value: "eph"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Fatalf("evicted series still queryable: %v", ids)
	}

	// Stable must still be queryable.
	ids, err = reg.Match(Selector{Matchers: []Matcher{{Name: "kind", Op: MatchEqual, Value: "stable"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != stableID {
		t.Fatalf("stable series lost: got %v want [%d]", ids, stableID)
	}
}
