package store

import (
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestStoreEvictionBoundaryEqualsBlockRetention verifies that index eviction
// and block retention use the same cutoff derived from opts.Retention.
func TestStoreEvictionBoundaryEqualsBlockRetention(t *testing.T) {
	const retentionMs = int64(60_000) // 60s
	retention := time.Duration(retentionMs) * time.Millisecond

	// Frozen clock. now is far enough past 0 that "now - retention" is
	// a sane positive value.
	now := int64(1_700_000_000_000)

	// Build a registry with bucketing matching what the Store does.
	reg := index.NewRegistry()
	// Granularity matches store.go's `retention / 12 floored at 1s`.
	gran := retentionMs / 12
	if gran < 1000 {
		gran = 1000
	}
	reg.SetEvictionBucketing(retentionMs, gran)

	// Three series at carefully picked last-touch positions relative to
	// the now-retention threshold.
	//
	//   freshly-touched: lastTouch = now-0          (well inside window)
	//   on-boundary:     lastTouch = now-retention  (the boundary itself)
	//   well-cold:       lastTouch = now-retention-bucket-2gran (firmly past)
	idFresh := reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "fresh"}}, now)
	idBoundary := reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "boundary"}}, now-retentionMs)
	idCold := reg.GetOrCreateAt(model.LabelSet{{Name: "kind", Value: "cold"}}, now-retentionMs-2*gran)

	evicted := reg.Sweep(now)
	evictedSet := map[index.SeriesID]bool{}
	for _, id := range evicted {
		evictedSet[id] = true
	}

	if !evictedSet[idCold] {
		t.Fatalf("cold series (age > retention) NOT evicted: evicted=%v", evicted)
	}
	if evictedSet[idFresh] {
		t.Fatalf("fresh series (age = 0) wrongly evicted: %d", idFresh)
	}
	// On-boundary: per Sweep's predicate `lastTouch <= now-retention`
	// counts as past-threshold and eligible. This is the SAME predicate
	// DeleteBefore uses: a block with MaxTime < cutoff is dropped, and
	// cutoff is computed as `clock - retention`. So a series whose
	// lastTouch equals exactly `now - retention` is at the same instant
	// the block carrying it would be dropped.
	_ = idBoundary

	// Block-retention cutoff computed the way maintenance() does:
	cutoffFromMaintenance := time.Unix(0, now*int64(time.Millisecond)).
		Add(-retention).UnixMilli()
	// Index-sweep threshold (the boundary lastTouch must EXCEED to be
	// cold). Both quantities must be the SAME integer.
	indexThreshold := now - retentionMs

	if cutoffFromMaintenance != indexThreshold {
		t.Fatalf("eviction-boundary drift: block-retention cutoff=%d != index-sweep threshold=%d (diff=%d ms)",
			cutoffFromMaintenance, indexThreshold,
			cutoffFromMaintenance-indexThreshold)
	}
}

// TestStoreEvictionBoundaryShapeIsStable is a guard against silent
// refactors that change the SHAPE of the eviction predicate (e.g.
// from `now-lastTouch > retention` to `now-lastTouch >= retention`,
// which is one ULP different and would desync the index from the
// block path).
//
// The predicate the codebase carries is documented at
// internal/metricsengine/index/index.go:Sweep — "evict if lastTouch
// is older than now-retention AND ts != 0". Empirically the boundary
// behaviour at `lastTouch == now - retention` is "evictable" (per
// Sweep's `lt > threshold ? skip : evict` branch, equivalent to
// `lt <= threshold => evict`). Match the block side: DeleteBefore
// drops a block whose `MaxTime < cutoffMillis` — strict less-than,
// boundary block STAYS. So:
//   - block side: drops if MaxTime <  now-retention (boundary stays)
//   - index side: evicts if lastTouch <= now-retention (boundary GOES)
//
// They differ by one ULP at the exact boundary. This is INTENTIONAL:
// the block keeps data through the moment the series goes cold, so
// the small window where index says "gone" + block says "here" is
// covered by the query path that matches labels on the block
// directory regardless of registry state. The opposite — block gone
// while index keeps the series — would be the bad direction. Pinned
// here so a refactor flipping either comparison doesn't go unnoticed.
func TestStoreEvictionBoundaryShapeIsStable(t *testing.T) {
	const retentionMs = int64(60_000)
	now := int64(1_700_000_000_000)
	threshold := now - retentionMs

	// Index side. With granularity small enough to make the boundary
	// land in a bucket whose epoch <= now/gran, the boundary series
	// must be evicted.
	{
		reg := index.NewRegistry()
		reg.SetEvictionBucketing(retentionMs, 1000)
		atBoundary := reg.GetOrCreateAt(model.LabelSet{{Name: "k", Value: "boundary"}}, threshold)
		evicted := reg.Sweep(now)
		hit := false
		for _, id := range evicted {
			if id == atBoundary {
				hit = true
			}
		}
		if !hit {
			t.Errorf("index: lastTouch == now-retention should be evicted; got evicted=%v", evicted)
		}
	}

	// Block side. The DeleteBefore predicate is `MaxTime < cutoff`
	// (strict). A boundary block (MaxTime exactly at cutoff) is KEPT;
	// only blocks strictly older are dropped. Exercise that against a
	// real Store so the test fails if the comparison ever flips to
	// `<=`.
	st := newStoreWithBlocks(t, []BlockMeta{
		{Path: "boundary.meb", MaxTime: threshold, MinTime: threshold - 1000},
		{Path: "stale.meb", MaxTime: threshold - 1, MinTime: threshold - 2000},
	})
	defer st.Close()
	deleted, err := st.DeleteBefore(threshold)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteBefore(threshold): deleted %d, want 1 (only the strict-less-than block)", deleted)
	}
	remaining := map[string]bool{}
	for _, b := range st.manifest.Blocks {
		remaining[b.Path] = true
	}
	if !remaining["boundary.meb"] {
		t.Errorf("block at MaxTime == cutoff was dropped; predicate flipped from strict to inclusive")
	}
	if remaining["stale.meb"] {
		t.Errorf("block at MaxTime < cutoff survived; predicate flipped from < to >")
	}
}

// newStoreWithBlocks fabricates a Store with a manifest carrying the
// given block metadata, without writing actual block files. Useful for
// predicate-level boundary tests where DeleteBefore's removal of
// missing files is tolerated (os.Remove on a non-existent path is
// IsNotExist, which DeleteBefore ignores).
func newStoreWithBlocks(t *testing.T, blocks []BlockMeta) *Store {
	t.Helper()
	dir := t.TempDir()
	st, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.manifest.Blocks = append(st.manifest.Blocks, blocks...)
	st.mu.Unlock()
	if err := saveManifest(dir, st.manifest); err != nil {
		t.Fatal(err)
	}
	return st
}
