package store

import (
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestPutIfCurrentSkippedAfterReset pins the generation guard that keeps a
// cold load which raced a compaction/retention reset from caching a directory
// for a just-deleted block. The load snapshots the generation before its disk
// read; a reset bumps it; the guarded put must then be a no-op.
func TestPutIfCurrentSkippedAfterReset(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	dir := block.Directory{Series: []block.DirectoryEntry{{
		SeriesID: 1,
		Labels:   model.LabelSet{{Name: "__name__", Value: "m"}},
	}}}

	cached := func(path string) bool {
		st.mu.RLock()
		defer st.mu.RUnlock()
		_, ok := st.directoryCache.peek(path)
		return ok
	}

	// A load that snapshots the current generation and finds no reset caches.
	st.mu.RLock()
	gen := st.cacheGen
	st.mu.RUnlock()
	if !st.putDirectoryIfCurrent("blk-a", dir, gen) {
		t.Fatal("put with the current generation should succeed")
	}
	if !cached("blk-a") {
		t.Fatal("entry should be cached after a current-generation put")
	}

	// A reset happens while a second load (holding the pre-reset gen) is
	// mid-flight: its put must be dropped.
	st.mu.Lock()
	st.resetBlockCaches()
	st.mu.Unlock()
	if st.putDirectoryIfCurrent("blk-b", dir, gen) {
		t.Fatal("put with a stale generation (after reset) must be skipped")
	}
	if cached("blk-b") {
		t.Fatal("stale-generation entry must not be cached")
	}

	// A load that snapshots the post-reset generation caches normally again,
	// on both caches.
	st.mu.RLock()
	gen2 := st.cacheGen
	st.mu.RUnlock()
	if gen2 == gen {
		t.Fatal("resetBlockCaches must bump the generation")
	}
	if !st.putDirectoryIfCurrent("blk-c", dir, gen2) {
		t.Fatal("put with the refreshed generation should succeed")
	}
	if !st.putResidentIfCurrent("blk-c", block.BuildResidentFromDirectory(dir), gen2) {
		t.Fatal("resident put with the refreshed generation should succeed")
	}
}
