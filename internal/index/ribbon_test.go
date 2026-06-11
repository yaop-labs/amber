package index

import (
	"fmt"
	"path/filepath"
	"testing"
)

// TestRibbonFilter_HighCardinalityBuild pins the window-width fix: with
// w=16 the ribbon solve failed from ~10k keys ("failed to build ribbon
// filter after retries"), which silently stripped sealed segments of their
// FTS prefilter on realistic UUID-bearing log bodies.
func TestRibbonFilter_HighCardinalityBuild(t *testing.T) {
	const n = 200_000
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%08x-%04x-tok-%012x",
			uint32(i)*2654435761, i&0xffff, uint64(i)*1140071481932319848))
	}

	f, err := BuildRibbonFilter(keys, 8)
	if err != nil {
		t.Fatalf("BuildRibbonFilter(%d keys): %v", n, err)
	}

	// Spot-check membership and round-trip through the on-disk format.
	for _, i := range []int{0, n / 2, n - 1} {
		if !f.Contains(keys[i]) {
			t.Fatalf("key %d not contained", i)
		}
	}
	path := filepath.Join(t.TempDir(), "f.filt")
	if err := f.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadRibbonFilter(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !loaded.Contains(keys[n/2]) {
		t.Fatal("loaded filter lost membership")
	}
}
