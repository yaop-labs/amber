package index

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestFTSIndex_RadixSnapshotLoadsThroughLibrary pins the byte compatibility
// contract of the vendored radix index: files written by the patched writer
// must load through the library's snapshot reader and answer searches.
func TestFTSIndex_RadixSnapshotLoadsThroughLibrary(t *testing.T) {
	idx := NewFTSIndex()
	ctx := context.Background()
	// Repeated tokens within one body exercise the tail duplicate check.
	if err := idx.Index(ctx, 11, "timeout waiting timeout for lock timeout"); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if err := idx.Index(ctx, 22, "payment authorized provider=stripe"); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if err := idx.Index(ctx, 33, "timeout in payment provider"); err != nil {
		t.Fatalf("Index: %v", err)
	}

	path := filepath.Join(t.TempDir(), "x.fidx")
	if err := idx.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadFTSIndex(path)
	if err != nil {
		t.Fatalf("LoadFTSIndex: %v", err)
	}

	ids, err := loaded.Search(ctx, "timeout", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	slices.Sort(ids)
	if len(ids) != 2 || ids[0] != 11 || ids[1] != 33 {
		t.Fatalf("search(timeout) = %v, want [11 33]", ids)
	}
	ids, err = loaded.Search(ctx, "payment", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("search(payment) = %v, want 2 docs", ids)
	}
}

// TestFTSIndex_BuildSpeed is a coarse guard against the O(docs²) addDoc
// regression: 50k UUID-bearing bodies must index in seconds, not minutes.
func TestFTSIndex_BuildSpeed(t *testing.T) {
	if testing.Short() {
		t.Skip("speed test")
	}
	idx := NewFTSIndex()
	ctx := context.Background()
	start := time.Now()
	for i := range 50_000 {
		body := fmt.Sprintf("request completed method=GET status=200 duration_ms=%d req_id=%08x-dead-beef-cafe-%012x", i, i*7, i*13)
		if err := idx.Index(ctx, uint64(i+1), body); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}
	elapsed := time.Since(start)
	t.Logf("indexed 50k bodies in %s (%.0f rec/s)", elapsed.Round(time.Millisecond), 50_000/elapsed.Seconds())
	if elapsed > 30*time.Second {
		t.Fatalf("FTS build took %s for 50k records — quadratic addDoc is back", elapsed)
	}
}
