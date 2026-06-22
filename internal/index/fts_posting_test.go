package index

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

// TestFTSIndex_SnapshotRoundTrip pins the posting-format contract: save,
// load, and search agree, including repeated tokens within one body.
func TestFTSIndex_SnapshotRoundTrip(t *testing.T) {
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

// BenchmarkFTSIndexBuild tracks the transient cost of building one segment's
// FTS index: 100k UUID-bearing bodies, the shape that made the old
// map[string]*postingBuilder build state the dominant seal-time allocation.
func BenchmarkFTSIndexBuild(b *testing.B) {
	bodies := make([]string, 100_000)
	for i := range bodies {
		bodies[i] = fmt.Sprintf("request completed method=GET status=200 duration_ms=%d req_id=%08x-dead-beef-cafe-%012x", i, i*7, i*13)
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		idx := NewFTSIndex()
		for i, body := range bodies {
			if err := idx.Index(ctx, uint64(i+1), body); err != nil {
				b.Fatal(err)
			}
		}
		idx.seal()
	}
}

// TestFTSIndex_HashCollisionChain pins the build-state collision contract:
// entries chained under one hash are resolved by comparing token bytes, and
// df==1 tokens sharing a hash land adjacent in the unique section so lookup
// returns all of them (a collision may add a foreign record, never lose one).
func TestFTSIndex_HashCollisionChain(t *testing.T) {
	idx := NewFTSIndex()
	// Craft two distinct tokens chained under one hash, as a real fnv64
	// collision would leave them (same map key, byte-verified chain).
	idx.arena = []byte("aaaabbbb")
	idx.entries = []ftsBuildEntry{
		{firstID: 1, tokOff: 0, tokLen: 4, next: -1}, // "aaaa"
		{firstID: 2, tokOff: 4, tokLen: 4, next: 0},  // "bbbb" → chains to "aaaa"
	}
	h := tokenHash("aaaa")
	idx.byHash = map[uint64]int32{h: 1}

	// Indexing "aaaa" again must walk the chain past "bbbb" and hit the
	// existing entry instead of creating a duplicate.
	if err := idx.Index(context.Background(), 7, "aaaa"); err != nil {
		t.Fatalf("Index: %v", err)
	}
	if len(idx.entries) != 2 {
		t.Fatalf("chain walk missed: %d entries, want 2", len(idx.entries))
	}

	idx.seal()
	// "aaaa" is now df==2 → dictionary; "bbbb" stays unique under h.
	if len(idx.tokens) != 1 || idx.tokens[0] != "aaaa" {
		t.Fatalf("dict = %v, want [aaaa]", idx.tokens)
	}
	// lookup/lookupUnique return ordinals (AFT3); map them back to entry IDs to
	// pin the collision contract at the record level.
	got, err := idx.mapOrdinalsToIDs(idx.lookup("aaaa"))
	if err != nil || len(got) != 2 || got[0] != 1 || got[1] != 7 {
		t.Fatalf("lookup(aaaa) ids = %v (err %v), want [1 7]", got, err)
	}
	gotU, err := idx.mapOrdinalsToIDs(idx.lookupUnique("aaaa"))
	if err != nil || len(gotU) != 1 || gotU[0] != 2 {
		t.Fatalf("lookupUnique under collision hash = %v (err %v), want [2]", gotU, err)
	}
}

func TestFTSIndex_TokenKeys(t *testing.T) {
	idx := NewFTSIndex()
	ctx := context.Background()
	_ = idx.Index(ctx, 1, "payment timeout")
	_ = idx.Index(ctx, 2, "payment authorized")

	keys := idx.TokenKeys()
	got := make(map[string]bool, len(keys))
	for _, k := range keys {
		got[string(k)] = true
	}
	// "authorized" stems to "author" in the multilingual pipeline.
	for _, want := range []string{"payment", "timeout", "author"} {
		if !got[want] {
			t.Fatalf("TokenKeys missing %q: %v", want, got)
		}
	}
	if len(keys) != 3 {
		t.Fatalf("TokenKeys = %d keys, want 3 unique", len(keys))
	}
}

func TestFTSIndex_MultiTokenAND(t *testing.T) {
	idx := NewFTSIndex()
	ctx := context.Background()
	_ = idx.Index(ctx, 1, "payment timeout upstream")
	_ = idx.Index(ctx, 2, "payment authorized")
	_ = idx.Index(ctx, 3, "timeout waiting")

	ids, err := idx.Search(ctx, "payment timeout", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("AND search = %v, want [1]", ids)
	}
	if ids, _ := idx.Search(ctx, "payment nonexistenttoken", 10); len(ids) != 0 {
		t.Fatalf("AND with absent token must be empty, got %v", ids)
	}
}

func TestFTSIndex_LoadRejectsCorruptAndForeign(t *testing.T) {
	idx := NewFTSIndex()
	_ = idx.Index(context.Background(), 1, "hello world")
	path := filepath.Join(t.TempDir(), "x.fidx")
	if err := idx.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0xff
	bad := filepath.Join(t.TempDir(), "bad.fidx")
	_ = os.WriteFile(bad, data, 0o644)
	if _, err := LoadFTSIndex(bad); err == nil {
		t.Fatal("corrupt index loaded without error")
	}

	old := filepath.Join(t.TempDir(), "old.fidx")
	_ = os.WriteFile(old, []byte("some old slicedradix snapshot bytes here"), 0o644)
	if _, err := LoadFTSIndex(old); err == nil {
		t.Fatal("foreign-format index loaded without error")
	}
}

// TestFTSIndex_SizeOnDisk documents the format's footprint: the radix
// snapshot held ~38 MB for a 100k-record segment; the posting format must
// stay under 8 MB for the same data shape.
func TestFTSIndex_SizeOnDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("size test")
	}
	idx := NewFTSIndex()
	ctx := context.Background()
	for i := range 100_000 {
		body := fmt.Sprintf("request completed method=GET status=200 duration_ms=%d req_id=%08x-dead-beef-cafe-%012x", i, i*7, i*13)
		_ = idx.Index(ctx, uint64(i+1), body)
	}
	path := filepath.Join(t.TempDir(), "x.fidx")
	if err := idx.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, _ := os.Stat(path)
	t.Logf("100k-record .fidx: %.1f MB", float64(info.Size())/1e6)
	if info.Size() > 8<<20 {
		t.Fatalf(".fidx = %d bytes for 100k records — format regressed", info.Size())
	}
}

// TestFTSIndex_UniqueSectionPreadPath pins the file-backed df==1 lookup:
// unique tokens are not resident after load and must be found via pread.
func TestFTSIndex_UniqueSectionPreadPath(t *testing.T) {
	idx := NewFTSIndex()
	ctx := context.Background()
	// "deadbeefcafe" appears once (df==1 → unique section); "timeout" twice
	// (dictionary).
	_ = idx.Index(ctx, 100, "timeout while calling deadbeefcafe")
	_ = idx.Index(ctx, 200, "timeout retrying")

	path := filepath.Join(t.TempDir(), "u.fidx")
	if err := idx.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	loaded, err := LoadFTSIndex(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.uniqHashes != nil {
		t.Fatal("loaded index must not keep the unique section resident")
	}
	if loaded.uniqCount == 0 {
		t.Fatal("unique section missing from loaded index")
	}

	ids, err := loaded.Search(ctx, "deadbeefcafe", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 || ids[0] != 100 {
		t.Fatalf("unique pread search = %v, want [100]", ids)
	}
	// AND across dictionary and unique sections.
	ids, err = loaded.Search(ctx, "timeout deadbeefcafe", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 1 || ids[0] != 100 {
		t.Fatalf("mixed AND search = %v, want [100]", ids)
	}
	if ids, _ := loaded.Search(ctx, "nosuchtokenanywhere", 10); len(ids) != 0 {
		t.Fatalf("absent token returned %v", ids)
	}
}
