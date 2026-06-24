package index

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
)

// BenchmarkFTSSearchCommonToken models the logs q3 scenario: a common-token
// full-text query over a loaded (file-backed) 100k-record segment, returning a
// full page. It exercises the AFT3 ordinal->ID mapping on the result set, which
// reads the ordinal table from disk; the point of the bench is that mapping cost.
func BenchmarkFTSSearchCommonToken(b *testing.B) {
	ctx := context.Background()
	idx := NewFTSIndex()
	rng := rand.New(rand.NewSource(7))
	id := uint64(1) << 44
	for i := 0; i < 100_000; i++ {
		id += uint64(rng.Intn(10_000) + 1) // sparse, ULID-like
		body := fmt.Sprintf("request completed status ok handler=api uid=%012x span=%012x",
			rng.Uint64()&0xffffffffffff, rng.Uint64()&0xffffffffffff)
		if err := idx.Index(ctx, id, body); err != nil {
			b.Fatal(err)
		}
	}
	path := filepath.Join(b.TempDir(), "bench.fidx")
	if err := idx.Save(path); err != nil {
		b.Fatal(err)
	}
	loaded, err := LoadFTSIndex(path)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res, err := loaded.Search(ctx, "request", 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(res) != 100 {
			b.Fatalf("got %d results, want 100", len(res))
		}
	}
}
