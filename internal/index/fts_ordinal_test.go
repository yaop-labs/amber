package index

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"
)

// TestFTSOrdinalSearchRoundtrip is the AFT3 correctness gate: with sparse,
// ULID-like entry IDs (so ordinals differ from IDs) and a mix of df>=2 (common
// words) and df==1 (unique hex) tokens, Search must return exactly the records
// containing each token — verified against a brute-force reference across all
// three execution paths: in-memory (freshly sealed), post-Save (file-backed
// unique section + ordinal table via pread, postings resident), and fully
// loaded from disk.
func TestFTSOrdinalSearchRoundtrip(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(2026))

	type doc struct {
		id   uint64
		body string
	}
	const n = 800
	docs := make([]doc, n)
	// Sparse IDs: a large base with non-uniform gaps, mimicking ULID-derived
	// uint64s whose high bits carry a timestamp.
	id := uint64(1) << 44
	common := []string{"error", "payment", "timeout", "checkout", "retry"}
	for i := range docs {
		id += uint64(rng.Intn(10_000) + 1)
		// each doc: a couple of common words + one unique hex token.
		w1 := common[rng.Intn(len(common))]
		w2 := common[rng.Intn(len(common))]
		uniq := fmt.Sprintf("req%012x", rng.Uint64()&0xffffffffffff)
		docs[i] = doc{id: id, body: fmt.Sprintf("%s %s %s", w1, w2, uniq)}
	}

	idx := NewFTSIndex()
	for _, d := range docs {
		if err := idx.Index(ctx, d.id, d.body); err != nil {
			t.Fatalf("Index: %v", err)
		}
	}

	// Pre-tokenize each body once; a query matches a doc iff the query's tokens
	// are all present in the doc's tokens (exactly what Search does). Computing
	// the expectation this way — over RAW queries — avoids re-stemming a stem
	// (the tokenizer is not idempotent for some tokens).
	bodyTokens := make([]map[string]bool, len(docs))
	for i, d := range docs {
		set := map[string]bool{}
		for _, tok := range TokenizeFTS(d.body) {
			if tok != "" {
				set[tok] = true
			}
		}
		bodyTokens[i] = set
	}
	expect := func(query string) []uint64 {
		qtoks := TokenizeFTS(query)
		var out []uint64
		for i, d := range docs {
			all := len(qtoks) > 0
			for _, qt := range qtoks {
				if !bodyTokens[i][qt] {
					all = false
					break
				}
			}
			if all {
				out = append(out, d.id)
			}
		}
		slices.Sort(out)
		return slices.Compact(out)
	}

	// Raw queries: every common word, a sample of raw unique strings, and an AND.
	queries := append([]string{}, common...)
	for i := 0; i < len(docs); i += 37 {
		queries = append(queries, docs[i].body[len(docs[i].body)-15:]) // the uniq token
	}
	queries = append(queries, "error payment", "checkout retry timeout")

	check := func(label string, f *FTSIndex) {
		for _, q := range queries {
			want := expect(q)
			got, err := f.Search(ctx, q, 0)
			if err != nil {
				t.Fatalf("%s Search(%q): %v", label, q, err)
			}
			slices.Sort(got)
			got = slices.Compact(got)
			if !slices.Equal(got, want) {
				t.Fatalf("%s Search(%q) = %v, want %v", label, q, got, want)
			}
		}
	}

	check("in-memory", idx)

	path := filepath.Join(t.TempDir(), "ord.fidx")
	if err := idx.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}
	check("post-save", idx) // file-backed unique + table via pread

	loaded, err := LoadFTSIndex(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	check("loaded", loaded)
}
