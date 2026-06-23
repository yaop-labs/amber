package index

import (
	"math/rand"
	"slices"
	"testing"
)

// intersectReference is the obvious set-intersection used as an oracle.
func intersectReference(a, b []uint64) []uint64 {
	seen := make(map[uint64]struct{}, len(a))
	for _, v := range a {
		seen[v] = struct{}{}
	}
	var out []uint64
	for _, v := range b {
		if _, ok := seen[v]; ok {
			out = append(out, v)
		}
	}
	slices.Sort(out)
	return out
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIntersectSortedEdgeCases(t *testing.T) {
	cases := []struct {
		name string
		a, b []uint64
	}{
		{"both empty", nil, nil},
		{"a empty", nil, []uint64{1, 2, 3}},
		{"b empty", []uint64{1, 2, 3}, nil},
		{"disjoint", []uint64{1, 3, 5}, []uint64{2, 4, 6}},
		{"identical", []uint64{1, 2, 3}, []uint64{1, 2, 3}},
		{"single overlap small-large", []uint64{500}, makeRange(0, 1000)},
		{"miss above range", []uint64{2000}, makeRange(0, 1000)},
		{"miss below range", []uint64{0}, []uint64{5, 6, 7, 8, 9, 10, 11, 12, 13, 14}},
		{"skewed sparse hits", []uint64{1, 17, 333, 999}, makeRange(0, 1000)},
		{"dup-free balanced", []uint64{2, 4, 6, 8}, []uint64{1, 4, 6, 9}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := intersectReference(tc.a, tc.b)
			got := IntersectSorted(tc.a, tc.b)
			if !equalU64(want, got) {
				t.Fatalf("IntersectSorted(%v,%v) = %v, want %v", tc.a, tc.b, got, want)
			}
			// Argument order must not matter.
			got2 := IntersectSorted(tc.b, tc.a)
			if !equalU64(want, got2) {
				t.Fatalf("IntersectSorted swapped = %v, want %v", got2, want)
			}
		})
	}
}

// TestIntersectSortedGallopMatchesLinear fuzzes skewed sizes that force the
// galloping path and compares against the reference oracle.
func TestIntersectSortedGallopMatchesLinear(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for iter := range 2000 {
		small := sortedSet(rng, rng.Intn(8), 5000)
		large := sortedSet(rng, 100+rng.Intn(4000), 5000)
		want := intersectReference(small, large)
		got := IntersectSorted(small, large)
		if !equalU64(want, got) {
			t.Fatalf("iter %d: got %v want %v", iter, got, want)
		}
	}
}

// BenchmarkIntersectSortedSkewed mirrors the span case: a small posting (one
// operation) galloped through a huge one (a whole service ≈ 1/N of all spans).
func BenchmarkIntersectSortedSkewed(b *testing.B) {
	rng := rand.New(rand.NewSource(7))
	large := sortedSet(rng, 4_000_000, 36_000_000)
	small := sortedSet(rng, 2_000, 36_000_000)
	b.ResetTimer()
	for b.Loop() {
		_ = IntersectSorted(small, large)
	}
}

func makeRange(lo, hi uint64) []uint64 {
	out := make([]uint64, 0, hi-lo)
	for v := lo; v < hi; v++ {
		out = append(out, v)
	}
	return out
}

// sortedSet returns n unique sorted values drawn from [0,max).
func sortedSet(rng *rand.Rand, n, max int) []uint64 {
	seen := make(map[uint64]struct{}, n)
	for len(seen) < n && len(seen) < max {
		seen[uint64(rng.Intn(max))] = struct{}{}
	}
	out := make([]uint64, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}
