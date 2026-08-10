package main

import (
	"slices"
	"testing"
)

func TestParseSizes(t *testing.T) {
	t.Parallel()

	got, err := parseSizes("10000, 100000,1000000")
	if err != nil {
		t.Fatal(err)
	}
	want := []int{10_000, 100_000, 1_000_000}
	if !slices.Equal(got, want) {
		t.Fatalf("parseSizes() = %v, want %v", got, want)
	}
}

func TestParseSizesRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	for _, value := range []string{"", "0", "-1", "10,nope"} {
		if _, err := parseSizes(value); err == nil {
			t.Fatalf("parseSizes(%q) error = nil, want non-nil", value)
		}
	}
}

func TestPercentileNearestRank(t *testing.T) {
	t.Parallel()

	values := []int64{1, 2, 3, 4, 5}
	if got := percentile(values, 0.50); got != 3 {
		t.Fatalf("p50 = %d, want 3", got)
	}
	if got := percentile(values, 0.99); got != 5 {
		t.Fatalf("p99 = %d, want 5", got)
	}
}

func TestQueriesForSizeHonorsBudgetAndFloor(t *testing.T) {
	t.Parallel()

	cfg := config{
		MaxQueries:       10_000,
		LinearScanBudget: 500_000_000,
		Repeats:          3,
	}
	if got := queriesForSize(cfg, 10_000); got != 10_000 {
		t.Fatalf("10k query count = %d, want 10000", got)
	}
	if got := queriesForSize(cfg, 1_000_000); got != 256 {
		t.Fatalf("1m query count = %d, want floor 256", got)
	}
}
