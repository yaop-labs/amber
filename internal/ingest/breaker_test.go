package ingest

import "testing"

func TestBatcher_IsBreakerOpen(t *testing.T) {
	t.Run("disabled when threshold is zero", func(t *testing.T) {
		b := &Batcher{breakerThreshold: 0}
		b.consecFailures.Store(1_000_000)
		if b.IsBreakerOpen() {
			t.Fatal("threshold=0 must disable the breaker")
		}
	})

	t.Run("opens at threshold", func(t *testing.T) {
		b := &Batcher{breakerThreshold: 3}
		for _, n := range []uint64{0, 1, 2} {
			b.consecFailures.Store(n)
			if b.IsBreakerOpen() {
				t.Fatalf("breaker open at n=%d, want closed", n)
			}
		}
		for _, n := range []uint64{3, 4, 100} {
			b.consecFailures.Store(n)
			if !b.IsBreakerOpen() {
				t.Fatalf("breaker closed at n=%d, want open", n)
			}
		}
	})
}
