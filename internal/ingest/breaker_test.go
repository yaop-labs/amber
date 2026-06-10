package ingest

import "testing"

func TestBatcher_IsBreakerOpen(t *testing.T) {
	t.Run("disabled when threshold is zero", func(t *testing.T) {
		b := &Batcher{logBreaker: 0, spanBreaker: 0}
		b.logFailures.Store(1_000_000)
		b.spanFailures.Store(1_000_000)
		if b.IsBreakerOpen() {
			t.Fatal("threshold=0 must disable the breaker")
		}
	})

	t.Run("log opens at threshold", func(t *testing.T) {
		b := &Batcher{logBreaker: 3, spanBreaker: 3}
		for _, n := range []uint64{0, 1, 2} {
			b.logFailures.Store(n)
			if b.IsLogBreakerOpen() || b.IsBreakerOpen() {
				t.Fatalf("log breaker open at n=%d, want closed", n)
			}
		}
		for _, n := range []uint64{3, 4, 100} {
			b.logFailures.Store(n)
			if !b.IsLogBreakerOpen() || !b.IsBreakerOpen() {
				t.Fatalf("log breaker closed at n=%d, want open", n)
			}
		}
		if b.IsSpanBreakerOpen() {
			t.Fatal("span breaker opened from log failures")
		}
	})

	t.Run("span opens at threshold", func(t *testing.T) {
		b := &Batcher{logBreaker: 3, spanBreaker: 3}
		for _, n := range []uint64{0, 1, 2} {
			b.spanFailures.Store(n)
			if b.IsSpanBreakerOpen() || b.IsBreakerOpen() {
				t.Fatalf("span breaker open at n=%d, want closed", n)
			}
		}
		for _, n := range []uint64{3, 4, 100} {
			b.spanFailures.Store(n)
			if !b.IsSpanBreakerOpen() || !b.IsBreakerOpen() {
				t.Fatalf("span breaker closed at n=%d, want open", n)
			}
		}
		if b.IsLogBreakerOpen() {
			t.Fatal("log breaker opened from span failures")
		}
	})
}

func TestBatcher_LaneConfigOverridesBase(t *testing.T) {
	b := NewBatcher(Deps{}, Config{
		BatchSize:        100,
		BatchTimeout:     10,
		QueueSize:        5,
		BreakerThreshold: 9,
		Logs: LaneConfig{
			QueueSize:        7,
			BreakerThreshold: 2,
		},
		Spans: LaneConfig{
			BatchSize:    20,
			BatchTimeout: 30,
		},
	})

	if cap(b.logQueue) != 7 {
		t.Fatalf("log queue cap = %d, want 7", cap(b.logQueue))
	}
	if cap(b.spanQueue) != 5 {
		t.Fatalf("span queue cap = %d, want inherited 5", cap(b.spanQueue))
	}
	if b.logBreaker != 2 {
		t.Fatalf("log breaker = %d, want 2", b.logBreaker)
	}
	if b.spanBreaker != 9 {
		t.Fatalf("span breaker = %d, want inherited 9", b.spanBreaker)
	}
	if b.logBatchSize != 100 {
		t.Fatalf("log batch size = %d, want inherited 100", b.logBatchSize)
	}
	if b.spanBatchSize != 20 {
		t.Fatalf("span batch size = %d, want 20", b.spanBatchSize)
	}
	if b.logBatchTimeout != 10 {
		t.Fatalf("log batch timeout = %v, want inherited 10", b.logBatchTimeout)
	}
	if b.spanBatchTimeout != 30 {
		t.Fatalf("span batch timeout = %v, want 30", b.spanBatchTimeout)
	}
}
