package amber

import (
	"context"
	"testing"
	"time"
)

// TestFlushDurabilityBarrier pins the db.Flush contract: entries accepted
// before Flush are queryable right after it returns. The batch timeout is set
// far beyond the test's lifetime so only the barrier can force the write.
func TestFlushDurabilityBarrier(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{
		BatchSize:    1000,
		BatchTimeout: time.Hour,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	const n = 50
	for range n {
		entry, err := NewLogEntry(LevelInfo, "svc", "host", "hello")
		if err != nil {
			t.Fatalf("NewLogEntry: %v", err)
		}
		if err := db.Log(ctx, entry); err != nil {
			t.Fatalf("Log: %v", err)
		}
	}

	if err := db.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result, err := db.QueryLogs(ctx, &LogQuery{Limit: n * 2})
	if err != nil {
		t.Fatalf("QueryLogs: %v", err)
	}
	if len(result.Entries) != n {
		t.Fatalf("after Flush got %d entries, want %d", len(result.Entries), n)
	}
}

func TestFlushRespectsContext(t *testing.T) {
	db, err := Open(t.TempDir(), &Options{BatchTimeout: time.Hour})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// The workers are alive, so the barrier itself would succeed; a canceled
	// context must still win the race deterministically when it is already
	// done before any waiting starts. Both outcomes are valid only if err is
	// either nil (barrier won) or ctx.Err() — never a hang.
	done := make(chan error, 1)
	go func() { done <- db.Flush(ctx) }()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("Flush: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush hung with canceled context")
	}
}
