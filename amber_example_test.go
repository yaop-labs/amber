package amber_test

// This file is the contract test for amber's embedded API: if it compiles
// using only the github.com/hnlbs/amber import, the public surface is
// sufficient to embed amber in another binary (e.g. forager).
//
// Do NOT add imports from github.com/hnlbs/amber/internal/... here.

import (
	"context"
	"testing"
	"time"

	"github.com/hnlbs/amber"
)

func TestEmbedded_RoundTrip(t *testing.T) {
	dir := t.TempDir()

	db, err := amber.Open(dir, &amber.Options{
		SegmentMaxRecords: 1000,
		BatchSize:         100,
		BatchTimeout:      50 * time.Millisecond,
		QueueSize:         1000,
		Cardinality: amber.CardinalityLimits{
			MaxAttrsPerEntry:  32,
			MaxAttrValueBytes: 1024,
		},
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	ctx := context.Background()

	entry := amber.LogEntry{
		Timestamp: time.Now(),
		Level:     amber.LevelInfo,
		Service:   "embed-test",
		Host:      "test-host",
		Body:      "hello from embedded amber",
		Attrs:     []amber.Attr{{Key: "env", Value: "test"}},
	}
	if err := db.Log(ctx, entry); err != nil {
		t.Fatalf("log: %v", err)
	}

	// BatchTimeout=50ms; give the flusher a turn before querying.
	time.Sleep(200 * time.Millisecond)

	result, err := db.QueryLogs(ctx, &amber.LogQuery{
		Services: []string{"embed-test"},
		Limit:    10,
	})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(result.Entries) == 0 {
		t.Fatal("expected at least one entry from embed-test service")
	}
}
