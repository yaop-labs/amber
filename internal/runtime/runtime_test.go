package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/query"
)

func TestNewReturnsMetricStoreOpenErrorWithoutHanging(t *testing.T) {
	dir := t.TempDir()
	metricsPath := filepath.Join(dir, "metrics-file")
	if err := os.WriteFile(metricsPath, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := New(context.Background(), Options{
			DataDir: dir,
			Metrics: MetricsOptions{
				Dir: metricsPath,
			},
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("New returned nil error, want metric store open error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("New hung after metric store open failure")
	}
}

func TestJoinS3Prefix(t *testing.T) {
	tests := []struct {
		name  string
		parts []string
		want  string
	}{
		{name: "empty base", parts: []string{"", "spans"}, want: "spans"},
		{name: "nested base", parts: []string{"amber", "spans"}, want: "amber/spans"},
		{name: "slashy", parts: []string{"/amber/", "/spans/"}, want: "amber/spans"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := joinS3Prefix(tt.parts...); got != tt.want {
				t.Fatalf("joinS3Prefix(%v) = %q, want %q", tt.parts, got, tt.want)
			}
		})
	}
}

func TestIngestInvalidatesQueryResultCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stack, err := New(ctx, Options{
		DataDir: t.TempDir(),
		Ingest: IngestOptions{
			BatchSize:    1,
			BatchTimeout: time.Hour,
			QueueSize:    16,
		},
		Metrics: MetricsOptions{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := stack.Close(closeCtx); err != nil {
			t.Fatal(err)
		}
	}()

	entry1, err := model.NewLogEntry(model.LevelInfo, "api", "", "one")
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Batcher.SendLog(entry1); err != nil {
		t.Fatal(err)
	}
	waitForRecords(t, stack, 1)

	q := &query.LogQuery{Services: []string{"api"}, Limit: 10}
	first, err := stack.Executor.ExecLog(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Entries) != 1 {
		t.Fatalf("first entries = %d, want 1", len(first.Entries))
	}
	second, err := stack.Executor.ExecLog(context.Background(), &query.LogQuery{Services: []string{"api"}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !second.CacheHit {
		t.Fatal("second query CacheHit = false, want true before next ingest")
	}

	entry2, err := model.NewLogEntry(model.LevelInfo, "api", "", "two")
	if err != nil {
		t.Fatal(err)
	}
	if err := stack.Batcher.SendLog(entry2); err != nil {
		t.Fatal(err)
	}
	waitForRecords(t, stack, 2)

	afterWrite, err := stack.Executor.ExecLog(context.Background(), &query.LogQuery{Services: []string{"api"}, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if afterWrite.CacheHit {
		t.Fatal("query after ingest CacheHit = true, want invalidated cache")
	}
	if len(afterWrite.Entries) != 2 {
		t.Fatalf("entries after ingest = %d, want 2", len(afterWrite.Entries))
	}
}

func waitForRecords(t *testing.T, stack *Stack, want uint64) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if stack.LogManager.ActiveRecordCount() >= want && stack.Batcher.QueueLen() == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d records; active=%d queue=%d", want, stack.LogManager.ActiveRecordCount(), stack.Batcher.QueueLen())
		case <-tick.C:
		}
	}
}
