package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/backup"
	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/storage"
)

func TestStatusReportsDegradedReasonsAndClosing(t *testing.T) {
	ready := &atomic.Bool{}
	ready.Store(true)
	stack := &Stack{ready: ready, degradedReasons: make(map[string]uint64)}
	stack.markDegradedN("index_bootstrap_failure", 2)
	stack.markDegraded("log_seal_index_failure")

	got := stack.Status()
	if !got.Ready || !got.Degraded || got.Closing {
		t.Fatalf("status = %+v, want ready+degraded+not-closing", got)
	}
	if len(got.Reasons) != 2 || got.Reasons[0].Code != "index_bootstrap_failure" || got.Reasons[0].Count != 2 {
		t.Fatalf("reasons = %+v", got.Reasons)
	}

	stack.statusMu.Lock()
	stack.closing = true
	stack.ready.Store(false)
	stack.statusMu.Unlock()
	got = stack.Status()
	if got.Ready || !got.Closing {
		t.Fatalf("closing status = %+v", got)
	}
}

func TestStatusReportsMetricsWALRepair(t *testing.T) {
	dataDir := t.TempDir()
	journal, err := otlpv4.OpenJournal(dataDir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	metricsDir := filepath.Join(dataDir, "metrics")
	if err := os.MkdirAll(metricsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(metricsDir, "head.wal"), []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}

	stack, err := New(context.Background(), Options{DataDir: dataDir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := stack.Close(closeCtx); err != nil {
			t.Fatal(err)
		}
	}()

	got := stack.Status()
	if !got.Degraded || !statusHasReason(got, "metrics_wal_tail_repaired", 1) {
		t.Fatalf("status = %+v, want metrics_wal_tail_repaired degraded reason", got)
	}
}

func TestStatusReportsCorruptBackupState(t *testing.T) {
	dataDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dataDir, dbmeta.BackupStateFileName), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	stack, err := New(context.Background(), Options{DataDir: dataDir, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := stack.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	status := stack.Status()
	if status.DatabaseID == "" || !status.Degraded || !statusHasReason(status, "backup_state_corrupt", 1) {
		t.Fatalf("status = %+v, want database ID and backup_state_corrupt", status)
	}
}

func statusHasReason(st Status, code string, count uint64) bool {
	for _, reason := range st.Reasons {
		if reason.Code == code && reason.Count == count {
			return true
		}
	}
	return false
}

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

func TestNewSetsMemoryLimit(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	defer debug.SetMemoryLimit(prev)

	const limit = 8 << 30 // high enough to never throttle the test binary

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stack, err := New(ctx, Options{
		DataDir:     t.TempDir(),
		MemoryLimit: limit,
		Metrics:     MetricsOptions{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = stack.Close(closeCtx)
	}()

	if got := debug.SetMemoryLimit(-1); got != limit {
		t.Fatalf("memory limit = %d, want %d", got, limit)
	}
}

func TestNewRejectsNegativeJournalRetention(t *testing.T) {
	_, err := New(context.Background(), Options{
		DataDir:   t.TempDir(),
		Retention: RetentionOptions{Journal: JournalRetentionOptions{MaxBytes: -1}},
	})
	if err == nil {
		t.Fatal("negative journal retention accepted")
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
	flushBatcher(t, stack)

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
	flushBatcher(t, stack)

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

func TestNewRebuildsSparseIndexFromSegments(t *testing.T) {
	dataDir := t.TempDir()
	ctx1, cancel1 := context.WithCancel(context.Background())
	stack1, err := New(ctx1, Options{
		DataDir: dataDir,
		Ingest: IngestOptions{
			BatchSize:    100,
			BatchTimeout: time.Hour,
			QueueSize:    16,
		},
		Metrics: MetricsOptions{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}

	ts := time.Unix(1_700_000_000, 123)
	entry := model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: ts,
		Level:     model.LevelInfo,
		Service:   "sparse-recovery",
		Body:      "durable and queryable",
	}
	if err := stack1.Batcher.SendLog(entry); err != nil {
		t.Fatal(err)
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := stack1.Batcher.Flush(flushCtx); err != nil {
		flushCancel()
		t.Fatal(err)
	}
	flushCancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := stack1.Close(closeCtx); err != nil {
		closeCancel()
		t.Fatal(err)
	}
	closeCancel()
	cancel1()

	// The persisted sparse cache is deliberately corrupt. Startup must derive
	// query visibility from segment metadata instead of trusting this file.
	if err := os.WriteFile(filepath.Join(dataDir, "logs", "sparse.idx"), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	stack2, err := New(ctx2, Options{DataDir: dataDir, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		cancel2()
		t.Fatalf("reopen with corrupt sparse cache: %v", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = stack2.Close(closeCtx)
		cancel2()
	}()

	result, err := stack2.Executor.ExecLog(context.Background(), &query.LogQuery{
		From:     ts.Add(-time.Second),
		To:       ts.Add(time.Second),
		Services: []string{"sparse-recovery"},
		Limit:    10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Entries) != 1 || result.Entries[0].Body != entry.Body {
		t.Fatalf("recovered query entries = %+v, want one %q entry", result.Entries, entry.Body)
	}
}

func TestCloseContinuesAfterBatchErrorAndReleasesDataDir(t *testing.T) {
	dataDir := t.TempDir()
	stack, err := New(context.Background(), Options{DataDir: dataDir, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	stack.bootstrapWG.Wait()

	// Inject a covered storage error. Close must report it but still shut down
	// the other components and release the data-directory lock.
	if err := stack.LogManager.Close(); err != nil {
		t.Fatal(err)
	}
	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Service: "close-error", Body: "fail"}
	if err := stack.Batcher.SendLog(entry); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := stack.Close(closeCtx)
	if first == nil {
		t.Fatal("Close hid covered batch write error")
	}
	if second := stack.Close(closeCtx); second == nil || second.Error() != first.Error() {
		t.Fatalf("second Close = %v, want stable %v", second, first)
	}

	reopened, err := New(context.Background(), Options{DataDir: dataDir, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatalf("data directory remained locked after terminal Close: %v", err)
	}
	if err := reopened.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeRequiresOTLPV4Journal(t *testing.T) {
	t.Run("new root enables v4", func(t *testing.T) {
		root := t.TempDir()
		stack, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}})
		if err != nil {
			t.Fatal(err)
		}
		if stack.OTLPJournal == nil {
			t.Fatalf("new-root status = %+v journal=%v", stack.Status(), stack.OTLPJournal)
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := stack.Close(closeCtx); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(filepath.Join(root, otlpv4.DirectoryName, otlpv4.FormatFileName)); err != nil {
			t.Fatalf("v4 format manifest: %v", err)
		}
	})

	t.Run("existing engine root without v4 is rejected", func(t *testing.T) {
		root := t.TempDir()
		if _, err := dbmeta.LoadOrCreate(root); err != nil {
			t.Fatal(err)
		}
		manager, err := storage.OpenSegmentManager(filepath.Join(root, "logs"), storage.DefaultRotationPolicy)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}}); err == nil {
			t.Fatal("New() error = nil for existing engine root without OTLP v4")
		}
		if _, err := os.Stat(filepath.Join(root, otlpv4.DirectoryName)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected root was modified with OTLP v4 journal: %v", err)
		}
	})

	t.Run("invalid journal path blocks open", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, otlpv4.DirectoryName), []byte("invalid"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}}); err == nil {
			t.Fatal("New() error = nil for invalid journal path")
		}
	})
}

func TestRuntimeReportsOTLPJournalWALRepair(t *testing.T) {
	root := t.TempDir()
	stack, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stack.Close(closeCtx); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(root, otlpv4.DirectoryName, "amber.wal")
	wal, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Write([]byte{1, 2, 3}); err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := reopened.Close(ctx); err != nil {
			t.Errorf("close reopened runtime: %v", err)
		}
	}()
	status := reopened.Status()
	if status.WALRepairEvents != 1 || !statusHasReason(status, "otlp_v4_wal_tail_repaired", 1) {
		t.Fatalf("status after OTLP WAL repair = %+v", status)
	}
}

func TestRuntimeJournalRetentionSurvivesRestartReplayAndBackup(t *testing.T) {
	root := t.TempDir()
	stack, err := New(context.Background(), Options{
		DataDir: root,
		Storage: StorageOptions{SegmentMaxRecords: 1, SegmentMaxBytes: 1 << 20},
		Ingest:  IngestOptions{BatchSize: 1, BatchTimeout: time.Hour, QueueSize: 16},
		Metrics: MetricsOptions{Disabled: true},
		Retention: RetentionOptions{
			Interval: 10 * time.Millisecond,
			Journal:  JournalRetentionOptions{MaxSegments: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		entry := model.LogEntry{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Now().Add(time.Duration(i) * time.Nanosecond),
			Level:     model.LevelInfo,
			Service:   "journal-retention",
			Body:      "entry",
		}
		if err := stack.Batcher.SendLog(entry); err != nil {
			t.Fatal(err)
		}
	}
	flushBatcher(t, stack)

	deadline := time.Now().Add(3 * time.Second)
	for {
		stats, statsErr := stack.OTLPJournal.Stats()
		if statsErr != nil {
			t.Fatal(statsErr)
		}
		if stats.TotalRecords == 1 && stats.Retention.DeletedRecords == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("journal retention did not converge: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := stack.Close(closeCtx); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()

	assertReplayCount := func(dataRoot string, want int) {
		t.Helper()
		count := 0
		if err := otlpv4.Replay(context.Background(), dataRoot, func(otlpv4.Envelope) error {
			count++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("Replay(%s) count = %d, want %d", dataRoot, count, want)
		}
	}
	assertReplayCount(root, 1)

	snapshot := filepath.Join(t.TempDir(), "snapshot")
	if _, err := backup.Create(context.Background(), root, snapshot); err != nil {
		t.Fatalf("backup after journal retention: %v", err)
	}
	assertReplayCount(filepath.Join(snapshot, backup.DataDirectoryName), 1)

	reopened, err := New(context.Background(), Options{DataDir: root, Metrics: MetricsOptions{Disabled: true}})
	if err != nil {
		t.Fatalf("reopen retained root: %v", err)
	}
	closeCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := reopened.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func flushBatcher(t *testing.T, stack *Stack) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := stack.Batcher.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
}
