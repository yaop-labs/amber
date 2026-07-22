package otlpv4

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
	"google.golang.org/protobuf/proto"
)

func TestJournalReplayPreservesOrderAndSemantics(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	want := []struct {
		signal  Signal
		request proto.Message
	}{
		{signal: SignalLogs, request: richLogsRequest()},
		{signal: SignalTraces, request: richTracesRequest()},
		{signal: SignalMetrics, request: richMetricsRequest()},
	}
	for i, item := range want {
		envelope, err := New(item.signal, FidelityOTLP, item.request)
		if err != nil {
			t.Fatalf("New(%d) error = %v", i, err)
		}
		if err := journal.Append(envelope, time.Unix(0, int64(i+1))); err != nil {
			t.Fatalf("Append(%d) error = %v", i, err)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var got []Envelope
	if err := Replay(context.Background(), root, func(envelope Envelope) error {
		got = append(got, envelope)
		return nil
	}); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("replayed %d envelopes, want %d", len(got), len(want))
	}
	for i, envelope := range got {
		request, err := envelope.Request()
		if err != nil {
			t.Fatalf("Request(%d) error = %v", i, err)
		}
		if envelope.Signal() != want[i].signal || !proto.Equal(request, want[i].request) {
			t.Fatalf("replayed envelope %d differs", i)
		}
	}
}

func TestJournalReopensAfterUnsealedWrites(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityNormalizedNative, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(envelope, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatalf("close reopened journal: %v", err)
	}
	count := 0
	if err := Replay(context.Background(), root, func(envelope Envelope) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed %d envelopes, want 1", count)
	}
}

func TestJournalStatsTrackActiveSealedAndReopenedState(t *testing.T) {
	root := t.TempDir()
	policy := storage.RotationPolicy{MaxRecords: 2, MaxBytes: 1 << 20}
	journal, err := OpenJournal(root, policy)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}

	initial, err := journal.Stats()
	if err != nil {
		t.Fatalf("initial Stats: %v", err)
	}
	if !initial.ActiveSegment || initial.SealedSegments != 0 || initial.TotalRecords != 0 || initial.SegmentBytes != 0 || initial.WALBytes != 0 {
		t.Fatalf("initial stats = %+v", initial)
	}
	if err := journal.Append(envelope, time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}
	active, err := journal.Stats()
	if err != nil {
		t.Fatalf("active Stats: %v", err)
	}
	if active.ActiveRecords != 1 || active.TotalRecords != 1 || active.WALBytes <= 0 {
		t.Fatalf("active stats = %+v", active)
	}
	if err := journal.Append(envelope, time.Unix(2, 0)); err != nil {
		t.Fatal(err)
	}
	rotated, err := journal.Stats()
	if err != nil {
		t.Fatalf("rotated Stats: %v", err)
	}
	if rotated.SealedSegments != 1 || !rotated.ActiveSegment || rotated.ActiveRecords != 0 || rotated.TotalRecords != 2 || rotated.WALBytes != 0 {
		t.Fatalf("rotated stats = %+v", rotated)
	}
	if err := journal.Append(envelope, time.Unix(3, 0)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(root, policy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened journal: %v", err)
		}
	}()
	stats, err := reopened.Stats()
	if err != nil {
		t.Fatalf("reopened Stats: %v", err)
	}
	if stats.SealedSegments != 2 || !stats.ActiveSegment || stats.ActiveRecords != 0 || stats.TotalRecords != 3 || stats.WALBytes != 0 {
		t.Fatalf("reopened stats = %+v", stats)
	}
}

func TestJournalStatsReportRepairedWALTail(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(root, DirectoryName, "amber.wal")
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

	reopened, err := OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened journal: %v", err)
		}
	}()
	stats, err := reopened.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.WALCorruptRecords != 1 || stats.WALBytes != 0 {
		t.Fatalf("repaired stats = %+v", stats)
	}
}

func TestReplayRejectsOpenJournal(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(envelope, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := Replay(context.Background(), root, func(Envelope) error { return nil }); err == nil {
		t.Fatal("Replay() error = nil for open journal")
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJournalRejectsMissingAndUnknownFormatManifest(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, DirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unknown.data"), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(root, storage.DefaultRotationPolicy); err == nil {
		t.Fatal("OpenJournal() error = nil for data without format manifest")
	}

	if err := os.Remove(filepath.Join(dir, "unknown.data")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, FormatFileName), []byte(`{
  "journal_format_version": 2,
  "envelope_magic": "AOT4",
  "envelope_version": 4
}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(root, storage.DefaultRotationPolicy); err == nil {
		t.Fatal("OpenJournal() error = nil for unknown journal format")
	}
}

func TestJournalRecoversStaleFormatManifestTemp(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, DirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, FormatFileName+".tmp"), []byte("torn"), 0o600); err != nil {
		t.Fatal(err)
	}
	journal, err := OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenJournal() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	if err := loadFormatManifest(dir); err != nil {
		t.Fatalf("loadFormatManifest() error = %v", err)
	}
}

func TestJournalRejectsSymlinkFormatManifest(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, DirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "format-target")
	if err := os.WriteFile(target, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, FormatFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenJournal(root, storage.DefaultRotationPolicy); err == nil {
		t.Fatal("OpenJournal() error = nil for symlink format manifest")
	}
}

func TestReplayHonorsCancellation(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(envelope, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Replay(ctx, root, func(Envelope) error { return nil }); err == nil {
		t.Fatal("Replay() error = nil for canceled context")
	}
}

func TestJournalPruneByAgeSurvivesRestartAndReplay(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := journal.Append(envelope, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(envelope, now.Add(-10*time.Minute)); err != nil {
		t.Fatal(err)
	}

	result, err := journal.Prune(now, RetentionPolicy{MaxAge: time.Hour})
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if result.DeletedSegments != 1 || result.DeletedRecords != 1 || result.DeletedBytes == 0 {
		t.Fatalf("prune result = %+v, want one deleted record and segment", result)
	}
	stats, err := journal.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalRecords != 1 || stats.Retention.Runs != 1 || stats.Retention.Failures != 0 || stats.Retention.DeletedRecords != 1 || stats.Retention.LastSuccessAt.IsZero() {
		t.Fatalf("stats after prune = %+v", stats)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := Replay(context.Background(), root, func(Envelope) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("replayed %d envelopes after prune, want 1", count)
	}
}

func TestJournalPruneRotatesExpiredActiveSegment(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := journal.Append(envelope, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	result, err := journal.Prune(now, RetentionPolicy{MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 1 || result.DeletedRecords != 1 {
		t.Fatalf("prune result = %+v, want expired active segment deleted", result)
	}
	stats, err := journal.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalRecords != 0 || stats.SealedSegments != 0 || !stats.ActiveSegment {
		t.Fatalf("stats after active prune = %+v", stats)
	}
}

func TestJournalPruneUsesAcceptanceTimeForNativeEntries(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	entry := model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: time.Unix(1, 0),
		Level:     model.LevelInfo,
		Service:   "backfill",
		Body:      "old event accepted now",
	}
	if err := journal.AppendNormalizedLogs([]model.LogEntry{entry}); err != nil {
		t.Fatal(err)
	}
	result, err := journal.Prune(time.Now().UTC(), RetentionPolicy{MaxAge: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 0 {
		t.Fatalf("native backfill expired from event time: %+v", result)
	}
}

func TestJournalPruneEnforcesSegmentLimitAndRejectsInvalidPolicy(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		if err := journal.Append(envelope, time.Unix(int64(i+1), 0)); err != nil {
			t.Fatal(err)
		}
	}
	result, err := journal.Prune(time.Now().UTC(), RetentionPolicy{MaxSegments: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 2 || result.DeletedRecords != 2 {
		t.Fatalf("segment-limit result = %+v", result)
	}
	if _, err := journal.Prune(time.Now().UTC(), RetentionPolicy{MaxBytes: -1}); err == nil {
		t.Fatal("negative max bytes accepted")
	}
	stats, err := journal.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.Retention.Runs != 2 || stats.Retention.Failures != 1 {
		t.Fatalf("retention failure stats = %+v", stats.Retention)
	}
}

func TestJournalPruneResumesDeletePendingWithDisabledPolicy(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(envelope, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	metaPath := filepath.Join(root, DirectoryName, "meta.json")
	payload, err := os.ReadFile(metaPath) //nolint:gosec
	if err != nil {
		t.Fatal(err)
	}
	var meta storage.StoreMeta
	if err := json.Unmarshal(payload, &meta); err != nil {
		t.Fatal(err)
	}
	for i := range meta.Segments {
		if meta.Segments[i].Sealed {
			meta.Segments[i].DeletePending = true
			break
		}
	}
	payload, err = json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, payload, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reopened.Prune(time.Now().UTC(), RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if result.DeletedSegments != 1 || result.DeletedRecords != 1 {
		t.Fatalf("pending prune result = %+v", result)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := Replay(context.Background(), root, func(Envelope) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("replayed %d pending-deleted records, want 0", count)
	}
}

func TestJournalConcurrentPruneIsSerialized(t *testing.T) {
	root := t.TempDir()
	journal, err := OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Error(err)
		}
	}()
	envelope, err := New(SignalLogs, FidelityOTLP, richLogsRequest())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 8 {
		if err := journal.Append(envelope, time.Unix(int64(i+1), 0)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, pruneErr := journal.Prune(time.Now().UTC(), RetentionPolicy{MaxSegments: 1})
			errs <- pruneErr
		}()
	}
	wg.Wait()
	close(errs)
	for pruneErr := range errs {
		if pruneErr != nil {
			t.Fatal(pruneErr)
		}
	}
	stats, err := journal.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.TotalRecords != 1 || stats.Retention.Runs != 4 || stats.Retention.Failures != 0 || stats.Retention.DeletedRecords != 7 {
		t.Fatalf("stats after concurrent prune = %+v", stats)
	}
}

func TestSelectRetentionSegmentsCombinesAgeCountAndBytes(t *testing.T) {
	now := time.Unix(10_000, 0)
	segments := []storage.SegmentMeta{
		{ID: 1, Sealed: true, MinTS: now.Add(-5 * time.Hour).UnixNano(), MaxTS: now.Add(-4 * time.Hour).UnixNano(), SizeBytes: 10},
		{ID: 2, Sealed: true, MinTS: now.Add(-3 * time.Hour).UnixNano(), MaxTS: now.Add(-2 * time.Hour).UnixNano(), SizeBytes: 20},
		{ID: 3, Sealed: true, MinTS: now.Add(-time.Hour).UnixNano(), MaxTS: now.Add(-time.Hour).UnixNano(), SizeBytes: 30},
		{ID: 4, Sealed: true, DeletePending: true, MinTS: now.UnixNano(), MaxTS: now.UnixNano(), SizeBytes: 40},
	}
	selected := selectRetentionSegments(segments, now, RetentionPolicy{
		MaxAge:      3 * time.Hour,
		MaxBytes:    30,
		MaxSegments: 2,
	})
	if len(selected) != 3 || selected[0].ID != 1 || selected[1].ID != 2 || selected[2].ID != 4 {
		t.Fatalf("selected = %+v, want ids 1,2,4", selected)
	}
}
