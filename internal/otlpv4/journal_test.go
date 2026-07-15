package otlpv4

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	envelope, err := New(SignalLogs, FidelityNormalizedV3, richLogsRequest())
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
