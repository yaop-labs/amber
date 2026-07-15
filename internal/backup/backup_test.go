package backup

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/fslock"
	"github.com/yaop-labs/amber/internal/storage"
)

func TestCreateVerifyRestoreRoundTrip(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "logs"), 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "logs", "amber.wal"), []byte("durable wal"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "LOCK"), nil, 0o600); err != nil {
		t.Fatalf("write root lock: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "logs", "LOCK"), nil, 0o600); err != nil {
		t.Fatalf("write nested lock: %v", err)
	}

	snapshot := filepath.Join(root, "snapshot")
	created, err := Create(context.Background(), source, snapshot)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Checkpoint == "" || !created.Manifest.Checkpoints.Logs.Present {
		t.Fatalf("unexpected create result: %+v", created)
	}
	if len(created.Manifest.Checkpoints.Logs.WALBoundaries) != 1 {
		t.Fatalf("log WAL boundaries = %+v", created.Manifest.Checkpoints.Logs.WALBoundaries)
	}
	state, err := dbmeta.LoadBackupState(source)
	if err != nil {
		t.Fatalf("load source backup state: %v", err)
	}
	if state.LastSuccessful == nil || state.LastSuccessful.Checkpoint != created.Checkpoint {
		t.Fatalf("source backup state = %+v, want checkpoint %s", state, created.Checkpoint)
	}
	for _, file := range created.Manifest.Files {
		if filepath.Base(file.Path) == fslock.LockFileName {
			t.Fatalf("lock file included in manifest: %s", file.Path)
		}
	}

	verified, err := Verify(context.Background(), snapshot)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if verified.Checkpoint != created.Checkpoint || verified.Manifest.Database != created.Manifest.Database {
		t.Fatalf("verification changed checkpoint: created=%+v verified=%+v", created, verified)
	}

	restored := filepath.Join(root, "restored")
	if _, err := Restore(context.Background(), snapshot, restored); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	payload, err := os.ReadFile(filepath.Join(restored, "logs", "amber.wal"))
	if err != nil {
		t.Fatalf("read restored WAL: %v", err)
	}
	if string(payload) != "durable wal" {
		t.Fatalf("restored WAL = %q", payload)
	}
	identity, err := dbmeta.Load(restored)
	if err != nil {
		t.Fatalf("load restored identity: %v", err)
	}
	if identity != created.Manifest.Database {
		t.Fatalf("restored identity = %+v, want %+v", identity, created.Manifest.Database)
	}
	state, err = dbmeta.LoadBackupState(restored)
	if err != nil {
		t.Fatalf("load restored backup state: %v", err)
	}
	if state.LastSuccessful == nil || state.LastSuccessful.Checkpoint != created.Checkpoint ||
		state.LastVerifiedRestore == nil || state.LastVerifiedRestore.Checkpoint != created.Checkpoint {
		t.Fatalf("restored backup state = %+v, want checkpoint %s", state, created.Checkpoint)
	}
}

func TestCreateRejectsLockedDataRoot(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	lock, err := fslock.Acquire(source)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer lock.Release()
	_, err = Create(context.Background(), source, filepath.Join(root, "snapshot"))
	if !errors.Is(err, fslock.ErrLocked) {
		t.Fatalf("Create error = %v, want ErrLocked", err)
	}
}

func TestCreateRejectsRemoteOnlySegment(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(filepath.Join(source, "logs"), 0o750); err != nil {
		t.Fatalf("mkdir logs: %v", err)
	}
	local := false
	meta := storage.StoreMeta{NextSegmentID: 2, Segments: []storage.SegmentMeta{{
		ID: 1, FileName: "seg_00000001.alog", Sealed: true, LocalPresent: &local,
	}}}
	payload, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "logs", "meta.json"), payload, 0o600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	_, err = Create(context.Background(), source, filepath.Join(root, "snapshot"))
	if err == nil || !strings.Contains(err.Error(), "remote-only") {
		t.Fatalf("Create error = %v, want remote-only rejection", err)
	}
}
func TestVerifyRejectsCorruptionAndIncompleteSnapshot(t *testing.T) {
	t.Run("checksum", func(t *testing.T) {
		root, snapshot := createFixtureSnapshot(t)
		_ = root
		if err := os.WriteFile(filepath.Join(snapshot, DataDirectoryName, "payload"), []byte("changed"), 0o600); err != nil {
			t.Fatalf("corrupt payload: %v", err)
		}
		if _, err := Verify(context.Background(), snapshot); err == nil || !strings.Contains(err.Error(), "mismatch") {
			t.Fatalf("Verify error = %v, want mismatch", err)
		}
	})

	t.Run("completion", func(t *testing.T) {
		_, snapshot := createFixtureSnapshot(t)
		if err := os.Remove(filepath.Join(snapshot, CompletionFileName)); err != nil {
			t.Fatalf("remove completion: %v", err)
		}
		if _, err := Verify(context.Background(), snapshot); err == nil {
			t.Fatal("Verify accepted snapshot without completion marker")
		}
	})

	t.Run("extra file", func(t *testing.T) {
		_, snapshot := createFixtureSnapshot(t)
		if err := os.WriteFile(filepath.Join(snapshot, DataDirectoryName, "extra"), nil, 0o600); err != nil {
			t.Fatalf("write extra file: %v", err)
		}
		if _, err := Verify(context.Background(), snapshot); err == nil {
			t.Fatal("Verify accepted unmanifested file")
		}
	})
}

func TestCreateAndVerifyRejectSymlinks(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.MkdirAll(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0o600); err != nil {
		t.Fatalf("write outside: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(source, "link")); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := Create(context.Background(), source, filepath.Join(root, "snapshot")); err == nil {
		t.Fatal("Create accepted source symlink")
	}

	_, snapshot := createFixtureSnapshot(t)
	if err := os.Remove(filepath.Join(snapshot, DataDirectoryName, "payload")); err != nil {
		t.Fatalf("remove payload: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(snapshot, DataDirectoryName, "payload")); err != nil {
		t.Fatalf("symlink snapshot payload: %v", err)
	}
	if _, err := Verify(context.Background(), snapshot); err == nil {
		t.Fatal("Verify accepted snapshot symlink")
	}
}

func TestManifestRejectsPathTraversal(t *testing.T) {
	manifest := Manifest{
		SnapshotFormatVersion: SnapshotFormatVersion,
		Database: dbmeta.Identity{
			FormatVersion: dbmeta.FormatVersion,
			ID:            "00112233445566778899aabbccddeeff",
		},
		CreatedAt: time.Now(),
		Files:     []FileEntry{{Path: "../escape", Size: 1, Mode: 0o600, SHA256: strings.Repeat("0", 64)}},
	}
	if err := manifest.validate(); err == nil {
		t.Fatal("manifest accepted path traversal")
	}
}

func TestRestoreRejectsExistingDestination(t *testing.T) {
	root, snapshot := createFixtureSnapshot(t)
	destination := filepath.Join(root, "existing")
	if err := os.Mkdir(destination, 0o750); err != nil {
		t.Fatalf("mkdir destination: %v", err)
	}
	if _, err := Restore(context.Background(), snapshot, destination); err == nil {
		t.Fatal("Restore accepted existing destination")
	}
}

func createFixtureSnapshot(t *testing.T) (string, string) {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("original"), 0o600); err != nil {
		t.Fatalf("write payload: %v", err)
	}
	snapshot := filepath.Join(root, "snapshot")
	if _, err := Create(context.Background(), source, snapshot); err != nil {
		t.Fatalf("Create fixture: %v", err)
	}
	return root, snapshot
}
