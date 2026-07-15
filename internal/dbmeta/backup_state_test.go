package dbmeta

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBackupStateRoundTrip(t *testing.T) {
	root := t.TempDir()
	now := time.Now().UTC()
	state := BackupState{
		LastSuccessful: &BackupCheckpoint{
			Checkpoint: strings.Repeat("a", 64), SnapshotCreatedAt: now, CompletedAt: now.Add(time.Second),
		},
		LastVerifiedRestore: &RestoreCheckpoint{
			Checkpoint: strings.Repeat("b", 64), SnapshotCreatedAt: now, VerifiedAt: now.Add(2 * time.Second),
		},
	}
	if err := SaveBackupState(root, state); err != nil {
		t.Fatalf("SaveBackupState: %v", err)
	}
	loaded, err := LoadBackupState(root)
	if err != nil {
		t.Fatalf("LoadBackupState: %v", err)
	}
	if loaded.LastSuccessful.Checkpoint != state.LastSuccessful.Checkpoint ||
		loaded.LastVerifiedRestore.Checkpoint != state.LastVerifiedRestore.Checkpoint {
		t.Fatalf("loaded state = %+v, want %+v", loaded, state)
	}
}

func TestLoadBackupStateMissingIsEmpty(t *testing.T) {
	state, err := LoadBackupState(t.TempDir())
	if err != nil {
		t.Fatalf("LoadBackupState: %v", err)
	}
	if state.Version != backupStateVersion || state.LastSuccessful != nil || state.LastVerifiedRestore != nil {
		t.Fatalf("missing state = %+v", state)
	}
}

func TestLoadBackupStateRejectsCorruption(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, BackupStateFileName), []byte(`{"version":1,"unknown":true}`), 0o600); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if _, err := LoadBackupState(root); err == nil {
		t.Fatal("accepted corrupt backup state")
	}
}
