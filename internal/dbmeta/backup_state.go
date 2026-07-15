package dbmeta

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// BackupStateFileName stores the latest successful backup and restore
	// checkpoints observed for this data root.
	BackupStateFileName = "BACKUP_STATE.json"
	backupStateVersion  = 1
)

// BackupState is operational metadata; engine recovery never depends on it.
type BackupState struct {
	Version             int                `json:"version"`
	LastSuccessful      *BackupCheckpoint  `json:"last_successful_backup,omitempty"`
	LastVerifiedRestore *RestoreCheckpoint `json:"last_verified_restore,omitempty"`
}

// BackupCheckpoint identifies one completely published and verified snapshot.
type BackupCheckpoint struct {
	Checkpoint        string    `json:"checkpoint"`
	SnapshotCreatedAt time.Time `json:"snapshot_created_at"`
	CompletedAt       time.Time `json:"completed_at"`
}

// RestoreCheckpoint records that a snapshot was verified before publication.
type RestoreCheckpoint struct {
	Checkpoint        string    `json:"checkpoint"`
	SnapshotCreatedAt time.Time `json:"snapshot_created_at"`
	VerifiedAt        time.Time `json:"verified_at"`
}

// LoadBackupState reads optional operational backup metadata.
func LoadBackupState(root string) (BackupState, error) {
	path := filepath.Join(root, BackupStateFileName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return BackupState{Version: backupStateVersion}, nil
	}
	if err != nil {
		return BackupState{}, fmt.Errorf("backup state: inspect: %w", err)
	}
	if !info.Mode().IsRegular() {
		return BackupState{}, errors.New("backup state: file is not regular")
	}
	payload, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return BackupState{}, fmt.Errorf("backup state: read: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var state BackupState
	if err := decoder.Decode(&state); err != nil {
		return BackupState{}, fmt.Errorf("backup state: parse: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return BackupState{}, errors.New("backup state: parse: trailing data")
	}
	if err := state.Validate(); err != nil {
		return BackupState{}, err
	}
	return state, nil
}

// SaveBackupState atomically persists operational backup metadata. The caller
// must hold the data-root lock or be constructing an unpublished restore root.
func SaveBackupState(root string, state BackupState) error {
	state.Version = backupStateVersion
	if err := state.Validate(); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("backup state: marshal: %w", err)
	}
	payload = append(payload, '\n')
	tmp, err := os.CreateTemp(root, ".backup-state-*.tmp")
	if err != nil {
		return fmt.Errorf("backup state: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup state: chmod temp: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup state: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("backup state: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("backup state: close temp: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(root, BackupStateFileName)); err != nil {
		return fmt.Errorf("backup state: publish: %w", err)
	}
	if err := syncDir(root); err != nil {
		return fmt.Errorf("backup state: sync root: %w", err)
	}
	return nil
}

// Validate rejects unsupported metadata and malformed checkpoint identifiers.
func (s BackupState) Validate() error {
	if s.Version != backupStateVersion {
		return fmt.Errorf("backup state: unsupported version %d", s.Version)
	}
	if s.LastSuccessful != nil {
		if err := validateCheckpoint(s.LastSuccessful.Checkpoint); err != nil {
			return fmt.Errorf("backup state: last successful backup: %w", err)
		}
		if s.LastSuccessful.SnapshotCreatedAt.IsZero() || s.LastSuccessful.CompletedAt.IsZero() {
			return errors.New("backup state: last successful backup timestamps are required")
		}
	}
	if s.LastVerifiedRestore != nil {
		if err := validateCheckpoint(s.LastVerifiedRestore.Checkpoint); err != nil {
			return fmt.Errorf("backup state: last verified restore: %w", err)
		}
		if s.LastVerifiedRestore.SnapshotCreatedAt.IsZero() || s.LastVerifiedRestore.VerifiedAt.IsZero() {
			return errors.New("backup state: last verified restore timestamps are required")
		}
	}
	return nil
}

func validateCheckpoint(checkpoint string) error {
	digest, err := hex.DecodeString(checkpoint)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != checkpoint {
		return errors.New("checkpoint must be a lowercase SHA-256 digest")
	}
	return nil
}
