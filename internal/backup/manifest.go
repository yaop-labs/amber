package backup

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/dbmeta"
)

const (
	// SnapshotFormatVersion is the current backup manifest format.
	SnapshotFormatVersion = 1
	ManifestFileName      = "manifest.json"
	CompletionFileName    = "COMPLETE"
	DataDirectoryName     = "data"
)

// Manifest is the complete, versioned description of one offline snapshot.
type Manifest struct {
	SnapshotFormatVersion int              `json:"snapshot_format_version"`
	Database              dbmeta.Identity  `json:"database"`
	CreatedAt             time.Time        `json:"created_at"`
	Checkpoints           CheckpointVector `json:"checkpoints"`
	Files                 []FileEntry      `json:"files"`
}

// CheckpointVector records the consistency boundary for every Amber engine.
type CheckpointVector struct {
	Logs     EngineCheckpoint `json:"logs"`
	Traces   EngineCheckpoint `json:"traces"`
	Metrics  EngineCheckpoint `json:"metrics"`
	Profiles EngineCheckpoint `json:"profiles"`
}

// EngineCheckpoint names the files that define one engine's offline state.
type EngineCheckpoint struct {
	Present        bool          `json:"present"`
	WALBoundaries  []WALBoundary `json:"wal_boundaries,omitempty"`
	ImmutableFiles []string      `json:"immutable_files,omitempty"`
	MutableFiles   []string      `json:"mutable_files,omitempty"`
	MetadataFiles  []string      `json:"metadata_files,omitempty"`
}

// WALBoundary pins the byte boundary copied for one WAL.
type WALBoundary struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// FileEntry authenticates one regular file copied from the data root.
type FileEntry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Mode   uint32 `json:"mode"`
	SHA256 string `json:"sha256"`
}

func marshalManifest(manifest Manifest) ([]byte, error) {
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("backup: marshal manifest: %w", err)
	}
	return append(payload, '\n'), nil
}

func parseManifest(payload []byte) (Manifest, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("backup: parse manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Manifest{}, errors.New("backup: parse manifest: trailing data")
	}
	if err := manifest.validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (m Manifest) validate() error {
	if m.SnapshotFormatVersion != SnapshotFormatVersion {
		return fmt.Errorf("backup: unsupported snapshot format version %d", m.SnapshotFormatVersion)
	}
	if err := m.Database.Validate(); err != nil {
		return fmt.Errorf("backup: manifest database: %w", err)
	}
	if m.CreatedAt.IsZero() {
		return errors.New("backup: manifest created_at is required")
	}
	if len(m.Files) == 0 {
		return errors.New("backup: manifest has no files")
	}

	files := make(map[string]FileEntry, len(m.Files))
	for i, file := range m.Files {
		if err := validateRelativePath(file.Path); err != nil {
			return fmt.Errorf("backup: manifest file %d: %w", i, err)
		}
		if file.Size < 0 {
			return fmt.Errorf("backup: manifest file %s has negative size", file.Path)
		}
		if file.Mode == 0 || file.Mode&^uint32(0o777) != 0 {
			return fmt.Errorf("backup: manifest file %s has invalid mode %#o", file.Path, file.Mode)
		}
		digest, err := hex.DecodeString(file.SHA256)
		if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != file.SHA256 {
			return fmt.Errorf("backup: manifest file %s has invalid sha256", file.Path)
		}
		if _, exists := files[file.Path]; exists {
			return fmt.Errorf("backup: manifest has duplicate file %s", file.Path)
		}
		files[file.Path] = file
	}
	if !slices.IsSortedFunc(m.Files, func(a, b FileEntry) int { return strings.Compare(a.Path, b.Path) }) {
		return errors.New("backup: manifest files are not sorted")
	}
	if _, ok := files[dbmeta.IdentityFileName]; !ok {
		return errors.New("backup: manifest omits database identity")
	}

	checkpoints := []struct {
		name       string
		prefix     string
		checkpoint EngineCheckpoint
	}{
		{name: "logs", prefix: "logs", checkpoint: m.Checkpoints.Logs},
		{name: "traces", prefix: "spans", checkpoint: m.Checkpoints.Traces},
		{name: "metrics", prefix: "metrics", checkpoint: m.Checkpoints.Metrics},
		{name: "profiles", prefix: "profiles", checkpoint: m.Checkpoints.Profiles},
	}
	for _, item := range checkpoints {
		if err := validateCheckpoint(item.name, item.prefix, item.checkpoint, files); err != nil {
			return err
		}
	}
	return nil
}

func validateCheckpoint(name, prefix string, checkpoint EngineCheckpoint, files map[string]FileEntry) error {
	referenced := make(map[string]struct{})
	checkPath := func(filePath string) error {
		if err := validateRelativePath(filePath); err != nil {
			return err
		}
		if _, ok := files[filePath]; !ok {
			return fmt.Errorf("references missing file %s", filePath)
		}
		if !strings.HasPrefix(filePath, prefix+"/") {
			return fmt.Errorf("references file outside %s engine: %s", name, filePath)
		}
		if _, duplicate := referenced[filePath]; duplicate {
			return fmt.Errorf("references file %s more than once", filePath)
		}
		referenced[filePath] = struct{}{}
		return nil
	}
	for _, boundary := range checkpoint.WALBoundaries {
		if err := checkPath(boundary.Path); err != nil {
			return fmt.Errorf("backup: %s checkpoint: %w", name, err)
		}
		if files[boundary.Path].Size != boundary.Size {
			return fmt.Errorf("backup: %s checkpoint WAL %s size mismatch", name, boundary.Path)
		}
	}
	for _, filePath := range checkpoint.ImmutableFiles {
		if err := checkPath(filePath); err != nil {
			return fmt.Errorf("backup: %s checkpoint: %w", name, err)
		}
	}
	for _, filePath := range checkpoint.MutableFiles {
		if err := checkPath(filePath); err != nil {
			return fmt.Errorf("backup: %s checkpoint: %w", name, err)
		}
	}
	for _, filePath := range checkpoint.MetadataFiles {
		if err := checkPath(filePath); err != nil {
			return fmt.Errorf("backup: %s checkpoint: %w", name, err)
		}
	}
	hasFiles := false
	for filePath := range files {
		if strings.HasPrefix(filePath, prefix+"/") {
			hasFiles = true
			if _, ok := referenced[filePath]; !ok {
				return fmt.Errorf("backup: %s checkpoint omits file %s", name, filePath)
			}
		}
	}
	if checkpoint.Present != hasFiles {
		return fmt.Errorf("backup: %s checkpoint presence does not match its files", name)
	}
	return nil
}

func validateRelativePath(filePath string) error {
	if filePath == "" || filePath == "." || strings.Contains(filePath, `\`) || path.IsAbs(filePath) {
		return fmt.Errorf("invalid relative path %q", filePath)
	}
	if cleaned := path.Clean(filePath); cleaned != filePath || strings.HasPrefix(cleaned, "../") {
		return fmt.Errorf("invalid relative path %q", filePath)
	}
	return nil
}
