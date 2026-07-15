// Package backup implements Amber's offline snapshot and restore protocol.
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/fslock"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/storage"
)

// Verification is the authenticated result of reading a complete snapshot.
type Verification struct {
	Manifest   Manifest
	Checkpoint string
}

// Create makes a complete offline snapshot of dataRoot at snapshotDir.
// dataRoot must not be open by Amber and snapshotDir must not exist.
func Create(ctx context.Context, dataRoot, snapshotDir string) (Verification, error) {
	root, destination, err := validateCreatePaths(dataRoot, snapshotDir)
	if err != nil {
		return Verification{}, err
	}
	lock, err := fslock.Acquire(root)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: acquire data-root lock: %w", err)
	}
	defer func() { _ = lock.Release() }()

	identity, err := dbmeta.LoadOrCreate(root)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: database identity: %w", err)
	}
	backupState, err := dbmeta.LoadBackupState(root)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: load operational state: %w", err)
	}
	sealed, err := validateLocalEventStores(root)
	if err != nil {
		return Verification{}, err
	}

	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return Verification{}, fmt.Errorf("backup: create snapshot parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".amber-snapshot-*.tmp")
	if err != nil {
		return Verification{}, fmt.Errorf("backup: create staging directory: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	dataDir := filepath.Join(staging, DataDirectoryName)
	if err := os.Mkdir(dataDir, 0o750); err != nil {
		return Verification{}, fmt.Errorf("backup: create data directory: %w", err)
	}
	files, err := copyDataRoot(ctx, root, dataDir)
	if err != nil {
		return Verification{}, err
	}
	if err := validateMetricsManifest(dataDir); err != nil {
		return Verification{}, err
	}
	manifest := Manifest{
		SnapshotFormatVersion: SnapshotFormatVersion,
		Database:              identity,
		CreatedAt:             time.Now().UTC(),
		Files:                 files,
	}
	manifest.Checkpoints = buildCheckpointVector(files, sealed)
	if err := manifest.validate(); err != nil {
		return Verification{}, err
	}
	payload, err := marshalManifest(manifest)
	if err != nil {
		return Verification{}, err
	}
	checkpoint := digestBytes(payload)
	if err := writeSyncedFile(filepath.Join(staging, ManifestFileName), payload); err != nil {
		return Verification{}, err
	}
	if err := writeSyncedFile(filepath.Join(staging, CompletionFileName), []byte(checkpoint+"\n")); err != nil {
		return Verification{}, err
	}
	if err := syncDirectoryTree(staging); err != nil {
		return Verification{}, fmt.Errorf("backup: sync snapshot: %w", err)
	}
	if err := os.Rename(staging, destination); err != nil {
		return Verification{}, fmt.Errorf("backup: publish snapshot: %w", err)
	}
	published = true
	if err := syncDir(parent); err != nil {
		return Verification{}, fmt.Errorf("backup: sync snapshot parent: %w", err)
	}
	verified := Verification{Manifest: manifest, Checkpoint: checkpoint}
	backupState.LastSuccessful = &dbmeta.BackupCheckpoint{
		Checkpoint:        checkpoint,
		SnapshotCreatedAt: manifest.CreatedAt,
		CompletedAt:       time.Now().UTC(),
	}
	if err := dbmeta.SaveBackupState(root, backupState); err != nil {
		return verified, fmt.Errorf("backup: snapshot published but operational state update failed: %w", err)
	}
	return verified, nil
}

// Verify authenticates the manifest, completion marker, file set, sizes, and
// checksums without modifying the snapshot.
func Verify(ctx context.Context, snapshotDir string) (Verification, error) {
	root, err := filepath.Abs(snapshotDir)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: resolve snapshot path: %w", err)
	}
	manifestPayload, err := readRegularFile(filepath.Join(root, ManifestFileName))
	if err != nil {
		return Verification{}, fmt.Errorf("backup: read manifest: %w", err)
	}
	checkpoint := digestBytes(manifestPayload)
	completion, err := readRegularFile(filepath.Join(root, CompletionFileName))
	if err != nil {
		return Verification{}, fmt.Errorf("backup: read completion marker: %w", err)
	}
	if string(completion) != checkpoint+"\n" {
		return Verification{}, errors.New("backup: completion marker does not authenticate manifest")
	}
	manifest, err := parseManifest(manifestPayload)
	if err != nil {
		return Verification{}, err
	}

	dataDir := filepath.Join(root, DataDirectoryName)
	actual, err := snapshotFileSet(dataDir)
	if err != nil {
		return Verification{}, err
	}
	if len(actual) != len(manifest.Files) {
		return Verification{}, fmt.Errorf("backup: snapshot file count %d does not match manifest %d", len(actual), len(manifest.Files))
	}
	for _, entry := range manifest.Files {
		if err := ctx.Err(); err != nil {
			return Verification{}, err
		}
		actualPath, ok := actual[entry.Path]
		if !ok {
			return Verification{}, fmt.Errorf("backup: snapshot file missing: %s", entry.Path)
		}
		size, digest, err := digestFile(actualPath)
		if err != nil {
			return Verification{}, fmt.Errorf("backup: verify %s: %w", entry.Path, err)
		}
		if size != entry.Size {
			return Verification{}, fmt.Errorf("backup: size mismatch for %s", entry.Path)
		}
		if digest != entry.SHA256 {
			return Verification{}, fmt.Errorf("backup: checksum mismatch for %s", entry.Path)
		}
	}
	identity, err := dbmeta.Load(dataDir)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: verify database identity: %w", err)
	}
	if identity != manifest.Database {
		return Verification{}, errors.New("backup: database identity does not match manifest")
	}
	if _, err := dbmeta.LoadBackupState(dataDir); err != nil {
		return Verification{}, fmt.Errorf("backup: verify operational state: %w", err)
	}
	if err := validateMetricsManifest(dataDir); err != nil {
		return Verification{}, err
	}
	return Verification{Manifest: manifest, Checkpoint: checkpoint}, nil
}

// Restore verifies snapshotDir and atomically publishes it as a new dataRoot.
// dataRoot must not already exist.
func Restore(ctx context.Context, snapshotDir, dataRoot string) (Verification, error) {
	verified, err := Verify(ctx, snapshotDir)
	if err != nil {
		return Verification{}, err
	}
	destination, err := filepath.Abs(dataRoot)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: resolve restore path: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return Verification{}, fmt.Errorf("backup: restore destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Verification{}, fmt.Errorf("backup: inspect restore destination: %w", err)
	}
	parent := filepath.Dir(destination)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return Verification{}, fmt.Errorf("backup: create restore parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".amber-restore-*.tmp")
	if err != nil {
		return Verification{}, fmt.Errorf("backup: create restore staging: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	snapshotData, err := filepath.Abs(filepath.Join(snapshotDir, DataDirectoryName))
	if err != nil {
		return Verification{}, fmt.Errorf("backup: resolve snapshot data: %w", err)
	}
	for _, entry := range verified.Manifest.Files {
		if err := ctx.Err(); err != nil {
			return Verification{}, err
		}
		source, err := secureSnapshotPath(snapshotData, entry.Path)
		if err != nil {
			return Verification{}, err
		}
		target := filepath.Join(staging, filepath.FromSlash(entry.Path))
		if err := copyFile(source, target, fs.FileMode(entry.Mode)); err != nil {
			return Verification{}, fmt.Errorf("backup: restore %s: %w", entry.Path, err)
		}
		size, digest, err := digestFile(target)
		if err != nil {
			return Verification{}, fmt.Errorf("backup: verify restored %s: %w", entry.Path, err)
		}
		if size != entry.Size || digest != entry.SHA256 {
			return Verification{}, fmt.Errorf("backup: snapshot changed while restoring %s", entry.Path)
		}
	}
	if err := validateMetricsManifest(staging); err != nil {
		return Verification{}, err
	}
	state, err := dbmeta.LoadBackupState(staging)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: load restored operational state: %w", err)
	}
	state.LastSuccessful = &dbmeta.BackupCheckpoint{
		Checkpoint:        verified.Checkpoint,
		SnapshotCreatedAt: verified.Manifest.CreatedAt,
		CompletedAt:       verified.Manifest.CreatedAt,
	}
	state.LastVerifiedRestore = &dbmeta.RestoreCheckpoint{
		Checkpoint:        verified.Checkpoint,
		SnapshotCreatedAt: verified.Manifest.CreatedAt,
		VerifiedAt:        time.Now().UTC(),
	}
	if err := dbmeta.SaveBackupState(staging, state); err != nil {
		return Verification{}, fmt.Errorf("backup: record verified restore: %w", err)
	}
	if err := syncDirectoryTree(staging); err != nil {
		return Verification{}, fmt.Errorf("backup: sync restored data: %w", err)
	}
	if err := os.Rename(staging, destination); err != nil {
		return Verification{}, fmt.Errorf("backup: publish restored data: %w", err)
	}
	published = true
	if err := syncDir(parent); err != nil {
		return Verification{}, fmt.Errorf("backup: sync restore parent: %w", err)
	}
	return verified, nil
}

func validateCreatePaths(dataRoot, snapshotDir string) (string, string, error) {
	root, err := filepath.Abs(dataRoot)
	if err != nil {
		return "", "", fmt.Errorf("backup: resolve data root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", "", fmt.Errorf("backup: inspect data root: %w", err)
	}
	if !info.IsDir() {
		return "", "", errors.New("backup: data root is not a directory")
	}
	destination, err := filepath.Abs(snapshotDir)
	if err != nil {
		return "", "", fmt.Errorf("backup: resolve snapshot path: %w", err)
	}
	if _, err := os.Lstat(destination); err == nil {
		return "", "", fmt.Errorf("backup: snapshot destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("backup: inspect snapshot destination: %w", err)
	}
	rel, err := filepath.Rel(root, destination)
	if err != nil {
		return "", "", fmt.Errorf("backup: compare snapshot and data paths: %w", err)
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return "", "", errors.New("backup: snapshot destination must be outside the data root")
	}
	return root, destination, nil
}

func validateLocalEventStores(root string) (map[string]bool, error) {
	sealed := make(map[string]bool)
	for _, dir := range []string{"logs", "spans"} {
		metaPath := filepath.Join(root, dir, "meta.json")
		payload, err := os.ReadFile(metaPath) //nolint:gosec
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("backup: read %s metadata: %w", dir, err)
		}
		var meta storage.StoreMeta
		if err := json.Unmarshal(payload, &meta); err != nil {
			return nil, fmt.Errorf("backup: parse %s metadata: %w", dir, err)
		}
		for _, segment := range meta.Segments {
			if _, ok := storage.ParseSegmentID(segment.FileName); !ok {
				return nil, fmt.Errorf("backup: %s metadata has invalid segment name %q", dir, segment.FileName)
			}
			relative := filepath.ToSlash(filepath.Join(dir, segment.FileName))
			if !segment.HasLocalCopy() {
				return nil, fmt.Errorf("backup: %s is remote-only; local snapshot would be incomplete", relative)
			}
			info, err := os.Stat(filepath.Join(root, filepath.FromSlash(relative))) //nolint:gosec
			if err != nil {
				return nil, fmt.Errorf("backup: required segment %s: %w", relative, err)
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("backup: required segment %s is not a regular file", relative)
			}
			sealed[relative] = segment.Sealed
		}
	}
	return sealed, nil
}

func copyDataRoot(ctx context.Context, root, destination string) ([]FileEntry, error) {
	var files []FileEntry
	err := filepath.WalkDir(root, func(source string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(root, source)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		if entry.Name() == fslock.LockFileName {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup: symlink is not supported: %s", relative)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backup: non-regular file is not supported: %s", relative)
		}
		relative = filepath.ToSlash(relative)
		if err := validateRelativePath(relative); err != nil {
			return err
		}
		target := filepath.Join(destination, filepath.FromSlash(relative))
		if err := copyFile(source, target, info.Mode().Perm()); err != nil {
			return fmt.Errorf("backup: copy %s: %w", relative, err)
		}
		size, digest, err := digestFile(target)
		if err != nil {
			return fmt.Errorf("backup: digest %s: %w", relative, err)
		}
		if size != info.Size() {
			return fmt.Errorf("backup: source file changed while copying: %s", relative)
		}
		files = append(files, FileEntry{Path: relative, Size: size, Mode: uint32(info.Mode().Perm()), SHA256: digest})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

func buildCheckpointVector(files []FileEntry, sealed map[string]bool) CheckpointVector {
	vector := CheckpointVector{}
	populate := func(prefix string, checkpoint *EngineCheckpoint, walName string) {
		for _, file := range files {
			if !strings.HasPrefix(file.Path, prefix+"/") {
				continue
			}
			checkpoint.Present = true
			base := filepath.Base(file.Path)
			switch {
			case base == walName:
				checkpoint.WALBoundaries = append(checkpoint.WALBoundaries, WALBoundary{Path: file.Path, Size: file.Size})
			case prefix != "metrics" && strings.HasSuffix(base, ".alog") && !sealed[file.Path]:
				checkpoint.MutableFiles = append(checkpoint.MutableFiles, file.Path)
			case sealed[file.Path], prefix == "metrics" && (strings.HasSuffix(base, ".meb") || strings.HasSuffix(base, ".mhb")):
				checkpoint.ImmutableFiles = append(checkpoint.ImmutableFiles, file.Path)
			default:
				checkpoint.MetadataFiles = append(checkpoint.MetadataFiles, file.Path)
			}
		}
	}
	populate("logs", &vector.Logs, "amber.wal")
	populate("spans", &vector.Traces, "amber.wal")
	populate("metrics", &vector.Metrics, "head.wal")
	return vector
}

func snapshotFileSet(dataDir string) (map[string]string, error) {
	files := make(map[string]string)
	err := filepath.WalkDir(dataDir, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(dataDir, filePath)
		if err != nil {
			return err
		}
		if relative == "." {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("backup: snapshot contains symlink: %s", relative)
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backup: snapshot contains non-regular file: %s", relative)
		}
		relative = filepath.ToSlash(relative)
		if err := validateRelativePath(relative); err != nil {
			return err
		}
		files[relative] = filePath
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("backup: inspect snapshot data: %w", err)
	}
	return files, nil
}

func secureSnapshotPath(root, relative string) (string, error) {
	if err := validateRelativePath(relative); err != nil {
		return "", err
	}
	current := root
	for _, component := range strings.Split(filepath.FromSlash(relative), string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("backup: snapshot path traverses symlink: %s", relative)
		}
	}
	return current, nil
}

func validateMetricsManifest(root string) error {
	metricsDir := filepath.Join(root, "metrics")
	manifestPath := filepath.Join(metricsDir, "manifest.json")
	payload, err := os.ReadFile(manifestPath) //nolint:gosec
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("backup: read restored metrics manifest: %w", err)
	}
	var manifest mestore.Manifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return fmt.Errorf("backup: parse metrics manifest: %w", err)
	}
	if manifest.Version < 0 || manifest.Version > 1 {
		return fmt.Errorf("backup: unsupported metrics manifest version %d", manifest.Version)
	}
	for _, block := range manifest.Blocks {
		if block.Path == "" || filepath.Base(block.Path) != block.Path || strings.Contains(block.Path, `\`) {
			return fmt.Errorf("backup: invalid metrics block path %q", block.Path)
		}
		if !strings.HasSuffix(block.Path, ".meb") && !strings.HasSuffix(block.Path, ".mhb") {
			return fmt.Errorf("backup: invalid metrics block extension %q", block.Path)
		}
		info, err := os.Lstat(filepath.Join(metricsDir, block.Path))
		if err != nil {
			return fmt.Errorf("backup: metrics manifest block %s: %w", block.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("backup: metrics manifest block %s is not regular", block.Path)
		}
	}
	return nil
}

func copyFile(source, target string, mode fs.FileMode) error {
	input, err := os.Open(source) //nolint:gosec
	if err != nil {
		return err
	}
	defer input.Close()
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		return err
	}
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode.Perm()) //nolint:gosec
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	return output.Close()
}

func writeSyncedFile(filePath string, payload []byte) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) //nolint:gosec
	if err != nil {
		return fmt.Errorf("backup: open %s: %w", filepath.Base(filePath), err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("backup: write %s: %w", filepath.Base(filePath), err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("backup: sync %s: %w", filepath.Base(filePath), err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("backup: close %s: %w", filepath.Base(filePath), err)
	}
	return nil
}

func readRegularFile(filePath string) ([]byte, error) {
	info, err := os.Lstat(filePath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return os.ReadFile(filePath) //nolint:gosec
}

func digestFile(filePath string) (int64, string, error) {
	file, err := os.Open(filePath) //nolint:gosec
	if err != nil {
		return 0, "", err
	}
	defer file.Close()
	hash := sha256.New()
	size, err := io.Copy(hash, file)
	if err != nil {
		return 0, "", err
	}
	return size, hex.EncodeToString(hash.Sum(nil)), nil
}

func digestBytes(payload []byte) string {
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func syncDirectoryTree(root string) error {
	var directories []string
	err := filepath.WalkDir(root, func(filePath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			directories = append(directories, filePath)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(directories, func(i, j int) bool { return len(directories[i]) > len(directories[j]) })
	for _, directory := range directories {
		if err := syncDir(directory); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(directory string) error {
	dir, err := os.Open(directory) //nolint:gosec
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
