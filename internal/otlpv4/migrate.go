package otlpv4

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
	"github.com/yaop-labs/amber/internal/storage"
)

const (
	MigrationFileName    = "OTLP_V4_MIGRATION.json"
	migrationFileVersion = 1
)

// MigrationResult identifies a verified, atomically published v4 root. Source
// remains untouched and is the rollback root.
type MigrationResult struct {
	SourceRoot  string            `json:"source_root"`
	TargetRoot  string            `json:"target_root"`
	DatabaseID  string            `json:"database_id"`
	RecordCount uint64            `json:"record_count"`
	SignalCount map[string]uint64 `json:"signal_count"`
	Digest      string            `json:"digest"`
}

type migrationState struct {
	Version      int               `json:"version"`
	Phase        string            `json:"phase"`
	SourceRoot   string            `json:"source_root"`
	TargetRoot   string            `json:"target_root"`
	DatabaseID   string            `json:"database_id"`
	SourceFormat int               `json:"source_format"`
	TargetFormat int               `json:"target_format"`
	RecordCount  uint64            `json:"record_count,omitempty"`
	SignalCount  map[string]uint64 `json:"signal_count,omitempty"`
	Digest       string            `json:"digest,omitempty"`
}

type replayDigest struct {
	hash   hashWriter
	total  uint64
	counts map[string]uint64
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

// MigrateLegacyV3 creates targetRoot from a cleanly closed legacy source. It
// never changes source data, and targetRoot must not exist.
func MigrateLegacyV3(ctx context.Context, sourceRoot, targetRoot string) (MigrationResult, error) {
	if err := ctx.Err(); err != nil {
		return MigrationResult{}, err
	}
	source, target, err := resolveMigrationPaths(sourceRoot, targetRoot)
	if err != nil {
		return MigrationResult{}, err
	}
	lock, err := fslock.Acquire(source)
	if err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: acquire source lock: %w", err)
	}
	defer func() { _ = lock.Release() }()
	identity, err := dbmeta.Load(source)
	if err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: load source identity: %w", err)
	}
	if _, err := os.Lstat(filepath.Join(source, DirectoryName)); err == nil {
		return MigrationResult{}, errors.New("otlpv4: source already contains a v4 journal")
	} else if !errors.Is(err, os.ErrNotExist) {
		return MigrationResult{}, fmt.Errorf("otlpv4: inspect source journal: %w", err)
	}

	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o750); err != nil { //nolint:gosec
		return MigrationResult{}, fmt.Errorf("otlpv4: create target parent: %w", err)
	}
	staging := filepath.Join(parent, "."+filepath.Base(target)+".otlp-v4-migrating")
	state := migrationState{
		Version: migrationFileVersion, Phase: "copying", SourceRoot: source, TargetRoot: target,
		DatabaseID: identity.ID, SourceFormat: 3, TargetFormat: int(FormatVersion),
	}
	if err := prepareMigrationStaging(staging, state); err != nil {
		return MigrationResult{}, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := copyMigrationRoot(ctx, source, staging); err != nil {
		return MigrationResult{}, err
	}
	state.Phase = "journal"
	if err := saveMigrationState(staging, state); err != nil {
		return MigrationResult{}, err
	}

	journal, err := OpenJournal(staging, storage.DefaultRotationPolicy)
	if err != nil {
		return MigrationResult{}, err
	}
	sourceDigest := newReplayDigest()
	sequence := int64(0)
	replayErr := ReplayLegacyV3(ctx, source, func(envelope Envelope) error {
		sequence++
		if err := sourceDigest.add(envelope); err != nil {
			return err
		}
		return journal.Append(envelope, time.Unix(0, sequence))
	})
	closeErr := journal.Close()
	if replayErr != nil {
		return MigrationResult{}, replayErr
	}
	if closeErr != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: close migrated journal: %w", closeErr)
	}

	state.Phase = "verifying"
	if err := saveMigrationState(staging, state); err != nil {
		return MigrationResult{}, err
	}
	targetDigest := newReplayDigest()
	if err := Replay(ctx, staging, func(envelope Envelope) error {
		if envelope.Fidelity() != FidelityNormalizedV3 {
			return errors.New("otlpv4: migrated journal contains non-v3 fidelity")
		}
		return targetDigest.add(envelope)
	}); err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: verify migrated journal: %w", err)
	}
	if !sourceDigest.equal(targetDigest) {
		return MigrationResult{}, errors.New("otlpv4: migrated semantic stream does not match source")
	}
	state.Phase = "complete"
	state.RecordCount = sourceDigest.total
	state.SignalCount = sourceDigest.counts
	state.Digest = sourceDigest.digest()
	if err := saveMigrationState(staging, state); err != nil {
		return MigrationResult{}, err
	}
	if err := syncMigrationTree(staging); err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: sync migration staging: %w", err)
	}
	if err := os.Rename(staging, target); err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: publish migrated root: %w", err)
	}
	published = true
	if err := syncMigrationDir(parent); err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: sync migration parent: %w", err)
	}
	return MigrationResult{
		SourceRoot: source, TargetRoot: target, DatabaseID: identity.ID,
		RecordCount: sourceDigest.total, SignalCount: cloneSignalCounts(sourceDigest.counts), Digest: sourceDigest.digest(),
	}, nil
}

func resolveMigrationPaths(sourceRoot, targetRoot string) (string, string, error) {
	source, err := filepath.Abs(sourceRoot)
	if err != nil {
		return "", "", fmt.Errorf("otlpv4: resolve source: %w", err)
	}
	target, err := filepath.Abs(targetRoot)
	if err != nil {
		return "", "", fmt.Errorf("otlpv4: resolve target: %w", err)
	}
	if source == target || pathWithin(target, source) || pathWithin(source, target) {
		return "", "", errors.New("otlpv4: source and target roots must not overlap")
	}
	info, err := os.Lstat(source)
	if err != nil {
		return "", "", fmt.Errorf("otlpv4: inspect source: %w", err)
	}
	if !info.IsDir() {
		return "", "", errors.New("otlpv4: source is not a directory")
	}
	if _, err := os.Lstat(target); err == nil {
		return "", "", errors.New("otlpv4: target already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", "", fmt.Errorf("otlpv4: inspect target: %w", err)
	}
	return source, target, nil
}

func pathWithin(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func prepareMigrationStaging(staging string, expected migrationState) error {
	info, err := os.Lstat(staging)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(staging, 0o750); err != nil { //nolint:gosec
			return fmt.Errorf("otlpv4: create migration staging: %w", err)
		}
		return saveMigrationState(staging, expected)
	}
	if err != nil {
		return fmt.Errorf("otlpv4: inspect migration staging: %w", err)
	}
	if !info.IsDir() {
		return errors.New("otlpv4: migration staging path is not a directory")
	}
	payload, err := readRegular(filepath.Join(staging, MigrationFileName))
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(staging)
		if readErr != nil || len(entries) != 0 {
			return errors.New("otlpv4: unowned migration staging directory exists")
		}
	} else if err != nil {
		return fmt.Errorf("otlpv4: read interrupted migration state: %w", err)
	} else {
		var previous migrationState
		if err := decodeStrictJSON(payload, &previous); err != nil {
			return fmt.Errorf("otlpv4: parse interrupted migration state: %w", err)
		}
		if previous.Version != migrationFileVersion || previous.SourceRoot != expected.SourceRoot ||
			previous.TargetRoot != expected.TargetRoot || previous.DatabaseID != expected.DatabaseID {
			return errors.New("otlpv4: migration staging belongs to another operation")
		}
	}
	if err := os.RemoveAll(staging); err != nil {
		return fmt.Errorf("otlpv4: clean interrupted migration staging: %w", err)
	}
	if err := os.Mkdir(staging, 0o750); err != nil { //nolint:gosec
		return fmt.Errorf("otlpv4: recreate migration staging: %w", err)
	}
	return saveMigrationState(staging, expected)
}

func copyMigrationRoot(ctx context.Context, source, staging string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		relative, err := filepath.Rel(source, path)
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
		if relative == MigrationFileName || relative == DirectoryName || strings.HasPrefix(relative, DirectoryName+string(filepath.Separator)) {
			return errors.New("otlpv4: source contains reserved v4 migration data")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("otlpv4: source contains symlink %s", relative)
		}
		target := filepath.Join(staging, relative)
		if entry.IsDir() {
			return os.Mkdir(target, 0o750) //nolint:gosec
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("otlpv4: source contains non-regular file %s", relative)
		}
		if err := copyMigrationFile(path, target, info.Mode().Perm()); err != nil {
			return fmt.Errorf("otlpv4: copy %s: %w", relative, err)
		}
		return nil
	})
}

func copyMigrationFile(source, target string, mode fs.FileMode) error {
	input, err := os.Open(source) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
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

func saveMigrationState(root string, state migrationState) error {
	payload, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("otlpv4: encode migration state: %w", err)
	}
	payload = append(payload, '\n')
	path := filepath.Join(root, MigrationFileName)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("otlpv4: create migration state: %w", err)
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("otlpv4: write migration state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("otlpv4: sync migration state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("otlpv4: close migration state: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("otlpv4: publish migration state: %w", err)
	}
	return syncMigrationDir(root)
}

func newReplayDigest() *replayDigest {
	return &replayDigest{hash: sha256.New(), counts: make(map[string]uint64)}
}

func (d *replayDigest) add(envelope Envelope) error {
	record, err := envelope.MarshalBinary()
	if err != nil {
		return err
	}
	var length [8]byte
	for i := range length {
		length[i] = byte(uint64(len(record)) >> (8 * i))
	}
	if _, err := d.hash.Write(length[:]); err != nil {
		return err
	}
	if _, err := d.hash.Write(record); err != nil {
		return err
	}
	d.total++
	d.counts[envelope.Signal().String()]++
	return nil
}

func (d *replayDigest) digest() string { return hex.EncodeToString(d.hash.Sum(nil)) }

func (d *replayDigest) equal(other *replayDigest) bool {
	if d.total != other.total || d.digest() != other.digest() || len(d.counts) != len(other.counts) {
		return false
	}
	for signal, count := range d.counts {
		if other.counts[signal] != count {
			return false
		}
	}
	return true
}

func cloneSignalCounts(counts map[string]uint64) map[string]uint64 {
	out := make(map[string]uint64, len(counts))
	for signal, count := range counts {
		out[signal] = count
	}
	return out
}

func syncMigrationTree(root string) error {
	var dirs []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, dir := range dirs {
		if err := syncMigrationDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func syncMigrationDir(path string) error {
	dir, err := os.Open(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
