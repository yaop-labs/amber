package otlpv4

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/fslock"
	"github.com/yaop-labs/amber/internal/storage"
)

// MigrationResult describes a verified copy-on-write v0.3 -> AOT4 migration.
// SourceRoot is left untouched and is the rollback root.
type MigrationResult struct {
	SourceRoot  string `json:"source_root"`
	TargetRoot  string `json:"target_root"`
	DatabaseID  string `json:"database_id"`
	RecordCount uint64 `json:"record_count"`
	Digest      string `json:"digest"`
}

// MigrateLegacyV3 copies a cleanly closed v0.3.0 root and adds a verified AOT4
// journal containing deterministic normalized legacy records. The source is
// never modified; targetRoot must not exist. The copy is published atomically.
func MigrateLegacyV3(ctx context.Context, sourceRoot, targetRoot string) (MigrationResult, error) {
	if err := ctx.Err(); err != nil {
		return MigrationResult{}, err
	}
	source, target, err := migrationPaths(sourceRoot, targetRoot)
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
	if _, err := os.Stat(filepath.Join(source, DirectoryName)); err == nil {
		return MigrationResult{}, errors.New("otlpv4: source already contains an AOT4 journal")
	} else if !errors.Is(err, os.ErrNotExist) {
		return MigrationResult{}, fmt.Errorf("otlpv4: inspect source journal: %w", err)
	}

	parent := filepath.Dir(target)
	if err := os.MkdirAll(parent, 0o750); err != nil { //nolint:gosec
		return MigrationResult{}, fmt.Errorf("otlpv4: create target parent: %w", err)
	}
	staging, err := os.MkdirTemp(parent, ".amber-migrate-*.tmp")
	if err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: create migration staging: %w", err)
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	if err := copyLegacyRoot(ctx, source, staging); err != nil {
		return MigrationResult{}, err
	}
	journal, err := OpenJournal(staging, storage.DefaultRotationPolicy)
	if err != nil {
		return MigrationResult{}, err
	}
	digest := sha256.New()
	var count uint64
	sequence := int64(0)
	replayErr := ReplayLegacyV3(ctx, source, func(envelope Envelope) error {
		sequence++
		encoded, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		if _, err := digest.Write(encoded); err != nil {
			return err
		}
		count++
		return journal.Append(envelope, time.Unix(0, sequence).UTC())
	})
	closeErr := journal.Close()
	if replayErr != nil {
		return MigrationResult{}, replayErr
	}
	if closeErr != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: close migrated journal: %w", closeErr)
	}
	verified, err := verifyMigrationJournal(ctx, staging, count, digest.Sum(nil))
	if err != nil {
		return MigrationResult{}, err
	}
	if err := syncMigrationRoot(staging); err != nil {
		return MigrationResult{}, err
	}
	if err := os.Rename(staging, target); err != nil {
		return MigrationResult{}, fmt.Errorf("otlpv4: publish migrated root: %w", err)
	}
	published = true
	if err := syncMigrationDir(parent); err != nil {
		return MigrationResult{}, err
	}
	return MigrationResult{SourceRoot: source, TargetRoot: target, DatabaseID: identity.ID, RecordCount: verified.count, Digest: hex.EncodeToString(verified.digest)}, nil
}

type migrationVerification struct {
	count  uint64
	digest []byte
}

func verifyMigrationJournal(ctx context.Context, root string, wantCount uint64, wantDigest []byte) (migrationVerification, error) {
	hash := sha256.New()
	var count uint64
	if err := Replay(ctx, root, func(envelope Envelope) error {
		encoded, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		_, _ = hash.Write(encoded)
		count++
		return nil
	}); err != nil {
		return migrationVerification{}, fmt.Errorf("otlpv4: verify migrated journal: %w", err)
	}
	digest := hash.Sum(nil)
	if count != wantCount || !equalBytes(digest, wantDigest) {
		return migrationVerification{}, errors.New("otlpv4: migrated journal digest mismatch")
	}
	return migrationVerification{count: count, digest: digest}, nil
}

func migrationPaths(sourceRoot, targetRoot string) (string, string, error) {
	source, err := filepath.Abs(sourceRoot)
	if err != nil {
		return "", "", fmt.Errorf("otlpv4: resolve source: %w", err)
	}
	target, err := filepath.Abs(targetRoot)
	if err != nil {
		return "", "", fmt.Errorf("otlpv4: resolve target: %w", err)
	}
	if source == target || pathWithin(source, target) || pathWithin(target, source) {
		return "", "", errors.New("otlpv4: source and target roots must not overlap")
	}
	info, err := os.Stat(source)
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

func pathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func copyLegacyRoot(ctx context.Context, source, target string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
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
		if relative == fslock.LockFileName {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0o750)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("otlpv4: legacy root contains non-regular file %s", relative)
		}
		if err := copyFile(path, destination); err != nil {
			return fmt.Errorf("otlpv4: copy legacy file %s: %w", relative, err)
		}
		return nil
	})
}

func copyFile(source, target string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	return errors.Join(copyErr, closeErr)
}

func syncMigrationRoot(root string) error {
	safeRoot, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { _ = safeRoot.Close() }()
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := safeRoot.OpenFile(rel, os.O_WRONLY, 0)
		if err != nil {
			return err
		}
		err = file.Sync()
		return errors.Join(err, file.Close())
	})
}

func syncMigrationDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
