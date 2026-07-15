// Package dbmeta owns database-wide metadata stored at the Amber data root.
package dbmeta

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	// IdentityFileName is the database identity file at the data root.
	IdentityFileName = "IDENTITY.json"
	// FormatVersion is the current database-wide format version.
	FormatVersion = 1
)

// Identity binds snapshots and restores to one logical Amber database.
type Identity struct {
	FormatVersion int    `json:"format_version"`
	ID            string `json:"id"`
}

// Load reads and validates an existing database identity.
func Load(root string) (Identity, error) {
	path := filepath.Join(root, IdentityFileName)
	info, err := os.Lstat(path)
	if err != nil {
		return Identity{}, fmt.Errorf("db identity: inspect: %w", err)
	}
	if !info.Mode().IsRegular() {
		return Identity{}, errors.New("db identity: identity file is not regular")
	}
	payload, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return Identity{}, fmt.Errorf("db identity: read: %w", err)
	}
	var identity Identity
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&identity); err != nil {
		return Identity{}, fmt.Errorf("db identity: parse: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return Identity{}, errors.New("db identity: parse: trailing data")
	}
	if err := identity.Validate(); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// LoadOrCreate loads the identity or atomically creates one for a new or
// legacy data root. The caller must hold the data-root lock.
func LoadOrCreate(root string) (Identity, error) {
	identity, err := Load(root)
	if err == nil {
		return identity, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return Identity{}, err
	}

	randomID := make([]byte, 16)
	if _, err := rand.Read(randomID); err != nil {
		return Identity{}, fmt.Errorf("db identity: generate: %w", err)
	}
	identity = Identity{FormatVersion: FormatVersion, ID: hex.EncodeToString(randomID)}
	if err := writeAtomic(root, identity); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// Validate checks the database-wide format and identity encoding.
func (i Identity) Validate() error {
	if i.FormatVersion != FormatVersion {
		return fmt.Errorf("db identity: unsupported format version %d", i.FormatVersion)
	}
	decoded, err := hex.DecodeString(i.ID)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != i.ID {
		return errors.New("db identity: id must be 16 lowercase hexadecimal bytes")
	}
	return nil
}

func writeAtomic(root string, identity Identity) error {
	payload, err := json.MarshalIndent(identity, "", "  ")
	if err != nil {
		return fmt.Errorf("db identity: marshal: %w", err)
	}
	payload = append(payload, '\n')

	tmp, err := os.CreateTemp(root, ".identity-*.tmp")
	if err != nil {
		return fmt.Errorf("db identity: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("db identity: chmod temp: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("db identity: write temp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("db identity: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("db identity: close temp: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(root, IdentityFileName)); err != nil {
		return fmt.Errorf("db identity: publish: %w", err)
	}
	if err := syncDir(root); err != nil {
		return fmt.Errorf("db identity: sync root: %w", err)
	}
	return nil
}

func syncDir(path string) error {
	dir, err := os.Open(path) //nolint:gosec
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
