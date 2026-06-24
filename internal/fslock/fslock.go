// Package fslock provides an advisory per-directory lock so two processes
// (or two embedded amber.Open calls) cannot operate on the same data
// directory at once - concurrent writers would silently corrupt the WAL,
// segments, and meta files.
//
// The lock is a flock(2) on a LOCK file inside the directory: it is released
// automatically by the kernel when the holding process dies, so a crash never
// leaves a stale lock behind.
package fslock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// LockFileName is the name of the lock file created inside the locked
// directory.
const LockFileName = "LOCK"

// ErrLocked is wrapped by Acquire when another process already holds the
// directory.
var ErrLocked = errors.New("fslock: directory is locked by another process")

// Lock is a held directory lock. Release it with Release; it is also
// released automatically when the process exits.
type Lock struct {
	f *os.File
}

// Acquire takes an exclusive non-blocking lock on dir. It creates the
// directory (and the LOCK file inside it) if missing. If another process -
// or another Lock in this process - holds the directory, it fails
// immediately with an error wrapping ErrLocked.
func Acquire(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("fslock: mkdir %s: %w", dir, err)
	}
	path := filepath.Join(dir, LockFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("fslock: open %s: %w", path, err)
	}
	if err := flock(f); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w: %s (held LOCK file: %s)", ErrLocked, dir, path)
	}
	return &Lock{f: f}, nil
}

// Release drops the lock. Safe to call on a nil receiver.
func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := l.f.Close() // closing the fd releases the flock
	l.f = nil
	return err
}
