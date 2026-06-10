package fslock

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireRelease(t *testing.T) {
	dir := t.TempDir()

	l1, err := Acquire(dir)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}

	// flock conflicts apply across file descriptors, including within one
	// process — exactly the double-Open case we guard against.
	if _, err := Acquire(dir); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire: want ErrLocked, got %v", err)
	}

	if err := l1.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	l2, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	if err := l2.Release(); err != nil {
		t.Fatalf("second Release: %v", err)
	}
}

func TestAcquireCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "data")
	l, err := Acquire(dir)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer l.Release()

	if _, err := os.Stat(filepath.Join(dir, LockFileName)); err != nil {
		t.Errorf("LOCK file missing: %v", err)
	}
}

func TestReleaseNil(t *testing.T) {
	var l *Lock
	if err := l.Release(); err != nil {
		t.Errorf("nil Release: %v", err)
	}
	l = &Lock{}
	if err := l.Release(); err != nil {
		t.Errorf("empty Release: %v", err)
	}
}
