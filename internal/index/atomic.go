package index

import (
	"fmt"
	"os"
	"path/filepath"
)

// atomicWriteFile writes data to path through a fsynced temp file and rename.
func atomicWriteFile(path string, data []byte) error {
	return atomicWrite(path, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

// atomicWrite is the streaming variant: callers receive a fresh tmp file and
// write to it; the helper handles fsync + rename + dir fsync.
func atomicWrite(path string, write func(*os.File) error) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("atomic: open tmp %s: %w", tmp, err)
	}
	if err := write(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic: rename %s -> %s: %w", tmp, path, err)
	}
	if d, err := os.Open(filepath.Dir(path)); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
