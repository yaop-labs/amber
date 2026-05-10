package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const metaFileName = "meta.json"

type SegmentMeta struct {
	ID          uint32 `json:"id"`
	FileName    string `json:"file_name"`
	MinTS       int64  `json:"min_ts"`
	MaxTS       int64  `json:"max_ts"`
	RecordCount uint64 `json:"record_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Sealed      bool   `json:"sealed"`

	// LastSyncedSize is the segment file offset that has been fsync'd to disk
	// at the most recent checkpoint. On reopen the file is truncated to this
	// length to discard any bytes that drifted past it without a Sync. Only
	// meaningful while !Sealed; sealed segments are durable in their entirety.
	LastSyncedSize int64 `json:"last_synced_size,omitempty"`

	// LastSyncedSeq is the WAL seq of the highest-seq record contained in a
	// block that has been fsync'd. WAL records with seq <= LastSyncedSeq are
	// already durable in the segment and must be skipped on replay; otherwise
	// a crash between saveMeta and wal.Truncate would re-apply them and create
	// duplicates. Only meaningful while !Sealed.
	LastSyncedSeq uint64 `json:"last_synced_seq,omitempty"`
}

type StoreMeta struct {
	NextSegmentID uint32        `json:"next_segment_id"`
	Segments      []SegmentMeta `json:"segments"`
}

func loadMeta(dir string) (*StoreMeta, error) {
	path := filepath.Join(dir, metaFileName)

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &StoreMeta{NextSegmentID: 1}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("meta: read %s: %w", path, err)
	}

	var m StoreMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("meta: parse %s: %w", path, err)
	}

	return &m, nil
}

func saveMeta(dir string, m *StoreMeta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("meta: marshal: %w", err)
	}

	tmp := filepath.Join(dir, metaFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("meta: open tmp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("meta: write tmp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("meta: sync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("meta: close tmp: %w", err)
	}

	dst := filepath.Join(dir, metaFileName)
	if err := os.Rename(tmp, dst); err != nil {
		return fmt.Errorf("meta: rename: %w", err)
	}

	// fsync the directory so the rename is durable across power loss; without
	// this the directory entry update may be lost while the file content
	// survives, leaving stale meta on disk.
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}

	return nil
}

func segmentFileName(id uint32) string {
	return fmt.Sprintf("seg_%08d.alog", id)
}
