package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const metaFileName = "meta.json"

// UploadState is the remote upload state of a sealed segment.
type UploadState uint8

const (
	// UploadStateLocal means the segment has not been uploaded.
	UploadStateLocal UploadState = 0

	// UploadStateUploaded means the segment and sidecars are in remote storage.
	UploadStateUploaded UploadState = 1
)

type SegmentMeta struct {
	ID          uint32 `json:"id"`
	FileName    string `json:"file_name"`
	MinTS       int64  `json:"min_ts"`
	MaxTS       int64  `json:"max_ts"`
	RecordCount uint64 `json:"record_count"`
	SizeBytes   int64  `json:"size_bytes"`
	Sealed      bool   `json:"sealed"`

	// LastSyncedSize is the fsynced active-segment file size.
	LastSyncedSize int64 `json:"last_synced_size,omitempty"`

	// LastSyncedSeq is the highest WAL sequence fsynced into the active segment.
	LastSyncedSeq uint64 `json:"last_synced_seq,omitempty"`

	// UploadState is meaningful only for sealed segments.
	UploadState UploadState `json:"upload_state,omitempty"`

	// UploadAttempts counts failed upload attempts since the last success.
	UploadAttempts uint32 `json:"upload_attempts,omitempty"`

	// LastUploadErr is the most recent upload error message.
	LastUploadErr string `json:"last_upload_err,omitempty"`

	// LocalPresent records whether the data file exists on local disk.
	// Nil means the value must be inferred for legacy metadata.
	LocalPresent *bool `json:"local_present,omitempty"`

	// DeletePending marks a sealed segment selected for terminal deletion.
	DeletePending bool `json:"delete_pending,omitempty"`
}

// HasLocalCopy reports whether the segment data file is expected locally.
func (s SegmentMeta) HasLocalCopy() bool {
	if s.LocalPresent == nil {
		return true
	}
	return *s.LocalPresent
}

type StoreMeta struct {
	NextSegmentID uint32        `json:"next_segment_id"`
	Segments      []SegmentMeta `json:"segments"`
}

func loadMeta(dir string) (*StoreMeta, error) {
	path := filepath.Join(dir, metaFileName)

	data, err := os.ReadFile(path) //nolint:gosec
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

	migrateLocalPresent(dir, &m)
	return &m, nil
}

// migrateLocalPresent infers LocalPresent for legacy metadata.
func migrateLocalPresent(dir string, m *StoreMeta) {
	for i := range m.Segments {
		if m.Segments[i].LocalPresent != nil {
			continue
		}
		fileName := m.Segments[i].FileName
		if _, ok := ParseSegmentID(fileName); !ok {
			absent := false
			m.Segments[i].LocalPresent = &absent
			continue
		}
		path := filepath.Join(dir, fileName)
		present := true
		if _, err := os.Stat(path); err != nil && os.IsNotExist(err) { //nolint:gosec // path validated by ParseSegmentID above
			present = false
		}
		m.Segments[i].LocalPresent = &present
	}
}

func saveMeta(dir string, m *StoreMeta) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("meta: marshal: %w", err)
	}

	tmp := filepath.Join(dir, metaFileName+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600) //nolint:gosec
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
	if err := os.Rename(tmp, dst); err != nil { //nolint:gosec
		return fmt.Errorf("meta: rename: %w", err)
	}

	// Sync the directory so the rename is durable.
	if d, err := os.Open(dir); err == nil { //nolint:gosec
		_ = d.Sync()
		_ = d.Close()
	}

	return nil
}

func segmentFileName(id uint32) string {
	return fmt.Sprintf("seg_%08d.alog", id)
}

// ParseSegmentID extracts the numeric ID from a segment file name.
func ParseSegmentID(fileName string) (uint32, bool) {
	var id uint32
	n, err := fmt.Sscanf(fileName, "seg_%08d.alog", &id)
	if err != nil || n != 1 {
		return 0, false
	}
	return id, true
}
