package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const metaFileName = "meta.json"

// UploadState tracks the S3 upload lifecycle of a sealed segment. Zero value
// is UploadStateLocal so segments without the field (older meta.json) are
// treated as not-yet-uploaded, which is the safe default when S3 is enabled.
type UploadState uint8

const (
	// UploadStateLocal: segment exists on local disk only. For nodes without
	// a remote store this is the terminal state.
	UploadStateLocal UploadState = 0
	// UploadStateUploaded: segment and all sidecars are durable in the remote
	// store. Retention may delete the local copy.
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

	// UploadState tracks remote-store upload progress for sealed segments.
	// Only meaningful when Sealed=true and a remote SegmentStore is configured.
	UploadState UploadState `json:"upload_state,omitempty"`

	// UploadAttempts counts failed upload attempts since the last success.
	// Reset to zero on UploadStateUploaded. Used to drive backoff in the
	// background uploader.
	UploadAttempts uint32 `json:"upload_attempts,omitempty"`

	// LastUploadErr is the most recent upload error message (truncated). Empty
	// on success. Diagnostic only — never read for control flow.
	LastUploadErr string `json:"last_upload_err,omitempty"`

	// LocalPresent records whether the segment's data file currently exists on
	// local disk. Decoupled from UploadState so the four combinations are
	// expressible: fresh-sealed (Local+true), dual-resident (Uploaded+true),
	// cold-evicted (Uploaded+false). Local+false is invalid and rejected by
	// MarkLocalEvicted.
	//
	// Pointer so the zero JSON value (field absent) is distinguishable from an
	// explicit false. Older meta.json predates the field; loadMeta calls
	// migrateLocalPresent to set it based on file existence.
	LocalPresent *bool `json:"local_present,omitempty"`
}

// HasLocalCopy reports whether the segment's data file is expected to be on
// local disk. Treats a nil LocalPresent (legacy meta before migration) as
// true so pre-tiering segments are not mistaken for evicted ones.
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

// migrateLocalPresent fills in SegmentMeta.LocalPresent for entries that lack
// the field (older meta.json). The decision is purely file-existence based:
// if the .alog is on disk, mark present; otherwise mark absent. This is a
// one-time, idempotent backfill — the caller (loadMeta) doesn't persist the
// migrated state, so on every restart the same backfill runs cheaply.
// Persisting only happens when a real mutation (MarkLocalEvicted, MarkUploaded,
// etc.) writes meta back.
func migrateLocalPresent(dir string, m *StoreMeta) {
	for i := range m.Segments {
		if m.Segments[i].LocalPresent != nil {
			continue
		}
		path := filepath.Join(dir, m.Segments[i].FileName)
		present := true
		if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
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

	// fsync the directory so the rename is durable across power loss; without
	// this the directory entry update may be lost while the file content
	// survives, leaving stale meta on disk.
	if d, err := os.Open(dir); err == nil { //nolint:gosec
		_ = d.Sync()
		_ = d.Close()
	}

	return nil
}

func segmentFileName(id uint32) string {
	return fmt.Sprintf("seg_%08d.alog", id)
}

// ParseSegmentID extracts the numeric ID from a segment file name produced
// by segmentFileName. Returns (0, false) if the name doesn't match the
// expected pattern. Used by reconcile paths that learn about segments via
// remote-store listings rather than the local meta.
func ParseSegmentID(fileName string) (uint32, bool) {
	var id uint32
	n, err := fmt.Sscanf(fileName, "seg_%08d.alog", &id)
	if err != nil || n != 1 {
		return 0, false
	}
	return id, true
}
