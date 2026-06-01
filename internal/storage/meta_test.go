package storage

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestMeta_MigrateLocalPresent_LegacyMeta covers the upgrade path: an older
// meta.json predates LocalPresent, but the data file is still on disk; load
// must mark it present so retention's local-tier pass doesn't treat
// pre-migration segments as already evicted.
func TestMeta_MigrateLocalPresent_LegacyMeta(t *testing.T) {
	dir := t.TempDir()

	legacy := `{
		"next_segment_id": 2,
		"segments": [
			{"id": 1, "file_name": "seg_00000001.alog", "sealed": true, "size_bytes": 100}
		]
	}`
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(legacy), 0600); err != nil {
		t.Fatalf("write meta: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "seg_00000001.alog"), []byte("x"), 0600); err != nil {
		t.Fatalf("write seg file: %v", err)
	}

	m, err := loadMeta(dir)
	if err != nil {
		t.Fatalf("loadMeta: %v", err)
	}
	if len(m.Segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(m.Segments))
	}
	seg := m.Segments[0]
	if seg.LocalPresent == nil {
		t.Fatal("LocalPresent should have been backfilled, got nil")
	}
	if !*seg.LocalPresent {
		t.Fatal("LocalPresent should be true: file is on disk")
	}
	if !seg.HasLocalCopy() {
		t.Fatal("HasLocalCopy() should be true after backfill")
	}
}

// TestMeta_MigrateLocalPresent_FileMissing covers the orphan case: meta
// predates LocalPresent and the data file is gone (manual delete, fresh
// reconcile node before fetch). Backfill must set present=false so a later
// query knows to refetch.
func TestMeta_MigrateLocalPresent_FileMissing(t *testing.T) {
	dir := t.TempDir()

	legacy := `{
		"next_segment_id": 2,
		"segments": [
			{"id": 1, "file_name": "seg_00000001.alog", "sealed": true, "upload_state": 1}
		]
	}`
	if err := os.WriteFile(filepath.Join(dir, "meta.json"), []byte(legacy), 0600); err != nil {
		t.Fatalf("write meta: %v", err)
	}

	m, err := loadMeta(dir)
	if err != nil {
		t.Fatalf("loadMeta: %v", err)
	}
	seg := m.Segments[0]
	if seg.LocalPresent == nil {
		t.Fatal("LocalPresent should have been backfilled")
	}
	if *seg.LocalPresent {
		t.Fatal("LocalPresent should be false: file is missing")
	}
}

// TestMeta_RoundTrip_PreservesLocalPresent confirms that an explicit false
// survives marshal/unmarshal — important because the field is a pointer
// with omitempty.
func TestMeta_RoundTrip_PreservesLocalPresent(t *testing.T) {
	absent := false
	in := &StoreMeta{
		NextSegmentID: 2,
		Segments: []SegmentMeta{{
			ID:           1,
			FileName:     "seg_00000001.alog",
			Sealed:       true,
			UploadState:  UploadStateUploaded,
			LocalPresent: &absent,
		}},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out StoreMeta
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Segments[0].LocalPresent == nil {
		t.Fatal("explicit false should not become nil after roundtrip")
	}
	if *out.Segments[0].LocalPresent {
		t.Fatal("expected false")
	}
}

// TestMarkLocalEvicted_RejectsNotUploaded confirms the data-loss guard:
// evicting a not-yet-uploaded segment would lose the only copy.
func TestMarkLocalEvicted_RejectsNotUploaded(t *testing.T) {
	sm, dir := newTestManager(t)
	defer sm.Close()
	_ = dir

	if err := sm.Write([]byte("hello"), 1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	segs := sm.Segments()
	if len(segs) != 1 {
		t.Fatalf("expected 1 sealed segment, got %d", len(segs))
	}

	err := sm.MarkLocalEvicted(segs[0].ID)
	if err == nil {
		t.Fatal("expected error when evicting non-uploaded segment")
	}
}

// TestMarkLocalEvicted_Idempotent confirms a second call is a no-op.
func TestMarkLocalEvicted_Idempotent(t *testing.T) {
	sm, _ := newTestManager(t)
	defer sm.Close()

	if err := sm.Write([]byte("hello"), 1); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	segs := sm.Segments()
	if err := sm.MarkUploaded(segs[0].ID); err != nil {
		t.Fatalf("MarkUploaded: %v", err)
	}
	if err := sm.MarkLocalEvicted(segs[0].ID); err != nil {
		t.Fatalf("first MarkLocalEvicted: %v", err)
	}
	if err := sm.MarkLocalEvicted(segs[0].ID); err != nil {
		t.Fatalf("second MarkLocalEvicted (idempotent): %v", err)
	}
}
