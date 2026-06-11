package index

import (
	"context"
	"path/filepath"
	"slices"
	"testing"
)

func TestSparseIndex_AddAndLookup(t *testing.T) {
	s := NewSparseIndex()
	s.Add(SegmentTimeRange{SegmentID: 1, FileName: "seg_1.alog", MinTS: 100, MaxTS: 200})
	s.Add(SegmentTimeRange{SegmentID: 2, FileName: "seg_2.alog", MinTS: 300, MaxTS: 400})
	s.Add(SegmentTimeRange{SegmentID: 3, FileName: "seg_3.alog", MinTS: 500, MaxTS: 600})

	got := s.Lookup(100, 600)
	if len(got) != 3 {
		t.Errorf("expected 3, got %d", len(got))
	}

	got = s.Lookup(150, 350)
	if len(got) != 2 {
		t.Errorf("expected 2 (seg1, seg2), got %d", len(got))
	}

	got = s.Lookup(700, 800)
	if len(got) != 0 {
		t.Errorf("expected 0, got %d", len(got))
	}
}

func TestSparseIndex_Lookup_SortedByMinTS(t *testing.T) {
	s := NewSparseIndex()
	s.Add(SegmentTimeRange{SegmentID: 3, MinTS: 500, MaxTS: 600})
	s.Add(SegmentTimeRange{SegmentID: 1, MinTS: 100, MaxTS: 200})
	s.Add(SegmentTimeRange{SegmentID: 2, MinTS: 300, MaxTS: 400})

	got := s.Lookup(0, 1000)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	if got[0].SegmentID != 1 || got[1].SegmentID != 2 || got[2].SegmentID != 3 {
		t.Errorf("not sorted by MinTS: %v", got)
	}
}

func TestSparseIndex_Remove(t *testing.T) {
	s := NewSparseIndex()
	s.Add(SegmentTimeRange{SegmentID: 1, MinTS: 100, MaxTS: 200})
	s.Add(SegmentTimeRange{SegmentID: 2, MinTS: 300, MaxTS: 400})
	s.Remove(1)

	got := s.Lookup(0, 1000)
	if len(got) != 1 || got[0].SegmentID != 2 {
		t.Errorf("expected only seg 2 after remove, got %v", got)
	}
}

func TestSparseIndex_Update(t *testing.T) {
	s := NewSparseIndex()
	s.Add(SegmentTimeRange{SegmentID: 1, MinTS: 100, MaxTS: 200})
	s.Add(SegmentTimeRange{SegmentID: 1, MinTS: 100, MaxTS: 500})

	if s.Size() != 1 {
		t.Errorf("expected 1 entry after update, got %d", s.Size())
	}
	got := s.Lookup(400, 600)
	if len(got) != 1 {
		t.Errorf("expected updated range to match, got %d", len(got))
	}
}

func TestSparseIndex_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	s := NewSparseIndex()
	s.Add(SegmentTimeRange{SegmentID: 1, FileName: "seg_1.alog", MinTS: 100, MaxTS: 200})
	s.Add(SegmentTimeRange{SegmentID: 2, FileName: "seg_2.alog", MinTS: 300, MaxTS: 400})

	if err := s.Save(dir); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadSparseIndex(dir)
	if err != nil {
		t.Fatalf("LoadSparseIndex: %v", err)
	}

	if loaded.Size() != 2 {
		t.Errorf("expected 2 after load, got %d", loaded.Size())
	}
	got := loaded.Lookup(150, 350)
	if len(got) != 2 {
		t.Errorf("expected 2 after load+lookup, got %d", len(got))
	}
}

func TestSparseIndex_LoadNotExist(t *testing.T) {
	dir := t.TempDir()
	s, err := LoadSparseIndex(dir)
	if err != nil {
		t.Fatalf("LoadSparseIndex non-existent: %v", err)
	}
	if s.Size() != 0 {
		t.Error("expected empty index")
	}
}

func TestBitmapIndex_AddAndGet(t *testing.T) {
	b := NewBitmapIndex()
	b.Add("ERROR", 1)
	b.Add("ERROR", 2)
	b.Add("ERROR", 3)
	b.Add("INFO", 4)

	errorIDs := b.getSortedShared("ERROR")
	if len(errorIDs) != 3 {
		t.Errorf("expected 3 ERROR entries, got %d", len(errorIDs))
	}

	infoIDs := b.getSortedShared("INFO")
	if len(infoIDs) != 1 {
		t.Errorf("expected 1 INFO entry, got %d", len(infoIDs))
	}
}

func TestBitmapIndex_Get_NotFound(t *testing.T) {
	b := NewBitmapIndex()
	if ids := b.getSortedShared("MISSING"); ids != nil {
		t.Error("expected nil set for unknown value")
	}
}

func TestMultiFieldIndex_Filter_SingleField(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)
	m.Add("level", "ERROR", 2)
	m.Add("level", "INFO", 3)

	result := m.Filter(map[string]string{"level": "ERROR"})
	if len(result) != 2 {
		t.Errorf("expected 2 ERROR entries, got %d", len(result))
	}
}

func TestMultiFieldIndex_Filter_MultiField_AND(t *testing.T) {
	m := NewMultiFieldIndex()
	// entryID=1: level=ERROR, service=api
	m.Add("level", "ERROR", 1)
	m.Add("service", "api", 1)
	// entryID=2: level=ERROR, service=worker
	m.Add("level", "ERROR", 2)
	m.Add("service", "worker", 2)
	// entryID=3: level=INFO, service=api
	m.Add("level", "INFO", 3)
	m.Add("service", "api", 3)

	result := m.Filter(map[string]string{
		"level":   "ERROR",
		"service": "api",
	})
	if len(result) != 1 {
		t.Errorf("expected 1, got %d", len(result))
	}
	if !slices.Contains(result, 1) {
		t.Error("expected entryID=1 in result")
	}
}

func TestMultiFieldIndex_Filter_UnknownField(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)

	result := m.Filter(map[string]string{"nonexistent": "value"})
	if len(result) != 0 {
		t.Error("expected empty result for unknown field")
	}
}

func TestMultiFieldIndex_FilterAny(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)
	m.Add("level", "FATAL", 2)
	m.Add("level", "INFO", 3)

	result := m.FilterAny("level", []string{"ERROR", "FATAL"})
	if len(result) != 2 {
		t.Errorf("expected 2 (ERROR+FATAL), got %d", len(result))
	}
}

func TestMultiFieldIndex_FilterMulti_ORWithinFieldANDAcross(t *testing.T) {
	m := NewMultiFieldIndex()
	// entryID=1: level=ERROR, service=api
	m.Add("level", "ERROR", 1)
	m.Add("service", "api", 1)
	// entryID=2: level=FATAL, service=worker
	m.Add("level", "FATAL", 2)
	m.Add("service", "worker", 2)
	// entryID=3: level=INFO, service=api
	m.Add("level", "INFO", 3)
	m.Add("service", "api", 3)

	result := m.FilterMulti(map[string][]string{
		"level":   {"ERROR", "FATAL"},
		"service": {"api", "worker"},
	})
	if len(result) != 2 {
		t.Fatalf("expected 2 (entries 1 and 2), got %d", len(result))
	}
	if !slices.Contains(result, 1) || !slices.Contains(result, 2) {
		t.Fatalf("expected entries 1 and 2, got %v", result)
	}

	if r := m.FilterMulti(map[string][]string{"level": {"WARN"}}); len(r) != 0 {
		t.Fatalf("expected empty result for absent value, got %d", len(r))
	}
}

func TestMultiFieldIndex_FilterMulti_ReturnsSorted(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 2)
	m.Add("level", "ERROR", 1)

	ids := m.FilterMulti(map[string][]string{"level": {"ERROR"}})
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("expected sorted [1 2], got %v", ids)
	}

	ids = m.FilterMulti(map[string][]string{"level": {"ERROR", "INFO"}})
	if len(ids) != 2 {
		t.Fatalf("expected 2, got %d", len(ids))
	}
}

func TestMultiFieldIndex_SaveAndLoad(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)
	m.Add("level", "ERROR", 2)
	m.Add("level", "INFO", 3)
	m.Add("service", "api", 1)
	m.Add("service", "worker", 2)

	path := filepath.Join(t.TempDir(), "test.bidx")
	if err := m.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadMultiFieldIndex(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	result := loaded.Filter(map[string]string{"level": "ERROR"})
	if len(result) != 2 {
		t.Errorf("after load: expected 2 ERROR, got %d", len(result))
	}

	result = loaded.Filter(map[string]string{"service": "api"})
	if len(result) != 1 {
		t.Errorf("after load: expected 1 api, got %d", len(result))
	}
}

func TestFTSIndex_IndexAndSearch(t *testing.T) {
	ctx := context.Background()
	f := NewFTSIndex()

	f.Index(ctx, 1, "connection refused to postgres error 500")
	f.Index(ctx, 2, "timeout waiting for connection database")
	f.Index(ctx, 3, "panic nil pointer dereference runtime")

	ids, err := f.Search(ctx, "connection", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) < 1 {
		t.Errorf("expected at least 1 result for 'connection', got %d", len(ids))
	}
}

func TestFTSIndex_Search_NotFound(t *testing.T) {
	ctx := context.Background()
	f := NewFTSIndex()
	f.Index(ctx, 1, "hello world")

	ids, err := f.Search(ctx, "nonexistent", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected 0 results, got %d", len(ids))
	}
}

func TestFTSIndex_SaveAndLoad(t *testing.T) {
	ctx := context.Background()
	f := NewFTSIndex()

	f.Index(ctx, 1, "connection refused postgres")
	f.Index(ctx, 2, "timeout database error")

	path := filepath.Join(t.TempDir(), "test.fidx")
	if err := f.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadFTSIndex(path)
	if err != nil {
		t.Fatalf("LoadFTSIndex: %v", err)
	}

	ids, err := loaded.Search(ctx, "connection", 10)
	if err != nil {
		t.Fatalf("Search after load: %v", err)
	}
	if len(ids) == 0 {
		t.Error("expected results after load, got 0")
	}
}

func TestFTSIndex_NumericSearch(t *testing.T) {
	ctx := context.Background()
	f := NewFTSIndex()

	f.Index(ctx, 1, "GET request 404 not found api users")
	f.Index(ctx, 2, "POST request 200 success api orders")
	f.Index(ctx, 3, "error 500 internal server error")

	ids, err := f.Search(ctx, "404", 10)
	if err != nil {
		t.Fatalf("Search numeric: %v", err)
	}
	if len(ids) == 0 {
		t.Error("expected result for numeric query '404'")
	}
}
