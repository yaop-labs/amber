package index

import (
	"context"
	"path/filepath"
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

	errorBM := b.getShared("ERROR")
	if errorBM.GetCardinality() != 3 {
		t.Errorf("expected 3 ERROR entries, got %d", errorBM.GetCardinality())
	}

	infoBM := b.getShared("INFO")
	if infoBM.GetCardinality() != 1 {
		t.Errorf("expected 1 INFO entry, got %d", infoBM.GetCardinality())
	}
}

func TestBitmapIndex_Get_NotFound(t *testing.T) {
	b := NewBitmapIndex()
	if bm := b.getShared("MISSING"); bm != nil {
		t.Error("expected nil bitmap for unknown value")
	}
}

func TestMultiFieldIndex_Filter_SingleField(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)
	m.Add("level", "ERROR", 2)
	m.Add("level", "INFO", 3)

	result := m.Filter(map[string]string{"level": "ERROR"})
	if result.GetCardinality() != 2 {
		t.Errorf("expected 2 ERROR entries, got %d", result.GetCardinality())
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
	if result.GetCardinality() != 1 {
		t.Errorf("expected 1, got %d", result.GetCardinality())
	}
	if !result.Contains(1) {
		t.Error("expected entryID=1 in result")
	}
}

func TestMultiFieldIndex_Filter_UnknownField(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)

	result := m.Filter(map[string]string{"nonexistent": "value"})
	if result.GetCardinality() != 0 {
		t.Error("expected empty result for unknown field")
	}
}

func TestMultiFieldIndex_FilterAny(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 1)
	m.Add("level", "FATAL", 2)
	m.Add("level", "INFO", 3)

	result := m.FilterAny("level", []string{"ERROR", "FATAL"})
	if result.GetCardinality() != 2 {
		t.Errorf("expected 2 (ERROR+FATAL), got %d", result.GetCardinality())
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
	if result.GetCardinality() != 2 {
		t.Fatalf("expected 2 (entries 1 and 2), got %d", result.GetCardinality())
	}
	if !result.Contains(1) || !result.Contains(2) {
		t.Fatalf("expected entries 1 and 2, got %v", result.ToArray())
	}

	if r := m.FilterMulti(map[string][]string{"level": {"WARN"}}); r.GetCardinality() != 0 {
		t.Fatalf("expected empty result for absent value, got %d", r.GetCardinality())
	}
}

func TestMultiFieldIndex_FilterMultiWithSorted_SingleValueFastPath(t *testing.T) {
	m := NewMultiFieldIndex()
	m.Add("level", "ERROR", 2)
	m.Add("level", "ERROR", 1)

	bm, sorted := m.FilterMultiWithSorted(map[string][]string{"level": {"ERROR"}})
	if bm.GetCardinality() != 2 {
		t.Fatalf("expected 2, got %d", bm.GetCardinality())
	}
	if sorted == nil {
		t.Fatal("single field+value must take the sorted fast path")
	}

	bm, sorted = m.FilterMultiWithSorted(map[string][]string{"level": {"ERROR", "INFO"}})
	if bm.GetCardinality() != 2 {
		t.Fatalf("expected 2, got %d", bm.GetCardinality())
	}
	if sorted != nil {
		t.Fatal("multi-value path must not claim a shared sorted slice")
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
	if result.GetCardinality() != 2 {
		t.Errorf("after load: expected 2 ERROR, got %d", result.GetCardinality())
	}

	result = loaded.Filter(map[string]string{"service": "api"})
	if result.GetCardinality() != 1 {
		t.Errorf("after load: expected 1 api, got %d", result.GetCardinality())
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
