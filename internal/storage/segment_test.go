package storage

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func tempSegmentPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.alog")
}

func writeAndClose(t *testing.T, path string, records [][]byte, timestamps []int64) {
	t.Helper()
	sw, err := OpenSegmentWriter(path)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	for i, rec := range records {
		ts := timestamps[i]
		if err := sw.WriteRecord(rec, ts); err != nil {
			t.Fatalf("WriteRecord[%d]: %v", i, err)
		}
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func nowNano() int64 {
	return time.Now().UnixNano()
}

func TestSegmentWriter_CreateFile(t *testing.T) {
	path := tempSegmentPath(t)
	sw, err := OpenSegmentWriter(path)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	sw.Close()

	info, err := statFile(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info <= 0 {
		t.Errorf("expected non-empty file, got size=%d", info)
	}
}

func TestSegmentWriter_CannotCreateExistingFile(t *testing.T) {
	path := tempSegmentPath(t)
	sw, _ := OpenSegmentWriter(path)
	sw.Close()

	_, err := OpenSegmentWriter(path)
	if err == nil {
		t.Error("expected error when creating existing segment, got nil")
	}
}

func TestSegmentWriter_RecordCount(t *testing.T) {
	path := tempSegmentPath(t)
	sw, _ := OpenSegmentWriter(path)

	for i := 0; i < 10; i++ {
		sw.WriteRecord([]byte(fmt.Sprintf("record-%d", i)), nowNano())
	}

	if sw.RecordCount() != 10 {
		t.Errorf("expected 10 records, got %d", sw.RecordCount())
	}
	sw.Close()
}

func TestSegmentWriter_WriteAfterClose(t *testing.T) {
	path := tempSegmentPath(t)
	sw, _ := OpenSegmentWriter(path)
	sw.Close()

	err := sw.WriteRecord([]byte("after close"), nowNano())
	if err == nil {
		t.Error("expected error writing after close")
	}
}

func TestSegmentReader_ReadHeader(t *testing.T) {
	path := tempSegmentPath(t)
	writeAndClose(t, path, [][]byte{[]byte("hello")}, []int64{nowNano()})

	sr, err := OpenSegmentReader(path, nil)
	if err != nil {
		t.Fatalf("OpenSegmentReader: %v", err)
	}
	defer sr.Close()
}

func TestSegmentReader_BadMagic(t *testing.T) {
	path := tempSegmentPath(t)

	f, _ := createFile(path)
	f.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	f.Close()

	_, err := OpenSegmentReader(path, nil)
	if err == nil {
		t.Error("expected error for bad magic, got nil")
	}
}

func TestSegment_RoundTrip_SingleRecord(t *testing.T) {
	path := tempSegmentPath(t)
	original := []byte("connection refused to postgres:5432 error 500")
	ts := nowNano()

	writeAndClose(t, path, [][]byte{original}, []int64{ts})

	sr, err := OpenSegmentReader(path, nil)
	if err != nil {
		t.Fatalf("OpenSegmentReader: %v", err)
	}
	defer sr.Close()

	var got [][]byte
	if err := sr.Scan(func(data []byte) error {
		got = append(got, data)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if !bytes.Equal(got[0], original) {
		t.Errorf("data mismatch: got %q, want %q", got[0], original)
	}
}

func TestSegment_RoundTrip_MultipleRecords(t *testing.T) {
	path := tempSegmentPath(t)

	const n = 1000
	records := make([][]byte, n)
	timestamps := make([]int64, n)
	base := time.Now().UnixNano()

	for i := 0; i < n; i++ {
		records[i] = []byte(fmt.Sprintf("log entry number %d with some content", i))
		timestamps[i] = base + int64(i)*int64(time.Millisecond)
	}

	writeAndClose(t, path, records, timestamps)

	sr, err := OpenSegmentReader(path, nil)
	if err != nil {
		t.Fatalf("OpenSegmentReader: %v", err)
	}
	defer sr.Close()

	var got [][]byte
	if err := sr.Scan(func(data []byte) error {
		got = append(got, data)
		return nil
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}

	if len(got) != n {
		t.Fatalf("expected %d records, got %d", n, len(got))
	}
	for i := range records {
		if !bytes.Equal(got[i], records[i]) {
			t.Errorf("record %d mismatch", i)
		}
	}
}

func TestSegment_RoundTrip_EmptyBody(t *testing.T) {
	path := tempSegmentPath(t)
	writeAndClose(t, path, [][]byte{{}}, []int64{nowNano()})

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	var got [][]byte
	sr.Scan(func(data []byte) error {
		got = append(got, data)
		return nil
	})

	if len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if len(got[0]) != 0 {
		t.Errorf("expected empty record, got %d bytes", len(got[0]))
	}
}

func TestSegment_Footer_RecordCount(t *testing.T) {
	path := tempSegmentPath(t)
	const n = 50

	records := make([][]byte, n)
	timestamps := make([]int64, n)
	for i := range records {
		records[i] = []byte("record")
		timestamps[i] = nowNano()
	}
	writeAndClose(t, path, records, timestamps)

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	if sr.Footer().RecordCount != n {
		t.Errorf("expected RecordCount=%d, got %d", n, sr.Footer().RecordCount)
	}
}

func TestSegment_Footer_MinMaxTS(t *testing.T) {
	path := tempSegmentPath(t)

	t1 := int64(1_000_000_000)
	t2 := int64(2_000_000_000)
	t3 := int64(3_000_000_000)

	writeAndClose(t, path,
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[]int64{t2, t1, t3},
	)

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	footer := sr.Footer()
	if footer.MinTS != t1 {
		t.Errorf("MinTS: got %d, want %d", footer.MinTS, t1)
	}
	if footer.MaxTS != t3 {
		t.Errorf("MaxTS: got %d, want %d", footer.MaxTS, t3)
	}
}

func TestSegment_Footer_BlockOffsets(t *testing.T) {
	path := tempSegmentPath(t)

	sw, _ := OpenSegmentWriter(path)
	sw.blockSize = 100

	base := nowNano()
	for i := 0; i < 20; i++ {
		data := bytes.Repeat([]byte("x"), 20)
		sw.WriteRecord(data, base+int64(i))
	}
	sw.Close()

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	footer := sr.Footer()
	if footer.BlockCount == 0 {
		t.Error("expected at least 1 block")
	}
	if footer.BlockCount < 2 {
		t.Errorf("expected multiple blocks with small blockSize, got %d", footer.BlockCount)
	}
}

func TestSegment_ScanTimeRange_NoOverlap(t *testing.T) {
	path := tempSegmentPath(t)

	writeAndClose(t, path,
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[]int64{100, 200, 300},
	)

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	count := 0
	err := sr.ScanTimeRange(400, 500, func([]byte) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("ScanTimeRange: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records, got %d", count)
	}
}

func TestSegment_ScanTimeRange_FullOverlap(t *testing.T) {
	path := tempSegmentPath(t)

	writeAndClose(t, path,
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[]int64{100, 200, 300},
	)

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	count := 0
	sr.ScanTimeRange(0, 1000, func([]byte) error {
		count++
		return nil
	})
	if count != 3 {
		t.Errorf("expected 3 records, got %d", count)
	}
}

func TestSegment_ScanTimeRange_PartialOverlap(t *testing.T) {
	path := tempSegmentPath(t)

	writeAndClose(t, path,
		[][]byte{[]byte("a"), []byte("b"), []byte("c")},
		[]int64{100, 200, 300},
	)

	sr, _ := OpenSegmentReader(path, nil)
	defer sr.Close()

	count := 0
	sr.ScanTimeRange(50, 150, func([]byte) error {
		count++
		return nil
	})
	if count != 3 {
		t.Errorf("expected 3 records (full segment scan on partial overlap), got %d", count)
	}
}

func TestSegment_Compression_ReducesSize(t *testing.T) {
	path := tempSegmentPath(t)

	record := bytes.Repeat([]byte("connection refused to postgres ERROR "), 100)
	records := make([][]byte, 50)
	timestamps := make([]int64, 50)
	for i := range records {
		records[i] = record
		timestamps[i] = nowNano()
	}
	writeAndClose(t, path, records, timestamps)

	rawSize := int64(len(record) * 50)
	fileSize, _ := statFile(path)

	if fileSize >= rawSize {
		t.Errorf("expected compression: fileSize=%d, rawSize=%d", fileSize, rawSize)
	}
	t.Logf("raw=%d bytes, compressed=%d bytes, ratio=%.2fx", rawSize, fileSize, float64(rawSize)/float64(fileSize))
}

func statFile(path string) (int64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func createFile(path string) (*os.File, error) {
	return os.Create(path)
}

// TestSegmentWriter_FailStopAfterSyncError pins the fsync-failure policy for
// the active segment: after a failed sync the writer rejects records and
// Close does not append a footer over an unknowable file state.
func TestSegmentWriter_FailStopAfterSyncError(t *testing.T) {
	sw, err := OpenSegmentWriter(filepath.Join(t.TempDir(), "seg.alog"))
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}
	if err := sw.WriteRecord(make([]byte, 16), 1); err != nil {
		t.Fatalf("WriteRecord: %v", err)
	}
	if err := sw.file.Close(); err != nil {
		t.Fatalf("close file: %v", err)
	}
	if _, err := sw.Sync(); err == nil {
		t.Fatal("expected Sync to fail on closed file")
	}
	if err := sw.WriteRecord(make([]byte, 16), 2); err == nil {
		t.Fatal("fail-stopped writer accepted a record")
	}
	if err := sw.Close(); err == nil {
		t.Fatal("Close on a failed writer must surface the failure")
	}
}
