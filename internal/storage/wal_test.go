package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func newTestWAL(t *testing.T) (*WAL, string) {
	t.Helper()
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	t.Cleanup(func() { wal.Close() })
	return wal, dir
}

func TestOpenWAL_CreatesFile(t *testing.T) {
	dir := t.TempDir()
	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	defer wal.Close()

	path := filepath.Join(dir, walFileName)
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("WAL file not created at %s", path)
	}
}

func TestOpenWAL_CreatesDir(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "nested", "data")

	wal, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("OpenWAL with nested dir: %v", err)
	}
	defer wal.Close()
}

func TestOpenWAL_Idempotent(t *testing.T) {
	dir := t.TempDir()

	wal1, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("first OpenWAL: %v", err)
	}
	wal1.Write([]byte("hello"))
	wal1.Close()

	wal2, err := OpenWAL(dir)
	if err != nil {
		t.Fatalf("second OpenWAL: %v", err)
	}
	defer wal2.Close()

	count := 0
	wal2.Replay(func(payload []byte) error {
		count++
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 record after reopen, got %d", count)
	}
}

func TestWAL_Write_Single(t *testing.T) {
	wal, _ := newTestWAL(t)

	if _, err := wal.Write([]byte("hello amber")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	size, _ := wal.Size()
	expected := int64(walHeaderSize + len("hello amber"))
	if size != expected {
		t.Errorf("expected size=%d, got %d", expected, size)
	}
}

func TestWAL_Write_Multiple(t *testing.T) {
	wal, _ := newTestWAL(t)

	payloads := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}

	for _, p := range payloads {
		if _, err := wal.Write(p); err != nil {
			t.Fatalf("Write %q: %v", p, err)
		}
	}

	var got [][]byte
	wal.Replay(func(payload []byte) error {
		got = append(got, payload)
		return nil
	})

	if len(got) != len(payloads) {
		t.Fatalf("expected %d records, got %d", len(payloads), len(got))
	}
	for i := range payloads {
		if string(got[i]) != string(payloads[i]) {
			t.Errorf("record %d: got %q, want %q", i, got[i], payloads[i])
		}
	}
}

func TestWAL_Write_Empty(t *testing.T) {
	wal, _ := newTestWAL(t)

	if _, err := wal.Write([]byte{}); err != nil {
		t.Fatalf("Write empty: %v", err)
	}

	count := 0
	wal.Replay(func(payload []byte) error {
		count++
		if len(payload) != 0 {
			t.Errorf("expected empty payload, got %d bytes", len(payload))
		}
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

func TestWAL_Write_LargePayload(t *testing.T) {
	wal, _ := newTestWAL(t)

	payload := make([]byte, 1024*1024)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	if _, err := wal.Write(payload); err != nil {
		t.Fatalf("Write large: %v", err)
	}

	var got []byte
	wal.Replay(func(p []byte) error {
		got = p
		return nil
	})

	if len(got) != len(payload) {
		t.Errorf("large payload: got %d bytes, want %d", len(got), len(payload))
	}
}

func TestWAL_Replay_Empty(t *testing.T) {
	wal, _ := newTestWAL(t)

	count, err := wal.Replay(func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("Replay empty WAL: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records, got %d", count)
	}
}

func TestWAL_Replay_Order(t *testing.T) {
	wal, _ := newTestWAL(t)

	const n = 100
	for i := 0; i < n; i++ {
		wal.Write([]byte(fmt.Sprintf("record-%03d", i)))
	}

	var got []string
	wal.Replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})

	if len(got) != n {
		t.Fatalf("expected %d records, got %d", n, len(got))
	}
	for i, s := range got {
		expected := fmt.Sprintf("record-%03d", i)
		if s != expected {
			t.Errorf("record %d: got %q, want %q", i, s, expected)
		}
	}
}

func TestWAL_Replay_AfterReopen(t *testing.T) {
	dir := t.TempDir()

	wal1, _ := OpenWAL(dir)
	wal1.Write([]byte("before-crash"))
	wal1.Write([]byte("also-before-crash"))
	wal1.Close()

	wal2, _ := OpenWAL(dir)
	defer wal2.Close()

	var got []string
	count, err := wal2.Replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatalf("Replay after reopen: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 records, got %d", count)
	}
	if got[0] != "before-crash" || got[1] != "also-before-crash" {
		t.Errorf("wrong records: %v", got)
	}
}

func TestWAL_Replay_CorruptedRecord_StopsGracefully(t *testing.T) {
	dir := t.TempDir()

	wal, _ := OpenWAL(dir)
	wal.Write([]byte("good-record-1"))
	wal.Write([]byte("good-record-2"))
	wal.Close()

	path := filepath.Join(dir, walFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xDE, 0xAD, 0xBE, 0xEF})
	f.Close()

	wal2, _ := OpenWAL(dir)
	defer wal2.Close()

	count, err := wal2.Replay(func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("Replay with corrupted tail should not error: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 good records, got %d", count)
	}
}

func TestWAL_Replay_TruncatedPayload_StopsGracefully(t *testing.T) {
	dir := t.TempDir()

	wal, _ := OpenWAL(dir)
	wal.Write([]byte("complete"))
	wal.Close()

	path := filepath.Join(dir, walFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	var header [walHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], walMagic)
	binary.LittleEndian.PutUint32(header[4:8], 0xDEADBEEF) // crc
	binary.LittleEndian.PutUint32(header[8:12], 100)       // length: 100 байт
	f.Write(header[:])
	f.Write([]byte("only 10 bytes")) // меньше чем обещали
	f.Close()

	wal2, _ := OpenWAL(dir)
	defer wal2.Close()

	count, err := wal2.Replay(func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("Replay with truncated payload should not error: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 complete record, got %d", count)
	}
}

func TestWAL_Truncate_ClearsFile(t *testing.T) {
	wal, _ := newTestWAL(t)

	wal.Write([]byte("record-1"))
	wal.Write([]byte("record-2"))

	if err := wal.Truncate(); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	size, _ := wal.Size()
	if size != 0 {
		t.Errorf("expected size=0 after truncate, got %d", size)
	}

	count, _ := wal.Replay(func([]byte) error { return nil })
	if count != 0 {
		t.Errorf("expected 0 records after truncate, got %d", count)
	}
}

func TestWAL_Truncate_CanWriteAfter(t *testing.T) {
	wal, _ := newTestWAL(t)

	wal.Write([]byte("before"))
	wal.Truncate()
	wal.Write([]byte("after"))

	var got []string
	wal.Replay(func(payload []byte) error {
		got = append(got, string(payload))
		return nil
	})

	if len(got) != 1 || got[0] != "after" {
		t.Errorf("expected [after], got %v", got)
	}
}

func TestWAL_ConcurrentWrites(t *testing.T) {
	wal, _ := newTestWAL(t)

	const goroutines = 10
	const perGoroutine = 50

	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			for j := 0; j < perGoroutine; j++ {
				payload := []byte(fmt.Sprintf("g%d-r%d", id, j))
				if _, err := wal.Write(payload); err != nil {
					t.Errorf("concurrent Write: %v", err)
				}
			}
			done <- struct{}{}
		}(i)
	}

	for range goroutines {
		<-done
	}

	count, err := wal.Replay(func([]byte) error { return nil })
	if err != nil {
		t.Fatalf("Replay after concurrent writes: %v", err)
	}
	if count != goroutines*perGoroutine {
		t.Errorf("expected %d records, got %d", goroutines*perGoroutine, count)
	}
}
