package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Labels:    model.LabelSet{{Name: "job", Value: "api"}},
		Type:      model.MetricTypeCounter,
		Timestamp: 1000,
		Value:     42,
	}
	if err := w.Append(want); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	if err := Replay(path, func(record Record) error {
		got = append(got, record)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if got[0].Value != want.Value || got[0].Timestamp != want.Timestamp {
		t.Fatalf("got %+v, want %+v", got[0], want)
	}
}

func TestAppendBatchReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendBatch([]Record{{Value: 1}, {Value: 2}, {Value: 3}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []int64
	if err := Replay(path, func(record Record) error {
		got = append(got, record.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("got %v, want [1 2 3]", got)
	}
}

func appendRecords(t *testing.T, path string, values ...int64) {
	t.Helper()
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range values {
		if err := w.Append(Record{Value: v, Timestamp: v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func replayValues(t *testing.T, path string) []int64 {
	t.Helper()
	var got []int64
	if err := Replay(path, func(r Record) error {
		got = append(got, r.Value)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

// A torn write (partial record at the tail) must not prevent replay: the
// valid prefix is recovered, the tail is truncated, and the WAL stays
// appendable with future appends visible to future replays.
func TestRecoverReplay_TornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	appendRecords(t, path, 1, 2, 3)

	// Simulate a crash mid-write: a full header announcing 100 bytes,
	// followed by only a few payload bytes.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], 100)
	binary.LittleEndian.PutUint32(header[4:8], 0xDEADBEEF)
	if _, err := f.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("torn")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	var got []int64
	stats, err := RecoverReplay(path, func(r Record) error {
		got = append(got, r.Value)
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverReplay: %v", err)
	}
	if stats.Records != 3 || len(got) != 3 {
		t.Fatalf("recovered %d records (%v), want 3", stats.Records, got)
	}
	if stats.TruncatedBytes != 12 {
		t.Errorf("TruncatedBytes = %d, want 12 (8 header + 4 payload)", stats.TruncatedBytes)
	}

	// The tail must be physically gone: appends after recovery stay reachable.
	appendRecords(t, path, 4)
	if got := replayValues(t, path); len(got) != 4 || got[3] != 4 {
		t.Fatalf("after repair+append replay = %v, want [1 2 3 4]", got)
	}
}

// A CRC mismatch (bit rot or torn payload of exactly-announced length) stops
// replay at the bad record instead of failing the open.
func TestRecoverReplay_CRCMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	appendRecords(t, path, 1, 2)

	// Flip a byte in the last record's payload.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	var got []int64
	stats, err := RecoverReplay(path, func(r Record) error {
		got = append(got, r.Value)
		return nil
	})
	if err != nil {
		t.Fatalf("RecoverReplay: %v", err)
	}
	if stats.Records != 1 || len(got) != 1 || got[0] != 1 {
		t.Fatalf("recovered %v (stats %+v), want just record 1", got, stats)
	}
	if stats.TruncatedBytes == 0 {
		t.Error("TruncatedBytes = 0, want > 0")
	}
}

// A garbage length field (header corruption) must also be treated as a
// corrupt tail, not an OOM or a hard error.
func TestRecoverReplay_OversizedLength(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	appendRecords(t, path, 7)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], maxRecordSize+1)
	if _, err := f.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	stats, err := RecoverReplay(path, func(Record) error { return nil })
	if err != nil {
		t.Fatalf("RecoverReplay: %v", err)
	}
	if stats.Records != 1 {
		t.Fatalf("Records = %d, want 1", stats.Records)
	}
	if stats.TruncatedBytes != 8 {
		t.Errorf("TruncatedBytes = %d, want 8", stats.TruncatedBytes)
	}
}

// Replay (read-only) must tolerate the same corruption without mutating the
// file.
func TestReplay_CorruptTail_ReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	appendRecords(t, path, 1, 2)

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("garbage-tail")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if got := replayValues(t, path); len(got) != 2 {
		t.Fatalf("replay = %v, want 2 records", got)
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() != after.Size() {
		t.Errorf("Replay mutated the file: %d -> %d bytes", before.Size(), after.Size())
	}
}

func TestTruncate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "head.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Truncate(); err != nil {
		t.Fatal(err)
	}
	var got int
	if err := Replay(path, func(record Record) error {
		got++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got != 0 {
		t.Fatalf("replayed %d records after truncate", got)
	}
}
