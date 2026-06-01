package wal

import (
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
