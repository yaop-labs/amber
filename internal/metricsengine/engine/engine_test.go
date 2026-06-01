package engine

import (
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	labels := model.LabelSet{{Name: "job", Value: "api"}}

	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 1000, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 2000, 20); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	blockPath := filepath.Join(dir, "recovered.meb")
	if err := recovered.FlushBlock(blockPath); err != nil {
		t.Fatal(err)
	}
	series, err := block.ReadFile(blockPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 {
		t.Fatalf("recovered series = %+v", series)
	}
	if series[0].Values[1] != 20 {
		t.Fatalf("last value = %d, want 20", series[0].Values[1])
	}
}

func TestAppendBatch(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		t.Fatal(err)
	}
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	ids, err := e.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 10},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("ids = %v, want same series id for both samples", ids)
	}
}

func TestPrepareAndCommitFlush(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "prepared.meb")
	if err := e.PrepareFlushBlock(path); err != nil {
		t.Fatal(err)
	}
	if e.BufferedSeries() != 1 {
		t.Fatalf("BufferedSeries after prepare = %d, want 1", e.BufferedSeries())
	}
	if err := e.CommitFlush(); err != nil {
		t.Fatal(err)
	}
	if e.BufferedSeries() != 0 {
		t.Fatalf("BufferedSeries after commit = %d, want 0", e.BufferedSeries())
	}
}
