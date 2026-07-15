package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber"
	"github.com/yaop-labs/amber/internal/backup"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestRunSemanticProbeChecksEveryEngine(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	source := filepath.Join(root, "source")
	db, err := amber.Open(source, &amber.Options{BatchTimeout: time.Hour})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	logEntry, err := amber.NewLogEntry(amber.LevelInfo, "api", "host", "drill log")
	if err != nil {
		t.Fatalf("new log: %v", err)
	}
	spanEntry, err := amber.NewSpanEntry(amber.TraceID{1}, amber.SpanID{2}, amber.SpanID{}, "api", "GET /drill")
	if err != nil {
		t.Fatalf("new span: %v", err)
	}
	spanEntry.EndTime = spanEntry.StartTime.Add(time.Millisecond)
	if err := db.Log(ctx, logEntry); err != nil {
		t.Fatalf("log: %v", err)
	}
	if err := db.Span(ctx, spanEntry); err != nil {
		t.Fatalf("span: %v", err)
	}
	if _, err := db.MetricStore().AppendBatch([]metricsengine.Sample{{
		Labels: metricsengine.LabelSet{{Name: metricsengine.MetricNameLabel, Value: "drill_requests"}},
		Type:   metricsengine.MetricTypeCounter, Timestamp: 1, Value: 1,
	}}); err != nil {
		t.Fatalf("append metric: %v", err)
	}
	if err := db.Flush(ctx); err != nil {
		t.Fatalf("flush events: %v", err)
	}
	if _, err := db.MetricStore().Flush(); err != nil {
		t.Fatalf("flush metrics: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	snapshot := filepath.Join(root, "snapshot")
	created, err := backup.Create(ctx, source, snapshot)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	restored := filepath.Join(root, "restored")
	verified, err := backup.Restore(ctx, snapshot, restored)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	probe, err := runSemanticProbe(ctx, restored, verified)
	if err != nil {
		t.Fatalf("runSemanticProbe: %v", err)
	}
	if !probe.Ready || probe.Degraded || probe.DatabaseID != created.Manifest.Database.ID || probe.RestoreCheckpoint != created.Checkpoint {
		t.Fatalf("probe identity/status = %+v", probe)
	}
	if probe.LogsReturned != 1 || probe.SpansReturned != 1 || probe.MetricNames != 1 || probe.MetricQueryGroups != 1 {
		t.Fatalf("probe engine results = %+v", probe)
	}
	if probe.OTLPReplayRecords != 3 || probe.OTLPSignalRecords["logs"] != 1 ||
		probe.OTLPSignalRecords["traces"] != 1 || probe.OTLPSignalRecords["metrics"] != 1 ||
		len(probe.OTLPReplaySHA256) != 64 {
		t.Fatalf("probe OTLP replay = %+v", probe)
	}
}
