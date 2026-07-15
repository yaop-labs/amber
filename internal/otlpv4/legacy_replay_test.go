package otlpv4

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	memodel "github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestReplayLegacyV3AllSignals(t *testing.T) {
	root := t.TempDir()
	writeLegacyLogAndSpan(t, root)
	writeLegacyMetrics(t, root)

	var first [][]byte
	var signals []Signal
	if err := ReplayLegacyV3(context.Background(), root, func(envelope Envelope) error {
		encoded, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		first = append(first, encoded)
		signals = append(signals, envelope.Signal())
		return nil
	}); err != nil {
		t.Fatalf("ReplayLegacyV3() error = %v", err)
	}
	wantSignals := []Signal{SignalLogs, SignalTraces, SignalMetrics, SignalMetrics, SignalMetrics}
	if len(signals) != len(wantSignals) {
		t.Fatalf("signals = %v, want %v", signals, wantSignals)
	}
	for i := range signals {
		if signals[i] != wantSignals[i] {
			t.Fatalf("signals = %v, want %v", signals, wantSignals)
		}
	}

	var second [][]byte
	if err := ReplayLegacyV3(context.Background(), root, func(envelope Envelope) error {
		encoded, err := envelope.MarshalBinary()
		second = append(second, encoded)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatal("legacy replay count changed")
	}
	for i := range first {
		if !bytes.Equal(first[i], second[i]) {
			t.Fatalf("legacy replay record %d is not deterministic", i)
		}
	}
}

func TestReplayLegacyV3RejectsUnclosedSource(t *testing.T) {
	root := t.TempDir()
	manager, err := storage.OpenSegmentManager(filepath.Join(root, "logs"), storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	entry := model.LogEntry{ID: model.MustNewEntryID(), Timestamp: time.Now(), Level: model.LevelInfo, Body: "pending"}
	var buffer bytes.Buffer
	if _, err := entry.WriteTo(&buffer); err != nil {
		t.Fatal(err)
	}
	if err := manager.Write(buffer.Bytes(), entry.Timestamp.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := ReplayLegacyV3(context.Background(), root, func(Envelope) error { return nil }); err == nil {
		t.Fatal("ReplayLegacyV3() error = nil for unsealed source")
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLegacyLogAndSpan(t *testing.T, root string) {
	t.Helper()
	logManager, err := storage.OpenSegmentManager(filepath.Join(root, "logs"), storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	logEntry := model.LogEntry{
		ID: model.MustNewEntryID(), Timestamp: time.Unix(0, 10), Level: model.LevelWarn,
		Service: "api", Host: "node", Body: "legacy", Attrs: []model.Attr{{Key: "k", Value: "v"}},
	}
	var buffer bytes.Buffer
	if _, err := logEntry.WriteTo(&buffer); err != nil {
		t.Fatal(err)
	}
	if err := logManager.Write(buffer.Bytes(), logEntry.Timestamp.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := logManager.Close(); err != nil {
		t.Fatal(err)
	}

	spanManager, err := storage.OpenSegmentManager(filepath.Join(root, "spans"), storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	spanEntry := model.SpanEntry{
		ID: model.MustNewEntryID(), TraceID: model.TraceID{1}, SpanID: model.SpanID{2},
		Service: "api", Operation: "GET /", StartTime: time.Unix(0, 20), EndTime: time.Unix(0, 30), Status: model.SpanStatusOK,
	}
	buffer.Reset()
	if _, err := spanEntry.WriteTo(&buffer); err != nil {
		t.Fatal(err)
	}
	if err := spanManager.Write(buffer.Bytes(), spanEntry.StartTime.UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := spanManager.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeLegacyMetrics(t *testing.T, root string) {
	t.Helper()
	store, err := metricsengine.OpenStore(filepath.Join(root, "metrics"))
	if err != nil {
		t.Fatal(err)
	}
	labels := metricsengine.LabelSet{{Name: metricsengine.MetricNameLabel, Value: "requests"}}
	if _, err := store.AppendBatch([]metricsengine.Sample{{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: 40, Value: 4}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSketches([]engine.SketchSample{
		{Labels: memodel.LabelSet{{Name: memodel.MetricNameLabel, Value: "explicit"}}, Timestamp: 50, Explicit: histogram.ExplicitFromValues([]float64{1}, []float64{0.5, 2})},
		{Labels: memodel.LabelSet{{Name: memodel.MetricNameLabel, Value: "exp"}}, Timestamp: 60, Exp: histogram.FromValues(4, []float64{1, 2})},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}
