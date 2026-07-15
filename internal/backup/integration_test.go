package backup_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/amber"
	"github.com/yaop-labs/amber/internal/backup"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestRestoredDatabaseOpensWithLogsAndMetrics(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	db, err := amber.Open(source, &amber.Options{BatchTimeout: time.Hour})
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	entry, err := amber.NewLogEntry(amber.LevelInfo, "api", "host", "restored log")
	if err != nil {
		t.Fatalf("new log: %v", err)
	}
	if err := db.Log(context.Background(), entry); err != nil {
		t.Fatalf("append log: %v", err)
	}
	labels := metricsengine.LabelSet{
		{Name: metricsengine.MetricNameLabel, Value: "requests_total"},
		{Name: "job", Value: "api"},
	}
	if _, err := db.MetricStore().AppendBatch([]metricsengine.Sample{
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: 0, Value: 1},
		{Labels: labels, Type: metricsengine.MetricTypeCounter, Timestamp: 60_000, Value: 61},
	}); err != nil {
		t.Fatalf("append metrics: %v", err)
	}
	if _, err := db.MetricStore().Flush(); err != nil {
		t.Fatalf("flush metrics: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	snapshot := filepath.Join(root, "snapshot")
	created, err := backup.Create(context.Background(), source, snapshot)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	if len(created.Manifest.Checkpoints.Metrics.ImmutableFiles) == 0 {
		t.Fatal("metrics checkpoint has no immutable block")
	}
	restored := filepath.Join(root, "restored")
	if _, err := backup.Restore(context.Background(), snapshot, restored); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	manifestPayload, err := os.ReadFile(filepath.Join(restored, "metrics", "manifest.json"))
	if err != nil {
		t.Fatalf("read restored metrics manifest: %v", err)
	}
	var manifest mestore.Manifest
	if err := json.Unmarshal(manifestPayload, &manifest); err != nil {
		t.Fatalf("parse restored metrics manifest: %v", err)
	}
	for _, block := range manifest.Blocks {
		if filepath.IsAbs(block.Path) || strings.Contains(block.Path, string(filepath.Separator)) {
			t.Fatalf("metrics block path not relocated: %s", block.Path)
		}
	}

	reopened, err := amber.Open(restored, &amber.Options{BatchTimeout: time.Hour})
	if err != nil {
		t.Fatalf("open restored database: %v", err)
	}
	defer reopened.Close()
	status := reopened.Status()
	if status.DatabaseID != created.Manifest.Database.ID || status.LastVerifiedRestore == nil ||
		status.LastVerifiedRestore.Checkpoint != created.Checkpoint {
		t.Fatalf("restored status = %+v", status)
	}
	logs, err := reopened.QueryLogs(context.Background(), &amber.LogQuery{Limit: 10})
	if err != nil {
		t.Fatalf("query restored logs: %v", err)
	}
	if len(logs.Entries) != 1 || logs.Entries[0].Body != "restored log" {
		t.Fatalf("restored logs = %+v", logs.Entries)
	}
	if names := reopened.MetricStore().MetricNames(); len(names) != 1 || names[0] != "requests_total" {
		t.Fatalf("restored metric names = %v", names)
	}
}
