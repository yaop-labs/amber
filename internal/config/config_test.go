package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default()

	if cfg.Storage.DataDir != "./data" {
		t.Errorf("DataDir: got %q, want ./data", cfg.Storage.DataDir)
	}
	if cfg.Storage.SegmentMaxRecords != 100_000 {
		t.Errorf("SegmentMaxRecords: got %d, want 100000", cfg.Storage.SegmentMaxRecords)
	}
	if cfg.Ingest.BatchSize != 1000 {
		t.Errorf("BatchSize: got %d, want 1000", cfg.Ingest.BatchSize)
	}
	if cfg.Ingest.QueueSize != 100_000 {
		t.Errorf("QueueSize: got %d, want 100000", cfg.Ingest.QueueSize)
	}
	if cfg.API.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr: got %q, want :8080", cfg.API.HTTPAddr)
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	cfg, err := Load("/nonexistent/config.yaml")
	if err != nil {
		t.Fatalf("Load should return default on missing file: %v", err)
	}
	if cfg.Storage.DataDir != "./data" {
		t.Errorf("expected default DataDir, got %q", cfg.Storage.DataDir)
	}
}

func TestLoad_ValidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	yaml := `
storage:
  data_dir: /tmp/amber-test
  segment_max_records: 500000
ingest:
  batch_size: 2000
  batch_timeout: 50ms
  queue_size: 5000
  logs:
    queue_size: 7000
  spans:
    batch_size: 500
    breaker_threshold: 3
api:
  http_addr: ":9090"
  grpc_addr: ":4318"
log:
  level: debug
  format: json
retention:
  interval: 30m
  logs:
    local_max_age: 24h
    max_age: 168h
    max_segments: 10
  spans:
    max_age: 72h
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Storage.DataDir != "/tmp/amber-test" {
		t.Errorf("DataDir: got %q", cfg.Storage.DataDir)
	}
	if cfg.Storage.SegmentMaxRecords != 500000 {
		t.Errorf("SegmentMaxRecords: got %d", cfg.Storage.SegmentMaxRecords)
	}
	if cfg.Ingest.BatchSize != 2000 {
		t.Errorf("BatchSize: got %d", cfg.Ingest.BatchSize)
	}
	if cfg.Ingest.BatchTimeout != 50*time.Millisecond {
		t.Errorf("BatchTimeout: got %v", cfg.Ingest.BatchTimeout)
	}
	if cfg.Ingest.Logs.QueueSize != 7000 {
		t.Errorf("Ingest.Logs.QueueSize: got %d", cfg.Ingest.Logs.QueueSize)
	}
	if cfg.Ingest.Spans.BatchSize != 500 {
		t.Errorf("Ingest.Spans.BatchSize: got %d", cfg.Ingest.Spans.BatchSize)
	}
	if cfg.Ingest.Spans.BreakerThreshold != 3 {
		t.Errorf("Ingest.Spans.BreakerThreshold: got %d", cfg.Ingest.Spans.BreakerThreshold)
	}
	if cfg.API.GRPCAddr != ":4318" {
		t.Errorf("GRPCAddr: got %q", cfg.API.GRPCAddr)
	}
	if cfg.Retention.Interval != 30*time.Minute {
		t.Errorf("Retention.Interval: got %v", cfg.Retention.Interval)
	}
	if cfg.Retention.Logs.LocalMaxAge != 24*time.Hour {
		t.Errorf("Logs.LocalMaxAge: got %v", cfg.Retention.Logs.LocalMaxAge)
	}
	if cfg.Retention.Logs.MaxAge != 168*time.Hour {
		t.Errorf("Logs.MaxAge: got %v", cfg.Retention.Logs.MaxAge)
	}
	if cfg.Retention.Logs.MaxSegments != 10 {
		t.Errorf("Logs.MaxSegments: got %d", cfg.Retention.Logs.MaxSegments)
	}
	if cfg.Retention.Spans.MaxAge != 72*time.Hour {
		t.Errorf("Spans.MaxAge: got %v", cfg.Retention.Spans.MaxAge)
	}
}

func TestLoad_LegacyRetentionRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	yaml := `
storage:
  data_dir: /tmp/amber-test
ingest:
  batch_size: 1
  queue_size: 1
api:
  http_addr: ":9090"
retention:
  max_age: 168h
  max_segments: 10
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for legacy flat retention shape, got nil")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	if err := os.WriteFile(path, []byte("{{invalid"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestValidate_MissingDataDir(t *testing.T) {
	cfg := Default()
	cfg.Storage.DataDir = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty data_dir")
	}
}

func TestValidate_InvalidBatchSize(t *testing.T) {
	cfg := Default()
	cfg.Ingest.BatchSize = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for batch_size=0")
	}

	cfg.Ingest.BatchSize = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative batch_size")
	}
}

func TestValidate_InvalidQueueSize(t *testing.T) {
	cfg := Default()
	cfg.Ingest.QueueSize = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for queue_size=0")
	}
}

func TestValidate_InvalidLaneQueueSize(t *testing.T) {
	cfg := Default()
	cfg.Ingest.Logs.QueueSize = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative logs queue_size")
	}
}

func TestValidate_MissingHTTPAddr(t *testing.T) {
	cfg := Default()
	cfg.API.HTTPAddr = ""
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty http_addr")
	}
}

func TestLoad_PartialOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")

	yaml := `
storage:
  data_dir: /custom/path
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Storage.DataDir != "/custom/path" {
		t.Errorf("DataDir should be overridden: got %q", cfg.Storage.DataDir)
	}
	// Defaults should remain for non-overridden fields
	if cfg.Ingest.BatchSize != 1000 {
		t.Errorf("BatchSize should keep default: got %d", cfg.Ingest.BatchSize)
	}
	if cfg.API.HTTPAddr != ":8080" {
		t.Errorf("HTTPAddr should keep default: got %q", cfg.API.HTTPAddr)
	}
}

func TestResolvedAPIKeys_NamedListWins(t *testing.T) {
	cfg := APIConfig{
		APIKey: "legacy",
		APIKeys: []NamedAPIKey{
			{Name: "ops", Key: "k1"},
			{Name: "billing", Key: "k2"},
		},
	}
	got := cfg.ResolvedAPIKeys()
	if len(got) != 2 || got[0].Name != "ops" || got[1].Name != "billing" {
		t.Errorf("named list ignored: %+v", got)
	}
}

func TestResolvedAPIKeys_LegacyFallback(t *testing.T) {
	cfg := APIConfig{APIKey: "legacy"}
	got := cfg.ResolvedAPIKeys()
	if len(got) != 1 || got[0].Name != "default" || got[0].Key != "legacy" {
		t.Errorf("legacy fallback wrong: %+v", got)
	}
}

func TestResolvedAPIKeys_EmptyDisables(t *testing.T) {
	cfg := APIConfig{}
	if got := cfg.ResolvedAPIKeys(); got != nil {
		t.Errorf("empty config should disable auth, got %+v", got)
	}
}
