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
	if cfg.Storage.DiskWarningFreeBytes != 2<<30 || cfg.Storage.DiskStopFreeBytes != 1<<30 {
		t.Errorf("disk watermarks: warning=%d stop=%d", cfg.Storage.DiskWarningFreeBytes, cfg.Storage.DiskStopFreeBytes)
	}
	if cfg.Ingest.BatchSize != 1000 {
		t.Errorf("BatchSize: got %d, want 1000", cfg.Ingest.BatchSize)
	}
	if cfg.Ingest.QueueSize != 100_000 {
		t.Errorf("QueueSize: got %d, want 100000", cfg.Ingest.QueueSize)
	}
	if cfg.API.HTTPAddr != "localhost:8080" {
		t.Errorf("HTTPAddr: got %q, want localhost:8080", cfg.API.HTTPAddr)
	}
	if !cfg.API.MetricsPublic {
		t.Error("MetricsPublic: got false, want backward-compatible true default")
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("default config should be valid: %v", err)
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	if _, err := Load("/nonexistent/config.yaml"); err == nil {
		t.Fatal("Load should reject an explicitly missing file")
	}
	cfg, err := LoadOptional("/nonexistent/config.yaml")
	if err != nil {
		t.Fatalf("LoadOptional should return defaults on missing file: %v", err)
	}
	if cfg.Storage.DataDir != "./data" {
		t.Errorf("expected default DataDir, got %q", cfg.Storage.DataDir)
	}
}

func TestLoad_UnknownFieldRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("storage:\n  data_dir: ./data\n  datta_dir: typo\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown YAML field to be rejected")
	}
}

func TestExampleConfigIsStrictlyLoadable(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config.example.yaml"))
	if err != nil {
		t.Fatalf("load config.example.yaml: %v", err)
	}
	if cfg.Storage.SegmentMaxRecords != Default().Storage.SegmentMaxRecords {
		t.Fatalf("example segment_max_records = %d, default = %d",
			cfg.Storage.SegmentMaxRecords, Default().Storage.SegmentMaxRecords)
	}
	if cfg.Storage.DiskWarningFreeBytes != Default().Storage.DiskWarningFreeBytes ||
		cfg.Storage.DiskStopFreeBytes != Default().Storage.DiskStopFreeBytes {
		t.Fatalf("example disk watermarks differ from defaults")
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
  metrics_public: false
  api_key: scraper-secret
  security:
    insecure: true
    danger_allow_bearer_over_plaintext: true
log:
  level: debug
  format: json
retention:
  interval: 30m
  journal:
    max_age: 336h
    max_bytes: 1073741824
    max_segments: 20
  logs:
    local_max_age: 24h
    max_age: 168h
    max_segments: 10
  spans:
    max_age: 72h
runtime:
  memory_limit: 838860800
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
	if cfg.API.MetricsPublic {
		t.Error("API.MetricsPublic: got true, want explicitly configured false")
	}
	if cfg.Retention.Interval != 30*time.Minute {
		t.Errorf("Retention.Interval: got %v", cfg.Retention.Interval)
	}
	if cfg.Retention.Journal.MaxAge != 336*time.Hour || cfg.Retention.Journal.MaxBytes != 1<<30 || cfg.Retention.Journal.MaxSegments != 20 {
		t.Errorf("Journal retention: got %+v", cfg.Retention.Journal)
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
	if cfg.Runtime.MemoryLimit != 838860800 {
		t.Errorf("Runtime.MemoryLimit: got %d", cfg.Runtime.MemoryLimit)
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

func TestValidate_DiskWatermarks(t *testing.T) {
	cfg := Default()
	cfg.Storage.DiskStopFreeBytes = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("zero stop watermark accepted")
	}

	cfg = Default()
	cfg.Storage.DiskWarningFreeBytes = cfg.Storage.DiskStopFreeBytes - 1
	if err := cfg.Validate(); err == nil {
		t.Fatal("warning watermark below stop watermark accepted")
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

func TestValidate_NegativeMemoryLimit(t *testing.T) {
	cfg := Default()
	cfg.Runtime.MemoryLimit = -1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for negative memory_limit")
	}
}

func TestValidate_RetentionLimits(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "interval", mutate: func(cfg *Config) { cfg.Retention.Interval = -time.Second }},
		{name: "logs local age", mutate: func(cfg *Config) { cfg.Retention.Logs.LocalMaxAge = -time.Second }},
		{name: "spans max bytes", mutate: func(cfg *Config) { cfg.Retention.Spans.MaxBytes = -1 }},
		{name: "journal max age", mutate: func(cfg *Config) { cfg.Retention.Journal.MaxAge = -time.Second }},
		{name: "journal max bytes", mutate: func(cfg *Config) { cfg.Retention.Journal.MaxBytes = -1 }},
		{name: "journal max segments", mutate: func(cfg *Config) { cfg.Retention.Journal.MaxSegments = -1 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("negative retention value accepted")
			}
		})
	}
}

func TestValidate_IndexBootstrapWorkers(t *testing.T) {
	cfg := Default()
	cfg.Runtime.IndexBootstrapWorkers = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error")
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
	if cfg.API.HTTPAddr != "localhost:8080" {
		t.Errorf("HTTPAddr should keep default: got %q", cfg.API.HTTPAddr)
	}
}

func TestValidate_NonLoopbackRequiresExplicitInsecureOrSecurity(t *testing.T) {
	cfg := Default()
	cfg.API.HTTPAddr = ":8080"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for unauthenticated non-loopback listener")
	}

	cfg.API.Security.Insecure = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("explicit insecure dev config should pass: %v", err)
	}
}

func TestValidate_LegacyAPIKeyProtectsNonLoopback(t *testing.T) {
	cfg := Default()
	cfg.API.HTTPAddr = ":8080"
	cfg.API.APIKey = "secret"
	cfg.API.Security.Insecure = true
	cfg.API.Security.DangerAllowBearerOverPlaintext = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("legacy api_key should map to Reef bearer config: %v", err)
	}
}

func TestValidate_ProtectedMetricsRequiresBearer(t *testing.T) {
	cfg := Default()
	cfg.API.MetricsPublic = false
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected protected metrics without bearer auth to be rejected")
	}

	cfg.API.APIKey = "scraper-secret"
	cfg.API.Security.DangerAllowBearerOverPlaintext = true
	if err := cfg.Validate(); err != nil {
		t.Fatalf("protected metrics with bearer auth: %v", err)
	}
}

func TestValidate_PartialTLSRejected(t *testing.T) {
	cfg := Default()
	cfg.API.Security.TLS.CertFile = "server.crt"
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected partial TLS config to be rejected")
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

func TestResolvedBearerConfig_LegacyFallback(t *testing.T) {
	cfg := APIConfig{APIKeys: []NamedAPIKey{{Name: "ops", Key: "secret"}}}
	got := cfg.ResolvedBearerConfig()
	if got == nil || len(got.Bearer) != 1 || got.Bearer[0].Name != "ops" || got.Bearer[0].Token != "secret" {
		t.Fatalf("legacy keys not mapped to bearer config: %+v", got)
	}
}
