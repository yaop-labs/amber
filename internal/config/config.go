// Package config loads and validates the on-disk YAML configuration.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage   StorageConfig   `yaml:"storage"`
	Ingest    IngestConfig    `yaml:"ingest"`
	API       APIConfig       `yaml:"api"`
	Log       LogConfig       `yaml:"log"`
	Retention RetentionConfig `yaml:"retention"`
	Debug     DebugConfig     `yaml:"debug"`
}

type DebugConfig struct {
	Pprof     bool   `yaml:"pprof"`
	PprofAddr string `yaml:"pprof_addr"`
}

type RetentionConfig struct {
	MaxAge      time.Duration `yaml:"max_age"`
	MaxBytes    int64         `yaml:"max_bytes"`
	MaxSegments int           `yaml:"max_segments"`
	Interval    time.Duration `yaml:"interval"`
}

type S3Config struct {
	Bucket   string `yaml:"bucket"`
	Prefix   string `yaml:"prefix"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"` // custom endpoint for MinIO/R2/etc.

	// ReconcileOnStart triggers a remote List at startup and adopts any sealed
	// segments not present in local meta. Default false: reconcile runs only
	// when local meta is empty (typical fresh-node case). Enable for paranoid
	// fleets where local state can diverge from S3 (e.g. partial restores).
	ReconcileOnStart bool `yaml:"reconcile_on_start"`
}

type StorageConfig struct {
	DataDir           string   `yaml:"data_dir"`
	SegmentMaxRecords uint64   `yaml:"segment_max_records"`
	SegmentMaxBytes   int64    `yaml:"segment_max_bytes"`
	IndexCacheSize    int      `yaml:"index_cache_size"`
	S3                S3Config `yaml:"s3"`
}

type IngestConfig struct {
	BatchSize             int           `yaml:"batch_size"`
	BatchTimeout          time.Duration `yaml:"batch_timeout"`
	QueueSize             int           `yaml:"queue_size"`
	ShutdownTimeout       time.Duration `yaml:"shutdown_timeout"`
	BreakerThreshold      int           `yaml:"breaker_threshold"`
	MaxAttrsPerEntry      int           `yaml:"max_attrs_per_entry"`
	MaxAttrValueBytes     int           `yaml:"max_attr_value_bytes"`
	MaxAttrKeysPerService int           `yaml:"max_attr_keys_per_service"`
}

type APIConfig struct {
	HTTPAddr          string        `yaml:"http_addr"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxRequestBytes   int64         `yaml:"max_request_bytes"`
	APIKey            string        `yaml:"api_key"`
	GRPCAddr          string        `yaml:"grpc_addr"`
}

type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default config tuned for a single mid-tier node and modest log volume.
// Each value is a starting point — every comment below records WHY this
// number, not just what it is.
func Default() *Config {
	return &Config{
		Storage: StorageConfig{
			DataDir: "./data",
			// 100k records ≈ 1 MiB compressed; heap-threshold pruning then
			// skips all but 1-2 segments per query → p50 stays 50-100ms.
			SegmentMaxRecords: 100_000,
			// 128 MiB safety cap; normal workloads hit the record limit first.
			SegmentMaxBytes: 128 << 20,
		},
		Ingest: IngestConfig{
			// 1000 amortizes WAL/segment syscalls and zstd framing without
			// stalling ingest on a single batch flush.
			BatchSize: 1000,
			// 100 ms is the tail-latency ceiling at low ingestion rates
			// where BatchSize is never reached before timeout.
			BatchTimeout: 100 * time.Millisecond,
			// 100k items ≈ 100 batches of 1000 — absorbs short bursts;
			// past this we surface backpressure via ErrQueueFull metric.
			QueueSize: 100_000,
			// 30 s drain budget on shutdown; longer hangs are FS pathology
			// and we'd rather lose tail items than block kubectl rollout.
			ShutdownTimeout: 30 * time.Second,
			// 10 consecutive WriteBatch failures opens the breaker. Low
			// enough to react before queue fills, high enough to ride out
			// transient blips (single fsync stall, brief disk full).
			BreakerThreshold: 10,
			// 64 attrs/entry — covers normal OTLP resource+log attrs with
			// headroom; anything over is a label-bomb.
			MaxAttrsPerEntry: 64,
			// 4 KiB per value — typical log line, plenty for stack frames,
			// stops megabyte payloads from bloating bitmap indexes.
			MaxAttrValueBytes: 4096,
			// 1024 unique attr keys per service is generous enough that no
			// healthy app trips it; request_id-style key bombs pass it fast.
			MaxAttrKeysPerService: 1024,
		},
		API: APIConfig{
			HTTPAddr: ":8080",
			// 30 s for body upload — OTLP batches can be MB-scale on
			// slow links.
			ReadTimeout: 30 * time.Second,
			// 5 s for headers — tight enough to kill slow-loris,
			// loose enough for intercontinental TLS handshakes.
			ReadHeaderTimeout: 5 * time.Second,
			// 30 s for response — query results can be large.
			WriteTimeout: 30 * time.Second,
			// 120 s keeps pooled connections from wedging idle workers
			// while still letting browsers / scrapers reuse sockets.
			IdleTimeout:     120 * time.Second,
			MaxRequestBytes: 32 << 20,
		},
		Debug: DebugConfig{
			PprofAddr: "localhost:6060",
		},
		Log: LogConfig{
			Level:  "info",
			Format: "text",
		},
		Retention: RetentionConfig{
			Interval: time.Hour,
		},
	}
}

func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return cfg, nil
	}
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid: %w", err)
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.Storage.DataDir == "" {
		return fmt.Errorf("storage.data_dir is required")
	}
	if c.Ingest.BatchSize <= 0 {
		return fmt.Errorf("ingest.batch_size must be positive")
	}
	if c.Ingest.QueueSize <= 0 {
		return fmt.Errorf("ingest.queue_size must be positive")
	}
	if c.API.HTTPAddr == "" {
		return fmt.Errorf("api.http_addr is required")
	}
	return nil
}
