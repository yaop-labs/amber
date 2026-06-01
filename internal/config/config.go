// Package config loads and validates the on-disk YAML configuration.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage   StorageConfig   `yaml:"storage"`
	Ingest    IngestConfig    `yaml:"ingest"`
	API       APIConfig       `yaml:"api"`
	Log       LogConfig       `yaml:"log"`
	Retention RetentionConfig `yaml:"retention"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Debug     DebugConfig     `yaml:"debug"`
}

// MetricsConfig governs the embedded metricsengine store. When Enabled the
// store opens at <storage.data_dir>/metrics by default; override with Dir.
// Limits and intervals map 1-to-1 onto metricsengine/store.Options; zero
// in any field defers to metricsengine's own default.
type MetricsConfig struct {
	Enabled             bool          `yaml:"enabled"`
	Dir                 string        `yaml:"dir"`
	FlushInterval       time.Duration `yaml:"flush_interval"`
	MaxBufferedSamples  int           `yaml:"max_buffered_samples"`
	MaxActiveSeries     int           `yaml:"max_active_series"`
	MaxLabelsPerSeries  int           `yaml:"max_labels_per_series"`
	Retention           time.Duration `yaml:"retention"`
	CompactionMinBlocks int           `yaml:"compaction_min_blocks"`
}

type DebugConfig struct {
	Pprof     bool   `yaml:"pprof"`
	PprofAddr string `yaml:"pprof_addr"`
}

// RetentionConfig groups two independently-tunable policies (logs, spans) plus
// a shared scheduler interval. Per-stream split lets traces age out faster
// than logs without yaml gymnastics. BREAKING CHANGE from earlier flat shape
// (MaxAge/MaxBytes/MaxSegments at top level) — no migration: rename keys in
// existing config files.
type RetentionConfig struct {
	Logs     StreamRetentionConfig `yaml:"logs"`
	Spans    StreamRetentionConfig `yaml:"spans"`
	Interval time.Duration         `yaml:"interval"`
}

// StreamRetentionConfig governs one stream's retention. Two tiers:
//
//   - Local tier (LocalMaxAge / LocalMaxBytes): evict the data file from
//     local disk while keeping the S3 object. Only applies when S3 is
//     configured and the segment has been uploaded. Zero = disabled.
//
//   - Global tier (MaxAge / MaxBytes / MaxSegments): delete the segment
//     everywhere, including S3. Zero = disabled. RequireUploaded (set by
//     runtime when S3 is enabled) still gates against deleting the only
//     durable copy if the uploader has fallen behind.
//
// Local thresholds should be < global thresholds; cleaner does not enforce
// this — it just runs the local pass first, so a misconfig where local > global
// effectively makes local a no-op.
type StreamRetentionConfig struct {
	LocalMaxAge   time.Duration `yaml:"local_max_age"`
	LocalMaxBytes int64         `yaml:"local_max_bytes"`

	MaxAge      time.Duration `yaml:"max_age"`
	MaxBytes    int64         `yaml:"max_bytes"`
	MaxSegments int           `yaml:"max_segments"`
}

// Enabled reports whether any retention threshold is set for this stream.
func (s StreamRetentionConfig) Enabled() bool {
	return s.LocalMaxAge > 0 || s.LocalMaxBytes > 0 ||
		s.MaxAge > 0 || s.MaxBytes > 0 || s.MaxSegments > 0
}

// HasLocalTier reports whether local-tier eviction is configured.
func (s StreamRetentionConfig) HasLocalTier() bool {
	return s.LocalMaxAge > 0 || s.LocalMaxBytes > 0
}

// HasGlobalTier reports whether global retention is configured.
func (s StreamRetentionConfig) HasGlobalTier() bool {
	return s.MaxAge > 0 || s.MaxBytes > 0 || s.MaxSegments > 0
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

// NamedAPIKey is a single (name, key) pair. The name appears in access logs
// and request context, the key is the bearer token. Multiple entries enable
// zero-downtime rotation: deploy with both old and new keys present, switch
// clients, then remove the old entry.
type NamedAPIKey struct {
	Name string `yaml:"name"`
	Key  string `yaml:"key"`
}

type APIConfig struct {
	HTTPAddr          string        `yaml:"http_addr"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxRequestBytes   int64         `yaml:"max_request_bytes"`

	// APIKey is the legacy single-key field, kept for backward compatibility.
	// If APIKeys is non-empty it wins; otherwise APIKey acts as a single
	// entry named "default".
	APIKey  string        `yaml:"api_key"`
	APIKeys []NamedAPIKey `yaml:"api_keys"`

	GRPCAddr string `yaml:"grpc_addr"`
}

// ResolvedAPIKeys returns the effective list of named keys after merging
// the legacy api_key field with api_keys. If neither is set, returns nil
// — middleware uses nil/empty as "auth disabled".
func (c APIConfig) ResolvedAPIKeys() []NamedAPIKey {
	if len(c.APIKeys) > 0 {
		return c.APIKeys
	}
	if c.APIKey != "" {
		return []NamedAPIKey{{Name: "default", Key: c.APIKey}}
	}
	return nil
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
		Metrics: MetricsConfig{
			// Enabled by default: amber's promise is one binary that holds
			// logs, traces, and metrics. Operators who don't want the metrics
			// store can flip this off; nothing else changes.
			Enabled: true,
			// Empty Dir resolves to <storage.data_dir>/metrics at runtime.
			// 24h retention covers the common rolling-window dashboard case
			// without locking us into longer storage commitments before the
			// cold tier lands.
			Retention: 24 * time.Hour,
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

	if err := detectLegacyRetention(data); err != nil {
		return nil, fmt.Errorf("config: %s: %w", path, err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config: invalid: %w", err)
	}

	return cfg, nil
}

// detectLegacyRetention surfaces a loud error when a config file still uses
// the old flat retention shape (max_age/max_bytes/max_segments at the
// retention top level). yaml.Unmarshal silently drops unknown keys; without
// this check a stale config would parse to RetentionConfig.Enabled() == false
// and retention would just stop running, which is worse than failing fast.
func detectLegacyRetention(data []byte) error {
	var probe struct {
		Retention map[string]any `yaml:"retention"`
	}
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return nil
	}
	legacy := []string{"max_age", "max_bytes", "max_segments"}
	var found []string
	for _, k := range legacy {
		if _, ok := probe.Retention[k]; ok {
			found = append(found, k)
		}
	}
	if len(found) == 0 {
		return nil
	}
	return fmt.Errorf("retention.{%s} moved under retention.logs / retention.spans (breaking change); please rename in your config", strings.Join(found, ","))
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
