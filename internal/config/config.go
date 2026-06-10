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

// MetricsConfig configures the embedded metrics store.
// Zero limits use the metricsengine defaults.
type MetricsConfig struct {
	Enabled             bool          `yaml:"enabled"`
	Dir                 string        `yaml:"dir"`
	FlushInterval       time.Duration `yaml:"flush_interval"`
	MaxBufferedSamples  int           `yaml:"max_buffered_samples"`
	MaxActiveSeries     int           `yaml:"max_active_series"`
	MaxLabelsPerSeries  int           `yaml:"max_labels_per_series"`
	Retention           time.Duration `yaml:"retention"`
	CompactionMinBlocks int           `yaml:"compaction_min_blocks"`
	// DogfoodInterval enables the in-process selfobs scraper.
	// Zero disables it.
	DogfoodInterval time.Duration `yaml:"dogfood_interval"`
}

type DebugConfig struct {
	Pprof     bool   `yaml:"pprof"`
	PprofAddr string `yaml:"pprof_addr"`
}

// RetentionConfig groups the per-stream retention policies.
type RetentionConfig struct {
	Logs     StreamRetentionConfig `yaml:"logs"`
	Spans    StreamRetentionConfig `yaml:"spans"`
	Interval time.Duration         `yaml:"interval"`
}

// StreamRetentionConfig configures local and global retention for one stream.
// Local limits remove only the local copy. Global limits remove the segment.
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

	// ReconcileOnStart adopts sealed remote segments at startup.
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
	BatchSize             int              `yaml:"batch_size"`
	BatchTimeout          time.Duration    `yaml:"batch_timeout"`
	QueueSize             int              `yaml:"queue_size"`
	ShutdownTimeout       time.Duration    `yaml:"shutdown_timeout"`
	BreakerThreshold      int              `yaml:"breaker_threshold"`
	Logs                  IngestLaneConfig `yaml:"logs"`
	Spans                 IngestLaneConfig `yaml:"spans"`
	MaxAttrsPerEntry      int              `yaml:"max_attrs_per_entry"`
	MaxAttrValueBytes     int              `yaml:"max_attr_value_bytes"`
	MaxAttrKeysPerService int              `yaml:"max_attr_keys_per_service"`
}

type IngestLaneConfig struct {
	BatchSize        int           `yaml:"batch_size"`
	BatchTimeout     time.Duration `yaml:"batch_timeout"`
	QueueSize        int           `yaml:"queue_size"`
	BreakerThreshold int           `yaml:"breaker_threshold"`
}

// NamedAPIKey is a named bearer token.
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

// ResolvedAPIKeys returns api_keys, or api_key as a single default key.
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

// Default returns the default configuration.
func Default() *Config {
	return &Config{
		Storage: StorageConfig{
			DataDir:           "./data",
			SegmentMaxRecords: 100_000,
			SegmentMaxBytes:   128 << 20,
		},
		Ingest: IngestConfig{
			BatchSize:             1000,
			BatchTimeout:          100 * time.Millisecond,
			QueueSize:             100_000,
			ShutdownTimeout:       30 * time.Second,
			BreakerThreshold:      10,
			MaxAttrsPerEntry:      64,
			MaxAttrValueBytes:     4096,
			MaxAttrKeysPerService: 1024,
		},
		API: APIConfig{
			HTTPAddr:          ":8080",
			ReadTimeout:       30 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxRequestBytes:   32 << 20,
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
			Enabled:   true,
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

// detectLegacyRetention rejects the old flat retention keys.
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
	if err := validateIngestLane("ingest.logs", c.Ingest.Logs); err != nil {
		return err
	}
	if err := validateIngestLane("ingest.spans", c.Ingest.Spans); err != nil {
		return err
	}
	if c.API.HTTPAddr == "" {
		return fmt.Errorf("api.http_addr is required")
	}
	return nil
}

func validateIngestLane(name string, lane IngestLaneConfig) error {
	if lane.BatchSize < 0 {
		return fmt.Errorf("%s.batch_size must be positive when set", name)
	}
	if lane.BatchTimeout < 0 {
		return fmt.Errorf("%s.batch_timeout must be positive when set", name)
	}
	if lane.QueueSize < 0 {
		return fmt.Errorf("%s.queue_size must be positive when set", name)
	}
	if lane.BreakerThreshold < 0 {
		return fmt.Errorf("%s.breaker_threshold must be positive when set", name)
	}
	return nil
}
