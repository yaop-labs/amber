// Package config loads and validates the on-disk YAML configuration.
package config

import (
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/tlsconf"
	"gopkg.in/yaml.v3"
)

// Config is the root of the standalone server's YAML configuration; each field
// is a top-level section.
type Config struct {
	Storage   StorageConfig   `yaml:"storage"`
	Ingest    IngestConfig    `yaml:"ingest"`
	API       APIConfig       `yaml:"api"`
	Log       LogConfig       `yaml:"log"`
	Retention RetentionConfig `yaml:"retention"`
	Metrics   MetricsConfig   `yaml:"metrics"`
	Runtime   RuntimeConfig   `yaml:"runtime"`
	Debug     DebugConfig     `yaml:"debug"`
}

// RuntimeConfig tunes the Go runtime for the standalone server.
type RuntimeConfig struct {
	// MemoryLimit sets the Go runtime soft memory limit in bytes
	// (debug.SetMemoryLimit; overrides GOMEMLIMIT). Zero disables it.
	MemoryLimit int64 `yaml:"memory_limit"`
	// IndexBootstrapWorkers bounds concurrent sidecar rebuilds during open.
	IndexBootstrapWorkers int `yaml:"index_bootstrap_workers"`
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
	// CacheBudget is the combined byte budget for the metric store's block
	// caches. Zero derives it from runtime.memory_limit/2 when a limit is
	// set; otherwise the store defaults (320+384 MiB) apply.
	CacheBudget int64 `yaml:"cache_budget"`
}

// DebugConfig configures debug/profiling endpoints.
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

// S3Config configures the optional S3-compatible storage tier.
type S3Config struct {
	Bucket   string `yaml:"bucket"`
	Prefix   string `yaml:"prefix"`
	Region   string `yaml:"region"`
	Endpoint string `yaml:"endpoint"` // custom endpoint for MinIO/R2/etc.

	// ReconcileOnStart adopts sealed remote segments at startup.
	ReconcileOnStart bool `yaml:"reconcile_on_start"`
}

// StorageConfig configures the data directory, segment rotation, and S3 tier.
type StorageConfig struct {
	DataDir           string `yaml:"data_dir"`
	SegmentMaxRecords uint64 `yaml:"segment_max_records"`
	// SegmentMaxBytes counts uncompressed record payload and is checked after a batch.
	SegmentMaxBytes int64    `yaml:"segment_max_bytes"`
	IndexCacheSize  int      `yaml:"index_cache_size"`
	S3              S3Config `yaml:"s3"`
}

// IngestConfig configures the batcher and its per-lane overrides.
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
	MaxServices           int              `yaml:"max_services"`
}

// IngestLaneConfig overrides ingest settings for one lane (logs or spans).
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

// APISecurityConfig configures Reef protection for Amber's external HTTP/gRPC
// surfaces. Empty TLS/auth is permitted only for local dev or explicit insecure
// mode; partial TLS/auth blocks fail validation through Reef.
type APISecurityConfig struct {
	TLS      tlsconf.ServerConfig `yaml:"tls"`
	Auth     bearer.ServerConfig  `yaml:"auth"`
	Insecure bool                 `yaml:"insecure"`
}

// APIConfig configures the HTTP and gRPC listeners and API keys.
type APIConfig struct {
	HTTPAddr          string        `yaml:"http_addr"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	MaxRequestBytes   int64         `yaml:"max_request_bytes"`
	// MetricsPublic leaves GET /metrics outside Reef bearer authentication.
	// Set false in production when the scraper can present a bearer token.
	MetricsPublic bool `yaml:"metrics_public"`

	// APIKey is the legacy single-key field, kept for backward compatibility.
	// If APIKeys is non-empty it wins; otherwise APIKey acts as a single
	// entry named "default".
	APIKey  string        `yaml:"api_key"`
	APIKeys []NamedAPIKey `yaml:"api_keys"`

	GRPCAddr string            `yaml:"grpc_addr"`
	Security APISecurityConfig `yaml:"security"`
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

// ResolvedBearerConfig returns Reef bearer auth config, preferring the new
// api.security.auth shape and falling back to legacy api_keys/api_key.
func (c APIConfig) ResolvedBearerConfig() *bearer.ServerConfig {
	if len(c.Security.Auth.Bearer) > 0 {
		return &c.Security.Auth
	}
	keys := c.ResolvedAPIKeys()
	if len(keys) == 0 {
		return nil
	}
	out := &bearer.ServerConfig{Bearer: make([]bearer.Key, 0, len(keys))}
	for _, k := range keys {
		out.Bearer = append(out.Bearer, bearer.Key{Name: k.Name, Token: k.Key})
	}
	return out
}

func (c APIConfig) reefSecurityConfigured() bool {
	auth := c.ResolvedBearerConfig()
	return c.Security.TLS.Enabled || (auth != nil && len(auth.Bearer) > 0)
}

// LogConfig configures the server's own logging (level, format).
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
			MaxServices:           10_000,
		},
		API: APIConfig{
			HTTPAddr:          "localhost:8080",
			ReadTimeout:       30 * time.Second,
			ReadHeaderTimeout: 5 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
			MaxRequestBytes:   32 << 20,
			MetricsPublic:     true,
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
		Runtime: RuntimeConfig{IndexBootstrapWorkers: 1},
	}
}

// Load reads, parses, and validates the YAML config at path, applying defaults.
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

// Validate reports the first invalid or inconsistent setting in the config.
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
	if err := c.validateAPISecurity(); err != nil {
		return err
	}
	if c.Runtime.MemoryLimit < 0 {
		return fmt.Errorf("runtime.memory_limit must be positive when set")
	}
	if c.Runtime.IndexBootstrapWorkers <= 0 {
		return fmt.Errorf("runtime.index_bootstrap_workers must be positive")
	}
	if c.Metrics.CacheBudget < 0 {
		return fmt.Errorf("metrics.cache_budget must be positive when set")
	}
	if c.Ingest.MaxServices < 0 {
		return fmt.Errorf("ingest.max_services must be positive when set")
	}
	return nil
}

func (c *Config) validateAPISecurity() error {
	if _, err := c.API.Security.TLS.Validate(); err != nil {
		return fmt.Errorf("api.security.tls: %w", err)
	}
	if _, err := c.API.ResolvedBearerConfig().Validate(); err != nil {
		return fmt.Errorf("api.security.auth: %w", err)
	}
	if !c.API.MetricsPublic {
		auth := c.API.ResolvedBearerConfig()
		if auth == nil || len(auth.Bearer) == 0 {
			return fmt.Errorf("api.metrics_public=false requires bearer authentication")
		}
	}
	secured := c.API.reefSecurityConfigured()
	if c.API.Security.Insecure && secured {
		return fmt.Errorf("api.security.insecure cannot be true when TLS or auth is configured")
	}
	if !secured && !c.API.Security.Insecure && apiExposesNonLoopback(c.API.HTTPAddr, c.API.GRPCAddr) {
		return fmt.Errorf("api.security.insecure must be true for plaintext unauthenticated non-loopback listeners")
	}
	return nil
}

func apiExposesNonLoopback(addrs ...string) bool {
	for _, addr := range addrs {
		if addr == "" {
			continue
		}
		if exposesNonLoopback(addr) {
			return true
		}
	}
	return false
}

func exposesNonLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return true
	}
	if strings.EqualFold(host, "localhost") {
		return false
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return true
	}
	return !ip.IsLoopback()
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
