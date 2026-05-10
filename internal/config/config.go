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

type StorageConfig struct {
	DataDir           string `yaml:"data_dir"`
	SegmentMaxRecords uint64 `yaml:"segment_max_records"`
	SegmentMaxBytes   int64  `yaml:"segment_max_bytes"`
	IndexCacheSize    int    `yaml:"index_cache_size"`
}

type IngestConfig struct {
	BatchSize        int           `yaml:"batch_size"`
	BatchTimeout     time.Duration `yaml:"batch_timeout"`
	QueueSize        int           `yaml:"queue_size"`
	ShutdownTimeout  time.Duration `yaml:"shutdown_timeout"`
	BreakerThreshold int           `yaml:"breaker_threshold"`
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

func Default() *Config {
	return &Config{
		Storage: StorageConfig{
			DataDir:           "./data",
			SegmentMaxRecords: 1_000_000,
			SegmentMaxBytes:   512 << 20,
		},
		Ingest: IngestConfig{
			BatchSize:        1000,
			BatchTimeout:     100 * time.Millisecond,
			QueueSize:        100_000,
			ShutdownTimeout:  30 * time.Second,
			BreakerThreshold: 10,
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
