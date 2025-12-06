package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ligustah/slurp/internal/progress"
	"gopkg.in/yaml.v3"
)

// Config defines configuration for the slurp CLI.
type Config struct {
	URL           string      `yaml:"url"`
	Bucket        string      `yaml:"bucket"`
	Object        string      `yaml:"object"`
	Workers       int         `yaml:"workers"`
	ChunkSize     int64       `yaml:"chunk_size"`
	Progress      bool        `yaml:"progress"`
	Force         bool        `yaml:"force"`
	StateInterval int         `yaml:"state_interval"`
	Retry         RetryConfig `yaml:"retry"`
}

// RetryConfig defines retry behavior.
type RetryConfig struct {
	Attempts   int           `yaml:"attempts"`
	Backoff    time.Duration `yaml:"backoff"`
	MaxBackoff time.Duration `yaml:"max_backoff"`
}

// Default returns a Config with sensible defaults.
func Default() Config {
	return Config{
		Workers:       16,
		ChunkSize:     256 * 1024 * 1024, // 256MB
		StateInterval: 10,
		Retry: RetryConfig{
			Attempts:   5,
			Backoff:    time.Second,
			MaxBackoff: 30 * time.Second,
		},
	}
}

// yamlConfig is used for YAML unmarshaling with string chunk size.
type yamlConfig struct {
	URL           string          `yaml:"url"`
	Bucket        string          `yaml:"bucket"`
	Object        string          `yaml:"object"`
	Workers       int             `yaml:"workers"`
	ChunkSize     string          `yaml:"chunk_size"`
	Progress      bool            `yaml:"progress"`
	Force         bool            `yaml:"force"`
	StateInterval int             `yaml:"state_interval"`
	Retry         yamlRetryConfig `yaml:"retry"`
}

type yamlRetryConfig struct {
	Attempts   int    `yaml:"attempts"`
	Backoff    string `yaml:"backoff"`
	MaxBackoff string `yaml:"max_backoff"`
}

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	var yc yamlConfig
	if err := yaml.Unmarshal(data, &yc); err != nil {
		return Config{}, fmt.Errorf("parse config file: %w", err)
	}

	cfg := Default()

	if yc.URL != "" {
		cfg.URL = yc.URL
	}
	if yc.Bucket != "" {
		cfg.Bucket = yc.Bucket
	}
	if yc.Object != "" {
		cfg.Object = yc.Object
	}
	if yc.Workers != 0 {
		cfg.Workers = yc.Workers
	}
	if yc.ChunkSize != "" {
		size, err := progress.ParseBytes(yc.ChunkSize)
		if err != nil {
			return Config{}, fmt.Errorf("parse chunk_size: %w", err)
		}
		cfg.ChunkSize = size
	}
	cfg.Progress = yc.Progress
	cfg.Force = yc.Force
	if yc.StateInterval != 0 {
		cfg.StateInterval = yc.StateInterval
	}
	if yc.Retry.Attempts != 0 {
		cfg.Retry.Attempts = yc.Retry.Attempts
	}
	if yc.Retry.Backoff != "" {
		d, err := time.ParseDuration(yc.Retry.Backoff)
		if err != nil {
			return Config{}, fmt.Errorf("parse retry.backoff: %w", err)
		}
		cfg.Retry.Backoff = d
	}
	if yc.Retry.MaxBackoff != "" {
		d, err := time.ParseDuration(yc.Retry.MaxBackoff)
		if err != nil {
			return Config{}, fmt.Errorf("parse retry.max_backoff: %w", err)
		}
		cfg.Retry.MaxBackoff = d
	}

	return cfg, nil
}

// LoadFromEnv loads configuration from environment variables.
// Environment variables use the SLURP_ prefix.
func (c *Config) LoadFromEnv() error {
	if v := os.Getenv("SLURP_URL"); v != "" {
		c.URL = v
	}
	if v := os.Getenv("SLURP_BUCKET"); v != "" {
		c.Bucket = v
	}
	if v := os.Getenv("SLURP_OBJECT"); v != "" {
		c.Object = v
	}
	if v := os.Getenv("SLURP_WORKERS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_WORKERS: %w", err)
		}
		c.Workers = n
	}
	if v := os.Getenv("SLURP_CHUNK_SIZE"); v != "" {
		size, err := progress.ParseBytes(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_CHUNK_SIZE: %w", err)
		}
		c.ChunkSize = size
	}
	if v := os.Getenv("SLURP_PROGRESS"); v != "" {
		c.Progress = v == "true" || v == "1"
	}
	if v := os.Getenv("SLURP_FORCE"); v != "" {
		c.Force = v == "true" || v == "1"
	}
	if v := os.Getenv("SLURP_STATE_INTERVAL"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_STATE_INTERVAL: %w", err)
		}
		c.StateInterval = n
	}
	if v := os.Getenv("SLURP_RETRY_ATTEMPTS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_RETRY_ATTEMPTS: %w", err)
		}
		c.Retry.Attempts = n
	}
	if v := os.Getenv("SLURP_RETRY_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_RETRY_BACKOFF: %w", err)
		}
		c.Retry.Backoff = d
	}
	if v := os.Getenv("SLURP_RETRY_MAX_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("parse SLURP_RETRY_MAX_BACKOFF: %w", err)
		}
		c.Retry.MaxBackoff = d
	}

	return nil
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.URL == "" {
		return errors.New("config: URL is required")
	}
	if c.Bucket == "" {
		return errors.New("config: bucket is required")
	}
	if c.Object == "" {
		return errors.New("config: object is required")
	}
	if c.Workers <= 0 {
		return errors.New("config: workers must be positive")
	}
	if c.ChunkSize <= 0 {
		return errors.New("config: chunk_size must be positive")
	}
	return nil
}

// Merge merges override values into c, returning a new Config.
// Zero values in override are ignored.
func (c Config) Merge(override Config) Config {
	if override.URL != "" {
		c.URL = override.URL
	}
	if override.Bucket != "" {
		c.Bucket = override.Bucket
	}
	if override.Object != "" {
		c.Object = override.Object
	}
	if override.Workers != 0 {
		c.Workers = override.Workers
	}
	if override.ChunkSize != 0 {
		c.ChunkSize = override.ChunkSize
	}
	if override.Progress {
		c.Progress = override.Progress
	}
	if override.Force {
		c.Force = override.Force
	}
	if override.StateInterval != 0 {
		c.StateInterval = override.StateInterval
	}
	if override.Retry.Attempts != 0 {
		c.Retry.Attempts = override.Retry.Attempts
	}
	if override.Retry.Backoff != 0 {
		c.Retry.Backoff = override.Retry.Backoff
	}
	if override.Retry.MaxBackoff != 0 {
		c.Retry.MaxBackoff = override.Retry.MaxBackoff
	}
	return c
}
