package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := Default()

	if cfg.Workers != 16 {
		t.Errorf("expected default workers 16, got %d", cfg.Workers)
	}
	if cfg.ChunkSize != 256*1024*1024 {
		t.Errorf("expected default chunk size 256MB, got %d", cfg.ChunkSize)
	}
	if cfg.Retry.Attempts != 5 {
		t.Errorf("expected default retry attempts 5, got %d", cfg.Retry.Attempts)
	}
	if cfg.Retry.Backoff != time.Second {
		t.Errorf("expected default retry backoff 1s, got %v", cfg.Retry.Backoff)
	}
	if cfg.Retry.MaxBackoff != 30*time.Second {
		t.Errorf("expected default retry max backoff 30s, got %v", cfg.Retry.MaxBackoff)
	}
	if cfg.StateInterval != 10 {
		t.Errorf("expected default state interval 10, got %d", cfg.StateInterval)
	}
}

func TestLoadFromYAML(t *testing.T) {
	yamlContent := `
workers: 32
chunk_size: 512MB
progress: true
state_interval: 5
retry:
  attempts: 10
  backoff: 2s
  max_backoff: 60s
`
	// Create temp file
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := LoadFromFile(configPath)
	if err != nil {
		t.Fatalf("LoadFromFile: %v", err)
	}

	if cfg.Workers != 32 {
		t.Errorf("expected workers 32, got %d", cfg.Workers)
	}
	if cfg.ChunkSize != 512*1024*1024 {
		t.Errorf("expected chunk size 512MB, got %d", cfg.ChunkSize)
	}
	if !cfg.Progress {
		t.Error("expected progress true")
	}
	if cfg.StateInterval != 5 {
		t.Errorf("expected state interval 5, got %d", cfg.StateInterval)
	}
	if cfg.Retry.Attempts != 10 {
		t.Errorf("expected retry attempts 10, got %d", cfg.Retry.Attempts)
	}
	if cfg.Retry.Backoff != 2*time.Second {
		t.Errorf("expected retry backoff 2s, got %v", cfg.Retry.Backoff)
	}
	if cfg.Retry.MaxBackoff != 60*time.Second {
		t.Errorf("expected retry max backoff 60s, got %v", cfg.Retry.MaxBackoff)
	}
}

func TestLoadFromEnv(t *testing.T) {
	// Set env vars
	t.Setenv("SLURP_WORKERS", "64")
	t.Setenv("SLURP_CHUNK_SIZE", "1GB")
	t.Setenv("SLURP_PROGRESS", "true")
	t.Setenv("SLURP_RETRY_ATTEMPTS", "3")
	t.Setenv("SLURP_RETRY_BACKOFF", "500ms")
	t.Setenv("SLURP_RETRY_MAX_BACKOFF", "10s")

	cfg := Default()
	if err := cfg.LoadFromEnv(); err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}

	if cfg.Workers != 64 {
		t.Errorf("expected workers 64, got %d", cfg.Workers)
	}
	if cfg.ChunkSize != 1024*1024*1024 {
		t.Errorf("expected chunk size 1GB, got %d", cfg.ChunkSize)
	}
	if !cfg.Progress {
		t.Error("expected progress true")
	}
	if cfg.Retry.Attempts != 3 {
		t.Errorf("expected retry attempts 3, got %d", cfg.Retry.Attempts)
	}
	if cfg.Retry.Backoff != 500*time.Millisecond {
		t.Errorf("expected retry backoff 500ms, got %v", cfg.Retry.Backoff)
	}
	if cfg.Retry.MaxBackoff != 10*time.Second {
		t.Errorf("expected retry max backoff 10s, got %v", cfg.Retry.MaxBackoff)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: Config{
				URL:       "https://example.com/file.tar.gz",
				Bucket:    "gs://my-bucket",
				Object:    "downloads/file.tar.gz",
				Workers:   16,
				ChunkSize: 256 * 1024 * 1024,
				Retry: RetryConfig{
					Attempts:   5,
					Backoff:    time.Second,
					MaxBackoff: 30 * time.Second,
				},
			},
			wantErr: false,
		},
		{
			name: "missing URL",
			cfg: Config{
				Bucket:    "gs://my-bucket",
				Object:    "downloads/file.tar.gz",
				Workers:   16,
				ChunkSize: 256 * 1024 * 1024,
			},
			wantErr: true,
		},
		{
			name: "missing bucket",
			cfg: Config{
				URL:       "https://example.com/file.tar.gz",
				Object:    "downloads/file.tar.gz",
				Workers:   16,
				ChunkSize: 256 * 1024 * 1024,
			},
			wantErr: true,
		},
		{
			name: "missing object",
			cfg: Config{
				URL:       "https://example.com/file.tar.gz",
				Bucket:    "gs://my-bucket",
				Workers:   16,
				ChunkSize: 256 * 1024 * 1024,
			},
			wantErr: true,
		},
		{
			name: "invalid workers",
			cfg: Config{
				URL:       "https://example.com/file.tar.gz",
				Bucket:    "gs://my-bucket",
				Object:    "downloads/file.tar.gz",
				Workers:   0,
				ChunkSize: 256 * 1024 * 1024,
			},
			wantErr: true,
		},
		{
			name: "invalid chunk size",
			cfg: Config{
				URL:       "https://example.com/file.tar.gz",
				Bucket:    "gs://my-bucket",
				Object:    "downloads/file.tar.gz",
				Workers:   16,
				ChunkSize: 0,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	base := Default()
	base.URL = "https://example.com/file.tar.gz"
	base.Bucket = "gs://bucket"
	base.Object = "file.tar.gz"
	base.Workers = 16

	override := Config{
		Workers: 32, // Override workers
		// Leave other fields at zero values
	}

	merged := base.Merge(override)

	// Should keep base values for non-overridden fields
	if merged.URL != "https://example.com/file.tar.gz" {
		t.Errorf("expected URL preserved, got %s", merged.URL)
	}
	if merged.Bucket != "gs://bucket" {
		t.Errorf("expected Bucket preserved, got %s", merged.Bucket)
	}
	if merged.ChunkSize != 256*1024*1024 {
		t.Errorf("expected ChunkSize preserved, got %d", merged.ChunkSize)
	}

	// Should use override values
	if merged.Workers != 32 {
		t.Errorf("expected Workers overridden to 32, got %d", merged.Workers)
	}
}

func TestLoadYAMLFileNotFound(t *testing.T) {
	_, err := LoadFromFile("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoadYAMLInvalid(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("invalid: [yaml: content"), 0644); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	_, err := LoadFromFile(configPath)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}
