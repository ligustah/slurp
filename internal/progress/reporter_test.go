package progress

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{100, "100 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1024 * 1024, "1.0 MiB"},
		{256 * 1024 * 1024, "256 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TiB"},
		{2.5 * 1024 * 1024 * 1024 * 1024, "2.5 TiB"},
	}

	for _, tt := range tests {
		result := FormatBytes(tt.input)
		if result != tt.expected {
			t.Errorf("FormatBytes(%d) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestParseBytes(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"100", 100},
		{"100B", 100},
		{"1KiB", 1024},
		{"1.5KiB", 1536},
		{"256MiB", 256 * 1024 * 1024},
		{"1GiB", 1024 * 1024 * 1024},
		{"1TiB", 1024 * 1024 * 1024 * 1024},
		// SI units
		{"1KB", 1000},
		{"1MB", 1000 * 1000},
		{"1GB", 1000 * 1000 * 1000},
	}

	for _, tt := range tests {
		result, err := ParseBytes(tt.input)
		if err != nil {
			t.Errorf("ParseBytes(%q): %v", tt.input, err)
			continue
		}
		if result != tt.expected {
			t.Errorf("ParseBytes(%q) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestParseBytesInvalid(t *testing.T) {
	_, err := ParseBytes("invalid")
	if err == nil {
		t.Error("expected error for invalid input")
	}
}

func TestReporterChunkTracking(t *testing.T) {
	reporter := NewReporter(Options{
		TotalSize:      1024,
		TotalShards:    4,
		Workers:        2,
		UpdateInterval: 100 * time.Millisecond,
	})

	// Test chunk tracking without starting the reporter
	reporter.ShardStarted()
	if reporter.inProgress.Load() != 1 {
		t.Errorf("expected 1 in-progress, got %d", reporter.inProgress.Load())
	}

	reporter.BytesWritten(256)
	reporter.ShardCompleted()
	if reporter.inProgress.Load() != 0 {
		t.Errorf("expected 0 in-progress after complete, got %d", reporter.inProgress.Load())
	}
	if reporter.completedShards.Load() != 1 {
		t.Errorf("expected 1 completed, got %d", reporter.completedShards.Load())
	}
	if reporter.completedBytes.Load() != 256 {
		t.Errorf("expected 256 bytes, got %d", reporter.completedBytes.Load())
	}

	reporter.ShardStarted()
	reporter.ShardFailed()
	if reporter.inProgress.Load() != 0 {
		t.Errorf("expected 0 in-progress after fail, got %d", reporter.inProgress.Load())
	}
}

func TestReporterStartStop(t *testing.T) {
	reporter := NewReporter(Options{
		TotalSize:      1024 * 1024,
		TotalShards:    4,
		Workers:        2,
		UpdateInterval: 10 * time.Millisecond,
		SourceURL:      "https://example.com/file.bin",
		ShardSize:      256 * 1024,
	})

	reporter.Start()

	// Simulate chunk progress
	reporter.ShardStarted()
	reporter.BytesWritten(256 * 1024)
	reporter.ShardCompleted()

	reporter.ShardStarted()
	reporter.BytesWritten(256 * 1024)
	reporter.ShardCompleted()

	time.Sleep(50 * time.Millisecond) // Let updates run

	reporter.Stop()

	// Verify state
	if reporter.completedShards.Load() != 2 {
		t.Errorf("expected 2 completed chunks, got %d", reporter.completedShards.Load())
	}
	if reporter.completedBytes.Load() != 512*1024 {
		t.Errorf("expected 512KB completed, got %d", reporter.completedBytes.Load())
	}
}
