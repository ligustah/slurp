package progress

import (
	"bytes"
	"strings"
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
		{1024, "1.00 KB"},
		{1536, "1.50 KB"},
		{1024 * 1024, "1.00 MB"},
		{256 * 1024 * 1024, "256.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
		{1024 * 1024 * 1024 * 1024, "1.00 TB"},
		{2.5 * 1024 * 1024 * 1024 * 1024, "2.50 TB"},
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
		{"1KB", 1024},
		{"1.5KB", 1536},
		{"256MB", 256 * 1024 * 1024},
		{"1GB", 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
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

func TestReporterBasic(t *testing.T) {
	var buf bytes.Buffer

	reporter := NewReporter(Options{
		TotalSize:      1024 * 1024,
		TotalChunks:    4,
		Workers:        2,
		Output:         &buf,
		UpdateInterval: 10 * time.Millisecond,
		SourceURL:      "https://example.com/file.bin",
		ChunkSize:      256 * 1024,
	})

	reporter.Start()

	// Simulate chunk progress
	reporter.ChunkStarted()
	reporter.ChunkCompleted(256 * 1024)

	reporter.ChunkStarted()
	reporter.ChunkCompleted(256 * 1024)

	time.Sleep(50 * time.Millisecond) // Let updates run

	reporter.Stop()

	output := buf.String()

	// Verify header was printed
	if !strings.Contains(output, "https://example.com/file.bin") {
		t.Error("expected source URL in output")
	}
	if !strings.Contains(output, "Chunks: 4") {
		t.Error("expected total chunks in output")
	}
	if !strings.Contains(output, "Workers: 2") {
		t.Error("expected workers in output")
	}
}

func TestReporterChunkTracking(t *testing.T) {
	var buf bytes.Buffer

	reporter := NewReporter(Options{
		TotalSize:      1024,
		TotalChunks:    4,
		Workers:        2,
		Output:         &buf,
		UpdateInterval: 100 * time.Millisecond,
	})

	// Test chunk tracking without starting the reporter
	reporter.ChunkStarted()
	if reporter.inProgress.Load() != 1 {
		t.Errorf("expected 1 in-progress, got %d", reporter.inProgress.Load())
	}

	reporter.ChunkCompleted(256)
	if reporter.inProgress.Load() != 0 {
		t.Errorf("expected 0 in-progress after complete, got %d", reporter.inProgress.Load())
	}
	if reporter.completedChunks.Load() != 1 {
		t.Errorf("expected 1 completed, got %d", reporter.completedChunks.Load())
	}
	if reporter.completedBytes.Load() != 256 {
		t.Errorf("expected 256 bytes, got %d", reporter.completedBytes.Load())
	}

	reporter.ChunkStarted()
	reporter.ChunkFailed()
	if reporter.inProgress.Load() != 0 {
		t.Errorf("expected 0 in-progress after fail, got %d", reporter.inProgress.Load())
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		input    time.Duration
		expected string
	}{
		{30 * time.Second, "30s"},
		{90 * time.Second, "1m 30s"},
		{3661 * time.Second, "1h 1m 1s"},
	}

	for _, tt := range tests {
		result := formatDuration(tt.input)
		if result != tt.expected {
			t.Errorf("formatDuration(%v) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}
