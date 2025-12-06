package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestHead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			t.Errorf("expected HEAD, got %s", r.Method)
		}
		w.Header().Set("Content-Length", "1024")
		w.Header().Set("ETag", `"abc123"`)
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", "Sat, 01 Jan 2025 00:00:00 GMT")
	}))
	defer server.Close()

	client := NewClient(DefaultOptions())
	info, err := client.Head(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	if info.Size != 1024 {
		t.Errorf("expected size 1024, got %d", info.Size)
	}
	if info.ETag != "abc123" {
		t.Errorf("expected ETag 'abc123', got %s", info.ETag)
	}
	if !info.AcceptsRanges {
		t.Error("expected AcceptsRanges to be true")
	}
	if info.ContentType != "application/octet-stream" {
		t.Errorf("expected content-type 'application/octet-stream', got %s", info.ContentType)
	}
}

func TestHeadNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := NewClient(DefaultOptions())
	_, err := client.Head(context.Background(), server.URL)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestGetRange(t *testing.T) {
	data := []byte("Hello, World! This is test data for range requests.")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Write(data)
			return
		}

		// Parse range header
		var start, end int64
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ = strconv.ParseInt(parts[0], 10, 64)
		end, _ = strconv.ParseInt(parts[1], 10, 64)

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		w.Header().Set("Content-Range", "bytes "+rangeHeader+"/"+strconv.Itoa(len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	client := NewClient(DefaultOptions())
	resp, err := client.GetRange(context.Background(), server.URL, 0, 4)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if string(body) != "Hello" {
		t.Errorf("expected 'Hello', got '%s'", string(body))
	}

	if resp.ETag != "test-etag" {
		t.Errorf("expected ETag 'test-etag', got %s", resp.ETag)
	}
}

func TestGetRangeNotSupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Server ignores Range header and returns full content
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(DefaultOptions())
	_, err := client.GetRange(context.Background(), server.URL, 0, 10)
	if err != ErrRangeNotSupported {
		t.Errorf("expected ErrRangeNotSupported, got %v", err)
	}
}

func TestRetryOnServerError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "10")
		w.Header().Set("Accept-Ranges", "bytes")
	}))
	defer server.Close()

	opts := DefaultOptions()
	opts.RetryBackoff = 10 * time.Millisecond
	opts.RetryMaxBackoff = 50 * time.Millisecond

	client := NewClient(opts)
	info, err := client.Head(context.Background(), server.URL)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}

	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if info.Size != 10 {
		t.Errorf("expected size 10, got %d", info.Size)
	}
}

func TestParseContentRange(t *testing.T) {
	tests := []struct {
		header string
		start  int64
		end    int64
		total  int64
	}{
		{"bytes 0-99/1000", 0, 99, 1000},
		{"bytes 100-199/1000", 100, 199, 1000},
		{"bytes 0-99/*", 0, 99, -1},
	}

	for _, tt := range tests {
		start, end, total, err := ParseContentRange(tt.header)
		if err != nil {
			t.Errorf("ParseContentRange(%q): %v", tt.header, err)
			continue
		}
		if start != tt.start || end != tt.end || total != tt.total {
			t.Errorf("ParseContentRange(%q) = (%d, %d, %d), want (%d, %d, %d)",
				tt.header, start, end, total, tt.start, tt.end, tt.total)
		}
	}
}

func TestCleanETag(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{`"abc123"`, "abc123"},
		{`W/"abc123"`, "abc123"},
		{"abc123", "abc123"},
		{`""`, ""},
	}

	for _, tt := range tests {
		result := cleanETag(tt.input)
		if result != tt.expected {
			t.Errorf("cleanETag(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	client := NewClient(DefaultOptions())
	_, err := client.Head(ctx, server.URL)
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
}
