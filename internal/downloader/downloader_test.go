package downloader

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

	slurphttp "github.com/ligustah/slurp/internal/http"
	"github.com/ligustah/slurp/pkg/sharded"
)

func TestDownloadBasic(t *testing.T) {
	// Create test data
	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Create test server that supports range requests
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"test-etag"`)
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Write(data)
			return
		}

		// Parse range header: bytes=start-end
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	// Create bucket
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Download
	err = Download(ctx, server.URL, bucket, "test/download.bin", Options{
		Workers:       4,
		ChunkSize:     256 * 1024, // 256KB chunks = 4 chunks
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}

	// Read back and verify
	reader, err := sharded.ReadFromBucket(ctx, bucket, "test/download.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(result) != len(data) {
		t.Fatalf("size mismatch: got %d, want %d", len(result), len(data))
	}

	for i := range data {
		if result[i] != data[i] {
			t.Fatalf("data mismatch at byte %d: got %d, want %d", i, result[i], data[i])
		}
	}
}

func TestDownloadResume(t *testing.T) {
	data := make([]byte, 512*1024) // 512KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"test-etag"`)
			return
		}

		rangeHeader := r.Header.Get("Range")
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.Header().Set("ETag", `"test-etag"`)
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// First download - write 2 of 4 chunks manually to simulate partial download
	chunkSize := int64(128 * 1024) // 4 chunks
	f, err := sharded.Write(ctx, bucket, "test/resume.bin",
		sharded.WithChunkSize(chunkSize),
		sharded.WithSize(int64(len(data))),
		sharded.WithMetadata(map[string]string{
			"source_url":  server.URL,
			"source_etag": "test-etag",
		}),
		sharded.WithStateInterval(1),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write only 2 chunks
	for i := 0; i < 2; i++ {
		chunk, err := f.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	// Don't complete - simulate interruption

	// Resume download - should pick up from existing state
	err = Download(ctx, server.URL, bucket, "test/resume.bin", Options{
		Workers:       4,
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Resume Download: %v", err)
	}

	// Read back and verify
	reader, err := sharded.ReadFromBucket(ctx, bucket, "test/resume.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(result) != len(data) {
		t.Fatalf("size mismatch after resume: got %d, want %d", len(result), len(data))
	}
}

func TestDownloadRangeNotSupported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		// No Accept-Ranges header
	}))
	defer server.Close()

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	err = Download(ctx, server.URL, bucket, "test/file.bin", Options{
		Workers:   4,
		ChunkSize: 100,
	})
	if err == nil {
		t.Error("expected error for range not supported")
	}
}

func TestDownloadNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	err = Download(ctx, server.URL, bucket, "test/file.bin", Options{
		Workers:   4,
		ChunkSize: 100,
	})
	if err == nil {
		t.Error("expected error for 404")
	}
}

func TestDownloadContextCancellation(t *testing.T) {
	data := make([]byte, 1024*1024)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			return
		}
		// Slow response
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusPartialContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	err = Download(ctx, server.URL, bucket, "test/file.bin", Options{
		Workers:   4,
		ChunkSize: 256 * 1024,
	})
	if err == nil {
		t.Error("expected error due to context cancellation")
	}
}

func TestFileInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("ETag", `"abc123"`)
	}))
	defer server.Close()

	ctx := context.Background()
	info, err := GetFileInfo(ctx, server.URL)
	if err != nil {
		t.Fatalf("GetFileInfo: %v", err)
	}

	if info.Size != 1000 {
		t.Errorf("expected size 1000, got %d", info.Size)
	}
	if !info.AcceptsRanges {
		t.Error("expected AcceptsRanges true")
	}
	if info.ETag != "abc123" {
		t.Errorf("expected ETag 'abc123', got %s", info.ETag)
	}
}

func TestCircuitBreaker(t *testing.T) {
	// Create a server that always fails chunk requests
	failCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "1048576") // 1MB
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"test"`)
			return
		}
		// Fail all chunk requests
		failCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	err = Download(ctx, server.URL, bucket, "test/circuit-breaker.bin", Options{
		Workers:                1, // Single worker to make failures predictable
		ChunkSize:              256 * 1024,
		MaxConsecutiveFailures: 3,
		HTTPOptions: slurphttp.Options{
			MaxIdleConnsPerHost: 1,
			RetryAttempts:       1, // Minimal retries for faster test
			RetryBackoff:        10 * time.Millisecond,
			RetryMaxBackoff:     50 * time.Millisecond,
		},
	})

	if err == nil {
		t.Fatal("expected circuit breaker error")
	}

	// Check that we got a CircuitBreakerError
	var cbErr *CircuitBreakerError
	if !errors.As(err, &cbErr) {
		t.Fatalf("expected CircuitBreakerError, got %T: %v", err, err)
	}

	if cbErr.ConsecutiveFailures != 3 {
		t.Errorf("expected 3 consecutive failures, got %d", cbErr.ConsecutiveFailures)
	}

	if len(cbErr.FailedChunks) == 0 {
		t.Error("expected FailedChunks to contain failure details")
	}

	t.Logf("Circuit breaker tripped after %d failures, %d chunks failed",
		cbErr.ConsecutiveFailures, len(cbErr.FailedChunks))
}

func TestCircuitBreakerResetsOnSuccess(t *testing.T) {
	// Create a server that fails intermittently but succeeds eventually
	requestNum := 0
	data := make([]byte, 512*1024) // 512KB = 2 chunks of 256KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"test"`)
			return
		}

		requestNum++
		// Fail first 2 requests, then succeed
		if requestNum <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		// Parse range and return data
		rangeHeader := strings.TrimPrefix(r.Header.Get("Range"), "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)
		if end >= int64(len(data)) {
			end = int64(len(data)) - 1
		}

		w.Header().Set("Content-Range", "bytes "+strconv.FormatInt(start, 10)+"-"+strconv.FormatInt(end, 10)+"/"+strconv.Itoa(len(data)))
		w.Header().Set("Content-Length", strconv.Itoa(int(end-start+1)))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Circuit breaker threshold is 5, but we only fail 2 times then succeed
	// So download should complete successfully (HTTP client has retries)
	err = Download(ctx, server.URL, bucket, "test/intermittent.bin", Options{
		Workers:                1,
		ChunkSize:              256 * 1024,
		MaxConsecutiveFailures: 5,
	})

	if err != nil {
		t.Fatalf("expected success after intermittent failures, got: %v", err)
	}

	// Verify file was downloaded correctly
	reader, err := sharded.ReadFromBucket(ctx, bucket, "test/intermittent.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(result) != len(data) {
		t.Errorf("expected %d bytes, got %d", len(data), len(result))
	}
}
