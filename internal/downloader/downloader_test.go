package downloader

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

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
