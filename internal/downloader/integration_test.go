//go:build integration

package downloader_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/internal/downloader"
	"github.com/ligustah/slurp/internal/testutils"
	"github.com/ligustah/slurp/pkg/sharded"
)

func TestIntegrationDownloadToMinio(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Generate test files of different sizes
	testFiles := []testutils.TestFile{
		{Name: "tiny.bin", Size: 1024},                 // 1KB
		{Name: "small.bin", Size: 1024 * 1024},         // 1MB
		{Name: "medium.bin", Size: 10 * 1024 * 1024},   // 10MB
		{Name: "large.bin", Size: 100 * 1024 * 1024},   // 100MB
		{Name: "xlarge.bin", Size: 1024 * 1024 * 1024}, // 1GB
	}

	t.Log("Generating test data...")
	for i := range testFiles {
		testFiles[i].Data = testutils.GenerateTestData(t, testFiles[i].Size)
	}

	// Start HTTP server with test files
	t.Log("Starting HTTP test server...")
	server := testutils.StartTestHTTPServer(t, testFiles)
	defer server.Close()

	// Start Minio container
	t.Log("Starting Minio container...")
	minio := testutils.StartMinioContainer(t, ctx, "test-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	// Open bucket using gocloud URL opener
	t.Log("Opening bucket via gocloud...")
	bucket, err := minio.OpenBucket(ctx)
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Test each file
	for _, tf := range testFiles {
		tf := tf // capture range variable
		t.Run(tf.Name, func(t *testing.T) {
			testDownloadAndRead(t, ctx, server.URL, bucket, tf)
		})
	}
}

// testDownloadAndRead downloads a file and verifies it can be read back correctly.
func testDownloadAndRead(t *testing.T, ctx context.Context, serverURL string, bucket *blob.Bucket, tf testutils.TestFile) {
	t.Helper()

	sourceURL := serverURL + "/" + tf.Name
	destObject := "downloads/" + tf.Name

	// Determine chunk size based on file size
	chunkSize := int64(10 * 1024 * 1024) // 10MB default
	if tf.Size < chunkSize {
		chunkSize = tf.Size / 4
		if chunkSize < 1024 {
			chunkSize = 1024
		}
	}

	t.Logf("Downloading %s (%d bytes) with chunk size %d...", tf.Name, tf.Size, chunkSize)

	// Download
	startTime := time.Now()
	err := downloader.Download(ctx, sourceURL, bucket, destObject, downloader.Options{
		Workers:       8,
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Download %s: %v", tf.Name, err)
	}
	downloadDuration := time.Since(startTime)
	t.Logf("Downloaded %s in %v (%.2f MB/s)", tf.Name, downloadDuration,
		float64(tf.Size)/downloadDuration.Seconds()/1024/1024)

	// Read back using sharded reader
	t.Logf("Reading back %s...", tf.Name)
	startTime = time.Now()
	reader, err := sharded.ReadFromBucket(ctx, bucket, destObject)
	if err != nil {
		t.Fatalf("ReadFromBucket %s: %v", tf.Name, err)
	}
	defer reader.Close()

	// For large files, read in chunks and compare
	if tf.Size > 100*1024*1024 {
		testutils.CompareReaderToData(t, reader, tf.Data)
	} else {
		result, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll %s: %v", tf.Name, err)
		}

		if !bytes.Equal(result, tf.Data) {
			t.Fatalf("Data mismatch for %s: got %d bytes, want %d bytes", tf.Name, len(result), len(tf.Data))
		}
	}

	readDuration := time.Since(startTime)
	t.Logf("Read %s in %v (%.2f MB/s)", tf.Name, readDuration,
		float64(tf.Size)/readDuration.Seconds()/1024/1024)
}

// TestIntegrationResume verifies resume functionality with real S3 storage.
func TestIntegrationResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data: 1MB file with 4 chunks of 256KB each
	totalSize := int64(1024 * 1024)
	chunkSize := int64(256 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Start HTTP server
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{{Name: "resume.bin", Size: totalSize, Data: data}})
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "resume-test-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	bucket, err := minio.OpenBucket(ctx)
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	destObject := "downloads/resume.bin"

	// Step 1: Manually write first 2 of 4 chunks to simulate partial download
	t.Log("Writing partial chunks to simulate interrupted download...")
	f, err := sharded.Write(ctx, bucket, destObject,
		sharded.WithChunkSize(chunkSize),
		sharded.WithSize(totalSize),
		sharded.WithMetadata(map[string]string{
			"source_url":  server.URL + "/resume.bin",
			"source_etag": "/resume.bin", // Matches cleaned ETag from test server
		}),
		sharded.WithStateInterval(1),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write only first 2 chunks
	for i := 0; i < 2; i++ {
		chunk, err := f.Next(ctx)
		if err != nil {
			t.Fatalf("Next chunk %d: %v", i, err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		if end > totalSize {
			end = totalSize
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	// Don't call f.Complete() - simulate interruption

	t.Logf("Wrote %d of %d chunks", f.CompletedCount(), f.TotalChunks())

	// Step 2: Resume download - should detect existing state and continue
	t.Log("Resuming download...")
	err = downloader.Download(ctx, server.URL+"/resume.bin", bucket, destObject, downloader.Options{
		Workers:       4,
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Resume Download: %v", err)
	}

	// Step 3: Verify the complete file
	t.Log("Verifying resumed download...")
	reader, err := sharded.ReadFromBucket(ctx, bucket, destObject)
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Fatalf("Data mismatch after resume: got %d bytes, want %d bytes", len(result), len(data))
	}

	t.Log("Resume test passed - data verified")
}

// TestIntegrationResumeWithContextCancel tests resume after context cancellation.
func TestIntegrationResumeWithContextCancel(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data: 5MB file to ensure we have time to cancel mid-download
	totalSize := int64(5 * 1024 * 1024)
	chunkSize := int64(512 * 1024) // 10 chunks
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Create a slow server that adds delay to simulate network latency
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", `"slow-file"`)
			return
		}

		requestCount++
		// Add delay after first few chunks to allow cancellation
		if requestCount > 2 {
			time.Sleep(100 * time.Millisecond)
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
			w.Header().Set("ETag", `"slow-file"`)
			w.Write(data)
			return
		}

		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= totalSize {
			end = totalSize - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("ETag", `"slow-file"`)
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "cancel-test-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	bucket, err := minio.OpenBucket(ctx)
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	destObject := "downloads/cancel-resume.bin"

	// Step 1: Start download and cancel it after a short time
	t.Log("Starting download that will be cancelled...")
	downloadCtx, downloadCancel := context.WithTimeout(ctx, 500*time.Millisecond)

	err = downloader.Download(downloadCtx, server.URL, bucket, destObject, downloader.Options{
		Workers:       2, // Fewer workers to make cancellation more predictable
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	downloadCancel()

	// We expect an error due to cancellation (but state should be saved)
	if err == nil {
		t.Log("Download completed before cancellation - test may not be effective")
	} else {
		t.Logf("Download cancelled as expected: %v", err)
	}

	// Step 2: Check that some state was saved
	reader, err := sharded.ReadFromBucket(ctx, bucket, destObject)
	if err == nil {
		// Manifest exists - download may have completed
		reader.Close()
		t.Log("Download completed before we could cancel - skipping resume portion")
		return
	}

	// Check if state file exists (partial download)
	stateExists, _ := bucket.Exists(ctx, ".sharded/"+destObject[:16]+"/state.json")
	if !stateExists {
		// Try to find the state file with correct hash
		iter := bucket.List(&blob.ListOptions{Prefix: ".sharded/"})
		for {
			obj, err := iter.Next(ctx)
			if err != nil {
				break
			}
			if strings.HasSuffix(obj.Key, "state.json") {
				stateExists = true
				t.Logf("Found state file: %s", obj.Key)
				break
			}
		}
	}

	if !stateExists {
		t.Skip("No state saved during cancellation - download was too fast or too slow")
	}

	// Step 3: Resume download
	t.Log("Resuming cancelled download...")
	err = downloader.Download(ctx, server.URL, bucket, destObject, downloader.Options{
		Workers:       4,
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Resume Download: %v", err)
	}

	// Step 4: Verify complete file
	t.Log("Verifying resumed download...")
	reader, err = sharded.ReadFromBucket(ctx, bucket, destObject)
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}
	defer reader.Close()

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Fatalf("Data mismatch after resume: got %d bytes, want %d bytes", len(result), len(data))
	}

	t.Log("Cancel/resume test passed - data verified")
}
