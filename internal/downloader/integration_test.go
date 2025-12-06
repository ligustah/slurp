//go:build integration

package downloader_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/internal/downloader"
	"github.com/ligustah/slurp/pkg/sharded"
)

// testFile defines a test file with size and expected checksum.
type testFile struct {
	name string
	size int64
	data []byte
}

func TestIntegrationDownloadToMinio(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Generate test files of different sizes
	testFiles := []testFile{
		{name: "tiny.bin", size: 1024},                 // 1KB
		{name: "small.bin", size: 1024 * 1024},         // 1MB
		{name: "medium.bin", size: 10 * 1024 * 1024},   // 10MB
		{name: "large.bin", size: 100 * 1024 * 1024},   // 100MB
		{name: "xlarge.bin", size: 1024 * 1024 * 1024}, // 1GB
	}

	t.Log("Generating test data...")
	for i := range testFiles {
		testFiles[i].data = generateTestData(t, testFiles[i].size)
	}

	// Start HTTP server with test files
	t.Log("Starting HTTP test server...")
	server := startTestHTTPServer(t, testFiles)
	defer server.Close()

	// Start Minio container
	t.Log("Starting Minio container...")
	minioContainer, bucketURL := startMinioContainer(t, ctx, "test-bucket")
	defer func() {
		if err := minioContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	// Open bucket using gocloud URL opener
	t.Log("Opening bucket via gocloud...")
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Test each file
	for _, tf := range testFiles {
		tf := tf // capture range variable
		t.Run(tf.name, func(t *testing.T) {
			testDownloadAndRead(t, ctx, server.URL, bucket, tf)
		})
	}
}

// generateTestData generates deterministic test data of the given size.
func generateTestData(t *testing.T, size int64) []byte {
	t.Helper()
	data := make([]byte, size)
	// Use deterministic pattern for smaller files, random for larger
	if size <= 10*1024*1024 {
		for i := range data {
			data[i] = byte(i % 256)
		}
	} else {
		// For large files, use random data but in chunks to avoid memory issues
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("generate random data: %v", err)
		}
	}
	return data
}

// startTestHTTPServer starts an HTTP server that serves test files with range request support.
func startTestHTTPServer(t *testing.T, files []testFile) *httptest.Server {
	t.Helper()

	fileMap := make(map[string][]byte)
	for _, f := range files {
		fileMap["/"+f.name] = f.data
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := fileMap[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}

		size := int64(len(data))

		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, r.URL.Path))
			return
		}

		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, r.URL.Path))
			w.Write(data)
			return
		}

		// Parse range header: bytes=start-end
		rangeHeader = strings.TrimPrefix(rangeHeader, "bytes=")
		parts := strings.Split(rangeHeader, "-")
		start, _ := strconv.ParseInt(parts[0], 10, 64)
		end, _ := strconv.ParseInt(parts[1], 10, 64)

		if end >= size {
			end = size - 1
		}

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.Header().Set("ETag", fmt.Sprintf(`"%s"`, r.URL.Path))
		w.WriteHeader(http.StatusPartialContent)
		w.Write(data[start : end+1])
	}))
}

// startMinioContainer starts a Minio container with a pre-created bucket.
// Returns the container and the gocloud bucket URL.
func startMinioContainer(t *testing.T, ctx context.Context, bucketName string) (testcontainers.Container, string) {
	t.Helper()

	const (
		accessKey = "minioadmin"
		secretKey = "minioadmin"
	)

	// Create a network for minio and mc to communicate
	networkName := fmt.Sprintf("minio-test-net-%d", time.Now().UnixNano())
	network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{
			Name: networkName,
		},
	})
	if err != nil {
		t.Fatalf("create network: %v", err)
	}
	t.Cleanup(func() { network.Remove(ctx) })

	// Start minio container
	minioReq := testcontainers.ContainerRequest{
		Image:        "minio/minio:latest",
		ExposedPorts: []string{"9000/tcp"},
		Networks:     []string{networkName},
		NetworkAliases: map[string][]string{
			networkName: {"minio"},
		},
		Env: map[string]string{
			"MINIO_ROOT_USER":     accessKey,
			"MINIO_ROOT_PASSWORD": secretKey,
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/ready").WithPort("9000"),
	}

	minioContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: minioReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}

	// Create bucket using mc container
	createBucketWithMC(t, ctx, networkName, accessKey, secretKey, bucketName)

	host, err := minioContainer.Host(ctx)
	if err != nil {
		t.Fatalf("get container host: %v", err)
	}

	port, err := minioContainer.MappedPort(ctx, "9000")
	if err != nil {
		t.Fatalf("get container port: %v", err)
	}

	endpoint := fmt.Sprintf("%s:%s", host, port.Port())

	// Build gocloud S3 URL with query parameters for minio
	// See: gocloud.dev/aws.V2ConfigFromURLParams and gocloud.dev/blob/s3blob.URLOpener
	// Note: AWS SDK warns about multipart checksum validation - these are harmless with Minio
	bucketURL := fmt.Sprintf("s3://%s?endpoint=http://%s&use_path_style=true&disable_https=true&region=us-east-1",
		bucketName,
		endpoint,
	)

	// Set AWS credentials via environment variables (gocloud reads these)
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)

	return minioContainer, bucketURL
}

// createBucketWithMC creates a bucket using a separate minio/mc container.
func createBucketWithMC(t *testing.T, ctx context.Context, networkName, accessKey, secretKey, bucketName string) {
	t.Helper()

	// mc container runs, creates the bucket, then exits
	mcReq := testcontainers.ContainerRequest{
		Image:      "minio/mc:latest",
		Networks:   []string{networkName},
		Entrypoint: []string{"/bin/sh", "-c"},
		Cmd: []string{
			fmt.Sprintf(
				"/usr/bin/mc config host add myminio http://minio:9000 %s %s && "+
					"/usr/bin/mc mb myminio/%s && "+
					"/usr/bin/mc policy set download myminio/%s; "+
					"exit 0",
				accessKey, secretKey, bucketName, bucketName,
			),
		},
		WaitingFor: wait.ForExit(),
	}

	mcContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: mcReq,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start mc container: %v", err)
	}
	defer mcContainer.Terminate(ctx)

	// Container runs and exits - WaitingFor: wait.ForExit() handles this
}

// testDownloadAndRead downloads a file and verifies it can be read back correctly.
func testDownloadAndRead(t *testing.T, ctx context.Context, serverURL string, bucket *blob.Bucket, tf testFile) {
	t.Helper()

	sourceURL := serverURL + "/" + tf.name
	destObject := "downloads/" + tf.name

	// Determine chunk size based on file size
	chunkSize := int64(10 * 1024 * 1024) // 10MB default
	if tf.size < chunkSize {
		chunkSize = tf.size / 4
		if chunkSize < 1024 {
			chunkSize = 1024
		}
	}

	t.Logf("Downloading %s (%d bytes) with chunk size %d...", tf.name, tf.size, chunkSize)

	// Download
	startTime := time.Now()
	err := downloader.Download(ctx, sourceURL, bucket, destObject, downloader.Options{
		Workers:       8,
		ChunkSize:     chunkSize,
		StateInterval: 1,
	})
	if err != nil {
		t.Fatalf("Download %s: %v", tf.name, err)
	}
	downloadDuration := time.Since(startTime)
	t.Logf("Downloaded %s in %v (%.2f MB/s)", tf.name, downloadDuration,
		float64(tf.size)/downloadDuration.Seconds()/1024/1024)

	// Read back using sharded reader
	t.Logf("Reading back %s...", tf.name)
	startTime = time.Now()
	reader, err := sharded.ReadFromBucket(ctx, bucket, destObject)
	if err != nil {
		t.Fatalf("ReadFromBucket %s: %v", tf.name, err)
	}
	defer reader.Close()

	// For large files, read in chunks and compare
	if tf.size > 100*1024*1024 {
		// Compare in 1MB chunks to avoid memory issues
		compareInChunks(t, reader, tf.data)
	} else {
		result, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll %s: %v", tf.name, err)
		}

		if !bytes.Equal(result, tf.data) {
			t.Fatalf("Data mismatch for %s: got %d bytes, want %d bytes", tf.name, len(result), len(tf.data))
		}
	}

	readDuration := time.Since(startTime)
	t.Logf("Read %s in %v (%.2f MB/s)", tf.name, readDuration,
		float64(tf.size)/readDuration.Seconds()/1024/1024)
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
	server := startTestHTTPServer(t, []testFile{{name: "resume.bin", size: totalSize, data: data}})
	defer server.Close()

	// Start Minio
	minioContainer, bucketURL := startMinioContainer(t, ctx, "resume-test-bucket")
	defer func() {
		if err := minioContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	bucket, err := blob.OpenBucket(ctx, bucketURL)
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
	minioContainer, bucketURL := startMinioContainer(t, ctx, "cancel-test-bucket")
	defer func() {
		if err := minioContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	bucket, err := blob.OpenBucket(ctx, bucketURL)
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

// compareInChunks compares reader output with expected data in chunks.
func compareInChunks(t *testing.T, reader io.Reader, expected []byte) {
	t.Helper()

	chunkSize := 1024 * 1024 // 1MB
	buf := make([]byte, chunkSize)
	offset := 0

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if offset+n > len(expected) {
				t.Fatalf("read more data than expected: offset=%d, n=%d, expected len=%d",
					offset, n, len(expected))
			}
			if !bytes.Equal(buf[:n], expected[offset:offset+n]) {
				t.Fatalf("data mismatch at offset %d", offset)
			}
			offset += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read error at offset %d: %v", offset, err)
		}
	}

	if offset != len(expected) {
		t.Fatalf("incomplete read: got %d bytes, want %d", offset, len(expected))
	}
}
