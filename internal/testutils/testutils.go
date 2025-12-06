//go:build integration

// Package testutils provides shared test infrastructure for integration tests.
package testutils

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
)

// TestFile defines a test file with size and data.
type TestFile struct {
	Name string
	Size int64
	Data []byte
}

// GenerateTestData generates test data of the given size.
// For files <= 10MB, uses deterministic pattern. For larger files, uses random data.
func GenerateTestData(t *testing.T, size int64) []byte {
	t.Helper()
	data := make([]byte, size)
	if size <= 10*1024*1024 {
		for i := range data {
			data[i] = byte(i % 256)
		}
	} else {
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("generate random data: %v", err)
		}
	}
	return data
}

// StartTestHTTPServer starts an HTTP server that serves test files with range request support.
func StartTestHTTPServer(t *testing.T, files []TestFile) *httptest.Server {
	t.Helper()

	fileMap := make(map[string][]byte)
	for _, f := range files {
		fileMap["/"+f.Name] = f.Data
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

// MinioEnv contains connection information for a Minio test environment.
type MinioEnv struct {
	Container testcontainers.Container
	BucketURL string
	Endpoint  string
	AccessKey string
	SecretKey string
}

// Close terminates the Minio container.
func (e *MinioEnv) Close(ctx context.Context) error {
	if e.Container != nil {
		return e.Container.Terminate(ctx)
	}
	return nil
}

// OpenBucket opens a gocloud bucket connection to the Minio environment.
func (e *MinioEnv) OpenBucket(ctx context.Context) (*blob.Bucket, error) {
	return blob.OpenBucket(ctx, e.BucketURL)
}

// StartMinioContainer starts a Minio container with a pre-created bucket.
// Returns a MinioEnv with connection information.
func StartMinioContainer(t *testing.T, ctx context.Context, bucketName string) *MinioEnv {
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
	bucketURL := fmt.Sprintf("s3://%s?endpoint=http://%s&use_path_style=true&disable_https=true&region=us-east-1",
		bucketName,
		endpoint,
	)

	// Set AWS credentials via environment variables (gocloud reads these)
	t.Setenv("AWS_ACCESS_KEY_ID", accessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", secretKey)

	return &MinioEnv{
		Container: minioContainer,
		BucketURL: bucketURL,
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
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
}

// CompareReaderToData compares reader output with expected data in chunks.
// This is memory-efficient for large files.
func CompareReaderToData(t *testing.T, reader io.Reader, expected []byte) {
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
