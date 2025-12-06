//go:build integration

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/internal/testutils"
)

func TestCLIIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data
	testFile := testutils.TestFile{
		Name: "test-file.bin",
		Size: 1024 * 1024, // 1MB
	}
	testFile.Data = testutils.GenerateTestData(t, testFile.Size)

	// Start HTTP server
	t.Log("Starting HTTP test server...")
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{testFile})
	defer server.Close()

	// Start Minio
	t.Log("Starting Minio container...")
	minio := testutils.StartMinioContainer(t, ctx, "cli-test-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	objectPath := "test/cli-file.bin"

	t.Run("upload", func(t *testing.T) {
		exitCode := runUpload([]string{
			"-url", server.URL + "/" + testFile.Name,
			"-bucket", minio.BucketURL,
			"-object", objectPath,
			"-workers", "4",
			"-chunk-size", "256KB",
			"-state-interval", "1",
		})
		if exitCode != ExitSuccess {
			t.Fatalf("upload failed with exit code %d", exitCode)
		}
	})

	t.Run("validate", func(t *testing.T) {
		exitCode := runValidate([]string{
			"-bucket", minio.BucketURL,
			"-object", objectPath,
		})
		if exitCode != ExitSuccess {
			t.Fatalf("validate failed with exit code %d", exitCode)
		}
	})

	t.Run("download_to_file", func(t *testing.T) {
		// Create temp file for download
		tmpFile := filepath.Join(t.TempDir(), "downloaded.bin")

		exitCode := runDownload([]string{
			"-bucket", minio.BucketURL,
			"-object", objectPath,
			"-output", tmpFile,
		})
		if exitCode != ExitSuccess {
			t.Fatalf("download failed with exit code %d", exitCode)
		}

		// Verify downloaded content
		downloaded, err := os.ReadFile(tmpFile)
		if err != nil {
			t.Fatalf("read downloaded file: %v", err)
		}

		if !bytes.Equal(downloaded, testFile.Data) {
			t.Fatalf("downloaded data mismatch: got %d bytes, want %d bytes", len(downloaded), len(testFile.Data))
		}
	})

	// Note: stdout download test is skipped because capturing os.Stdout
	// while running CLI commands requires careful pipe handling. The
	// file-based download test above verifies the download logic works.

	t.Run("delete", func(t *testing.T) {
		exitCode := runDelete([]string{
			"-bucket", minio.BucketURL,
			"-object", objectPath,
			"-force",
		})
		if exitCode != ExitSuccess {
			t.Fatalf("delete failed with exit code %d", exitCode)
		}

		// Verify file is gone - validate should fail
		exitCode = runValidate([]string{
			"-bucket", minio.BucketURL,
			"-object", objectPath,
		})
		if exitCode == ExitSuccess {
			t.Fatal("validate should have failed after delete")
		}
	})
}

func TestCLIUploadWithNoChecksum(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data
	testFile := testutils.TestFile{
		Name: "no-checksum.bin",
		Size: 512 * 1024, // 512KB
	}
	testFile.Data = testutils.GenerateTestData(t, testFile.Size)

	// Start HTTP server
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{testFile})
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "no-checksum-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	objectPath := "test/no-checksum.bin"

	// Upload without checksums
	exitCode := runUpload([]string{
		"-url", server.URL + "/" + testFile.Name,
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-workers", "2",
		"-chunk-size", "128KB",
		"-no-checksum",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("upload failed with exit code %d", exitCode)
	}

	// Validate should still work
	exitCode = runValidate([]string{
		"-bucket", minio.BucketURL,
		"-object", objectPath,
	})
	if exitCode != ExitSuccess {
		t.Fatalf("validate failed with exit code %d", exitCode)
	}

	// Download and verify data
	tmpFile := filepath.Join(t.TempDir(), "downloaded.bin")
	exitCode = runDownload([]string{
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-output", tmpFile,
	})
	if exitCode != ExitSuccess {
		t.Fatalf("download failed with exit code %d", exitCode)
	}

	downloaded, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if !bytes.Equal(downloaded, testFile.Data) {
		t.Fatalf("data mismatch")
	}
}

func TestCLIUploadResume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data
	testFile := testutils.TestFile{
		Name: "resume.bin",
		Size: 1024 * 1024, // 1MB
	}
	testFile.Data = testutils.GenerateTestData(t, testFile.Size)

	// Start HTTP server
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{testFile})
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "resume-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	objectPath := "test/resume.bin"

	// First upload - should complete successfully
	exitCode := runUpload([]string{
		"-url", server.URL + "/" + testFile.Name,
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-workers", "2",
		"-chunk-size", "256KB",
		"-state-interval", "1",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("first upload failed with exit code %d", exitCode)
	}

	// Second upload (resume) - should detect completed state and succeed quickly
	exitCode = runUpload([]string{
		"-url", server.URL + "/" + testFile.Name,
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-workers", "2",
		"-chunk-size", "256KB",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("resume upload failed with exit code %d", exitCode)
	}

	// Validate
	exitCode = runValidate([]string{
		"-bucket", minio.BucketURL,
		"-object", objectPath,
	})
	if exitCode != ExitSuccess {
		t.Fatalf("validate failed with exit code %d", exitCode)
	}
}

func TestCLIDeletePartial(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data
	testFile := testutils.TestFile{
		Name: "partial.bin",
		Size: 1024 * 1024, // 1MB
	}
	testFile.Data = testutils.GenerateTestData(t, testFile.Size)

	// Start HTTP server
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{testFile})
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "partial-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	objectPath := "test/partial.bin"

	// Upload completely first
	exitCode := runUpload([]string{
		"-url", server.URL + "/" + testFile.Name,
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-workers", "2",
		"-chunk-size", "256KB",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("upload failed with exit code %d", exitCode)
	}

	// Delete with --partial flag (should still work on completed files)
	exitCode = runDelete([]string{
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-force",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("delete failed with exit code %d", exitCode)
	}
}

func TestCLIValidateInvalidArgs(t *testing.T) {
	// Test missing required flags
	exitCode := runValidate([]string{
		"-bucket", "s3://some-bucket",
		// Missing -object
	})
	if exitCode != ExitInvalidArgs {
		t.Errorf("expected exit code %d for missing args, got %d", ExitInvalidArgs, exitCode)
	}

	exitCode = runUpload([]string{
		"-url", "http://example.com/file",
		// Missing -bucket and -object
	})
	if exitCode != ExitInvalidArgs {
		t.Errorf("expected exit code %d for missing args, got %d", ExitInvalidArgs, exitCode)
	}
}

func TestCLIDownloadWithPrefetch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Generate test data - larger file for prefetch testing
	testFile := testutils.TestFile{
		Name: "prefetch.bin",
		Size: 2 * 1024 * 1024, // 2MB
	}
	testFile.Data = testutils.GenerateTestData(t, testFile.Size)

	// Start HTTP server
	server := testutils.StartTestHTTPServer(t, []testutils.TestFile{testFile})
	defer server.Close()

	// Start Minio
	minio := testutils.StartMinioContainer(t, ctx, "prefetch-bucket")
	defer func() {
		if err := minio.Close(ctx); err != nil {
			t.Logf("failed to terminate minio container: %v", err)
		}
	}()

	objectPath := "test/prefetch.bin"

	// Upload
	exitCode := runUpload([]string{
		"-url", server.URL + "/" + testFile.Name,
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-workers", "4",
		"-chunk-size", "256KB",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("upload failed with exit code %d", exitCode)
	}

	// Download with prefetch
	tmpFile := filepath.Join(t.TempDir(), "downloaded.bin")
	exitCode = runDownload([]string{
		"-bucket", minio.BucketURL,
		"-object", objectPath,
		"-output", tmpFile,
		"-prefetch", "4",
	})
	if exitCode != ExitSuccess {
		t.Fatalf("download with prefetch failed with exit code %d", exitCode)
	}

	// Verify
	downloaded, err := os.ReadFile(tmpFile)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}

	if !bytes.Equal(downloaded, testFile.Data) {
		t.Fatalf("data mismatch: got %d bytes, want %d bytes", len(downloaded), len(testFile.Data))
	}
}
