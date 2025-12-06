package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/internal/downloader"
	slurphttp "github.com/ligustah/slurp/internal/http"
	"github.com/ligustah/slurp/internal/progress"
)

// runUpload fetches a file from an HTTP URL and stores it as a sharded file
// in object storage. Supports parallel downloads with resume capability.
func runUpload(args []string) int {
	fs := flag.NewFlagSet("upload", flag.ExitOnError)

	url := fs.String("url", "", "Source URL to download (required)")
	bucket := fs.String("bucket", "", "Destination bucket URL (required)")
	object := fs.String("object", "", "Destination object path (required)")
	workers := fs.Int("workers", 16, "Number of parallel workers")
	chunkSize := fs.String("chunk-size", "256MB", "Size of each chunk")
	showProgress := fs.Bool("progress", false, "Show progress output")
	retryAttempts := fs.Int("retry-attempts", 5, "Max retry attempts per chunk")
	retryBackoff := fs.Duration("retry-backoff", 1*time.Second, "Initial retry backoff")
	retryMaxBackoff := fs.Duration("retry-max-backoff", 30*time.Second, "Max retry backoff")
	stateInterval := fs.Int("state-interval", 10, "Persist state every N chunks")
	force := fs.Bool("force", false, "Force restart, ignoring existing state")
	noChecksum := fs.Bool("no-checksum", false, "Skip checksum computation (relies on object store integrity)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp upload [options]

Fetch a file from an HTTP URL and store it as a sharded file in object storage.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return ExitInvalidArgs
	}

	// Validate required flags
	if *url == "" || *bucket == "" || *object == "" {
		fmt.Fprintln(os.Stderr, "Error: -url, -bucket, and -object are required")
		fs.Usage()
		return ExitInvalidArgs
	}

	// Parse chunk size
	chunkBytes, err := progress.ParseBytes(*chunkSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid chunk size: %v\n", err)
		return ExitInvalidArgs
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n[slurp] Received interrupt, shutting down...")
		cancel()
	}()

	// Open bucket
	bkt, err := blob.OpenBucket(ctx, *bucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bucket: %v\n", err)
		return ExitStorageError
	}
	defer bkt.Close()

	// Get file info
	info, err := downloader.GetFileInfo(ctx, *url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error accessing source URL: %v\n", err)
		return ExitSourceNotAccess
	}

	if !info.AcceptsRanges {
		fmt.Fprintln(os.Stderr, "Error: Server does not support range requests")
		return ExitRangeNotSupported
	}

	totalChunks := int((info.Size + chunkBytes - 1) / chunkBytes)

	// Setup progress reporter
	var reporter *progress.Reporter
	if *showProgress {
		reporter = progress.NewReporter(progress.Options{
			TotalSize:      info.Size,
			TotalChunks:    totalChunks,
			Workers:        *workers,
			UpdateInterval: 5 * time.Second,
			SourceURL:      *url,
			ChunkSize:      chunkBytes,
		})
		reporter.Start()
		defer reporter.Stop()
	}

	// Download
	err = downloader.Download(ctx, *url, bkt, *object, downloader.Options{
		Workers:       *workers,
		ChunkSize:     chunkBytes,
		StateInterval: *stateInterval,
		Progress:      reporter,
		Force:         *force,
		NoChecksum:    *noChecksum,
		HTTPOptions: slurphttp.Options{
			MaxIdleConnsPerHost: *workers * 2,
			Timeout:             30 * time.Second,
			RetryAttempts:       *retryAttempts,
			RetryBackoff:        *retryBackoff,
			RetryMaxBackoff:     *retryMaxBackoff,
		},
	})

	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "[slurp] Upload interrupted, state saved for resume")
			return ExitGeneralError
		}
		if err == downloader.ErrRangeNotSupported {
			fmt.Fprintln(os.Stderr, "Error: Server does not support range requests")
			return ExitRangeNotSupported
		}
		if contains(err.Error(), "etag mismatch") {
			fmt.Fprintln(os.Stderr, "Error: Source file has changed since last upload attempt")
			fmt.Fprintln(os.Stderr, "Use --force to restart from scratch")
			return ExitSourceChanged
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitGeneralError
	}

	fmt.Fprintf(os.Stderr, "[slurp] Upload complete: %s/%s\n", *bucket, *object)
	fmt.Fprintf(os.Stderr, "[slurp] Manifest: %s/%s.manifest.json\n", *bucket, *object)

	return ExitSuccess
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
