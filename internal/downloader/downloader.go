package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"gocloud.dev/blob"

	slurphttp "github.com/ligustah/slurp/internal/http"
	"github.com/ligustah/slurp/internal/progress"
	"github.com/ligustah/slurp/pkg/sharded"
)

// Options configures the downloader.
type Options struct {
	// Workers is the number of parallel download workers.
	Workers int

	// ShardSize is the size of each download shard.
	ShardSize int64

	// StateInterval is how often to persist state (every N chunks).
	StateInterval int

	// Progress is an optional progress reporter.
	Progress *progress.Reporter

	// Force forces a restart, ignoring existing state.
	Force bool

	// HTTPOptions configures the HTTP client.
	HTTPOptions slurphttp.Options

	// MaxConsecutiveFailures is the number of consecutive shard failures
	// before the circuit breaker trips and stops the download.
	// Set to 0 to disable (default: 10).
	MaxConsecutiveFailures int

	// NoChecksum disables SHA256 checksum computation during writes.
	// This improves write performance when relying on object store integrity.
	NoChecksum bool

	// StateSaveInterval is how often to persist download state for resume.
	// Set to 0 to disable periodic saves (state still saved on completion).
	// Default: 1 minute.
	StateSaveInterval time.Duration
}

// FailedShard records information about a shard that failed to download.
type FailedShard struct {
	Index int   // Shard index
	Error error // The error that occurred
}

// CircuitBreakerError is returned when too many consecutive failures occur.
// It contains details about the failures that triggered the circuit breaker.
//
// This error is returned when:
//   - MaxConsecutiveFailures consecutive shard downloads fail
//   - The circuit breaker threshold is exceeded
//
// Use errors.As to extract this error and inspect FailedShards for details.
type CircuitBreakerError struct {
	ConsecutiveFailures int           // Number of consecutive failures
	FailedShards        []FailedShard // Details of failed shards
}

func (e *CircuitBreakerError) Error() string {
	return fmt.Sprintf("circuit breaker tripped: %d consecutive failures", e.ConsecutiveFailures)
}

// FileInfo contains metadata about the remote file.
type FileInfo struct {
	Size          int64
	ETag          string
	AcceptsRanges bool
}

// ErrRangeNotSupported is returned when the server doesn't support range requests.
var ErrRangeNotSupported = errors.New("downloader: server does not support range requests")

// GetFileInfo fetches metadata about a remote file.
func GetFileInfo(ctx context.Context, url string) (*FileInfo, error) {
	client := slurphttp.NewClient(slurphttp.DefaultOptions())
	info, err := client.Head(ctx, url)
	if err != nil {
		return nil, err
	}

	return &FileInfo{
		Size:          info.Size,
		ETag:          info.ETag,
		AcceptsRanges: info.AcceptsRanges,
	}, nil
}

// Download downloads a file from url to the bucket.
func Download(ctx context.Context, url string, bucket *blob.Bucket, dest string, opts Options) error {
	// Apply defaults
	if opts.Workers <= 0 {
		opts.Workers = 16
	}
	if opts.ShardSize <= 0 {
		opts.ShardSize = 256 * 1024 * 1024
	}
	if opts.StateInterval <= 0 {
		opts.StateInterval = 10
	}
	if opts.MaxConsecutiveFailures <= 0 {
		opts.MaxConsecutiveFailures = 10
	}
	if opts.StateSaveInterval == 0 {
		opts.StateSaveInterval = 1 * time.Minute
	}
	if opts.HTTPOptions.MaxIdleConnsPerHost == 0 {
		opts.HTTPOptions = slurphttp.DefaultOptions()
	}

	// Create HTTP client
	client := slurphttp.NewClient(opts.HTTPOptions)

	// Get file info
	info, err := client.Head(ctx, url)
	if err != nil {
		return fmt.Errorf("get file info: %w", err)
	}

	if !info.AcceptsRanges {
		return ErrRangeNotSupported
	}

	// Create sharded file
	f, err := sharded.Write(ctx, bucket, dest,
		sharded.WithShardSize(opts.ShardSize),
		sharded.WithSize(info.Size),
		sharded.WithMetadata(map[string]string{
			"source_url":  url,
			"source_etag": info.ETag,
		}),
		sharded.WithStateInterval(opts.StateInterval),
		sharded.WithChecksum(!opts.NoChecksum),
	)
	if err != nil {
		return fmt.Errorf("create sharded file: %w", err)
	}

	// Check for source change if resuming
	if storedETag := f.Metadata()["source_etag"]; storedETag != "" && storedETag != info.ETag {
		if opts.Force {
			if err := f.Reset(ctx); err != nil {
				return fmt.Errorf("reset state: %w", err)
			}
		} else {
			return fmt.Errorf("source file changed (etag mismatch: stored=%s, current=%s)", storedETag, info.ETag)
		}
	}

	// Force reset if requested
	if opts.Force && f.CompletedCount() > 0 {
		if err := f.Reset(ctx); err != nil {
			return fmt.Errorf("reset state: %w", err)
		}
	}

	// Set resume state on progress reporter if resuming
	if opts.Progress != nil && f.CompletedCount() > 0 {
		opts.Progress.SetResumeState(f.CompletedCount(), f.CompletedBytes())
	}

	// Circuit breaker state
	var (
		cbMu                  sync.Mutex
		consecutiveFailures   int
		failedShards          []FailedShard
		circuitBreakerTripped bool
	)

	// Create cancellable context for circuit breaker
	cbCtx, cbCancel := context.WithCancel(ctx)
	defer cbCancel()

	// Start background state saver goroutine
	stateSaverDone := make(chan struct{})
	stateSaverStop := make(chan struct{})
	go func() {
		defer close(stateSaverDone)
		ticker := time.NewTicker(opts.StateSaveInterval)
		defer ticker.Stop()

		lastSaved := 0
		for {
			select {
			case <-ticker.C:
				current := f.CompletedCount()
				if current > lastSaved {
					if err := f.SaveState(ctx); err == nil {
						lastSaved = current
					}
					// Ignore errors - eventual consistency
				}
			case <-stateSaverStop:
				// Final save after all workers are done - use background context
				// since the parent context may be cancelled
				if err := f.SaveState(context.Background()); err != nil {
					slog.Warn("failed to save final state", "error", err)
				}
				return
			}
		}
	}()

	// Create worker pool
	type shardJob struct {
		shard *sharded.Shard
	}

	jobs := make(chan shardJob, opts.Workers)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := downloadShard(cbCtx, client, url, job.shard, opts.Progress)

				cbMu.Lock()
				if err != nil {
					// Only log and track failures if not shutting down
					if ctx.Err() == nil {
						slog.Warn("shard failed", "shard", job.shard.Index(), "error", err)
						consecutiveFailures++
						failedShards = append(failedShards, FailedShard{
							Index: job.shard.Index(),
							Error: err,
						})

						if consecutiveFailures >= opts.MaxConsecutiveFailures {
							circuitBreakerTripped = true
							cbCancel() // Stop all workers
						}
					}
				} else {
					consecutiveFailures = 0 // Reset on success
				}
				tripped := circuitBreakerTripped
				cbMu.Unlock()

				if tripped {
					return
				}
			}
		}()
	}

	// Feed jobs to workers
	var jobFeederErr error
	go func() {
		defer close(jobs)
		for {
			shard, err := f.Next(cbCtx)
			if err == io.EOF {
				return
			}
			if err == sharded.ErrShardFilled {
				continue // Skip already-completed shards
			}
			if err != nil {
				// Context cancellation is expected during shutdown
				if cbCtx.Err() == nil {
					jobFeederErr = err
					slog.Error("job feeder error", "error", err)
				}
				return
			}

			select {
			case jobs <- shardJob{shard: shard}:
			case <-cbCtx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Signal state saver to do final save and exit (after workers have called Abort())
	close(stateSaverStop)
	<-stateSaverDone

	// Check circuit breaker
	cbMu.Lock()
	if circuitBreakerTripped {
		cbErr := &CircuitBreakerError{
			ConsecutiveFailures: consecutiveFailures,
			FailedShards:        failedShards,
		}
		cbMu.Unlock()
		return cbErr
	}
	cbMu.Unlock()

	// Check context
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Check job feeder error
	if jobFeederErr != nil {
		return fmt.Errorf("job feeder: %w", jobFeederErr)
	}

	// Complete the sharded file
	if err := f.Complete(ctx); err != nil {
		return fmt.Errorf("complete sharded file: %w", err)
	}

	return nil
}

// downloadShard downloads a single shard with resume support.
// If the connection drops mid-download, it will retry from where it left off.
func downloadShard(ctx context.Context, client *slurphttp.Client, url string, shard *sharded.Shard, reporter *progress.Reporter) error {
	// Early exit if context already cancelled (shutdown in progress)
	if ctx.Err() != nil {
		shard.Abort()
		return ctx.Err()
	}

	if reporter != nil {
		reporter.ShardStarted()
	}

	// Ensure shard is closed/aborted on any exit path
	var success bool
	defer func() {
		if !success {
			shard.Abort() // Clean up writer without updating state
		}
	}()

	shardStart := shard.Offset()
	shardEnd := shardStart + shard.Length() - 1
	expectedSize := shard.Length()

	downloadStart := time.Now()
	slog.Info("shard download started", "shard", shard.Index(), "offset", shardStart, "size", expectedSize)

	var totalWritten int64

	// Download with resume support - retry from where we left off on clean disconnects.
	// We only retry if the server closes cleanly without error; errors (like timeouts) fail immediately.
	const maxRetries = 10 // Safety limit for retries within a shard
	retries := 0

	for totalWritten < expectedSize {
		currentStart := shardStart + totalWritten
		resp, err := client.GetRange(ctx, url, currentStart, shardEnd)
		if err != nil {
			if reporter != nil {
				reporter.ShardFailed()
			}
			return fmt.Errorf("download shard %d (offset %d): %w", shard.Index(), totalWritten, err)
		}

		// Verify Content-Length matches what we expect
		expectedRemaining := expectedSize - totalWritten
		if resp.ContentLength > 0 && resp.ContentLength != expectedRemaining {
			resp.Body.Close()
			if reporter != nil {
				reporter.ShardFailed()
			}
			return fmt.Errorf("shard %d: server returned Content-Length %d, expected %d",
				shard.Index(), resp.ContentLength, expectedRemaining)
		}

		// Wrap shard in progress writer for real-time byte tracking
		var dst io.Writer = shard
		if reporter != nil {
			dst = &progressWriter{w: shard, reporter: reporter}
		}

		n, err := io.Copy(dst, resp.Body)
		resp.Body.Close()

		totalWritten += n

		if err != nil {
			// Any error during copy (timeout, network error, etc) should fail immediately
			// We don't retry on errors - only on clean disconnects (EOF with partial data)
			if reporter != nil {
				reporter.ShardFailed()
			}
			return fmt.Errorf("write shard %d: %w", shard.Index(), err)
		}

		// If we didn't get all the data but no error, the server closed the connection cleanly
		if totalWritten < expectedSize {
			retries++
			if retries >= maxRetries {
				if reporter != nil {
					reporter.ShardFailed()
				}
				return fmt.Errorf("shard %d: too many retries (%d), only received %d of %d bytes",
					shard.Index(), retries, totalWritten, expectedSize)
			}
			// Continue to retry from new offset
			continue
		}
	}

	// Verify we received all expected data
	if totalWritten != expectedSize {
		if reporter != nil {
			reporter.ShardFailed()
		}
		return fmt.Errorf("shard %d: received %d bytes, expected %d", shard.Index(), totalWritten, expectedSize)
	}

	slog.Info("shard download complete", "shard", shard.Index(), "size", totalWritten, "download_time", time.Since(downloadStart))

	// Close the shard - this triggers the GCS upload
	closeStart := time.Now()
	if err := shard.Close(); err != nil {
		if reporter != nil {
			reporter.ShardFailed()
		}
		return fmt.Errorf("close shard %d: %w", shard.Index(), err)
	}
	slog.Info("shard uploaded", "shard", shard.Index(), "size", totalWritten, "upload_time", time.Since(closeStart))

	success = true // Mark success so defer doesn't abort
	if reporter != nil {
		reporter.ShardCompleted()
	}

	return nil
}

// progressWriter wraps a writer and reports bytes written to a progress reporter.
type progressWriter struct {
	w        io.Writer
	reporter *progress.Reporter
}

func (pw *progressWriter) Write(p []byte) (n int, err error) {
	n, err = pw.w.Write(p)
	if n > 0 && pw.reporter != nil {
		pw.reporter.BytesWritten(int64(n))
	}
	return n, err
}
