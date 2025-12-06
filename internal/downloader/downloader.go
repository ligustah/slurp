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

	// ChunkSize is the size of each download chunk.
	ChunkSize int64

	// StateInterval is how often to persist state (every N chunks).
	StateInterval int

	// Progress is an optional progress reporter.
	Progress *progress.Reporter

	// Force forces a restart, ignoring existing state.
	Force bool

	// HTTPOptions configures the HTTP client.
	HTTPOptions slurphttp.Options

	// MaxConsecutiveFailures is the number of consecutive chunk failures
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

// FailedChunk records information about a chunk that failed to download.
type FailedChunk struct {
	Index int   // Chunk index
	Error error // The error that occurred
}

// CircuitBreakerError is returned when too many consecutive failures occur.
// It contains details about the failures that triggered the circuit breaker.
//
// This error is returned when:
//   - MaxConsecutiveFailures consecutive chunk downloads fail
//   - The circuit breaker threshold is exceeded
//
// Use errors.As to extract this error and inspect FailedChunks for details.
type CircuitBreakerError struct {
	ConsecutiveFailures int           // Number of consecutive failures
	FailedChunks        []FailedChunk // Details of failed chunks
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
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 256 * 1024 * 1024
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
		sharded.WithChunkSize(opts.ChunkSize),
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

	// Circuit breaker state
	var (
		cbMu                  sync.Mutex
		consecutiveFailures   int
		failedChunks          []FailedChunk
		circuitBreakerTripped bool
	)

	// Create cancellable context for circuit breaker
	cbCtx, cbCancel := context.WithCancel(ctx)
	defer cbCancel()

	// Start background state saver goroutine
	stateSaverDone := make(chan struct{})
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
			case <-cbCtx.Done():
				// Final save before exit
				if err := f.SaveState(ctx); err != nil {
					slog.Warn("failed to save final state", "error", err)
				}
				return
			}
		}
	}()

	// Create worker pool
	type chunkJob struct {
		chunk *sharded.Chunk
	}

	jobs := make(chan chunkJob, opts.Workers)
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				err := downloadChunk(cbCtx, client, url, job.chunk, opts.Progress)

				cbMu.Lock()
				if err != nil {
					slog.Warn("chunk failed", "chunk", job.chunk.Index(), "error", err)
					consecutiveFailures++
					failedChunks = append(failedChunks, FailedChunk{
						Index: job.chunk.Index(),
						Error: err,
					})

					if consecutiveFailures >= opts.MaxConsecutiveFailures {
						circuitBreakerTripped = true
						cbCancel() // Stop all workers
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
			chunk, err := f.Next(cbCtx)
			if err == io.EOF {
				return
			}
			if err == sharded.ErrChunkFilled {
				continue // Skip already-completed chunks
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
			case jobs <- chunkJob{chunk: chunk}:
			case <-cbCtx.Done():
				return
			}
		}
	}()

	// Wait for completion
	wg.Wait()

	// Signal state saver to do final save and exit
	cbCancel()
	<-stateSaverDone

	// Check circuit breaker
	cbMu.Lock()
	if circuitBreakerTripped {
		cbErr := &CircuitBreakerError{
			ConsecutiveFailures: consecutiveFailures,
			FailedChunks:        failedChunks,
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

// downloadChunk downloads a single chunk with resume support.
// If the connection drops mid-download, it will retry from where it left off.
func downloadChunk(ctx context.Context, client *slurphttp.Client, url string, chunk *sharded.Chunk, reporter *progress.Reporter) error {
	if reporter != nil {
		reporter.ChunkStarted()
	}

	// Ensure chunk is closed/aborted on any exit path
	var success bool
	defer func() {
		if !success {
			chunk.Abort() // Clean up writer without updating state
		}
	}()

	chunkStart := chunk.Offset()
	chunkEnd := chunkStart + chunk.Length() - 1
	expectedSize := chunk.Length()

	downloadStart := time.Now()
	slog.Info("chunk download started", "chunk", chunk.Index(), "offset", chunkStart, "size", expectedSize)

	var totalWritten int64

	// Download with resume support - retry from where we left off on clean disconnects.
	// We only retry if the server closes cleanly without error; errors (like timeouts) fail immediately.
	const maxRetries = 10 // Safety limit for retries within a chunk
	retries := 0

	for totalWritten < expectedSize {
		currentStart := chunkStart + totalWritten
		resp, err := client.GetRange(ctx, url, currentStart, chunkEnd)
		if err != nil {
			if reporter != nil {
				reporter.ChunkFailed()
			}
			return fmt.Errorf("download chunk %d (offset %d): %w", chunk.Index(), totalWritten, err)
		}

		// Verify Content-Length matches what we expect
		expectedRemaining := expectedSize - totalWritten
		if resp.ContentLength > 0 && resp.ContentLength != expectedRemaining {
			resp.Body.Close()
			if reporter != nil {
				reporter.ChunkFailed()
			}
			return fmt.Errorf("chunk %d: server returned Content-Length %d, expected %d",
				chunk.Index(), resp.ContentLength, expectedRemaining)
		}

		// Wrap chunk in progress writer for real-time byte tracking
		var dst io.Writer = chunk
		if reporter != nil {
			dst = &progressWriter{w: chunk, reporter: reporter}
		}

		n, err := io.Copy(dst, resp.Body)
		resp.Body.Close()

		totalWritten += n

		if err != nil {
			// Any error during copy (timeout, network error, etc) should fail immediately
			// We don't retry on errors - only on clean disconnects (EOF with partial data)
			if reporter != nil {
				reporter.ChunkFailed()
			}
			return fmt.Errorf("write chunk %d: %w", chunk.Index(), err)
		}

		// If we didn't get all the data but no error, the server closed the connection cleanly
		if totalWritten < expectedSize {
			retries++
			if retries >= maxRetries {
				if reporter != nil {
					reporter.ChunkFailed()
				}
				return fmt.Errorf("chunk %d: too many retries (%d), only received %d of %d bytes",
					chunk.Index(), retries, totalWritten, expectedSize)
			}
			// Continue to retry from new offset
			continue
		}
	}

	// Verify we received all expected data
	if totalWritten != expectedSize {
		if reporter != nil {
			reporter.ChunkFailed()
		}
		return fmt.Errorf("chunk %d: received %d bytes, expected %d", chunk.Index(), totalWritten, expectedSize)
	}

	slog.Info("chunk download complete", "chunk", chunk.Index(), "size", totalWritten, "download_time", time.Since(downloadStart))

	// Close the chunk - this triggers the GCS upload
	closeStart := time.Now()
	if err := chunk.Close(); err != nil {
		if reporter != nil {
			reporter.ChunkFailed()
		}
		return fmt.Errorf("close chunk %d: %w", chunk.Index(), err)
	}
	slog.Info("chunk uploaded", "chunk", chunk.Index(), "size", totalWritten, "upload_time", time.Since(closeStart))

	success = true // Mark success so defer doesn't abort
	if reporter != nil {
		reporter.ChunkCompleted()
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
