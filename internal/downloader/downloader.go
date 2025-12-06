package downloader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

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

	// Complete the sharded file
	if err := f.Complete(ctx); err != nil {
		return fmt.Errorf("complete sharded file: %w", err)
	}

	return nil
}

// downloadChunk downloads a single chunk.
func downloadChunk(ctx context.Context, client *slurphttp.Client, url string, chunk *sharded.Chunk, reporter *progress.Reporter) error {
	if reporter != nil {
		reporter.ChunkStarted()
	}

	startByte := chunk.Offset()
	endByte := startByte + chunk.Length() - 1

	resp, err := client.GetRange(ctx, url, startByte, endByte)
	if err != nil {
		if reporter != nil {
			reporter.ChunkFailed()
		}
		return fmt.Errorf("download chunk %d: %w", chunk.Index(), err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(chunk, resp.Body)
	if err != nil {
		if reporter != nil {
			reporter.ChunkFailed()
		}
		return fmt.Errorf("write chunk %d: %w", chunk.Index(), err)
	}

	if err := chunk.Close(); err != nil {
		if reporter != nil {
			reporter.ChunkFailed()
		}
		return fmt.Errorf("close chunk %d: %w", chunk.Index(), err)
	}

	if reporter != nil {
		reporter.ChunkCompleted(n)
	}

	return nil
}
