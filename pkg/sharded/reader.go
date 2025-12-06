package sharded

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"gocloud.dev/blob"
)

// chunkResult holds the result of fetching a chunk.
type chunkResult struct {
	reader io.ReadCloser
	err    error
}

// Reader reads a sharded file, streaming all chunks in order.
type Reader struct {
	bucket    *blob.Bucket
	ownBucket bool // true if we opened the bucket and should close it
	manifest  *Manifest
	opts      Options

	currentChunk   int
	currentReader  io.ReadCloser
	checksumReader *checksumReader
	closed         bool

	// Prefetch support: worker pool with per-chunk result channels
	prefetchCount int
	prefetchCtx   context.Context
	prefetchStop  context.CancelFunc
	prefetchOnce  sync.Once
	prefetchWg    sync.WaitGroup
	workCh        chan int           // chunk indices to fetch
	results       []chan chunkResult // result channel per chunk
}

// Read opens a sharded file for reading.
// It returns an io.ReadCloser that streams all chunks in order.
func Read(ctx context.Context, bucketURL string, dest string, options ...Option) (*Reader, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("sharded: open bucket: %w", err)
	}

	reader, err := ReadFromBucket(ctx, bucket, dest, options...)
	if err != nil {
		bucket.Close()
		return nil, err
	}
	reader.ownBucket = true
	return reader, nil
}

// ReadFromBucket opens a sharded file from an existing bucket handle.
func ReadFromBucket(ctx context.Context, bucket *blob.Bucket, dest string, options ...Option) (*Reader, error) {
	opts := Options{}
	for _, opt := range options {
		opt(&opts)
	}

	// Load manifest
	manifestPath := dest + ".manifest.json"
	data, err := bucket.ReadAll(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("sharded: read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("sharded: unmarshal manifest: %w", err)
	}

	r := &Reader{
		bucket:        bucket,
		manifest:      &manifest,
		opts:          opts,
		prefetchCount: opts.PrefetchCount,
	}

	return r, nil
}

// startPrefetch initializes the prefetch worker pool.
func (r *Reader) startPrefetch() {
	if r.prefetchCount <= 0 {
		return
	}

	r.prefetchCtx, r.prefetchStop = context.WithCancel(context.Background())
	r.workCh = make(chan int, r.prefetchCount)

	// Create a result channel for each chunk
	r.results = make([]chan chunkResult, len(r.manifest.Chunks))
	for i := range r.results {
		r.results[i] = make(chan chunkResult, 1)
	}

	// Start worker pool
	for i := 0; i < r.prefetchCount; i++ {
		r.prefetchWg.Add(1)
		go r.prefetchWorker()
	}

	// Queue all chunks for prefetching
	go r.queueChunks()
}

// prefetchWorker fetches chunks from the work channel.
func (r *Reader) prefetchWorker() {
	defer r.prefetchWg.Done()

	for {
		select {
		case <-r.prefetchCtx.Done():
			return
		case idx, ok := <-r.workCh:
			if !ok {
				return
			}
			reader, err := r.openChunkWithCtx(r.prefetchCtx, idx)
			select {
			case r.results[idx] <- chunkResult{reader: reader, err: err}:
			case <-r.prefetchCtx.Done():
				if reader != nil {
					reader.Close()
				}
				return
			}
		}
	}
}

// queueChunks sends all chunk indices to the work channel.
func (r *Reader) queueChunks() {
	defer close(r.workCh)
	for i := 0; i < len(r.manifest.Chunks); i++ {
		select {
		case <-r.prefetchCtx.Done():
			return
		case r.workCh <- i:
		}
	}
}

// Read reads data from the sharded file.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	// Start prefetch on first read
	r.prefetchOnce.Do(r.startPrefetch)

	for {
		// If we have a current reader, try to read from it
		if r.currentReader != nil {
			n, err = r.currentReader.Read(p)
			if err == io.EOF {
				// Verify checksum if enabled
				if r.opts.VerifyChecksum && r.checksumReader != nil {
					expected := r.manifest.Chunks[r.currentChunk-1].Checksum
					actual := r.checksumReader.Sum()
					if expected != actual {
						return 0, fmt.Errorf("sharded: checksum mismatch for chunk %d: expected %s, got %s",
							r.currentChunk-1, expected, actual)
					}
				}

				// Close current reader and try next chunk
				r.currentReader.Close()
				r.currentReader = nil
				r.checksumReader = nil

				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}

		// Open next chunk
		if r.currentChunk >= len(r.manifest.Chunks) {
			return 0, io.EOF
		}

		var reader io.ReadCloser

		// Get chunk - either from prefetch result channel or synchronously
		if r.results != nil {
			result := <-r.results[r.currentChunk]
			if result.err != nil {
				return 0, fmt.Errorf("sharded: open chunk %d: %w", r.currentChunk, result.err)
			}
			reader = result.reader
		} else {
			// No prefetching, open synchronously
			reader, err = r.openChunk(r.currentChunk)
			if err != nil {
				return 0, err
			}
		}

		r.currentReader = reader
		r.currentChunk++

		if r.opts.VerifyChecksum {
			r.checksumReader = &checksumReader{
				reader: reader,
				hash:   sha256.New(),
			}
			r.currentReader = r.checksumReader
		}
	}
}

// openChunk opens a chunk synchronously.
func (r *Reader) openChunk(idx int) (io.ReadCloser, error) {
	return r.openChunkWithCtx(context.Background(), idx)
}

// openChunkWithCtx opens a chunk with a context for cancellation.
func (r *Reader) openChunkWithCtx(ctx context.Context, idx int) (io.ReadCloser, error) {
	chunk := r.manifest.Chunks[idx]
	path := r.manifest.PartsPrefix + chunk.Object

	reader, err := r.bucket.NewReader(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("sharded: open chunk %d: %w", idx, err)
	}
	return reader, nil
}

// Close closes the reader and releases resources.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Stop prefetch workers
	if r.prefetchStop != nil {
		r.prefetchStop()
		r.prefetchWg.Wait()

		// Drain and close any prefetched readers that weren't consumed
		for i := r.currentChunk; i < len(r.results); i++ {
			select {
			case result := <-r.results[i]:
				if result.reader != nil {
					result.reader.Close()
				}
			default:
				// No result yet, worker was cancelled
			}
		}
	}

	if r.currentReader != nil {
		r.currentReader.Close()
		r.currentReader = nil
	}

	// Only close the bucket if we opened it
	if r.ownBucket {
		return r.bucket.Close()
	}
	return nil
}

// Manifest returns the manifest for the sharded file.
func (r *Reader) Manifest() *Manifest {
	return r.manifest
}

// checksumReader wraps a reader and computes checksum as data is read.
type checksumReader struct {
	reader io.Reader
	hash   io.Writer
}

func (c *checksumReader) Read(p []byte) (n int, err error) {
	n, err = c.reader.Read(p)
	if n > 0 {
		c.hash.Write(p[:n])
	}
	return n, err
}

func (c *checksumReader) Close() error {
	if closer, ok := c.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (c *checksumReader) Sum() string {
	if h, ok := c.hash.(interface{ Sum([]byte) []byte }); ok {
		return hex.EncodeToString(h.Sum(nil))
	}
	return ""
}
