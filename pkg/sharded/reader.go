package sharded

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"

	"gocloud.dev/blob"
)

// Reader reads a sharded file, streaming all chunks in order.
type Reader struct {
	bucket   *blob.Bucket
	manifest *Manifest
	opts     Options

	currentChunk   int
	currentReader  io.ReadCloser
	checksumReader *checksumReader
	closed         bool
}

// Read opens a sharded file for reading.
// It returns an io.ReadCloser that streams all chunks in order.
func Read(ctx context.Context, bucketURL string, dest string, options ...Option) (*Reader, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("sharded: open bucket: %w", err)
	}

	return ReadFromBucket(ctx, bucket, dest, options...)
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

	return &Reader{
		bucket:   bucket,
		manifest: &manifest,
		opts:     opts,
	}, nil
}

// Read reads data from the sharded file.
func (r *Reader) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}

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

		chunk := r.manifest.Chunks[r.currentChunk]
		path := r.manifest.PartsPrefix + chunk.Object

		reader, err := r.bucket.NewReader(context.Background(), path, nil)
		if err != nil {
			return 0, fmt.Errorf("sharded: open chunk %d: %w", r.currentChunk, err)
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

// Close closes the reader and releases resources.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.currentReader != nil {
		r.currentReader.Close()
		r.currentReader = nil
	}

	return r.bucket.Close()
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
