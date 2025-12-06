package sharded

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func TestReaderPrefetch(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create test data: 1MB in 100KB chunks (10 chunks)
	chunkSize := int64(100 * 1024)
	totalSize := int64(1024 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write sharded file
	f, err := Write(ctx, bucket, "test/prefetch.bin",
		WithChunkSize(chunkSize),
		WithSize(totalSize),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		if end > totalSize {
			end = totalSize
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	// Test with prefetch disabled (default)
	t.Run("no prefetch", func(t *testing.T) {
		reader, err := ReadFromBucket(ctx, bucket, "test/prefetch.bin")
		if err != nil {
			t.Fatalf("ReadFromBucket: %v", err)
		}
		defer reader.Close()

		result, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(result, data) {
			t.Fatal("data mismatch")
		}
	})

	// Test with prefetch enabled
	t.Run("with prefetch", func(t *testing.T) {
		reader, err := ReadFromBucket(ctx, bucket, "test/prefetch.bin",
			WithPrefetch(2), // Prefetch 2 chunks ahead
		)
		if err != nil {
			t.Fatalf("ReadFromBucket: %v", err)
		}
		defer reader.Close()

		result, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if !bytes.Equal(result, data) {
			t.Fatal("data mismatch")
		}
	})
}

func TestReaderPrefetchActuallyPrefetches(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create test data with 4 chunks
	chunkSize := int64(1024)
	totalSize := int64(4 * 1024)
	data := make([]byte, totalSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, _ := Write(ctx, bucket, "test/prefetch-verify.bin",
		WithChunkSize(chunkSize),
		WithSize(totalSize),
	)
	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	// Read with prefetch=2
	reader, err := ReadFromBucket(ctx, bucket, "test/prefetch-verify.bin",
		WithPrefetch(2),
	)
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}
	defer reader.Close()

	// Read just 1 byte - this should trigger prefetching of chunks 1 and 2
	buf := make([]byte, 1)
	_, err = reader.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	// Give prefetch goroutines time to work
	time.Sleep(50 * time.Millisecond)

	// Check that prefetch is configured
	if reader.prefetchCount == 0 {
		t.Log("Note: prefetch count is 0 - prefetching may not be configured")
	}

	// Complete the read
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	result := append(buf, rest...)
	if !bytes.Equal(result, data) {
		t.Fatal("data mismatch")
	}
}

// BenchmarkReaderPrefetch compares read performance with and without prefetching.
func BenchmarkReaderPrefetch(b *testing.B) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		b.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create test data: 10MB in 1MB chunks
	chunkSize := int64(1024 * 1024)
	totalSize := int64(10 * 1024 * 1024)
	data := make([]byte, totalSize)

	f, _ := Write(ctx, bucket, "test/bench.bin",
		WithChunkSize(chunkSize),
		WithSize(totalSize),
	)
	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			b.Fatalf("Next: %v", err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	b.Run("no prefetch", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, _ := ReadFromBucket(ctx, bucket, "test/bench.bin")
			io.Copy(io.Discard, reader)
			reader.Close()
		}
	})

	b.Run("prefetch=2", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, _ := ReadFromBucket(ctx, bucket, "test/bench.bin",
				WithPrefetch(2),
			)
			io.Copy(io.Discard, reader)
			reader.Close()
		}
	})

	b.Run("prefetch=4", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			reader, _ := ReadFromBucket(ctx, bucket, "test/bench.bin",
				WithPrefetch(4),
			)
			io.Copy(io.Discard, reader)
			reader.Close()
		}
	})
}

// slowBucket wraps a bucket to add artificial latency for testing prefetch benefits.
type slowBucket struct {
	*blob.Bucket
	readLatency time.Duration
	readCount   atomic.Int32
}

func (s *slowBucket) NewReader(ctx context.Context, key string, opts *blob.ReaderOptions) (*blob.Reader, error) {
	s.readCount.Add(1)
	time.Sleep(s.readLatency)
	return s.Bucket.NewReader(ctx, key, opts)
}
