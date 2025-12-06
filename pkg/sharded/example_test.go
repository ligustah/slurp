package sharded_test

import (
	"context"
	"fmt"
	"io"
	"os"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"

	"github.com/username/slurp/pkg/sharded"
)

func Example_writeWithKnownSize() {
	ctx := context.Background()
	bucket, _ := blob.OpenBucket(ctx, "mem://")
	defer bucket.Close()

	// Create a sharded file with known total size
	f, _ := sharded.Write(ctx, bucket, "path/to/file.tar.gz",
		sharded.WithChunkSize(256*1024*1024),
		sharded.WithSize(1024*1024*1024), // 1GB total
		sharded.WithMetadata(map[string]string{
			"source_url":  "https://example.com/file.tar.gz",
			"source_etag": "abc123",
		}),
	)

	// Write chunks - Next() returns io.EOF when all chunks accounted for
	for {
		chunk, err := f.Next(ctx)
		if err == sharded.ErrChunkFilled {
			continue // Already written (resume case)
		}
		if err == io.EOF {
			break // All chunks done
		}
		if err != nil {
			panic(err)
		}

		// Fetch data for this chunk's range
		fmt.Printf("Writing chunk %d: offset=%d, length=%d\n",
			chunk.Index(), chunk.Offset(), chunk.Length())

		// In real code: fetch from source using chunk.Offset() and chunk.Length()
		// data := fetchRange(sourceURL, chunk.Offset(), chunk.Length())
		// io.Copy(chunk, data)

		chunk.Close() // Persists chunk and updates state
	}

	// Finalize - writes manifest, cleans up state
	f.Complete(ctx)
}

func Example_writeStreaming() {
	ctx := context.Background()
	bucket, _ := blob.OpenBucket(ctx, "mem://")
	defer bucket.Close()

	// Create a sharded file without known size (streaming mode)
	f, _ := sharded.Write(ctx, bucket, "path/to/file.tar.gz",
		sharded.WithChunkSize(256*1024*1024),
		// No WithSize - streaming mode
	)

	// Simulate streaming data
	totalChunks := 4
	for i := 0; i < totalChunks; i++ {
		chunk, err := f.Next(ctx)
		if err == sharded.ErrChunkFilled {
			continue
		}
		if err != nil {
			panic(err)
		}

		fmt.Printf("Writing chunk %d: offset=%d\n", chunk.Index(), chunk.Offset())

		// Write data to chunk
		// io.Copy(chunk, data)

		chunk.Close()
	}

	// Finalize when done streaming
	f.Complete(ctx)
}

func Example_resume() {
	ctx := context.Background()
	bucket, _ := blob.OpenBucket(ctx, "mem://")
	defer bucket.Close()

	currentETag := "abc123"

	// Same Write() call handles resume - checks for existing state
	f, _ := sharded.Write(ctx, bucket, "path/to/file.tar.gz",
		sharded.WithChunkSize(256*1024*1024),
		sharded.WithSize(1024*1024*1024),
		sharded.WithMetadata(map[string]string{
			"source_etag": currentETag,
		}),
	)

	// Check if source has changed since last attempt
	if f.Metadata()["source_etag"] != currentETag {
		fmt.Println("Source changed, need to restart")
		f.Reset(ctx) // Clear existing state
	}

	// Same loop - ErrChunkFilled skips already-written chunks
	for {
		chunk, err := f.Next(ctx)
		if err == sharded.ErrChunkFilled {
			fmt.Printf("Chunk %d already filled, skipping\n", chunk.Index())
			continue
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}

		fmt.Printf("Writing chunk %d\n", chunk.Index())
		chunk.Close()
	}

	f.Complete(ctx)
}

func Example_read() {
	ctx := context.Background()

	// Read a sharded file - streams all chunks in order
	f, _ := sharded.Read(ctx, "mem://bucket", "path/to/file.tar.gz")
	defer f.Close()

	// Copy to destination
	io.Copy(os.Stdout, f)
}

func Example_readWithVerification() {
	ctx := context.Background()

	// Read with checksum verification
	f, _ := sharded.Read(ctx, "mem://bucket", "path/to/file.tar.gz",
		sharded.WithVerifyChecksum(true),
	)
	defer f.Close()

	io.Copy(os.Stdout, f)
}
