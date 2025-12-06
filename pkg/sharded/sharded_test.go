package sharded

import (
	"bytes"
	"context"
	"io"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func TestWriteAndRead(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Test data: 1MB split into 256KB chunks (4 chunks)
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	chunkSize := int64(256 * 1024)

	// Write sharded file
	f, err := Write(ctx, bucket, "test/file.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithMetadata(map[string]string{"test": "value"}),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	chunkIndex := 0
	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}

		// Write chunk data
		start := chunk.Offset()
		end := start + chunk.Length()
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		n, err := chunk.Write(data[start:end])
		if err != nil {
			t.Fatalf("chunk.Write: %v", err)
		}
		if n != int(end-start) {
			t.Fatalf("wrote %d bytes, expected %d", n, end-start)
		}

		if err := chunk.Close(); err != nil {
			t.Fatalf("chunk.Close: %v", err)
		}
		chunkIndex++
	}

	if chunkIndex != 4 {
		t.Fatalf("expected 4 chunks, got %d", chunkIndex)
	}

	if err := f.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Read back
	reader, err := ReadFromBucket(ctx, bucket, "test/file.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Fatalf("data mismatch: got %d bytes, expected %d", len(result), len(data))
	}

	// Check manifest metadata
	if reader.Manifest().Metadata["test"] != "value" {
		t.Fatalf("expected metadata 'test'='value', got %v", reader.Manifest().Metadata)
	}
}

func TestResume(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	data := make([]byte, 1024*1024) // 1MB
	for i := range data {
		data[i] = byte(i % 256)
	}
	chunkSize := int64(256 * 1024) // 4 chunks

	// First write session - write only 2 chunks
	f1, err := Write(ctx, bucket, "test/resume.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithStateInterval(1), // Save state after every chunk
	)
	if err != nil {
		t.Fatalf("Write (first): %v", err)
	}

	for i := 0; i < 2; i++ {
		chunk, err := f1.Next(ctx)
		if err != nil {
			t.Fatalf("Next (first session, chunk %d): %v", i, err)
		}

		start := chunk.Offset()
		end := start + chunk.Length()
		chunk.Write(data[start:end])
		chunk.Close()
	}
	// Don't complete - simulate interruption

	// Second write session - should resume
	f2, err := Write(ctx, bucket, "test/resume.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
	)
	if err != nil {
		t.Fatalf("Write (second): %v", err)
	}

	if f2.CompletedCount() != 2 {
		t.Fatalf("expected 2 completed chunks from resume, got %d", f2.CompletedCount())
	}

	filledCount := 0
	writtenCount := 0

	for {
		chunk, err := f2.Next(ctx)
		if err == io.EOF {
			break
		}
		if err == ErrChunkFilled {
			filledCount++
			continue
		}
		if err != nil {
			t.Fatalf("Next (second session): %v", err)
		}

		start := chunk.Offset()
		end := start + chunk.Length()
		chunk.Write(data[start:end])
		chunk.Close()
		writtenCount++
	}

	if filledCount != 2 {
		t.Fatalf("expected 2 filled chunks, got %d", filledCount)
	}
	if writtenCount != 2 {
		t.Fatalf("expected 2 written chunks, got %d", writtenCount)
	}

	if err := f2.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Verify data
	reader, err := ReadFromBucket(ctx, bucket, "test/resume.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Fatalf("data mismatch after resume")
	}
}

func TestReset(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	data := make([]byte, 512*1024) // 512KB
	chunkSize := int64(256 * 1024) // 2 chunks

	// First write with metadata
	f1, err := Write(ctx, bucket, "test/reset.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithMetadata(map[string]string{"etag": "abc123"}),
		WithStateInterval(1),
	)
	if err != nil {
		t.Fatalf("Write (first): %v", err)
	}

	// Write first chunk
	chunk, _ := f1.Next(ctx)
	chunk.Write(data[:chunkSize])
	chunk.Close()

	// Second session with different metadata
	f2, err := Write(ctx, bucket, "test/reset.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithMetadata(map[string]string{"etag": "def456"}),
	)
	if err != nil {
		t.Fatalf("Write (second): %v", err)
	}

	// Check stored metadata differs from new
	if f2.Metadata()["etag"] != "abc123" {
		t.Fatalf("expected stored etag 'abc123', got %s", f2.Metadata()["etag"])
	}

	// Reset to start fresh
	if err := f2.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}

	if f2.CompletedCount() != 0 {
		t.Fatalf("expected 0 completed after reset, got %d", f2.CompletedCount())
	}
}

func TestChecksumVerification(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	data := []byte("hello world test data for checksum verification")
	chunkSize := int64(20)

	// Write
	f, err := Write(ctx, bucket, "test/checksum.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
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
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	// Read with checksum verification
	reader, err := ReadFromBucket(ctx, bucket, "test/checksum.bin",
		WithVerifyChecksum(true),
	)
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll with checksum verification: %v", err)
	}

	if !bytes.Equal(result, data) {
		t.Fatalf("data mismatch")
	}
}

func TestWriteWithoutChecksum(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	data := []byte("test data without checksum computation")
	chunkSize := int64(20)

	// Write with checksums disabled
	f, err := Write(ctx, bucket, "test/no-checksum.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithChecksum(false),
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
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	// Read manifest and verify no checksums
	reader, err := ReadFromBucket(ctx, bucket, "test/no-checksum.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	manifest := reader.Manifest()
	for i, chunk := range manifest.Chunks {
		if chunk.Checksum != "" {
			t.Errorf("chunk %d has checksum %q, expected empty", i, chunk.Checksum)
		}
	}

	// Verify data is still correct
	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Fatalf("data mismatch")
	}
}

func TestReadWithVerificationSkipsEmptyChecksums(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	data := []byte("data written without checksums")
	chunkSize := int64(15)

	// Write without checksums
	f, err := Write(ctx, bucket, "test/verify-skip.bin",
		WithChunkSize(chunkSize),
		WithSize(int64(len(data))),
		WithChecksum(false),
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
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		chunk.Write(data[start:end])
		chunk.Close()
	}
	f.Complete(ctx)

	// Read with verification enabled - should NOT error, just skip verification
	reader, err := ReadFromBucket(ctx, bucket, "test/verify-skip.bin",
		WithVerifyChecksum(true),
	)
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll with verify checksum: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Fatalf("data mismatch")
	}
}

func TestStreamingMode(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	chunkSize := int64(100)

	// Write without known size (streaming mode)
	f, err := Write(ctx, bucket, "test/streaming.bin",
		WithChunkSize(chunkSize),
		// No WithSize
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write 3 chunks
	for i := 0; i < 3; i++ {
		chunk, err := f.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}

		data := bytes.Repeat([]byte{byte(i)}, int(chunkSize))
		chunk.Write(data)
		chunk.Close()
	}

	if err := f.Complete(ctx); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Read back
	reader, err := ReadFromBucket(ctx, bucket, "test/streaming.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket: %v", err)
	}

	result, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	if len(result) != 300 {
		t.Fatalf("expected 300 bytes, got %d", len(result))
	}

	// Verify manifest total size
	if reader.Manifest().TotalSize != 300 {
		t.Fatalf("expected manifest total size 300, got %d", reader.Manifest().TotalSize)
	}
}
