package sharded

import (
	"context"
	"io"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func TestValidate(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create a valid sharded file
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, err := Write(ctx, bucket, "test/valid.bin",
		WithChunkSize(64*1024),
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

	// Validate should succeed
	result, err := Validate(ctx, bucket, "test/valid.bin")
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if !result.Valid {
		t.Errorf("expected valid, got invalid: %v", result.Errors)
	}

	if result.TotalSize != int64(len(data)) {
		t.Errorf("expected total size %d, got %d", len(data), result.TotalSize)
	}

	if result.ChunkCount != 4 {
		t.Errorf("expected 4 chunks, got %d", result.ChunkCount)
	}

	if len(result.Errors) != 0 {
		t.Errorf("expected no errors, got %v", result.Errors)
	}
}

func TestValidateMissingChunk(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create a valid sharded file
	data := make([]byte, 256*1024)
	f, _ := Write(ctx, bucket, "test/missing-chunk.bin",
		WithChunkSize(64*1024),
		WithSize(int64(len(data))),
	)

	var manifest *Manifest
	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
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

	// Get manifest to find chunk path
	reader, _ := ReadFromBucket(ctx, bucket, "test/missing-chunk.bin")
	manifest = reader.Manifest()
	reader.Close()

	// Delete one chunk to simulate corruption
	chunkPath := manifest.PartsPrefix + manifest.Chunks[1].Object
	bucket.Delete(ctx, chunkPath)

	// Validate should fail
	result, err := Validate(ctx, bucket, "test/missing-chunk.bin")
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if result.Valid {
		t.Error("expected invalid due to missing chunk")
	}

	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d: %v", len(result.Errors), result.Errors)
	}

	if result.MissingChunks != 1 {
		t.Errorf("expected 1 missing chunk, got %d", result.MissingChunks)
	}
}

func TestValidateSizeMismatch(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create a valid sharded file
	data := make([]byte, 256*1024)
	f, _ := Write(ctx, bucket, "test/size-mismatch.bin",
		WithChunkSize(64*1024),
		WithSize(int64(len(data))),
	)

	var manifest *Manifest
	for {
		chunk, err := f.Next(ctx)
		if err == io.EOF {
			break
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

	// Get manifest to find chunk path
	reader, _ := ReadFromBucket(ctx, bucket, "test/size-mismatch.bin")
	manifest = reader.Manifest()
	reader.Close()

	// Overwrite one chunk with wrong size
	chunkPath := manifest.PartsPrefix + manifest.Chunks[0].Object
	bucket.WriteAll(ctx, chunkPath, []byte("too small"), nil)

	// Validate should fail
	result, err := Validate(ctx, bucket, "test/size-mismatch.bin")
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}

	if result.Valid {
		t.Error("expected invalid due to size mismatch")
	}

	if result.SizeMismatches != 1 {
		t.Errorf("expected 1 size mismatch, got %d", result.SizeMismatches)
	}
}

func TestValidateNonExistent(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Validate non-existent file should return error
	_, err = Validate(ctx, bucket, "test/does-not-exist.bin")
	if err == nil {
		t.Fatal("expected error validating non-existent file")
	}
}
