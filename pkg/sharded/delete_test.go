package sharded

import (
	"context"
	"io"
	"testing"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/memblob"
)

func TestDelete(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create a sharded file
	data := make([]byte, 256*1024) // 256KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	f, err := Write(ctx, bucket, "test/delete-me.bin",
		WithChunkSize(64*1024), // 4 chunks
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

	// Verify file exists and is readable
	reader, err := ReadFromBucket(ctx, bucket, "test/delete-me.bin")
	if err != nil {
		t.Fatalf("ReadFromBucket before delete: %v", err)
	}
	manifest := reader.Manifest()
	reader.Close()

	// Count objects before delete
	beforeCount := countObjects(t, ctx, bucket, "")
	t.Logf("Objects before delete: %d", beforeCount)

	// Delete the sharded file
	err = Delete(ctx, bucket, "test/delete-me.bin")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Verify manifest is gone
	_, err = ReadFromBucket(ctx, bucket, "test/delete-me.bin")
	if err == nil {
		t.Fatal("expected error reading deleted file")
	}

	// Verify chunks are gone
	for _, chunk := range manifest.Chunks {
		path := manifest.PartsPrefix + chunk.Object
		exists, err := bucket.Exists(ctx, path)
		if err != nil {
			t.Fatalf("check chunk exists: %v", err)
		}
		if exists {
			t.Errorf("chunk %s should be deleted", path)
		}
	}

	// Count objects after delete
	afterCount := countObjects(t, ctx, bucket, "")
	t.Logf("Objects after delete: %d", afterCount)

	if afterCount != 0 {
		t.Errorf("expected 0 objects after delete, got %d", afterCount)
	}
}

func TestDeleteNonExistent(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Delete non-existent file should return error
	err = Delete(ctx, bucket, "test/does-not-exist.bin")
	if err == nil {
		t.Fatal("expected error deleting non-existent file")
	}
}

func TestDeletePartialState(t *testing.T) {
	ctx := context.Background()
	bucket, err := blob.OpenBucket(ctx, "mem://")
	if err != nil {
		t.Fatalf("open bucket: %v", err)
	}
	defer bucket.Close()

	// Create a partial sharded file (not completed)
	data := make([]byte, 256*1024)
	f, err := Write(ctx, bucket, "test/partial.bin",
		WithChunkSize(64*1024),
		WithSize(int64(len(data))),
		WithStateInterval(1),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Write only 2 of 4 chunks
	for i := 0; i < 2; i++ {
		chunk, err := f.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		start := chunk.Offset()
		end := start + chunk.Length()
		chunk.Write(data[start:end])
		chunk.Close()
	}
	// Don't complete - leave in partial state

	// Count objects (should have state file + 2 chunks)
	beforeCount := countObjects(t, ctx, bucket, "")
	t.Logf("Objects before delete: %d", beforeCount)

	// DeletePartial should clean up incomplete files
	err = DeletePartial(ctx, bucket, "test/partial.bin")
	if err != nil {
		t.Fatalf("DeletePartial: %v", err)
	}

	// Count objects after delete
	afterCount := countObjects(t, ctx, bucket, "")
	t.Logf("Objects after delete: %d", afterCount)

	if afterCount != 0 {
		t.Errorf("expected 0 objects after delete, got %d", afterCount)
	}
}

func countObjects(t *testing.T, ctx context.Context, bucket *blob.Bucket, prefix string) int {
	t.Helper()
	count := 0
	iter := bucket.List(&blob.ListOptions{Prefix: prefix})
	for {
		_, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("list objects: %v", err)
		}
		count++
	}
	return count
}
