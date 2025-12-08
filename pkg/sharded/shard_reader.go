package sharded

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"
)

// ShardReader provides random access to individual shards in a sharded file.
// Unlike Reader which streams all shards sequentially, ShardReader allows
// opening specific shards by index for parallel processing.
type ShardReader struct {
	bucket   *blob.Bucket
	manifest *Manifest
}

// ShardHandle represents an open shard with its metadata and reader.
type ShardHandle struct {
	Index    int
	Offset   int64
	Size     int64
	Checksum string
	io.ReadCloser
}

// OpenShards opens a sharded file for random access to individual shards.
// Use Manifest() to get shard count and total size, then Open() to read specific shards.
// The caller must call Close() when done.
func OpenShards(ctx context.Context, bucketURL, object string) (*ShardReader, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("sharded: open bucket: %w", err)
	}

	manifestPath := object + ".manifest.json"
	data, err := bucket.ReadAll(ctx, manifestPath)
	if err != nil {
		bucket.Close()
		return nil, fmt.Errorf("sharded: read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		bucket.Close()
		return nil, fmt.Errorf("sharded: unmarshal manifest: %w", err)
	}

	return &ShardReader{
		bucket:   bucket,
		manifest: &manifest,
	}, nil
}

// OpenShardsFromManifest opens a sharded file using a previously saved manifest.
// Use this when resuming a download.
// The caller must call Close() when done.
func OpenShardsFromManifest(ctx context.Context, bucketURL string, manifest *Manifest) (*ShardReader, error) {
	bucket, err := blob.OpenBucket(ctx, bucketURL)
	if err != nil {
		return nil, fmt.Errorf("sharded: open bucket: %w", err)
	}

	return &ShardReader{
		bucket:   bucket,
		manifest: manifest,
	}, nil
}

// Manifest returns the manifest for the sharded file.
// Use this to get TotalSize for pre-allocating output files,
// and len(Shards) to know how many shards to process.
func (r *ShardReader) Manifest() *Manifest {
	return r.manifest
}

// Open opens a specific shard by index and returns a handle with metadata and reader.
// The caller must close the returned ShardHandle when done.
func (r *ShardReader) Open(ctx context.Context, idx int) (*ShardHandle, error) {
	if idx < 0 || idx >= len(r.manifest.Shards) {
		return nil, fmt.Errorf("sharded: shard index %d out of range [0, %d)", idx, len(r.manifest.Shards))
	}

	shard := r.manifest.Shards[idx]
	path := r.manifest.PartsPrefix + shard.Object

	reader, err := r.bucket.NewReader(ctx, path, nil)
	if err != nil {
		return nil, fmt.Errorf("sharded: open shard %d: %w", idx, err)
	}

	return &ShardHandle{
		Index:      idx,
		Offset:     shard.Offset,
		Size:       shard.Size,
		Checksum:   shard.Checksum,
		ReadCloser: reader,
	}, nil
}

// Close closes the ShardReader and releases resources.
func (r *ShardReader) Close() error {
	return r.bucket.Close()
}
