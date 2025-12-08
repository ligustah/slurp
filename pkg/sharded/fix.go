package sharded

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"gocloud.dev/blob"
)

// FixShard re-downloads a single shard by index.
//
// The source URL must be stored in manifest metadata under "source_url".
//
// If the manifest entry is corrupted (empty object/size), it will be
// recomputed based on the shard_size and index. The manifest will be
// updated with the corrected entry after successful download.
//
// Returns an error if:
//   - The manifest doesn't exist
//   - The manifest doesn't contain a source_url in metadata
//   - The shard index is out of range
//   - The download fails
//   - Checksum verification fails (if checksum present)
func FixShard(ctx context.Context, bucket *blob.Bucket, dest string, shardIdx int, httpClient *http.Client) error {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	// Read manifest
	manifestPath := dest + ".manifest.json"
	data, err := bucket.ReadAll(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("unmarshal manifest: %w", err)
	}

	// Get source URL from metadata
	sourceURL := manifest.Metadata["source_url"]
	if sourceURL == "" {
		return fmt.Errorf("manifest missing source_url in metadata")
	}

	// Validate shard index
	if shardIdx < 0 || shardIdx >= len(manifest.Shards) {
		return fmt.Errorf("shard index %d out of range [0, %d)", shardIdx, len(manifest.Shards))
	}

	shard := &manifest.Shards[shardIdx]

	// Check if manifest entry is corrupted and needs reconstruction
	needsManifestUpdate := false
	if shard.Object == "" || shard.Size == 0 {
		// Reconstruct shard info from manifest metadata
		shard.Object = fmt.Sprintf("shard-%06d", shardIdx)
		shard.Offset = int64(shardIdx) * manifest.ShardSize

		// Size is shard_size for all but possibly the last shard
		if shardIdx == len(manifest.Shards)-1 {
			// Last shard - compute from total size
			shard.Size = manifest.TotalSize - shard.Offset
		} else {
			shard.Size = manifest.ShardSize
		}

		fmt.Printf("Reconstructed shard %d: object=%s offset=%d size=%d\n",
			shardIdx, shard.Object, shard.Offset, shard.Size)
		needsManifestUpdate = true
	}

	// Download the shard
	if err := fixShard(ctx, httpClient, bucket, sourceURL, manifest.PartsPrefix, shardIdx, shard); err != nil {
		return err
	}

	// Update manifest if we reconstructed the shard info
	if needsManifestUpdate {
		manifest.Shards[shardIdx] = *shard
		updatedData, err := json.MarshalIndent(&manifest, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal updated manifest: %w", err)
		}
		if err := bucket.WriteAll(ctx, manifestPath, updatedData, nil); err != nil {
			return fmt.Errorf("write updated manifest: %w", err)
		}
		fmt.Println("Updated manifest with reconstructed shard info")
	}

	return nil
}

// fixShard downloads and writes a single shard.
func fixShard(ctx context.Context, client *http.Client, bucket *blob.Bucket, sourceURL, partsPrefix string, idx int, shard *ShardInfo) error {
	// Create range request
	req, err := http.NewRequestWithContext(ctx, "GET", sourceURL, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	// Set range header for this shard
	rangeEnd := shard.Offset + shard.Size - 1
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", shard.Offset, rangeEnd))

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	// Write to bucket with checksum verification
	path := partsPrefix + shard.Object
	writer, err := bucket.NewWriter(ctx, path, nil)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}

	hash := sha256.New()
	tee := io.TeeReader(resp.Body, hash)

	written, err := io.Copy(writer, tee)
	if err != nil {
		writer.Close()
		return fmt.Errorf("write: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	if written != shard.Size {
		// Delete the bad shard
		bucket.Delete(ctx, path)
		return fmt.Errorf("size mismatch: expected %d, got %d", shard.Size, written)
	}

	// Verify checksum if present
	if shard.Checksum != "" {
		actual := hex.EncodeToString(hash.Sum(nil))
		if actual != shard.Checksum {
			// Delete the bad shard
			bucket.Delete(ctx, path)
			return fmt.Errorf("checksum mismatch: expected %s, got %s", shard.Checksum, actual)
		}
	}

	return nil
}
