package sharded

import (
	"context"
	"encoding/json"
	"fmt"

	"gocloud.dev/blob"
)

// ValidationResult contains the results of validating a sharded file.
type ValidationResult struct {
	Valid          bool     // true if all shards exist and sizes match
	TotalSize      int64    // total size from manifest
	ShardCount     int      // number of shards in manifest
	MissingShards  int      // number of shards that don't exist
	SizeMismatches int      // number of shards with wrong size
	Errors         []string // detailed error messages
}

// Validate checks that a sharded file is complete and all shards exist with correct sizes.
// It reads shard metadata from the object store without downloading the actual data.
// Returns a ValidationResult with details about any issues found.
//
// Returns an error if:
//   - The manifest doesn't exist (error wraps gcerrors.NotFound)
//   - The manifest JSON is malformed (encoding/json error)
//   - Cannot access object store to check shard attributes (network/permission error)
//   - The context is cancelled (context.Canceled or context.DeadlineExceeded)
//
// Note: Missing shards or size mismatches are NOT returned as errors.
// Instead, they are reported in the ValidationResult with Valid=false.
func Validate(ctx context.Context, bucket *blob.Bucket, dest string) (*ValidationResult, error) {
	// Read manifest
	manifestPath := dest + ".manifest.json"
	data, err := bucket.ReadAll(ctx, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("sharded: read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("sharded: unmarshal manifest: %w", err)
	}

	result := &ValidationResult{
		Valid:      true,
		TotalSize:  manifest.TotalSize,
		ShardCount: len(manifest.Shards),
		Errors:     make([]string, 0),
	}

	// Check each shard
	for i, shard := range manifest.Shards {
		path := manifest.PartsPrefix + shard.Object

		attrs, err := bucket.Attributes(ctx, path)
		if err != nil {
			if isNotExist(err) {
				result.Valid = false
				result.MissingShards++
				result.Errors = append(result.Errors,
					fmt.Sprintf("shard %d missing: %s", i, path))
				continue
			}
			return nil, fmt.Errorf("sharded: check shard %d: %w", i, err)
		}

		if attrs.Size != shard.Size {
			result.Valid = false
			result.SizeMismatches++
			result.Errors = append(result.Errors,
				fmt.Sprintf("shard %d size mismatch: expected %d, got %d",
					i, shard.Size, attrs.Size))
		}
	}

	return result, nil
}
