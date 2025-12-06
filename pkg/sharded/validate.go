package sharded

import (
	"context"
	"encoding/json"
	"fmt"

	"gocloud.dev/blob"
)

// ValidationResult contains the results of validating a sharded file.
type ValidationResult struct {
	Valid          bool     // true if all chunks exist and sizes match
	TotalSize      int64    // total size from manifest
	ChunkCount     int      // number of chunks in manifest
	MissingChunks  int      // number of chunks that don't exist
	SizeMismatches int      // number of chunks with wrong size
	Errors         []string // detailed error messages
}

// Validate checks that a sharded file is complete and all chunks exist with correct sizes.
// It reads chunk metadata from the object store without downloading the actual data.
// Returns a ValidationResult with details about any issues found.
//
// Returns an error if:
//   - The manifest doesn't exist (error wraps gcerrors.NotFound)
//   - The manifest JSON is malformed (encoding/json error)
//   - Cannot access object store to check chunk attributes (network/permission error)
//   - The context is cancelled (context.Canceled or context.DeadlineExceeded)
//
// Note: Missing chunks or size mismatches are NOT returned as errors.
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
		ChunkCount: len(manifest.Chunks),
		Errors:     make([]string, 0),
	}

	// Check each chunk
	for _, chunk := range manifest.Chunks {
		path := manifest.PartsPrefix + chunk.Object

		attrs, err := bucket.Attributes(ctx, path)
		if err != nil {
			if isNotExist(err) {
				result.Valid = false
				result.MissingChunks++
				result.Errors = append(result.Errors,
					fmt.Sprintf("chunk %d missing: %s", chunk.Index, path))
				continue
			}
			return nil, fmt.Errorf("sharded: check chunk %d: %w", chunk.Index, err)
		}

		if attrs.Size != chunk.Size {
			result.Valid = false
			result.SizeMismatches++
			result.Errors = append(result.Errors,
				fmt.Sprintf("chunk %d size mismatch: expected %d, got %d",
					chunk.Index, chunk.Size, attrs.Size))
		}
	}

	return result, nil
}
