package sharded

import (
	"context"
	"encoding/json"
	"fmt"

	"gocloud.dev/blob"
)

// Delete removes a completed sharded file and all its chunks from storage.
// It reads the manifest to find all chunks, deletes them, then deletes the manifest.
//
// Returns an error if:
//   - The manifest doesn't exist (error wraps gcerrors.NotFound)
//   - The manifest JSON is malformed (encoding/json error)
//   - A chunk cannot be deleted (permission denied, network error)
//   - The context is cancelled (context.Canceled or context.DeadlineExceeded)
func Delete(ctx context.Context, bucket *blob.Bucket, dest string) error {
	// Read manifest
	manifestPath := dest + ".manifest.json"
	data, err := bucket.ReadAll(ctx, manifestPath)
	if err != nil {
		return fmt.Errorf("sharded: read manifest: %w", err)
	}

	var manifest Manifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return fmt.Errorf("sharded: unmarshal manifest: %w", err)
	}

	// Delete all chunks
	for _, chunk := range manifest.Chunks {
		path := manifest.PartsPrefix + chunk.Object
		if err := bucket.Delete(ctx, path); err != nil && !isNotExist(err) {
			return fmt.Errorf("sharded: delete chunk %s: %w", path, err)
		}
	}

	// Delete manifest
	if err := bucket.Delete(ctx, manifestPath); err != nil {
		return fmt.Errorf("sharded: delete manifest: %w", err)
	}

	return nil
}

// DeletePartial removes an incomplete sharded file (one that was never completed).
// It finds the state file and any written chunks, and removes them.
// This is useful for cleaning up failed or interrupted uploads.
//
// Returns an error if:
//   - Neither state file nor manifest exists (error wraps gcerrors.NotFound)
//   - A chunk cannot be deleted (permission denied, network error)
//   - The context is cancelled (context.Canceled or context.DeadlineExceeded)
func DeletePartial(ctx context.Context, bucket *blob.Bucket, dest string) error {
	// Use destination-based prefix (same logic as Write)
	partsPrefix := dest + ".shards/"

	// Try to read state file for chunk info
	statePath := partsPrefix + "state.json"
	data, err := bucket.ReadAll(ctx, statePath)
	if err != nil {
		if isNotExist(err) {
			// No state file - check if there's a completed manifest instead
			manifestPath := dest + ".manifest.json"
			if exists, _ := bucket.Exists(ctx, manifestPath); exists {
				return Delete(ctx, bucket, dest)
			}
			return fmt.Errorf("sharded: no state or manifest found for %s", dest)
		}
		return fmt.Errorf("sharded: read state: %w", err)
	}

	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("sharded: unmarshal state: %w", err)
	}

	// Delete all chunks listed in state (any status that has an object)
	for _, chunk := range s.Chunks {
		if chunk.Object != "" {
			path := partsPrefix + chunk.Object
			if err := bucket.Delete(ctx, path); err != nil && !isNotExist(err) {
				return fmt.Errorf("sharded: delete chunk %s: %w", path, err)
			}
		}
	}

	// Delete state file
	if err := bucket.Delete(ctx, statePath); err != nil && !isNotExist(err) {
		return fmt.Errorf("sharded: delete state: %w", err)
	}

	return nil
}
