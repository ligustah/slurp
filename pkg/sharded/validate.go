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

// ValidateOptions configures validation behavior.
type ValidateOptions struct {
	// Workers is the number of parallel workers for checking shards. Default is 1.
	Workers int
	// OnProgress is called after checking each shard with (checked, total) counts.
	OnProgress func(checked, total int)
}

// ValidateOption configures validation.
type ValidateOption func(*ValidateOptions)

// WithValidateWorkers sets the number of parallel workers for validation.
func WithValidateWorkers(n int) ValidateOption {
	return func(o *ValidateOptions) {
		o.Workers = n
	}
}

// WithValidateProgress sets a callback for progress updates during validation.
func WithValidateProgress(fn func(checked, total int)) ValidateOption {
	return func(o *ValidateOptions) {
		o.OnProgress = fn
	}
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
func Validate(ctx context.Context, bucket *blob.Bucket, dest string, options ...ValidateOption) (*ValidationResult, error) {
	opts := ValidateOptions{Workers: 1}
	for _, opt := range options {
		opt(&opts)
	}
	if opts.Workers < 1 {
		opts.Workers = 1
	}

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

	total := len(manifest.Shards)
	if total == 0 {
		return &ValidationResult{
			Valid:      true,
			TotalSize:  manifest.TotalSize,
			ShardCount: 0,
		}, nil
	}

	type shardResult struct {
		idx      int
		missing  bool
		mismatch bool
		err      error
		errMsg   string
	}

	workCh := make(chan int)
	resultCh := make(chan shardResult)

	// Start workers
	for w := 0; w < opts.Workers; w++ {
		go func() {
			for idx := range workCh {
				shard := manifest.Shards[idx]
				path := manifest.PartsPrefix + shard.Object
				r := shardResult{idx: idx}

				attrs, err := bucket.Attributes(ctx, path)
				if err != nil {
					if isNotExist(err) {
						r.missing = true
						r.errMsg = fmt.Sprintf("shard %d missing: %s", idx, path)
					} else {
						r.err = fmt.Errorf("sharded: check shard %d: %w", idx, err)
					}
				} else if attrs.Size != shard.Size {
					r.mismatch = true
					r.errMsg = fmt.Sprintf("shard %d size mismatch: expected %d, got %d",
						idx, shard.Size, attrs.Size)
				}

				resultCh <- r
			}
		}()
	}

	// Send work in background
	go func() {
		for i := 0; i < total; i++ {
			workCh <- i
		}
		close(workCh)
	}()

	// Collect results
	result := &ValidationResult{
		Valid:      true,
		TotalSize:  manifest.TotalSize,
		ShardCount: total,
		Errors:     make([]string, 0),
	}

	for i := 0; i < total; i++ {
		r := <-resultCh
		if r.err != nil {
			return nil, r.err
		}
		if r.missing {
			result.MissingShards++
			result.Errors = append(result.Errors, r.errMsg)
		}
		if r.mismatch {
			result.SizeMismatches++
			result.Errors = append(result.Errors, r.errMsg)
		}
		if opts.OnProgress != nil {
			opts.OnProgress(i+1, total)
		}
	}

	result.Valid = result.MissingShards == 0 && result.SizeMismatches == 0
	return result, nil
}
