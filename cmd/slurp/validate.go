package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/pkg/sharded"
)

// runValidate checks that a sharded file is complete and all chunks exist
// with correct sizes. Reports validation status without downloading data.
func runValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ExitOnError)

	bucket := fs.String("bucket", "", "Bucket URL (required)")
	object := fs.String("object", "", "Object path (required)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp validate [options]

Verify that a sharded file is complete and all chunks exist with correct sizes.
Does not download actual chunk data - only checks metadata.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return ExitInvalidArgs
	}

	// Validate required flags
	if *bucket == "" || *object == "" {
		fmt.Fprintln(os.Stderr, "Error: -bucket and -object are required")
		fs.Usage()
		return ExitInvalidArgs
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	// Open bucket
	bkt, err := blob.OpenBucket(ctx, *bucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bucket: %v\n", err)
		return ExitStorageError
	}
	defer bkt.Close()

	// Validate
	result, err := sharded.Validate(ctx, bkt, *object)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitStorageError
	}

	// Print results
	fmt.Printf("File: %s\n", *object)
	fmt.Printf("Total size: %d bytes\n", result.TotalSize)
	fmt.Printf("Chunks: %d\n", result.ChunkCount)

	if result.Valid {
		fmt.Println("Status: VALID")
		return ExitSuccess
	}

	fmt.Println("Status: INVALID")
	fmt.Printf("Missing chunks: %d\n", result.MissingChunks)
	fmt.Printf("Size mismatches: %d\n", result.SizeMismatches)

	if len(result.Errors) > 0 {
		fmt.Println("\nErrors:")
		for _, e := range result.Errors {
			fmt.Printf("  - %s\n", e)
		}
	}

	return ExitValidationFailed
}
