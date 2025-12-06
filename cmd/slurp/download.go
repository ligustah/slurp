package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/pkg/sharded"
)

// runDownload reads a sharded file from object storage and writes it to
// a local file or stdout. Supports prefetching for improved throughput.
func runDownload(args []string) int {
	fs := flag.NewFlagSet("download", flag.ExitOnError)

	bucket := fs.String("bucket", "", "Source bucket URL (required)")
	object := fs.String("object", "", "Source object path (required)")
	output := fs.String("output", "-", "Output file path (- for stdout)")
	prefetch := fs.Int("prefetch", 2, "Number of chunks to prefetch")
	verify := fs.Bool("verify", false, "Verify checksums while reading")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp download [options]

Read a sharded file from object storage and write to local file or stdout.

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

	// Handle signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n[slurp] Received interrupt, shutting down...")
		cancel()
	}()

	// Open bucket
	bkt, err := blob.OpenBucket(ctx, *bucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bucket: %v\n", err)
		return ExitStorageError
	}
	defer bkt.Close()

	// Open sharded reader
	opts := []sharded.Option{
		sharded.WithPrefetch(*prefetch),
	}
	if *verify {
		opts = append(opts, sharded.WithVerifyChecksum(true))
	}

	reader, err := sharded.ReadFromBucket(ctx, bkt, *object, opts...)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening sharded file: %v\n", err)
		return ExitStorageError
	}
	defer reader.Close()

	// Open output
	var out io.Writer
	if *output == "-" {
		out = os.Stdout
	} else {
		f, err := os.Create(*output)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
			return ExitGeneralError
		}
		defer f.Close()
		out = f
	}

	// Copy data
	n, err := io.Copy(out, reader)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "\n[slurp] Download interrupted")
			return ExitGeneralError
		}
		fmt.Fprintf(os.Stderr, "Error reading data: %v\n", err)
		return ExitGeneralError
	}

	if *output != "-" {
		fmt.Fprintf(os.Stderr, "[slurp] Downloaded %d bytes to %s\n", n, *output)
	}

	return ExitSuccess
}
