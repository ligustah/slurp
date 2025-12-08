package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/pkg/sharded"
)

// runFix repairs a sharded file by re-downloading a specific shard.
func runFix(args []string) int {
	fs := flag.NewFlagSet("fix", flag.ExitOnError)

	bucket := fs.String("bucket", "", "Bucket URL (required)")
	object := fs.String("object", "", "Object path (required)")
	shard := fs.Int("shard", -1, "Shard index to fix (required)")
	timeout := fs.Duration("timeout", 0, "HTTP timeout (0 = no timeout)")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp fix [options]

Re-download a specific shard from the source URL stored in manifest metadata.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return ExitInvalidArgs
	}

	// Validate required flags
	if *bucket == "" || *object == "" || *shard < 0 {
		fmt.Fprintln(os.Stderr, "Error: -bucket, -object, and -shard are required")
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

	// HTTP client (timeout 0 means no timeout)
	httpClient := &http.Client{}
	if *timeout > 0 {
		httpClient.Timeout = *timeout
	}

	fmt.Printf("Fixing shard %d...\n", *shard)

	err = sharded.FixShard(ctx, bkt, *object, *shard, httpClient)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitStorageError
	}

	fmt.Println("Done")
	return ExitSuccess
}
