package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/pkg/sharded"
)

// runDelete removes a sharded file and all its chunks from object storage.
// By default prompts for confirmation unless --force is specified.
func runDelete(args []string) int {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)

	bucket := fs.String("bucket", "", "Bucket URL (required)")
	object := fs.String("object", "", "Object path (required)")
	force := fs.Bool("force", false, "Skip confirmation prompt")
	partial := fs.Bool("partial", false, "Delete incomplete/partial upload state")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp delete [options]

Remove a sharded file and all its chunks from object storage.

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

	// Confirm deletion unless --force
	if !*force {
		fmt.Printf("Delete sharded file %s from %s? [y/N]: ", *object, *bucket)
		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		response = strings.TrimSpace(strings.ToLower(response))
		if response != "y" && response != "yes" {
			fmt.Fprintln(os.Stderr, "Cancelled")
			return ExitSuccess
		}
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

	// Delete
	if *partial {
		err = sharded.DeletePartial(ctx, bkt, *object)
	} else {
		err = sharded.Delete(ctx, bkt, *object)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitStorageError
	}

	fmt.Fprintf(os.Stderr, "[slurp] Deleted: %s/%s\n", *bucket, *object)
	return ExitSuccess
}
