package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ligustah/slurp/pkg/sharded"
)

func runDownload(args []string) int {
	fs := flag.NewFlagSet("download", flag.ExitOnError)

	bucket := fs.String("bucket", "", "Source bucket URL (required)")
	object := fs.String("object", "", "Source object path (required)")
	output := fs.String("output", "", "Output file path (required)")
	workers := fs.Int("workers", 1, "Number of parallel download workers")
	verify := fs.Bool("verify", false, "Verify checksums while reading")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: slurp download [options]

Read a sharded file from object storage and write to a local file.
Use -workers for parallel downloads (much faster for large files).
Supports resume after crash.

Options:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return ExitInvalidArgs
	}

	if *bucket == "" || *object == "" || *output == "" {
		fmt.Fprintln(os.Stderr, "Error: -bucket, -object, and -output are required")
		fs.Usage()
		return ExitInvalidArgs
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Fprintln(os.Stderr, "\n[slurp] Received interrupt, shutting down...")
		cancel()
	}()

	return downloadToFile(ctx, *bucket, *object, *output, *workers, *verify)
}

func downloadToFile(ctx context.Context, bucketURL, object, output string, workers int, verify bool) int {
	progressPath := output + ".slurp-progress"

	// Fetch manifest from bucket
	sr, err := sharded.OpenShards(ctx, bucketURL, object)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitStorageError
	}
	defer sr.Close()

	manifest := sr.Manifest()
	total := len(manifest.Shards)

	// Load progress (completed shard indices)
	completed := loadProgress(progressPath)
	remaining := total - len(completed)

	if remaining == 0 {
		os.Remove(progressPath)
		fmt.Fprintf(os.Stderr, "[slurp] Download complete: %s\n", output)
		return ExitSuccess
	}

	if len(completed) > 0 {
		fmt.Fprintf(os.Stderr, "[slurp] Resuming: %d/%d shards remaining\n", remaining, total)
	}

	// Open output file
	f, err := os.OpenFile(output, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		return ExitGeneralError
	}
	defer f.Close()

	// Pre-allocate on fresh start
	if len(completed) == 0 && manifest.TotalSize > 0 {
		if err := f.Truncate(manifest.TotalSize); err != nil {
			fmt.Fprintf(os.Stderr, "Error allocating file: %v\n", err)
			return ExitGeneralError
		}
	}

	// Open progress file for appending
	progress, err := os.OpenFile(progressPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening progress file: %v\n", err)
		return ExitGeneralError
	}
	defer progress.Close()

	type result struct {
		idx int
		err error
	}

	workCh := make(chan int)
	resultCh := make(chan result)

	// Start workers
	for w := 0; w < workers; w++ {
		go func() {
			for idx := range workCh {
				r := result{idx: idx}

				handle, err := sr.Open(ctx, idx)
				if err != nil {
					r.err = err
					resultCh <- r
					continue
				}

				err = writeShardToFile(handle, f, handle.Offset, handle.Size, handle.Checksum, verify)
				handle.Close()
				if err != nil {
					r.err = fmt.Errorf("shard %d: %w", idx, err)
				}

				resultCh <- r
			}
		}()
	}

	// Send work (skip completed shards)
	go func() {
		for i := 0; i < total; i++ {
			if !completed[i] {
				workCh <- i
			}
		}
		close(workCh)
	}()

	// Collect results
	var firstErr error
	done := 0
	for i := 0; i < remaining; i++ {
		r := <-resultCh
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
		} else {
			fmt.Fprintf(progress, "%d\n", r.idx)
			progress.Sync()
			done++
		}
		fmt.Fprintf(os.Stderr, "\r[slurp] Downloading: %d/%d shards", len(completed)+done, total)
	}
	fmt.Fprintln(os.Stderr)

	if firstErr != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", firstErr)
		fmt.Fprintf(os.Stderr, "[slurp] Run again to resume\n")
		return ExitStorageError
	}

	// All done
	os.Remove(progressPath)
	fmt.Fprintf(os.Stderr, "[slurp] Downloaded %d bytes to %s\n", manifest.TotalSize, output)
	return ExitSuccess
}

func loadProgress(path string) map[int]bool {
	completed := make(map[int]bool)
	data, err := os.ReadFile(path)
	if err != nil {
		return completed
	}
	for _, line := range strings.Split(string(data), "\n") {
		if idx, err := strconv.Atoi(strings.TrimSpace(line)); err == nil {
			completed[idx] = true
		}
	}
	return completed
}

func writeShardToFile(r io.Reader, f *os.File, offset, size int64, checksum string, verify bool) error {
	buf := make([]byte, 4*1024*1024)
	var written int64
	hash := sha256.New()

	for {
		n, readErr := r.Read(buf)
		if n > 0 {
			if verify {
				hash.Write(buf[:n])
			}
			nw, writeErr := f.WriteAt(buf[:n], offset)
			if writeErr != nil {
				return fmt.Errorf("write: %w", writeErr)
			}
			written += int64(nw)
			offset += int64(nw)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("read: %w", readErr)
		}
	}

	if written != size {
		return fmt.Errorf("size mismatch: expected %d, got %d", size, written)
	}

	if verify && checksum != "" {
		actual := hex.EncodeToString(hash.Sum(nil))
		if actual != checksum {
			return fmt.Errorf("checksum mismatch")
		}
	}

	return nil
}
