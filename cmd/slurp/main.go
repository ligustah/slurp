package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/s3blob"

	"github.com/ligustah/slurp/internal/config"
	"github.com/ligustah/slurp/internal/downloader"
	slurphttp "github.com/ligustah/slurp/internal/http"
	"github.com/ligustah/slurp/internal/progress"
)

// Exit codes as documented in README.
const (
	ExitSuccess           = 0
	ExitGeneralError      = 1
	ExitInvalidArgs       = 2
	ExitSourceNotAccess   = 3
	ExitRangeNotSupported = 4
	ExitStorageError      = 5
	ExitSourceChanged     = 6
)

func main() {
	os.Exit(run())
}

func run() int {
	// Parse flags
	var (
		url             = flag.String("url", "", "Source URL to download (required)")
		bucket          = flag.String("bucket", "", "Destination bucket URL (required)")
		object          = flag.String("object", "", "Destination object path (required)")
		workers         = flag.Int("workers", 0, "Number of parallel workers (default 16)")
		chunkSize       = flag.String("chunk-size", "", "Size of each chunk (default 256MB)")
		showProgress    = flag.Bool("progress", false, "Show progress output")
		retryAttempts   = flag.Int("retry-attempts", 0, "Max retry attempts per chunk (default 5)")
		retryBackoff    = flag.Duration("retry-backoff", 0, "Initial retry backoff (default 1s)")
		retryMaxBackoff = flag.Duration("retry-max-backoff", 0, "Max retry backoff (default 30s)")
		stateInterval   = flag.Int("state-interval", 0, "Persist state every N chunks (default 10)")
		force           = flag.Bool("force", false, "Force restart, ignoring existing state")
		configFile      = flag.String("config", "", "Path to config file (YAML)")
	)

	flag.Parse()

	// Load configuration
	cfg := config.Default()

	// Load from file if specified
	if *configFile != "" {
		fileCfg, err := config.LoadFromFile(*configFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading config file: %v\n", err)
			return ExitInvalidArgs
		}
		cfg = cfg.Merge(fileCfg)
	}

	// Load from environment
	if err := cfg.LoadFromEnv(); err != nil {
		fmt.Fprintf(os.Stderr, "Error loading environment config: %v\n", err)
		return ExitInvalidArgs
	}

	// Apply command line flags (highest priority)
	if *url != "" {
		cfg.URL = *url
	}
	if *bucket != "" {
		cfg.Bucket = *bucket
	}
	if *object != "" {
		cfg.Object = *object
	}
	if *workers != 0 {
		cfg.Workers = *workers
	}
	if *chunkSize != "" {
		size, err := progress.ParseBytes(*chunkSize)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid chunk size: %v\n", err)
			return ExitInvalidArgs
		}
		cfg.ChunkSize = size
	}
	if *showProgress {
		cfg.Progress = true
	}
	if *retryAttempts != 0 {
		cfg.Retry.Attempts = *retryAttempts
	}
	if *retryBackoff != 0 {
		cfg.Retry.Backoff = *retryBackoff
	}
	if *retryMaxBackoff != 0 {
		cfg.Retry.MaxBackoff = *retryMaxBackoff
	}
	if *stateInterval != 0 {
		cfg.StateInterval = *stateInterval
	}
	if *force {
		cfg.Force = true
	}

	// Validate
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Configuration error: %v\n", err)
		flag.Usage()
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
	bkt, err := blob.OpenBucket(ctx, cfg.Bucket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening bucket: %v\n", err)
		return ExitStorageError
	}
	defer bkt.Close()

	// Calculate total chunks
	info, err := downloader.GetFileInfo(ctx, cfg.URL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error accessing source URL: %v\n", err)
		return ExitSourceNotAccess
	}

	if !info.AcceptsRanges {
		fmt.Fprintf(os.Stderr, "Error: Server does not support range requests\n")
		return ExitRangeNotSupported
	}

	totalChunks := int((info.Size + cfg.ChunkSize - 1) / cfg.ChunkSize)

	// Setup progress reporter
	var reporter *progress.Reporter
	if cfg.Progress {
		reporter = progress.NewReporter(progress.Options{
			TotalSize:      info.Size,
			TotalChunks:    totalChunks,
			Workers:        cfg.Workers,
			Output:         os.Stdout,
			UpdateInterval: 500 * time.Millisecond,
			SourceURL:      cfg.URL,
			ChunkSize:      cfg.ChunkSize,
		})
		reporter.Start()
		defer reporter.Stop()
	}

	// Download
	err = downloader.Download(ctx, cfg.URL, bkt, cfg.Object, downloader.Options{
		Workers:       cfg.Workers,
		ChunkSize:     cfg.ChunkSize,
		StateInterval: cfg.StateInterval,
		Progress:      reporter,
		Force:         cfg.Force,
		HTTPOptions: slurphttp.Options{
			MaxIdleConnsPerHost: cfg.Workers * 2,
			Timeout:             30 * time.Second,
			RetryAttempts:       cfg.Retry.Attempts,
			RetryBackoff:        cfg.Retry.Backoff,
			RetryMaxBackoff:     cfg.Retry.MaxBackoff,
		},
	})

	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "[slurp] Download interrupted, state saved for resume")
			return ExitGeneralError
		}
		if err == downloader.ErrRangeNotSupported {
			fmt.Fprintf(os.Stderr, "Error: Server does not support range requests\n")
			return ExitRangeNotSupported
		}
		// Check for source changed error
		if contains(err.Error(), "etag mismatch") {
			fmt.Fprintf(os.Stderr, "Error: Source file has changed since last download attempt\n")
			fmt.Fprintf(os.Stderr, "Use --force to restart from scratch\n")
			return ExitSourceChanged
		}
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return ExitGeneralError
	}

	fmt.Fprintf(os.Stderr, "[slurp] Download complete: %s/%s\n", cfg.Bucket, cfg.Object)
	fmt.Fprintf(os.Stderr, "[slurp] Manifest: %s/%s.manifest.json\n", cfg.Bucket, cfg.Object)

	return ExitSuccess
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsAt(s, substr, 0))
}

func containsAt(s, substr string, start int) bool {
	for i := start; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
