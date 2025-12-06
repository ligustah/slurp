// Package downloader orchestrates parallel HTTP downloads to cloud storage.
//
// This package coordinates between the HTTP client and sharded.File to
// download large files in parallel chunks. It manages the worker pool and
// handles graceful shutdown.
//
// # Usage
//
// The main entry point is the Download function:
//
//	err := downloader.Download(ctx, url, bucket, destObject, Options{
//	    Workers:   16,
//	    ChunkSize: 256 * 1024 * 1024,
//	    Progress:  progressReporter,
//	})
//
// # Worker Pool
//
// Workers receive chunk assignments from a channel, make HTTP range requests,
// and stream responses directly to sharded.File. Failed chunks are retried
// with exponential backoff.
//
// # Graceful Shutdown
//
// On SIGINT/SIGTERM:
//   - Stop accepting new chunk assignments
//   - Wait for in-progress chunks to complete
//   - Persist current state via sharded.File
package downloader
