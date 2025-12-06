// Package http provides an HTTP client optimized for large file downloads.
//
// This package handles:
//   - Connection pooling for high parallelism
//   - HEAD requests to get file metadata
//   - Range requests for chunked downloads
//   - Retry with exponential backoff
//   - ETag validation
//
// # Usage
//
//	client := http.NewClient(Options{
//	    MaxIdleConnsPerHost: 100,
//	    Timeout:             30 * time.Second,
//	    RetryAttempts:       5,
//	})
//
//	// Get file info
//	info, err := client.Head(ctx, url)
//	// info.Size, info.ETag, info.AcceptsRanges
//
//	// Download a range
//	resp, err := client.GetRange(ctx, url, startByte, endByte)
//	defer resp.Body.Close()
package http
