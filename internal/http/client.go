package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Common errors.
var (
	ErrRangeNotSupported = errors.New("http: server does not support range requests")
	ErrNotFound          = errors.New("http: resource not found")
	ErrForbidden         = errors.New("http: access forbidden")
	ErrUnauthorized      = errors.New("http: unauthorized")
	ErrServerError       = errors.New("http: server error")
)

// Options configures the HTTP client.
type Options struct {
	// MaxIdleConnsPerHost sets the maximum idle connections per host.
	// Default: 100
	MaxIdleConnsPerHost int

	// Timeout for individual requests.
	// Default: 30s
	Timeout time.Duration

	// RetryAttempts is the maximum number of retry attempts.
	// Default: 5
	RetryAttempts int

	// RetryBackoff is the initial backoff duration.
	// Default: 1s
	RetryBackoff time.Duration

	// RetryMaxBackoff is the maximum backoff duration.
	// Default: 30s
	RetryMaxBackoff time.Duration
}

// DefaultOptions returns options with sensible defaults.
func DefaultOptions() Options {
	return Options{
		MaxIdleConnsPerHost: 100,
		Timeout:             30 * time.Second,
		RetryAttempts:       5,
		RetryBackoff:        time.Second,
		RetryMaxBackoff:     30 * time.Second,
	}
}

// FileInfo contains metadata about a remote file.
type FileInfo struct {
	Size          int64
	ETag          string
	AcceptsRanges bool
	ContentType   string
	LastModified  time.Time
}

// RangeResponse represents a response from a range request.
type RangeResponse struct {
	Body          io.ReadCloser
	ContentLength int64
	ETag          string
}

// Client is an HTTP client optimized for large file downloads.
type Client struct {
	client *http.Client
	opts   Options
}

// NewClient creates a new HTTP client with the given options.
func NewClient(opts Options) *Client {
	transport := &http.Transport{
		MaxIdleConnsPerHost: opts.MaxIdleConnsPerHost,
		MaxIdleConns:        opts.MaxIdleConnsPerHost * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true, // We want raw bytes for range requests
	}

	return &Client{
		client: &http.Client{
			Transport: transport,
			Timeout:   opts.Timeout,
		},
		opts: opts,
	}
}

// Head performs a HEAD request to get file metadata.
func (c *Client) Head(ctx context.Context, url string) (*FileInfo, error) {
	var info *FileInfo
	var lastErr error

	for attempt := 0; attempt <= c.opts.RetryAttempts; attempt++ {
		if attempt > 0 {
			if err := c.backoff(ctx, attempt); err != nil {
				return nil, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, resp.Status)
			continue
		}

		if err := checkStatusCode(resp.StatusCode); err != nil {
			return nil, err
		}

		info = &FileInfo{
			Size:          resp.ContentLength,
			ETag:          cleanETag(resp.Header.Get("ETag")),
			AcceptsRanges: resp.Header.Get("Accept-Ranges") == "bytes",
			ContentType:   resp.Header.Get("Content-Type"),
		}

		if lm := resp.Header.Get("Last-Modified"); lm != "" {
			if t, err := http.ParseTime(lm); err == nil {
				info.LastModified = t
			}
		}

		return info, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("head request failed after %d attempts: %w", c.opts.RetryAttempts+1, lastErr)
	}
	return info, nil
}

// GetRange performs a range request to download a portion of the file.
// startByte and endByte are inclusive (like HTTP Range header).
func (c *Client) GetRange(ctx context.Context, url string, startByte, endByte int64) (*RangeResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= c.opts.RetryAttempts; attempt++ {
		if attempt > 0 {
			if err := c.backoff(ctx, attempt); err != nil {
				return nil, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startByte, endByte))

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		// Server errors are retryable
		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, resp.Status)
			continue
		}

		// Check for successful range response
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
				return nil, ErrRangeNotSupported
			}
			if err := checkStatusCode(resp.StatusCode); err != nil {
				return nil, err
			}
		}

		// If server returns 200 instead of 206, it doesn't support range requests
		if resp.StatusCode == http.StatusOK {
			// Check if the Content-Range header is present (some servers return 200 but with the range)
			if resp.Header.Get("Content-Range") == "" {
				resp.Body.Close()
				return nil, ErrRangeNotSupported
			}
		}

		return &RangeResponse{
			Body:          resp.Body,
			ContentLength: resp.ContentLength,
			ETag:          cleanETag(resp.Header.Get("ETag")),
		}, nil
	}

	return nil, fmt.Errorf("range request failed after %d attempts: %w", c.opts.RetryAttempts+1, lastErr)
}

// Get performs a simple GET request.
func (c *Client) Get(ctx context.Context, url string) (io.ReadCloser, error) {
	var lastErr error

	for attempt := 0; attempt <= c.opts.RetryAttempts; attempt++ {
		if attempt > 0 {
			if err := c.backoff(ctx, attempt); err != nil {
				return nil, err
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode >= 500 {
			resp.Body.Close()
			lastErr = fmt.Errorf("%w: %d %s", ErrServerError, resp.StatusCode, resp.Status)
			continue
		}

		if err := checkStatusCode(resp.StatusCode); err != nil {
			resp.Body.Close()
			return nil, err
		}

		return resp.Body, nil
	}

	return nil, fmt.Errorf("get request failed after %d attempts: %w", c.opts.RetryAttempts+1, lastErr)
}

// backoff waits for an exponentially increasing duration with jitter.
func (c *Client) backoff(ctx context.Context, attempt int) error {
	backoff := c.opts.RetryBackoff * time.Duration(1<<uint(attempt-1))
	if backoff > c.opts.RetryMaxBackoff {
		backoff = c.opts.RetryMaxBackoff
	}

	// Add jitter: 0.5 to 1.5 of backoff
	jitter := time.Duration(float64(backoff) * (0.5 + rand.Float64()))

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
		return nil
	}
}

// checkStatusCode returns an appropriate error for non-success status codes.
func checkStatusCode(code int) error {
	switch {
	case code >= 200 && code < 300:
		return nil
	case code == http.StatusNotFound:
		return ErrNotFound
	case code == http.StatusForbidden:
		return ErrForbidden
	case code == http.StatusUnauthorized:
		return ErrUnauthorized
	default:
		return fmt.Errorf("unexpected status code: %d", code)
	}
}

// cleanETag removes quotes from an ETag value.
func cleanETag(etag string) string {
	etag = strings.TrimPrefix(etag, "W/")
	etag = strings.Trim(etag, `"`)
	return etag
}

// ParseContentRange parses a Content-Range header value.
// Returns start, end, total bytes. Total may be -1 if unknown.
func ParseContentRange(header string) (start, end, total int64, err error) {
	// Format: bytes start-end/total or bytes start-end/*
	header = strings.TrimPrefix(header, "bytes ")
	parts := strings.Split(header, "/")
	if len(parts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range format: %s", header)
	}

	rangeParts := strings.Split(parts[0], "-")
	if len(rangeParts) != 2 {
		return 0, 0, 0, fmt.Errorf("invalid Content-Range format: %s", header)
	}

	start, err = strconv.ParseInt(rangeParts[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid start byte: %w", err)
	}

	end, err = strconv.ParseInt(rangeParts[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("invalid end byte: %w", err)
	}

	if parts[1] == "*" {
		total = -1
	} else {
		total, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("invalid total bytes: %w", err)
		}
	}

	return start, end, total, nil
}
