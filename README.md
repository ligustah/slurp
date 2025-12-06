# Slurp

High-performance parallel file downloader to cloud storage.

## Features

- Parallel chunked downloads using HTTP range requests
- Direct streaming to cloud storage (no local disk required)
- Automatic resume for interrupted downloads
- Storage-agnostic via gocloud.dev/blob (GCS, S3, Azure, etc.)
- Progress reporting
- Configurable chunk size and worker count

## Installation

```bash
go install github.com/username/slurp/cmd/slurp@latest
```

## Quick Start

```bash
# Download a large file to GCS
slurp --url "https://example.com/file.tar.gz" \
      --bucket "gs://my-bucket" \
      --object "downloads/file.tar.gz"

# Resume automatically if interrupted
slurp --url "https://example.com/file.tar.gz" \
      --bucket "gs://my-bucket" \
      --object "downloads/file.tar.gz"
```

## How It Works

1. HEAD request to get file size and check range request support
2. Split file into chunks (default 256MB)
3. Download chunks in parallel using worker goroutines
4. Stream each chunk directly to cloud storage
5. Write manifest on completion for later retrieval

## Reading Downloaded Files

Use the sharded package to read chunked files back:

```go
f, _ := sharded.Read(ctx, "gs://my-bucket", "downloads/file.tar.gz")
defer f.Close()
io.Copy(dst, f)  // Streams all chunks in order
```

## Documentation

- [CLI Reference](cmd/slurp/README.md)
- [sharded Package](pkg/sharded/doc.go)

## License

MIT
