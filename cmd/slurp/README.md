# slurp CLI

Download files from HTTP URLs to cloud storage with parallel chunked transfers.

## Usage

```bash
slurp [flags]
```

## Required Flags

| Flag | Description |
|------|-------------|
| `--url` | Source URL to download |
| `--bucket` | Destination bucket URL (e.g., `gs://my-bucket`, `s3://my-bucket`) |
| `--object` | Destination object path within the bucket |

## Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--workers` | 16 | Number of parallel download workers |
| `--chunk-size` | 256MB | Size of each download chunk |
| `--progress` | false | Show progress output to stdout |
| `--retry-attempts` | 5 | Maximum retry attempts per chunk |
| `--retry-backoff` | 1s | Initial backoff duration for retries |
| `--retry-max-backoff` | 30s | Maximum backoff duration |
| `--state-interval` | 10 | Persist state every N completed chunks |
| `--force` | false | Force restart, ignoring existing state |
| `--config` | | Path to config file (YAML) |

## Environment Variables

All flags can be set via environment variables with `SLURP_` prefix:

```bash
export SLURP_WORKERS=32
export SLURP_CHUNK_SIZE=512MB
slurp --url "..." --bucket "..." --object "..."
```

## Configuration File

Create a YAML config file:

```yaml
# slurp.yaml
workers: 32
chunk_size: 512MB
progress: true
retry:
  attempts: 5
  backoff: 1s
  max_backoff: 30s
```

Use with: `slurp --config slurp.yaml --url "..." --bucket "..." --object "..."`

## Examples

### Basic Download

```bash
slurp --url "https://example.com/large-file.tar.gz" \
      --bucket "gs://my-bucket" \
      --object "downloads/large-file.tar.gz"
```

### High-Performance Download

```bash
slurp --url "https://example.com/huge-file.tar.gz" \
      --bucket "gs://my-bucket" \
      --object "downloads/huge-file.tar.gz" \
      --workers 64 \
      --chunk-size 512MB \
      --progress
```

### Download to S3

```bash
slurp --url "https://example.com/file.tar.gz" \
      --bucket "s3://my-bucket?region=us-east-1" \
      --object "downloads/file.tar.gz"
```

### Force Restart

```bash
slurp --url "https://example.com/file.tar.gz" \
      --bucket "gs://my-bucket" \
      --object "downloads/file.tar.gz" \
      --force
```

## Resume Behavior

Slurp automatically resumes interrupted downloads:

1. State is stored based on the destination object path
2. On startup, checks if state exists in the bucket
3. If source ETag matches (stored in metadata), resumes from where it left off
4. If source changed, requires `--force` to restart

## Output

### Progress Mode (`--progress`)

```
[slurp] Downloading: https://example.com/file.tar.gz
[slurp] Total size: 2.5 TB | Chunks: 10240 x 256MB | Workers: 16
[slurp] Progress: 45.2% | 1.13 TB / 2.5 TB | Speed: 1.2 GB/s | ETA: 18m 32s
[slurp] Chunks: 4628 completed | 16 in-progress | 5596 pending
```

### Completion

```
[slurp] Download complete: gs://my-bucket/downloads/file.tar.gz
[slurp] Manifest: gs://my-bucket/downloads/file.tar.gz.manifest.json
```

## Exit Codes

| Code | Description |
|------|-------------|
| 0 | Success |
| 1 | General error |
| 2 | Invalid arguments |
| 3 | Source URL not accessible |
| 4 | Range requests not supported |
| 5 | Storage error |
| 6 | Source file changed (ETag mismatch) |
