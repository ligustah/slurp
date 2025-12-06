package sharded

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"
)

// ErrChunkFilled is returned by File.Next when the chunk at the current index
// has already been written in a previous session. Callers should continue to
// the next chunk.
var ErrChunkFilled = errors.New("chunk already filled")

// ErrSourceChanged is returned when metadata indicates the source has changed
// since the last write attempt.
var ErrSourceChanged = errors.New("source changed since last attempt")

// Manifest describes a completed sharded file.
type Manifest struct {
	TotalSize   int64             `json:"total_size"`
	ChunkSize   int64             `json:"chunk_size"`
	PartsPrefix string            `json:"parts_prefix"`
	Chunks      []ChunkInfo       `json:"chunks"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CompletedAt time.Time         `json:"completed_at"`
}

// ChunkInfo describes a single chunk in the manifest.
type ChunkInfo struct {
	Index    int    `json:"index"`
	Object   string `json:"object"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum,omitempty"`
}

// ChunkStatus represents the state of a chunk during download.
type ChunkStatus string

const (
	// ChunkPending means the chunk has not been started yet.
	ChunkPending ChunkStatus = "pending"
	// ChunkInProgress means the chunk is currently being downloaded.
	ChunkInProgress ChunkStatus = "in_progress"
	// ChunkCompleted means the chunk has been successfully written.
	ChunkCompleted ChunkStatus = "completed"
)

// ChunkState tracks the state of a single chunk during download.
type ChunkState struct {
	Index    int         `json:"index"`
	Status   ChunkStatus `json:"status"`
	Object   string      `json:"object,omitempty"`
	Size     int64       `json:"size,omitempty"`
	Checksum string      `json:"checksum,omitempty"`
}

// state tracks write progress for resume support.
type state struct {
	TotalSize   int64             `json:"total_size,omitempty"`
	ChunkSize   int64             `json:"chunk_size"`
	PartsPrefix string            `json:"parts_prefix"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Chunks      []ChunkState      `json:"chunks"`
	StartedAt   time.Time         `json:"started_at"`
}

// Options configures sharded file operations.
type Options struct {
	ChunkSize       int64
	Size            int64
	Metadata        map[string]string
	VerifyChecksum  bool
	ComputeChecksum bool // Compute checksums during writes (default: true)
	StateInterval   int  // Persist state every N completed chunks
	PrefetchCount   int  // Number of chunks to prefetch ahead (0 = disabled)
}

// Option is a functional option for configuring sharded operations.
type Option func(*Options)

// WithChunkSize sets the size of each chunk.
func WithChunkSize(size int64) Option {
	return func(o *Options) {
		o.ChunkSize = size
	}
}

// WithSize sets the total size of the file. When set, File.Next returns io.EOF
// after all chunks have been accounted for.
func WithSize(size int64) Option {
	return func(o *Options) {
		o.Size = size
	}
}

// WithMetadata sets caller-defined metadata stored in the manifest.
func WithMetadata(metadata map[string]string) Option {
	return func(o *Options) {
		o.Metadata = metadata
	}
}

// WithVerifyChecksum enables checksum verification during reads.
// When enabled and a chunk has no stored checksum, verification is skipped for that chunk.
func WithVerifyChecksum(verify bool) Option {
	return func(o *Options) {
		o.VerifyChecksum = verify
	}
}

// WithChecksum enables or disables SHA256 checksum computation during writes.
// When disabled, chunks are written without computing checksums, which improves
// write performance but prevents later verification with WithVerifyChecksum.
// Default is true (checksums enabled).
//
// Disabling checksums is useful when relying on object store integrity guarantees
// (e.g., S3/GCS built-in checksums) and write performance is critical.
func WithChecksum(compute bool) Option {
	return func(o *Options) {
		o.ComputeChecksum = compute
	}
}

// WithStateInterval sets how often to persist state (every N completed chunks).
func WithStateInterval(n int) Option {
	return func(o *Options) {
		o.StateInterval = n
	}
}

// WithPrefetch sets the number of chunks to prefetch ahead during reads.
// This can improve read performance by overlapping I/O with processing.
// Set to 0 to disable prefetching (default).
func WithPrefetch(n int) Option {
	return func(o *Options) {
		o.PrefetchCount = n
	}
}

// File represents a sharded file being written.
type File struct {
	bucket      *blob.Bucket
	dest        string
	opts        Options
	partsPrefix string

	mu             sync.Mutex
	state          *state
	currentIndex   int
	completedCount int
	totalChunks    int
	closed         bool
}

// Write creates or resumes a sharded file write operation.
// If state exists from a previous incomplete write, it will be loaded for resume.
func Write(ctx context.Context, bucket *blob.Bucket, dest string, options ...Option) (*File, error) {
	opts := Options{
		StateInterval:   10,   // Default: persist state every 10 chunks
		ComputeChecksum: true, // Default: compute checksums
	}
	for _, opt := range options {
		opt(&opts)
	}

	if opts.ChunkSize <= 0 {
		return nil, errors.New("sharded: chunk size must be positive")
	}

	// Generate parts prefix from destination hash
	hash := sha256.Sum256([]byte(dest))
	partsPrefix := ".sharded/" + hex.EncodeToString(hash[:8]) + "/"

	f := &File{
		bucket:       bucket,
		dest:         dest,
		opts:         opts,
		partsPrefix:  partsPrefix,
		currentIndex: 0,
	}

	// Calculate total chunks if size is known
	if opts.Size > 0 {
		f.totalChunks = int((opts.Size + opts.ChunkSize - 1) / opts.ChunkSize)
	}

	// Try to load existing state
	if err := f.loadState(ctx); err != nil {
		return nil, fmt.Errorf("sharded: load state: %w", err)
	}

	return f, nil
}

// loadState attempts to load existing state for resume.
func (f *File) loadState(ctx context.Context) error {
	statePath := f.partsPrefix + "state.json"
	data, err := f.bucket.ReadAll(ctx, statePath)
	if err != nil {
		if isNotExist(err) {
			// No existing state, start fresh
			f.state = &state{
				TotalSize:   f.opts.Size,
				ChunkSize:   f.opts.ChunkSize,
				PartsPrefix: f.partsPrefix,
				Metadata:    f.opts.Metadata,
				Chunks:      []ChunkState{},
				StartedAt:   time.Now(),
			}
			return nil
		}
		return err
	}

	var s state
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("unmarshal state: %w", err)
	}

	f.state = &s
	f.partsPrefix = s.PartsPrefix

	// Count completed chunks and reset in_progress chunks to pending (they were interrupted)
	for i := range f.state.Chunks {
		if f.state.Chunks[i].Status == ChunkCompleted {
			f.completedCount++
		} else if f.state.Chunks[i].Status == ChunkInProgress {
			// Interrupted chunk - mark as pending for retry
			f.state.Chunks[i].Status = ChunkPending
		}
	}

	// Recalculate total chunks if we now have size
	if f.opts.Size > 0 && s.TotalSize == 0 {
		f.state.TotalSize = f.opts.Size
		f.totalChunks = int((f.opts.Size + f.opts.ChunkSize - 1) / f.opts.ChunkSize)
	} else if s.TotalSize > 0 {
		f.totalChunks = int((s.TotalSize + s.ChunkSize - 1) / s.ChunkSize)
	}

	return nil
}

// SaveState persists the current state for resume.
// Call this periodically from the main goroutine. Thread-safe.
// With eventual consistency, it's OK if some updates are lost - the blobs
// in storage are the source of truth and can be rediscovered on resume.
func (f *File) SaveState(ctx context.Context) error {
	f.mu.Lock()
	data, err := json.MarshalIndent(f.state, "", "  ")
	f.mu.Unlock()
	if err != nil {
		return err
	}
	statePath := f.partsPrefix + "state.json"
	return f.bucket.WriteAll(ctx, statePath, data, nil)
}

// Metadata returns the metadata stored in the current state.
// Use this to check values like source ETag for resume validation.
func (f *File) Metadata() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state == nil {
		return nil
	}
	return f.state.Metadata
}

// Reset discards existing state and starts fresh.
func (f *File) Reset(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Delete all existing chunks
	for _, chunk := range f.state.Chunks {
		if chunk.Object != "" {
			path := f.partsPrefix + chunk.Object
			if err := f.bucket.Delete(ctx, path); err != nil && !isNotExist(err) {
				return fmt.Errorf("delete chunk %s: %w", path, err)
			}
		}
	}

	// Delete state file
	statePath := f.partsPrefix + "state.json"
	if err := f.bucket.Delete(ctx, statePath); err != nil && !isNotExist(err) {
		return fmt.Errorf("delete state: %w", err)
	}

	// Reset to fresh state
	f.state = &state{
		TotalSize:   f.opts.Size,
		ChunkSize:   f.opts.ChunkSize,
		PartsPrefix: f.partsPrefix,
		Metadata:    f.opts.Metadata,
		Chunks:      []ChunkState{},
		StartedAt:   time.Now(),
	}
	f.currentIndex = 0
	f.completedCount = 0

	return nil
}

// Next returns the next chunk to be written.
// Returns ErrChunkFilled if the chunk was already written (resume case).
// Returns io.EOF when all chunks have been accounted for (requires WithSize).
func (f *File) Next(ctx context.Context) (*Chunk, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, errors.New("sharded: file is closed")
	}

	// Check if we've reached the end (only if size is known)
	if f.totalChunks > 0 && f.currentIndex >= f.totalChunks {
		return nil, io.EOF
	}

	idx := f.currentIndex
	f.currentIndex++

	// Calculate chunk offset and length
	offset := int64(idx) * f.opts.ChunkSize
	length := f.opts.ChunkSize
	if f.opts.Size > 0 && offset+length > f.opts.Size {
		length = f.opts.Size - offset
	}

	chunk := &Chunk{
		file:            f,
		index:           idx,
		offset:          offset,
		length:          length,
		computeChecksum: f.opts.ComputeChecksum,
	}

	// Look up chunk state
	chunkState := f.findChunk(idx)
	if chunkState != nil {
		if chunkState.Status == ChunkCompleted {
			// Already completed - return info for verification if needed
			chunk.info = &ChunkInfo{
				Index:    chunkState.Index,
				Object:   chunkState.Object,
				Size:     chunkState.Size,
				Checksum: chunkState.Checksum,
			}
			return chunk, ErrChunkFilled
		}
		// Pending chunk (from interrupted run) - mark as in_progress
		chunkState.Status = ChunkInProgress
	} else {
		// New chunk - add to state as in_progress
		f.state.Chunks = append(f.state.Chunks, ChunkState{
			Index:  idx,
			Status: ChunkInProgress,
		})
	}

	return chunk, nil
}

// findChunk returns the chunk state for the given index, or nil if not found.
// Must be called with f.mu held.
func (f *File) findChunk(idx int) *ChunkState {
	for i := range f.state.Chunks {
		if f.state.Chunks[i].Index == idx {
			return &f.state.Chunks[i]
		}
	}
	return nil
}

// Complete finalizes the sharded file, writing the manifest and cleaning up state.
func (f *File) Complete(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return errors.New("sharded: file is already closed")
	}
	f.closed = true

	// Build manifest from completed chunks
	manifest := Manifest{
		TotalSize:   f.state.TotalSize,
		ChunkSize:   f.state.ChunkSize,
		PartsPrefix: f.partsPrefix,
		Metadata:    f.state.Metadata,
		CompletedAt: time.Now(),
	}

	// Build sorted chunks slice - only include completed chunks
	manifest.Chunks = make([]ChunkInfo, f.completedCount)
	for _, cs := range f.state.Chunks {
		if cs.Status == ChunkCompleted {
			manifest.Chunks[cs.Index] = ChunkInfo{
				Index:    cs.Index,
				Object:   cs.Object,
				Size:     cs.Size,
				Checksum: cs.Checksum,
			}
		}
	}

	// Calculate total size if not set
	if manifest.TotalSize == 0 {
		for _, chunk := range manifest.Chunks {
			manifest.TotalSize += chunk.Size
		}
	}

	// Write manifest
	manifestPath := f.dest + ".manifest.json"
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := f.bucket.WriteAll(ctx, manifestPath, manifestData, nil); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Delete state file
	statePath := f.partsPrefix + "state.json"
	if err := f.bucket.Delete(ctx, statePath); err != nil && !isNotExist(err) {
		return fmt.Errorf("delete state: %w", err)
	}

	return nil
}

// CompletedCount returns the number of chunks that have been written.
func (f *File) CompletedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completedCount
}

// TotalChunks returns the total number of chunks, or 0 if size is unknown.
func (f *File) TotalChunks() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.totalChunks
}

// Chunk represents a single chunk being written.
type Chunk struct {
	file            *File
	index           int
	offset          int64
	length          int64
	computeChecksum bool
	info            *ChunkInfo // Set if chunk was already filled

	mu           sync.Mutex
	writer       *blob.Writer
	writerCancel context.CancelFunc // Cancel to abort the write
	hash         *sha256Writer
	size         int64
	closed       bool
}

// Index returns the chunk index (0, 1, 2, ...).
func (c *Chunk) Index() int {
	return c.index
}

// Offset returns the byte offset in the source data.
func (c *Chunk) Offset() int64 {
	return c.offset
}

// Length returns the expected size of this chunk.
func (c *Chunk) Length() int64 {
	return c.length
}

// Write writes data to the chunk.
func (c *Chunk) Write(p []byte) (n int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, errors.New("sharded: chunk is closed")
	}

	// Lazy initialization
	if c.writer == nil {
		ctx, cancel := context.WithCancel(context.Background())
		c.writerCancel = cancel

		objectName := fmt.Sprintf("chunk-%06d", c.index)
		path := c.file.partsPrefix + objectName

		w, err := c.file.bucket.NewWriter(ctx, path, nil)
		if err != nil {
			cancel()
			return 0, fmt.Errorf("create chunk writer: %w", err)
		}
		c.writer = w
		if c.computeChecksum {
			c.hash = &sha256Writer{hash: sha256.New()}
		}
	}

	n, err = c.writer.Write(p)
	if err != nil {
		return n, err
	}

	if c.hash != nil {
		c.hash.Write(p[:n])
	}
	c.size += int64(n)

	return n, nil
}

// Abort cancels the chunk write and cleans up any partial data from storage.
// Use this when a chunk download fails or is cancelled.
// Marks the chunk as pending so it can be retried.
// Safe to call multiple times or after Close.
func (c *Chunk) Abort() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.closed = true

	if c.writer != nil {
		// Cancel the context first to abort the upload
		if c.writerCancel != nil {
			c.writerCancel()
		}
		// Must still close writer to release resources
		c.writer.Close()

		// Delete any partial data that was uploaded before cancellation.
		// GCS resumable uploads may have committed partial buffers.
		ctx := context.Background()
		objectName := fmt.Sprintf("chunk-%06d", c.index)
		path := c.file.partsPrefix + objectName
		c.file.bucket.Delete(ctx, path) // Best effort, ignore errors
	}

	// Mark chunk as pending so it can be retried
	c.file.mu.Lock()
	chunkState := c.file.findChunk(c.index)
	if chunkState != nil {
		chunkState.Status = ChunkPending
	}
	c.file.mu.Unlock()
}

// Close closes the chunk, persisting it to storage and updating in-memory state.
// Does NOT persist state to storage - call File.SaveState() periodically from
// the main goroutine for eventual consistency.
func (c *Chunk) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	c.closed = true

	// If chunk was already filled, nothing to do
	if c.info != nil {
		return nil
	}

	// If writer was never created (no writes), nothing to do
	if c.writer == nil {
		return nil
	}

	// Close the writer - this commits the blob to storage
	if err := c.writer.Close(); err != nil {
		return fmt.Errorf("close chunk writer: %w", err)
	}

	// Update in-memory state - mark chunk as completed
	objectName := fmt.Sprintf("chunk-%06d", c.index)
	checksum := ""
	if c.hash != nil {
		checksum = c.hash.Sum()
	}

	c.file.mu.Lock()
	chunkState := c.file.findChunk(c.index)
	if chunkState != nil {
		chunkState.Status = ChunkCompleted
		chunkState.Object = objectName
		chunkState.Size = c.size
		chunkState.Checksum = checksum
	}
	c.file.completedCount++
	c.file.mu.Unlock()

	return nil
}

// sha256Writer wraps a hash for computing checksums.
type sha256Writer struct {
	hash io.Writer
	sum  []byte
}

func (w *sha256Writer) Write(p []byte) (int, error) {
	return w.hash.Write(p)
}

func (w *sha256Writer) Sum() string {
	if h, ok := w.hash.(interface{ Sum([]byte) []byte }); ok {
		return hex.EncodeToString(h.Sum(nil))
	}
	return ""
}

// isNotExist returns true if the error indicates the object doesn't exist.
func isNotExist(err error) bool {
	return gcerrors.Code(err) == gcerrors.NotFound
}
