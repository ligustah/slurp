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

// ErrShardFilled is returned by File.Next when the shard at the current index
// has already been written in a previous session. Callers should continue to
// the next shard.
var ErrShardFilled = errors.New("shard already filled")

// ErrSourceChanged is returned when metadata indicates the source has changed
// since the last write attempt.
var ErrSourceChanged = errors.New("source changed since last attempt")

// Manifest describes a completed sharded file.
type Manifest struct {
	TotalSize   int64             `json:"total_size"`
	ShardSize   int64             `json:"shard_size"`
	PartsPrefix string            `json:"parts_prefix"`
	Shards      []ShardInfo       `json:"shards"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CompletedAt time.Time         `json:"completed_at"`
}

// ShardInfo describes a single shard in the manifest.
// The index is implicit from the array position.
type ShardInfo struct {
	Object   string `json:"object"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum,omitempty"`
}

// ShardStatus represents the state of a shard during download.
type ShardStatus string

const (
	// ShardPending means the shard has not been started yet.
	ShardPending ShardStatus = "pending"
	// ShardInProgress means the shard is currently being downloaded.
	ShardInProgress ShardStatus = "in_progress"
	// ShardCompleted means the shard has been successfully written.
	ShardCompleted ShardStatus = "completed"
)

// ShardState tracks the state of a single shard during download.
// The index is implicit from the array position.
type ShardState struct {
	Status   ShardStatus `json:"status"`
	Object   string      `json:"object,omitempty"`
	Offset   int64       `json:"offset,omitempty"`
	Size     int64       `json:"size,omitempty"`
	Checksum string      `json:"checksum,omitempty"`
}

// state tracks write progress for resume support.
type state struct {
	TotalSize   int64             `json:"total_size,omitempty"`
	ShardSize   int64             `json:"shard_size"`
	PartsPrefix string            `json:"parts_prefix"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Shards      []ShardState      `json:"shards"`
	StartedAt   time.Time         `json:"started_at"`
}

// Options configures sharded file operations.
type Options struct {
	ShardSize       int64
	Size            int64
	Metadata        map[string]string
	VerifyChecksum  bool
	ComputeChecksum bool // Compute checksums during writes (default: true)
	StateInterval   int  // Persist state every N completed shards
	PrefetchCount   int  // Number of shards to prefetch ahead (0 = disabled)
}

// Option is a functional option for configuring sharded operations.
type Option func(*Options)

// WithShardSize sets the size of each shard.
func WithShardSize(size int64) Option {
	return func(o *Options) {
		o.ShardSize = size
	}
}

// WithSize sets the total size of the file. When set, File.Next returns io.EOF
// after all shards have been accounted for.
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
// When enabled and a shard has no stored checksum, verification is skipped for that shard.
func WithVerifyChecksum(verify bool) Option {
	return func(o *Options) {
		o.VerifyChecksum = verify
	}
}

// WithChecksum enables or disables SHA256 checksum computation during writes.
// When disabled, shards are written without computing checksums, which improves
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

// WithStateInterval sets how often to persist state (every N completed shards).
func WithStateInterval(n int) Option {
	return func(o *Options) {
		o.StateInterval = n
	}
}

// WithPrefetch sets the number of shards to prefetch ahead during reads.
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
	totalShards    int
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

	if opts.ShardSize <= 0 {
		return nil, errors.New("sharded: shard size must be positive")
	}

	// Use destination-based prefix for chunks
	partsPrefix := dest + ".shards/"

	f := &File{
		bucket:       bucket,
		dest:         dest,
		opts:         opts,
		partsPrefix:  partsPrefix,
		currentIndex: 0,
	}

	// Calculate total chunks if size is known
	if opts.Size > 0 {
		f.totalShards = int((opts.Size + opts.ShardSize - 1) / opts.ShardSize)
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
				ShardSize:   f.opts.ShardSize,
				PartsPrefix: f.partsPrefix,
				Metadata:    f.opts.Metadata,
				Shards:      []ShardState{},
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

	// Count completed shards and reset in_progress shards to pending (they were interrupted)
	for i := range f.state.Shards {
		if f.state.Shards[i].Status == ShardCompleted {
			f.completedCount++
		} else if f.state.Shards[i].Status == ShardInProgress {
			// Interrupted shard - mark as pending for retry
			f.state.Shards[i].Status = ShardPending
		}
	}

	// Recalculate total shards if we now have size
	if f.opts.Size > 0 && s.TotalSize == 0 {
		f.state.TotalSize = f.opts.Size
		f.totalShards = int((f.opts.Size + f.opts.ShardSize - 1) / f.opts.ShardSize)
	} else if s.TotalSize > 0 {
		f.totalShards = int((s.TotalSize + s.ShardSize - 1) / s.ShardSize)
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

	// Delete all existing shards
	for _, shard := range f.state.Shards {
		if shard.Object != "" {
			path := f.partsPrefix + shard.Object
			if err := f.bucket.Delete(ctx, path); err != nil && !isNotExist(err) {
				return fmt.Errorf("delete shard %s: %w", path, err)
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
		ShardSize:   f.opts.ShardSize,
		PartsPrefix: f.partsPrefix,
		Metadata:    f.opts.Metadata,
		Shards:      []ShardState{},
		StartedAt:   time.Now(),
	}
	f.currentIndex = 0
	f.completedCount = 0

	return nil
}

// Next returns the next shard to be written.
// Returns ErrShardFilled if the shard was already written (resume case).
// Returns io.EOF when all shards have been accounted for (requires WithSize).
// Returns context error if the context is cancelled.
func (f *File) Next(ctx context.Context) (*Shard, error) {
	// Check context first to avoid marking shards as in_progress during shutdown
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.closed {
		return nil, errors.New("sharded: file is closed")
	}

	// Check if we've reached the end (only if size is known)
	if f.totalShards > 0 && f.currentIndex >= f.totalShards {
		return nil, io.EOF
	}

	idx := f.currentIndex
	f.currentIndex++

	// Calculate shard offset and length
	offset := int64(idx) * f.opts.ShardSize
	length := f.opts.ShardSize
	if f.opts.Size > 0 && offset+length > f.opts.Size {
		length = f.opts.Size - offset
	}

	shard := &Shard{
		file:            f,
		index:           idx,
		offset:          offset,
		length:          length,
		computeChecksum: f.opts.ComputeChecksum,
	}

	// Look up shard state by index
	shardState := f.findShard(idx)
	if shardState != nil {
		if shardState.Status == ShardCompleted {
			// Already completed - return info for verification if needed
			shard.info = &ShardInfo{
				Object:   shardState.Object,
				Offset:   shardState.Offset,
				Size:     shardState.Size,
				Checksum: shardState.Checksum,
			}
			return shard, ErrShardFilled
		}
		// Pending shard (from interrupted run) - mark as in_progress
		shardState.Status = ShardInProgress
	} else {
		// New shard - ensure array is large enough and set state
		for len(f.state.Shards) <= idx {
			f.state.Shards = append(f.state.Shards, ShardState{Status: ShardPending})
		}
		f.state.Shards[idx].Status = ShardInProgress
	}

	return shard, nil
}

// findShard returns the shard state for the given index, or nil if not found.
// Must be called with f.mu held.
func (f *File) findShard(idx int) *ShardState {
	if idx < len(f.state.Shards) {
		return &f.state.Shards[idx]
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

	// Build manifest from completed shards
	manifest := Manifest{
		TotalSize:   f.state.TotalSize,
		ShardSize:   f.state.ShardSize,
		PartsPrefix: f.partsPrefix,
		Metadata:    f.state.Metadata,
		CompletedAt: time.Now(),
	}

	// Build shards slice from state (index is implicit from array position)
	manifest.Shards = make([]ShardInfo, len(f.state.Shards))
	for i, ss := range f.state.Shards {
		manifest.Shards[i] = ShardInfo{
			Object:   ss.Object,
			Offset:   ss.Offset,
			Size:     ss.Size,
			Checksum: ss.Checksum,
		}
	}

	// Calculate total size if not set
	if manifest.TotalSize == 0 {
		for _, shard := range manifest.Shards {
			manifest.TotalSize += shard.Size
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

// CompletedCount returns the number of shards that have been written.
func (f *File) CompletedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.completedCount
}

// CompletedBytes returns the total bytes of all completed shards.
func (f *File) CompletedBytes() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var total int64
	for _, ss := range f.state.Shards {
		if ss.Status == ShardCompleted {
			total += ss.Size
		}
	}
	return total
}

// TotalShards returns the total number of shards, or 0 if size is unknown.
func (f *File) TotalShards() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.totalShards
}

// Shard represents a single shard being written.
type Shard struct {
	file            *File
	index           int
	offset          int64
	length          int64
	computeChecksum bool
	info            *ShardInfo // Set if chunk was already filled

	mu           sync.Mutex
	writer       *blob.Writer
	writerCancel context.CancelFunc // Cancel to abort the write
	hash         *sha256Writer
	size         int64
	closed       bool
}

// Index returns the shard index (0, 1, 2, ...).
func (s *Shard) Index() int {
	return s.index
}

// Offset returns the byte offset in the source data.
func (s *Shard) Offset() int64 {
	return s.offset
}

// Length returns the expected size of this shard.
func (s *Shard) Length() int64 {
	return s.length
}

// Write writes data to the shard.
func (s *Shard) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, errors.New("sharded: shard is closed")
	}

	// Lazy initialization
	if s.writer == nil {
		ctx, cancel := context.WithCancel(context.Background())
		s.writerCancel = cancel

		objectName := fmt.Sprintf("shard-%06d", s.index)
		path := s.file.partsPrefix + objectName

		w, err := s.file.bucket.NewWriter(ctx, path, nil)
		if err != nil {
			cancel()
			return 0, fmt.Errorf("create shard writer: %w", err)
		}
		s.writer = w
		if s.computeChecksum {
			s.hash = &sha256Writer{hash: sha256.New()}
		}
	}

	n, err = s.writer.Write(p)
	if err != nil {
		return n, err
	}

	if s.hash != nil {
		s.hash.Write(p[:n])
	}
	s.size += int64(n)

	return n, nil
}

// Abort cancels the shard write and cleans up any partial data from storage.
// Use this when a shard download fails or is cancelled.
// Marks the shard as pending so it can be retried.
// Safe to call multiple times or after Close.
func (s *Shard) Abort() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	s.closed = true

	if s.writer != nil {
		// Cancel the context first to abort the upload
		if s.writerCancel != nil {
			s.writerCancel()
		}
		// Must still close writer to release resources
		s.writer.Close()

		// Delete any partial data that was uploaded before cancellation.
		// GCS resumable uploads may have committed partial buffers.
		ctx := context.Background()
		objectName := fmt.Sprintf("shard-%06d", s.index)
		path := s.file.partsPrefix + objectName
		s.file.bucket.Delete(ctx, path) // Best effort, ignore errors
	}

	// Mark shard as pending so it can be retried
	s.file.mu.Lock()
	shardState := s.file.findShard(s.index)
	if shardState != nil {
		shardState.Status = ShardPending
	}
	s.file.mu.Unlock()
}

// Close closes the shard, persisting it to storage and updating in-memory state.
// Does NOT persist state to storage - call File.SaveState() periodically from
// the main goroutine for eventual consistency.
func (s *Shard) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	// If shard was already filled, nothing to do
	if s.info != nil {
		return nil
	}

	// If writer was never created (no writes), nothing to do
	if s.writer == nil {
		return nil
	}

	// Close the writer - this commits the blob to storage
	if err := s.writer.Close(); err != nil {
		return fmt.Errorf("close shard writer: %w", err)
	}

	// Update in-memory state - mark shard as completed
	objectName := fmt.Sprintf("shard-%06d", s.index)
	checksum := ""
	if s.hash != nil {
		checksum = s.hash.Sum()
	}

	s.file.mu.Lock()
	shardState := s.file.findShard(s.index)
	if shardState != nil {
		shardState.Status = ShardCompleted
		shardState.Object = objectName
		shardState.Offset = s.offset
		shardState.Size = s.size
		shardState.Checksum = checksum
	}
	s.file.completedCount++
	s.file.mu.Unlock()

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
