package progress

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"
)

// Options configures the progress reporter.
type Options struct {
	// TotalSize is the total size in bytes to download.
	TotalSize int64

	// TotalChunks is the total number of chunks.
	TotalChunks int

	// Workers is the number of parallel workers.
	Workers int

	// UpdateInterval is how often to log progress.
	// Default: 5s
	UpdateInterval time.Duration

	// SourceURL is the URL being downloaded (for display).
	SourceURL string

	// ChunkSize is the size of each chunk (for display).
	ChunkSize int64
}

// Reporter outputs progress information via slog.
type Reporter struct {
	opts Options

	completedBytes  atomic.Int64
	completedChunks atomic.Int32
	inProgress      atomic.Int32
	startTime       time.Time
	lastBytes       int64
	lastUpdate      time.Time

	mu      sync.Mutex
	stopCh  chan struct{}
	doneCh  chan struct{}
	stopped bool
}

// NewReporter creates a new progress reporter.
func NewReporter(opts Options) *Reporter {
	if opts.UpdateInterval == 0 {
		opts.UpdateInterval = 5 * time.Second
	}

	return &Reporter{
		opts:   opts,
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
}

// Start begins outputting progress information.
func (r *Reporter) Start() {
	r.startTime = time.Now()
	r.lastUpdate = r.startTime

	slog.Info("starting download",
		"url", r.opts.SourceURL,
		"size", humanize.IBytes(uint64(r.opts.TotalSize)),
		"chunks", r.opts.TotalChunks,
		"chunk_size", humanize.IBytes(uint64(r.opts.ChunkSize)),
		"workers", r.opts.Workers,
	)

	go r.updateLoop()
}

// Stop stops the progress reporter and waits for final output.
func (r *Reporter) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()

	close(r.stopCh)
	<-r.doneCh
}

// ChunkStarted marks a chunk as in progress.
func (r *Reporter) ChunkStarted() {
	r.inProgress.Add(1)
}

// ChunkCompleted marks a chunk as completed.
// Note: bytes should already have been reported via BytesWritten during streaming.
func (r *Reporter) ChunkCompleted() {
	r.completedChunks.Add(1)
	r.inProgress.Add(-1)
}

// ChunkFailed marks a chunk as failed (removes from in-progress).
func (r *Reporter) ChunkFailed() {
	r.inProgress.Add(-1)
}

// BytesWritten adds to the running byte count for in-progress tracking.
// Call this as data streams in, before ChunkCompleted.
func (r *Reporter) BytesWritten(n int64) {
	r.completedBytes.Add(n)
}

// SetResumeState sets the initial progress when resuming a download.
// Call this before Start() to show accurate progress from the beginning.
func (r *Reporter) SetResumeState(completedChunks int, completedBytes int64) {
	r.completedChunks.Store(int32(completedChunks))
	r.completedBytes.Store(completedBytes)
	r.lastBytes = completedBytes
}

// updateLoop periodically logs progress.
func (r *Reporter) updateLoop() {
	defer close(r.doneCh)
	ticker := time.NewTicker(r.opts.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.logFinalStatus()
			return
		case <-ticker.C:
			r.logProgress()
		}
	}
}

// logProgress outputs the current progress.
func (r *Reporter) logProgress() {
	now := time.Now()
	completed := r.completedBytes.Load()
	completedChunks := int(r.completedChunks.Load())
	inProgress := int(r.inProgress.Load())

	// Calculate speed
	elapsed := now.Sub(r.lastUpdate).Seconds()
	if elapsed < 0.1 {
		elapsed = 0.1
	}
	bytesThisPeriod := completed - r.lastBytes
	speed := float64(bytesThisPeriod) / elapsed

	r.lastUpdate = now
	r.lastBytes = completed

	// Calculate percentage and ETA
	var percent float64
	var eta string
	if r.opts.TotalSize > 0 {
		percent = float64(completed) / float64(r.opts.TotalSize) * 100
		if speed > 0 {
			remaining := float64(r.opts.TotalSize - completed)
			etaSeconds := remaining / speed
			eta = humanize.RelTime(time.Now(), time.Now().Add(time.Duration(etaSeconds)*time.Second), "", "")
		} else {
			eta = "calculating"
		}
	}

	pending := r.opts.TotalChunks - completedChunks - inProgress
	if pending < 0 {
		pending = 0
	}

	slog.Info("progress",
		"percent", fmt.Sprintf("%.1f%%", percent),
		"completed", humanize.IBytes(uint64(completed)),
		"total", humanize.IBytes(uint64(r.opts.TotalSize)),
		"speed", humanize.IBytes(uint64(speed))+"/s",
		"eta", eta,
		"chunks_done", completedChunks,
		"chunks_active", inProgress,
		"chunks_pending", pending,
	)
}

// logFinalStatus outputs the final status.
func (r *Reporter) logFinalStatus() {
	completed := r.completedBytes.Load()
	completedChunks := int(r.completedChunks.Load())
	duration := time.Since(r.startTime)
	var avgSpeed float64
	if duration.Seconds() > 0 {
		avgSpeed = float64(completed) / duration.Seconds()
	}

	slog.Info("download complete",
		"completed", humanize.IBytes(uint64(completed)),
		"total", humanize.IBytes(uint64(r.opts.TotalSize)),
		"chunks", completedChunks,
		"duration", duration.Round(time.Second).String(),
		"avg_speed", humanize.IBytes(uint64(avgSpeed))+"/s",
	)
}

// FormatBytes formats bytes as a human-readable string.
func FormatBytes(b int64) string {
	return humanize.IBytes(uint64(b))
}

// ParseBytes parses a human-readable byte string (e.g., "256MB").
func ParseBytes(s string) (int64, error) {
	b, err := humanize.ParseBytes(s)
	if err != nil {
		return 0, fmt.Errorf("invalid byte string: %s", s)
	}
	return int64(b), nil
}
