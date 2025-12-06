package progress

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Options configures the progress reporter.
type Options struct {
	// TotalSize is the total size in bytes to download.
	TotalSize int64

	// TotalChunks is the total number of chunks.
	TotalChunks int

	// Workers is the number of parallel workers.
	Workers int

	// Output is where to write progress output.
	// Default: os.Stdout
	Output io.Writer

	// UpdateInterval is how often to update the progress display.
	// Default: 500ms
	UpdateInterval time.Duration

	// SourceURL is the URL being downloaded (for display).
	SourceURL string

	// ChunkSize is the size of each chunk (for display).
	ChunkSize int64
}

// Reporter outputs human-readable progress information.
type Reporter struct {
	opts Options

	mu              sync.Mutex
	completedBytes  atomic.Int64
	completedChunks atomic.Int32
	inProgress      atomic.Int32
	startTime       time.Time
	lastUpdate      time.Time
	lastBytes       int64
	stopCh          chan struct{}
	stopped         bool
}

// NewReporter creates a new progress reporter.
func NewReporter(opts Options) *Reporter {
	if opts.Output == nil {
		opts.Output = os.Stdout
	}
	if opts.UpdateInterval == 0 {
		opts.UpdateInterval = 500 * time.Millisecond
	}

	return &Reporter{
		opts:   opts,
		stopCh: make(chan struct{}),
	}
}

// Start begins outputting progress information.
func (r *Reporter) Start() {
	r.startTime = time.Now()
	r.lastUpdate = r.startTime

	// Print header
	fmt.Fprintf(r.opts.Output, "[slurp] Downloading: %s\n", r.opts.SourceURL)
	fmt.Fprintf(r.opts.Output, "[slurp] Total size: %s | Chunks: %d x %s | Workers: %d\n",
		formatBytes(r.opts.TotalSize),
		r.opts.TotalChunks,
		formatBytes(r.opts.ChunkSize),
		r.opts.Workers,
	)

	go r.updateLoop()
}

// Stop stops the progress reporter.
func (r *Reporter) Stop() {
	r.mu.Lock()
	if r.stopped {
		r.mu.Unlock()
		return
	}
	r.stopped = true
	r.mu.Unlock()

	close(r.stopCh)
}

// ChunkStarted marks a chunk as in progress.
func (r *Reporter) ChunkStarted() {
	r.inProgress.Add(1)
}

// ChunkCompleted marks a chunk as completed.
func (r *Reporter) ChunkCompleted(size int64) {
	r.completedBytes.Add(size)
	r.completedChunks.Add(1)
	r.inProgress.Add(-1)
}

// ChunkFailed marks a chunk as failed (removes from in-progress).
func (r *Reporter) ChunkFailed() {
	r.inProgress.Add(-1)
}

// updateLoop periodically updates the progress display.
func (r *Reporter) updateLoop() {
	ticker := time.NewTicker(r.opts.UpdateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			r.printFinalStatus()
			return
		case <-ticker.C:
			r.printProgress()
		}
	}
}

// printProgress outputs the current progress.
func (r *Reporter) printProgress() {
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
			eta = formatDuration(time.Duration(etaSeconds * float64(time.Second)))
		} else {
			eta = "calculating..."
		}
	}

	pending := r.opts.TotalChunks - completedChunks - inProgress
	if pending < 0 {
		pending = 0
	}

	fmt.Fprintf(r.opts.Output, "\r[slurp] Progress: %.1f%% | %s / %s | Speed: %s/s | ETA: %s    ",
		percent,
		formatBytes(completed),
		formatBytes(r.opts.TotalSize),
		formatBytes(int64(speed)),
		eta,
	)
	fmt.Fprintf(r.opts.Output, "\n[slurp] Chunks: %d completed | %d in-progress | %d pending    \033[A",
		completedChunks,
		inProgress,
		pending,
	)
}

// printFinalStatus outputs the final status.
func (r *Reporter) printFinalStatus() {
	completed := r.completedBytes.Load()
	completedChunks := int(r.completedChunks.Load())
	duration := time.Since(r.startTime)
	avgSpeed := float64(completed) / duration.Seconds()

	fmt.Fprintf(r.opts.Output, "\r[slurp] Progress: 100.0%% | %s / %s | Speed: %s/s | Complete!    \n",
		formatBytes(completed),
		formatBytes(r.opts.TotalSize),
		formatBytes(int64(avgSpeed)),
	)
	fmt.Fprintf(r.opts.Output, "[slurp] Chunks: %d completed | 0 in-progress | 0 pending    \n",
		completedChunks,
	)
	fmt.Fprintf(r.opts.Output, "[slurp] Total time: %s | Average speed: %s/s\n",
		formatDuration(duration),
		formatBytes(int64(avgSpeed)),
	)
}

// formatBytes formats bytes as a human-readable string.
func formatBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case b >= TB:
		return fmt.Sprintf("%.2f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.2f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.2f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.2f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// formatDuration formats a duration as a human-readable string.
func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	if d < time.Hour {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", m, s)
	}
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%dh %dm %ds", h, m, s)
}

// FormatBytes is exported for use by other packages.
func FormatBytes(b int64) string {
	return formatBytes(b)
}

// ParseBytes parses a human-readable byte string (e.g., "256MB").
func ParseBytes(s string) (int64, error) {
	var multiplier int64 = 1
	s = trimSuffix(s, " ")

	switch {
	case hasSuffix(s, "TB"):
		multiplier = 1024 * 1024 * 1024 * 1024
		s = s[:len(s)-2]
	case hasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		s = s[:len(s)-2]
	case hasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		s = s[:len(s)-2]
	case hasSuffix(s, "KB"):
		multiplier = 1024
		s = s[:len(s)-2]
	case hasSuffix(s, "B"):
		s = s[:len(s)-1]
	}

	var value float64
	_, err := fmt.Sscanf(s, "%f", &value)
	if err != nil {
		return 0, fmt.Errorf("invalid byte string: %s", s)
	}

	return int64(value * float64(multiplier)), nil
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

func trimSuffix(s, suffix string) string {
	for hasSuffix(s, suffix) {
		s = s[:len(s)-len(suffix)]
	}
	return s
}
