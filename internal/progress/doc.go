// Package progress provides progress reporting for downloads.
//
// This package outputs human-readable progress information to stdout,
// including completion percentage, transfer speed, and ETA.
//
// # Usage
//
//	reporter := progress.NewReporter(Options{
//	    TotalSize:   totalBytes,
//	    TotalChunks: numChunks,
//	    Output:      os.Stdout,
//	})
//
//	reporter.Start()
//	defer reporter.Stop()
//
//	// Update as chunks complete
//	reporter.ChunkCompleted(chunkSize)
//
// # Output Format
//
//	[slurp] Downloading: https://example.com/file.tar.gz
//	[slurp] Total size: 2.5 TB | Chunks: 10240 x 256MB | Workers: 16
//	[slurp] Progress: 45.2% | 1.13 TB / 2.5 TB | Speed: 1.2 GB/s | ETA: 18m 32s
//	[slurp] Chunks: 4628 completed | 16 in-progress | 5596 pending
package progress
