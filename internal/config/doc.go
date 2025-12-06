// Package config defines configuration structures for the slurp CLI.
//
// Configuration can be provided via:
//   - Command-line flags
//   - Environment variables (SLURP_ prefix)
//   - YAML configuration file
//
// # Structure
//
//	type Config struct {
//	    URL         string
//	    Bucket      string
//	    Object      string
//	    Workers     int
//	    ChunkSize   int64
//	    Progress    bool
//	    Force       bool
//	    Retry       RetryConfig
//	}
//
//	type RetryConfig struct {
//	    Attempts   int
//	    Backoff    time.Duration
//	    MaxBackoff time.Duration
//	}
package config
