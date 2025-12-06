// Package sharded provides types for reading and writing sharded files in cloud storage.
//
// This package enables storing large files as multiple shards in cloud storage
// with a manifest for reassembly. It handles state persistence for resumable
// operations and is storage-agnostic via gocloud.dev/blob.
//
// # Writing
//
// Use [Write] to create a sharded file. Call [File.Next] to get successive shards,
// write data to each shard, then call [File.Complete] to finalize.
//
// Options:
//   - [WithShardSize]: Size of each shard (required)
//   - [WithSize]: Total size, enables io.EOF when all shards filled (optional)
//   - [WithMetadata]: Caller-defined metadata stored in manifest (optional)
//
// # Resume
//
// The same [Write] call handles resume automatically. If state exists from a
// previous incomplete write, [File.Next] returns [ErrShardFilled] for shards
// already written. Use [File.Metadata] to validate stored metadata (e.g., check
// source ETag hasn't changed). Call [File.Reset] to discard state and start over.
//
// # Shard
//
// [File.Next] returns a [Shard] which provides:
//   - Index: Shard number (0, 1, 2, ...)
//   - Offset: Byte offset in the source data
//   - Length: Expected size of this shard
//   - io.WriteCloser: Write data and Close to persist
//
// # Reading
//
// Use [Read] to open a sharded file. It returns an io.ReadCloser that streams
// all shards in order, transparently handling the manifest.
//
// # Storage Layout
//
//	{bucket}/{dest}.shards/shard-000000
//	{bucket}/{dest}.shards/shard-000001
//	{bucket}/{dest}.shards/state.json     (during writes, deleted on completion)
//	{bucket}/{dest}.manifest.json         (on completion)
//
// # Manifest Format
//
//	{
//	  "total_size": 1073741824,
//	  "shard_size": 268435456,
//	  "parts_prefix": "path/to/file.bin.shards/",
//	  "shards": [
//	    {"object": "shard-000000", "offset": 0, "size": 268435456, "checksum": "..."},
//	    ...
//	  ],
//	  "metadata": {"source_url": "...", "source_etag": "..."},
//	  "completed_at": "2025-01-15T10:30:00Z"
//	}
//
// See example_test.go for usage examples.
package sharded
