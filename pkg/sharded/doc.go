// Package sharded provides types for reading and writing sharded files in cloud storage.
//
// This package enables storing large files as multiple chunks in cloud storage
// with a manifest for reassembly. It handles state persistence for resumable
// operations and is storage-agnostic via gocloud.dev/blob.
//
// # Writing
//
// Use [Write] to create a sharded file. Call [File.Next] to get successive chunks,
// write data to each chunk, then call [File.Complete] to finalize.
//
// Options:
//   - [WithChunkSize]: Size of each chunk (required)
//   - [WithSize]: Total size, enables io.EOF when all chunks filled (optional)
//   - [WithMetadata]: Caller-defined metadata stored in manifest (optional)
//
// # Resume
//
// The same [Write] call handles resume automatically. If state exists from a
// previous incomplete write, [File.Next] returns [ErrChunkFilled] for chunks
// already written. Use [File.Metadata] to validate stored metadata (e.g., check
// source ETag hasn't changed). Call [File.Reset] to discard state and start over.
//
// # Chunk
//
// [File.Next] returns a [Chunk] which provides:
//   - Index: Chunk number (0, 1, 2, ...)
//   - Offset: Byte offset in the source data
//   - Length: Expected size of this chunk
//   - io.WriteCloser: Write data and Close to persist
//
// # Reading
//
// Use [Read] to open a sharded file. It returns an io.ReadCloser that streams
// all chunks in order, transparently handling the manifest.
//
// # Storage Layout
//
//	{bucket}/{dest}.shards/chunk-000000
//	{bucket}/{dest}.shards/chunk-000001
//	{bucket}/{dest}.shards/state.json     (during writes, deleted on completion)
//	{bucket}/{dest}.manifest.json         (on completion)
//
// # Manifest Format
//
//	{
//	  "total_size": 1073741824,
//	  "chunk_size": 268435456,
//	  "parts_prefix": "path/to/file.bin.shards/",
//	  "chunks": [
//	    {"index": 0, "object": "chunk-000000", "size": 268435456, "checksum": "..."},
//	    ...
//	  ],
//	  "metadata": {"source_url": "...", "source_etag": "..."},
//	  "completed_at": "2025-01-15T10:30:00Z"
//	}
//
// See example_test.go for usage examples.
package sharded
