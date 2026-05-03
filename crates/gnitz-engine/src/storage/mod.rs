//! Storage subsystem: WAL, shards, MemTable, merge, cursors, and tables.
//!
//! Only the items listed under "Public API" are part of the official surface.
//! Engine code imports from `crate::storage::{Type, fn}`.

// Internal — not accessible outside storage/
mod bloom;
mod xor8;
mod wal;
mod manifest;
mod columnar;
mod heap;
mod compact;
mod shard_file;
mod shard_reader;
mod shard_index;
mod merge;
mod batch;
mod memtable;
mod read_cursor;
mod table;
mod partitioned_table;
pub(crate) mod batch_pool;
mod error;

// ── Public API ──────────────────────────────────────────────────────────────
pub use table::{Table, FlushOutcome, FlushWork};
pub use partitioned_table::{PartitionedTable, partition_for_key, partition_arena_size};
pub use read_cursor::CursorHandle;
pub use batch::{Batch, ConsolidatedBatch, write_to_batch, decode_mem_batch_from_wal_block};
pub use merge::{MemBatch, scatter_copy, scatter_multi_source};
pub use error::StorageError;

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use read_cursor::{DRAIN_BUFFER, ReadCursor, create_cursor_from_snapshots};
pub(crate) use columnar::compare_rows;
pub(crate) use merge::{BlobCacheGuard, DirectWriter};
pub(crate) use batch::carve_writer_slices;

/// Append the `.tmp` suffix to a CStr basename and return a new CString.
pub(super) fn cstr_with_tmp_suffix(base: &std::ffi::CStr) -> Result<std::ffi::CString, error::StorageError> {
    let b = base.to_bytes();
    let mut v = Vec::with_capacity(b.len() + 4);
    v.extend_from_slice(b);
    v.extend_from_slice(b".tmp");
    std::ffi::CString::new(v).map_err(|_| error::StorageError::InvalidPath)
}

