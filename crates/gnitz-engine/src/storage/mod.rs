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
mod range_key;
mod table;
mod partitioned_table;
pub(crate) mod batch_pool;
mod error;

// ── Public API ──────────────────────────────────────────────────────────────
pub use table::{Table, FlushOutcome, FlushWork, Persistence};
pub use partitioned_table::{PartitionedTable, partition_for_key, partition_for_pk_bytes, partition_arena_size};
pub use read_cursor::CursorHandle;
pub use batch::{Batch, ConsolidatedBatch, write_to_batch, decode_mem_batch_from_wal_block};
pub use merge::{MemBatch, scatter_copy, scatter_multi_source};
pub use error::StorageError;

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use read_cursor::{DrainGuard, ReadCursor, DDL_SCAN_CHUNK_ROWS};
#[cfg(test)]
pub(crate) use read_cursor::REWIND_CALLS;
pub(crate) use columnar::{compare_pk_bytes, compare_rows, compare_rows_fixedint_nonnull, encode_order_preserving_pk, opk_key, with_payload_cmp};
pub(crate) use range_key::{increment_key_in_place, range_cut_points, range_group_cut_points};
pub(crate) use merge::{BlobCacheGuard, DirectWriter, pk_sort_key};
pub(crate) use batch::carve_writer_slices;
pub(crate) use manifest::PkBuf;

/// Append the `.tmp` suffix to a CStr basename and return a new CString.
pub(super) fn cstr_with_tmp_suffix(base: &std::ffi::CStr) -> Result<std::ffi::CString, error::StorageError> {
    let b = base.to_bytes();
    let mut v = Vec::with_capacity(b.len() + 4);
    v.extend_from_slice(b);
    v.extend_from_slice(b".tmp");
    std::ffi::CString::new(v).map_err(|_| error::StorageError::InvalidPath)
}

