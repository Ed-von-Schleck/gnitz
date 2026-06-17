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
mod batch_wire;
mod memtable;
mod read_cursor;
mod range_key;
mod table;
mod partitioned_table;
pub(crate) mod batch_pool;
mod error;

#[cfg(test)]
mod data_roundtrip_proptest;

// ── Public API ──────────────────────────────────────────────────────────────
pub use table::{Table, FlushOutcome, FlushWork, Persistence};
pub use partitioned_table::{PartitionedTable, partition_for_key, partition_for_pk_bytes, partition_arena_size};
pub use read_cursor::CursorHandle;
pub use batch::{Batch, ConsolidatedBatch, write_to_batch};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use merge::{MemBatch, scatter_copy, scatter_multi_source};
pub use error::StorageError;

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use read_cursor::{DrainGuard, ReadCursor, DDL_SCAN_CHUNK_ROWS};
#[cfg(test)]
pub(crate) use read_cursor::REWIND_CALLS;
#[cfg(test)]
pub(crate) use partitioned_table::partial_flush_lsn_fixture;
pub(crate) use columnar::{compare_pk_bytes, compare_rows, compare_rows_fixedint_nonnull, opk_key, with_payload_cmp, with_pk_ord};
pub(crate) use range_key::{increment_key_in_place, range_cut_points, range_group_cut_points};
pub(crate) use merge::{BlobCacheGuard, DirectWriter, pk_sort_key};
pub(crate) use batch::carve_writer_slices;
pub(crate) use batch::{BatchBuilder, index_meta_schema_desc, INDEX_META_COL_NAMES,
                       make_index_schema, project_schema};
pub(crate) use manifest::PkBuf;

/// Append the `.tmp` suffix to a CStr basename and return a new CString.
pub(super) fn cstr_with_tmp_suffix(base: &std::ffi::CStr) -> Result<std::ffi::CString, error::StorageError> {
    let b = base.to_bytes();
    let mut v = Vec::with_capacity(b.len() + 4);
    v.extend_from_slice(b);
    v.extend_from_slice(b".tmp");
    std::ffi::CString::new(v).map_err(|_| error::StorageError::InvalidPath)
}

