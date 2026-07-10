//! Storage subsystem: WAL, shards, MemTable, merge, cursors, and tables.
//!
//! Only the items listed under "Public API" are part of the official surface.
//! Engine code imports from `crate::storage::{Type, fn}`.

// Internal — not accessible outside storage/
// L3 LSM lives under `lsm/`; the `StorageError` leaf stays at storage level.
mod error;
mod lsm;

// L2 representation lives under `repr/`. It has no facade of its own; the leaf
// items are re-exported below and the submodules aliased here so the LSM siblings
// keep their `super::<mod>` paths and the in-storage `with_payload_cmp!` /
// `crate::storage::batch_pool` paths resolve without touching the moved bodies.
mod repr;
pub(crate) use repr::batch_pool;
use repr::{batch, batch_wire, columnar, merge, range_key, scatter};

#[cfg(test)]
mod data_roundtrip_proptest;

// ── Public API ──────────────────────────────────────────────────────────────
pub use batch::{write_to_batch, Batch};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use error::StorageError;
pub use lsm::partitioned_table::{
    partition_for_key, partition_for_pk_bytes, partition_range, PartitionedTable, Routing, NUM_PARTITIONS,
};

pub use lsm::table::{FlushOutcome, FlushWork, RecoverySource, Table};
pub use merge::MemBatch;
pub use scatter::{scatter_copy, scatter_multi_source};

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use batch::carve_writer_slices;
pub(crate) use batch::{BatchBuilder, Layout, MAX_WIRE_REGIONS};
pub(crate) use columnar::{cmp_col_window, compare_rows, compare_rows_fixedint_nonnull, with_payload_cmp};
// The PK key primitives live in `schema::key`; out-of-storage callers keep the
// storage facade.
pub(crate) use crate::schema::key::{compare_pk_bytes, compare_pk_ordering, opk_key, pack_pk_be, pk_bytes_eq};
pub(crate) use lsm::manifest::{partition_manifest_path, peek_generation, PkBuf, STATE_FORMAT};
#[cfg(test)]
pub(crate) use lsm::partitioned_table::partial_flush_lsn_fixture;
#[cfg(test)]
pub(crate) use lsm::read_cursor::REWIND_CALLS;
pub(crate) use lsm::read_cursor::{DrainGuard, ReadCursor, DDL_SCAN_CHUNK_ROWS};
pub(crate) use lsm::spill::{KeyProducer, SpillSort};
pub(crate) use merge::{BlobCacheGuard, DirectWriter};
pub(crate) use range_key::{increment_key_in_place, range_cut_points};
pub(crate) use repr::wal::{
    block_size as wal_block_size, stamp_checksum as wal_stamp_checksum,
    write_header_and_directory as wal_write_header_and_directory,
};

/// Convert a path string to a `CString`, mapping an interior NUL to
/// `InvalidPath` — the one conversion every storage path takes.
pub(super) fn cstr(s: impl Into<Vec<u8>>) -> Result<std::ffi::CString, error::StorageError> {
    std::ffi::CString::new(s).map_err(|_| error::StorageError::InvalidPath)
}

/// Append the `.tmp` suffix to a CStr basename and return a new CString.
pub(super) fn cstr_with_tmp_suffix(base: &std::ffi::CStr) -> Result<std::ffi::CString, error::StorageError> {
    let b = base.to_bytes();
    let mut v = Vec::with_capacity(b.len() + 4);
    v.extend_from_slice(b);
    v.extend_from_slice(b".tmp");
    std::ffi::CString::new(v).map_err(|_| error::StorageError::InvalidPath)
}
