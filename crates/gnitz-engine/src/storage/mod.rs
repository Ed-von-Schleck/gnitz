//! Storage subsystem: WAL, shards, run sets, merge, cursors, and tables.
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
use repr::{batch, batch_wire, columnar, merge, scatter};

#[cfg(test)]
mod data_roundtrip_proptest;

// ── Public API ──────────────────────────────────────────────────────────────
pub use batch::{range_rows, write_to_batch, Batch};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use error::StorageError;
pub use lsm::flush_barrier::{flush_barrier, FlushRound};
pub use lsm::table::{RecoverySource, Table};
pub use merge::MemBatch;
pub(crate) use scatter::route_rows_by_pk;
pub use scatter::{scatter_copy, scatter_multi_source};

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use batch::carve_writer_slices;
pub(crate) use batch::{BatchBuilder, Layout};
pub(crate) use batch_wire::{compute_wire_props, schema_wire_safe, wire_header_dir_size, wire_region_sizes};
// `ColumnarSource` is deliberately NOT re-exported: it adds only the Z-set
// weight, and every out-of-storage consumer (the comparators, the group-key
// extractors, the row appenders) reads rows through `gnitz_expr::RowSource`.
pub(crate) use columnar::{
    cmp_col_window, compare_rows, compare_rows_except, compare_rows_fixedint_nonnull, with_payload_cmp,
};
// The OPK key cluster is NOT re-exported here: `schema::key` owns it and every
// caller names `crate::schema::key::X`. Re-exporting it split one §1/§6 rule
// across two import paths, visibly — `ops/reduce/sort.rs` and
// `catalog/scan_spec.rs` each imported from both in adjacent lines.
pub(crate) use gnitz_wire::wal::write_header_and_directory as wal_write_header_and_directory;
pub(crate) use lsm::child_dir::{cluster_children, remove_child, subdir_names, ChildAddr};
pub(crate) use lsm::index_gather::BoundedIndexCursor;
pub(crate) use lsm::manifest::{peek_header, topology_word};
#[cfg(test)]
pub(crate) use lsm::read_cursor::REWIND_CALLS;
pub(crate) use lsm::read_cursor::{empty as empty_cursor, DrainGuard, PkSetGather, ReadCursor};
pub(crate) use lsm::repartition::repartition_relation;
pub(crate) use lsm::spill::{KeyProducer, SpillSort};
pub(crate) use merge::{
    prorated_blob_cap, relocate_german_string_vec, BlobCache, BlobCacheGuard, DirectWriter, RowComparator,
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
