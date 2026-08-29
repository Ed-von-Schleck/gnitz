//! Storage subsystem: WAL, shards, run sets, merge, cursors, and tables.
//!
//! Engine code imports from `crate::storage::{Type, fn}`. What a consumer can
//! *name* is what the re-exports below spell `pub use`; a `pub(crate) use` is
//! the storage facade for this crate's own rungs and nothing more. The
//! submodules stay private, so a `pub` item inside one is still dead-code
//! checked — only the two lists below and `batch_pool` escape that.
//!
//! Naming is not the whole surface: a type returned by a published signature is
//! reachable without being nameable, which is how `ReadCursor` and
//! `SourceCursor` are used from outside while re-exported `pub(crate)`. Their
//! methods are API in practice; treat a change to one as a breaking change.

// Internal — not accessible outside storage/
// L3 LSM lives under `lsm/`. The leaves that belong to no layer stay at storage
// level: the `StorageError` type, and `spill` — a bounded external merge sort of
// fixed-stride byte records that touches no batch, schema or shard. `SpillSort`
// serves the server's CREATE UNIQUE INDEX pre-flight; its `sort_indices` is the
// shared indirect sort of a flat record buffer, which `index_gather` also drives.
mod error;
mod lsm;
mod spill;

// L2 representation lives under `repr/`. It has no facade of its own; the leaf
// items are re-exported below and the submodules aliased here so the LSM siblings
// keep their `super::<mod>` paths and the in-storage `with_payload_cmp!` /
// `crate::storage::batch_pool` paths resolve without touching the moved bodies.
mod repr;
pub use repr::batch_pool;
use repr::{batch, batch_wire, columnar, merge, scatter};

#[cfg(test)]
mod data_roundtrip_proptest;

// ── Relations, batches and the flush path ───────────────────────────────────
pub use batch::Batch;
pub use batch::MAX_BATCH_REGIONS;
pub(crate) use batch::{range_rows, write_to_batch};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use error::StorageError;
pub use lsm::flush_barrier::{flush_barrier, FlushRound};
pub(crate) use lsm::table::enforce_unique_pk;
pub use lsm::table::{RecoverySource, Table};
pub use merge::MemBatch;
pub use scatter::route_rows_by_pk;
pub(crate) use scatter::{batch_project_index, scatter_unified_sources};

// ── Operator hot-path types ──────────────────────────────────────────────────
pub use batch::{BatchBuilder, Layout};
pub use batch_wire::wire_block_size;
// `ColumnarSource` is deliberately NOT re-exported: it adds only the Z-set
// weight, and every out-of-storage consumer (the comparators, the group-key
// extractors, the row appenders) reads rows through `gnitz_expr::RowSource`.
// `cmp_col_window` is NOT re-exported: it lives in `gnitz-wire`, where the
// client-side comparators can reach the same STRING/BLOB-before-fixed-width rule.
// `compare_rows_fixedint_nonnull` and `with_payload_cmp!` are NOT re-exported:
// picking between the two payload comparators is a merge-seat decision, and
// every seat is in storage. The macro names the comparator through
// `crate::storage::columnar`, which resolves only from inside storage.
pub(crate) use columnar::{compare_rows, compare_rows_except};
// The OPK key cluster is NOT re-exported here: `schema::key` owns it and every
// caller names `crate::schema::key::X`. Re-exporting it split one byte-order
// rule across two import paths, visibly — `ops/reduce/sort.rs` and
// `catalog/scan_spec.rs` each imported from both in adjacent lines.
pub(crate) use lsm::child_dir::{
    fsync_dir, reclaim_retired_children, remove_child, state_child_manifests, subdir_names, ChildAddr,
};
pub(crate) use lsm::index_gather::{BoundedIndexCursor, SourceCursor};
pub(crate) use lsm::manifest::{peek_header, topology_word};
pub(crate) use lsm::read_cursor::{empty as empty_cursor, key_list_range, PkSetGather, ReadCursor};
pub(crate) use lsm::repartition::repartition_relation;
pub(crate) use lsm::run::StoredRow;
pub use merge::BlobCacheGuard;
pub(crate) use merge::{mem_batch_to_unified, prorated_blob_cap, relocate_german_string_vec, BlobCache};
pub use spill::{KeyProducer, SpillSort};

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
