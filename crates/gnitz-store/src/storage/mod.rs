//! Storage subsystem: a Z-set as bytes — in memory, on the wire, on disk — plus
//! the address that says which Z-set.
//!
//! What a consumer outside this crate can *name* is what the re-exports below
//! spell `pub use`: rows, and the address that says which store holds them. The
//! store behind that address is not nameable — `relation` owns every one. A
//! `pub(crate) use` is this crate's own facade; the submodules stay private, so
//! a `pub` item inside one is still dead-code checked.
//!
//! Naming is not the whole surface: a type reached through a published signature
//! or variant is reachable without being nameable, so a `pub(crate)` re-export
//! can still be API in practice — treat a change to one such type's methods as a
//! breaking change.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

// Internal — not accessible outside storage/
// L3 LSM lives under `lsm/`. The leaves that belong to no layer stay at storage
// level: the `StorageError` type, and `spill` — a bounded external merge sort of
// fixed-stride byte records that touches no batch, schema or shard. `SpillSort`
// serves the server's CREATE UNIQUE INDEX pre-flight; its `sort_indices` is the
// shared indirect sort of a flat record buffer, which `index_gather` also drives.
mod error;
mod lsm;
mod spill;

use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;

use gnitz_foundation::posix_io;

// L2 representation lives under `repr/`. It has no facade of its own; the leaf
// items are re-exported below and the submodules aliased here so the LSM siblings
// keep their `super::<mod>` paths and the in-storage `with_payload_cmp!` /
// `crate::storage::batch_pool` paths resolve without touching the moved bodies.
mod repr;
pub use repr::batch_pool;
use repr::{batch, batch_builder, batch_wire, columnar, merge, scatter};

#[cfg(test)]
mod suites;

// ── Relations, batches and the flush path ───────────────────────────────────
pub use batch::Batch;
pub use batch::MAX_BATCH_REGIONS;
pub(crate) use batch::{range_rows, write_to_batch};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use error::{StorageError, StoreError};
pub(crate) use lsm::flush_barrier::{flush_barrier, FlushRound};
pub(crate) use lsm::table::{RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
pub use merge::MemBatch;
pub use scatter::batch_project_index;
pub use scatter::route_rows_by_pk;
pub(crate) use scatter::scatter_unified_sources;

// ── Operator hot-path types ──────────────────────────────────────────────────
pub use batch::Layout;
pub use batch_builder::BatchBuilder;
pub use batch_wire::wire_block_size;
pub use batch_wire::WireChunk;
// `ColumnarSource` is deliberately NOT re-exported: it adds only the Z-set
// weight, and every out-of-storage consumer (the comparators, the group-key
// extractors, the row appenders) reads rows through `gnitz_expr::RowSource`.
// `cmp_col_window` is NOT re-exported: it lives in `gnitz-wire`, where the
// client-side comparators can reach the same STRING/BLOB-before-fixed-width rule.
// `compare_rows_fixedint_nonnull` and `with_payload_cmp!` are NOT re-exported:
// picking between the two payload comparators is a merge-seat decision, and
// every seat is in storage. The macro names the comparator through
// `crate::storage::columnar`, which resolves only from inside storage.
pub use columnar::compare_rows;
// The equal-PK group bracket, reached from `ops` as well as from inside repr.
pub(crate) use columnar::pk_group_end;
// The three generic payload-cell readers: one spelling for every `RowSource`,
// which is what lets a catalog decoder read a `Batch`, a `StoredRow` and a
// positioned `ReadCursor` through the same call.
pub use columnar::compare_rows_except;
pub use columnar::{payload_bytes, payload_is_null, payload_str, payload_string, payload_u64};
// The OPK key cluster is NOT re-exported here: `schema::key` owns it and every
// caller names `crate::schema::key::X`. Re-exporting it split one byte-order
// rule across two import paths, visibly — `ops/reduce/sort.rs` and
// `read/scan_spec.rs` each imported from both in adjacent lines.
pub(crate) use lsm::child_dir::children_at_generation;
pub use lsm::child_dir::fsync_dir;
pub(crate) use lsm::child_dir::reclaim_retired_children;
pub(crate) use lsm::child_dir::{create_child, remove_child};
// `ChildAddr` and `Slot` are *names*, not stores: the crash-fixture and relayout
// tests assert on the on-disk shape through them.
pub(crate) use lsm::child_dir::subdir_names;
pub use lsm::child_dir::{ChildAddr, Slot};
pub(crate) use lsm::index_gather::BoundedIndexCursor;
pub use lsm::index_gather::SourceCursor;
pub(crate) use lsm::read_cursor::empty as empty_cursor;
pub(crate) use lsm::read_cursor::SkeletonKeys;
pub use lsm::read_cursor::{PkSetGather, ReadCursor};
pub(crate) use lsm::repartition::repartition_relation;
pub use lsm::run::StoredRow;
pub(crate) use merge::BlobCacheGuard;
pub(crate) use merge::{mem_batch_to_unified, prorated_blob_cap, relocate_german_string_vec, run_merge, BlobCache};
pub use spill::{KeyProducer, SpillSort};

/// Convert a path string to a `CString`, mapping an interior NUL to
/// `InvalidPath` — the one conversion every storage path takes.
pub(super) fn cstr(s: impl Into<Vec<u8>>) -> Result<std::ffi::CString, error::StorageError> {
    std::ffi::CString::new(s).map_err(|_| error::StorageError::InvalidPath)
}

/// Path strings as `CString`s — the compaction input list (a `Vec<String>`), the
/// barrier's by-path fdatasync sweep list (borrowed `&str`s off the live
/// entries) and the relayout's own publish list take the same conversion.
pub(super) fn to_cstrings<S: AsRef<str>>(
    paths: impl IntoIterator<Item = S>,
) -> Result<Vec<std::ffi::CString>, error::StorageError> {
    paths.into_iter().map(|p| cstr(p.as_ref())).collect()
}

/// A file written as `<path>.tmp` and renamed onto `path` by [`commit`]. Dropped
/// uncommitted — an error return, a panic, or an abandoned flush — it unlinks
/// the `.tmp`, so a failed write leaves nothing behind.
///
/// [`commit`]: StagedFile::commit
pub(super) struct StagedFile {
    file: File,
    tmp_path: CString,
    final_path: CString,
    committed: bool,
}

impl StagedFile {
    pub(super) fn create(path: &CStr) -> Result<Self, error::StorageError> {
        let tmp_path = cstr([path.to_bytes(), STAGING_SUFFIX.as_bytes()].concat())?;
        let file = File::from(posix_io::open_owned(
            &tmp_path,
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
        )?);
        Ok(StagedFile {
            file,
            tmp_path,
            final_path: path.to_owned(),
            committed: false,
        })
    }

    pub(super) fn file(&self) -> &File {
        &self.file
    }

    /// Raw because the flush barrier hands whole chunks of these to
    /// `IORING_OP_FSYNC` at once.
    pub(super) fn fd(&self) -> libc::c_int {
        self.file.as_raw_fd()
    }

    pub(super) fn sync(&self) -> std::io::Result<()> {
        self.file.sync_data()
    }

    pub(super) fn commit(mut self) -> Result<(), error::StorageError> {
        posix_io::renameat(libc::AT_FDCWD, &self.tmp_path, libc::AT_FDCWD, &self.final_path)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.committed {
            unsafe { libc::unlink(self.tmp_path.as_ptr()) };
        }
    }
}

/// Publish `parts`, concatenated, as `<dir>/<filename>`: staged as a `.tmp`,
/// `fdatasync`ed, renamed, with the directory fsynced either side of the rename.
/// An uncommitted [`StagedFile`] unlinks itself, so a failure leaves no `.tmp`.
/// Mode `0o644`.
pub fn publish_file_sync(dir: &str, filename: &str, parts: &[&[u8]]) -> Result<(), StorageError> {
    let staged = StagedFile::create(&cstr(format!("{dir}/{filename}"))?)?;
    let mut offset = 0u64;
    for part in parts {
        staged.file().write_all_at(part, offset)?;
        offset += part.len() as u64;
    }
    staged.sync()?;
    fsync_dir(dir)?;
    staged.commit()?;
    fsync_dir(dir)
}

/// The suffix [`StagedFile`] stages under — also how startup GC names a stray
/// manifest `.tmp`.
pub(super) const STAGING_SUFFIX: &str = ".tmp";
