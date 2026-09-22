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
// serves the server's CREATE UNIQUE INDEX pre-flight.
mod error;
mod lsm;
mod spill;

use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};

// L2 representation lives under `repr/`. It has no facade of its own; the leaf
// items are re-exported below and the submodules aliased here so the LSM siblings
// keep their `super::<mod>` paths and the `crate::storage::batch_pool` path
// resolves without touching the moved bodies.
mod repr;
pub use repr::batch_pool;
use repr::{batch, batch_builder, batch_wire, merge, scatter, seek};

#[cfg(test)]
mod suites;

// ── Relations, batches and the flush path ───────────────────────────────────
pub use batch::Batch;
pub use batch::MAX_BATCH_REGIONS;
pub(crate) use batch::{range_rows, RowMark};
pub use batch_wire::decode_mem_batch_from_wal_block;
pub use error::{StorageError, StoreError};
pub(crate) use lsm::flush_barrier::flush_barrier;
pub(crate) use lsm::table::{RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
pub use merge::MemBatch;
pub(crate) use scatter::UnifiedSet;
pub use scatter::{reset_slots, route_rows_by_pk};

// ── Operator hot-path types ──────────────────────────────────────────────────
pub use batch::Layout;
pub use batch_builder::BatchBuilder;
pub use batch_wire::WireChunk;
// `ColumnarSource` stays inside storage: everything out of it reads rows
// through `gnitz_expr::RowSource`.
// The PK group brackets, reached from `ops` as well as from inside repr.
pub(crate) use seek::{pk_group_end, pk_prefix_group_end};
// The OPK key cluster is NOT re-exported here: `schema::key` owns it and every
// caller names `crate::schema::key::X`, so one byte-order rule has one import
// path.
pub(crate) use lsm::child_dir::children_at_generation;
pub(crate) use lsm::child_dir::reclaim_retired_children;
pub(crate) use lsm::child_dir::remove_child;
pub(crate) use lsm::child_dir::subdir_names;
pub use lsm::child_dir::Slot;
// Other crates' tests assert on the on-disk layout through these.
pub use lsm::child_dir::{ChildAddr, ChildKind};
pub(crate) use lsm::index_gather::BoundedIndexCursor;
pub use lsm::index_gather::SourceCursor;
pub(crate) use lsm::read_cursor::empty as empty_cursor;
pub(crate) use lsm::read_cursor::SkeletonKeys;
pub use lsm::read_cursor::{PkSetGather, ReadCursor};
pub(crate) use lsm::repartition::repartition_relation;
pub use lsm::run::StoredRow;
pub(crate) use merge::BlobCacheGuard;
pub(crate) use merge::{prorated_blob_cap, relocate_german_string_vec, run_merge, BlobCache};
pub use spill::{KeyProducer, SpillSort};

/// A file written as `<path>.tmp` and renamed onto `path` by [`commit`]; the
/// `.tmp` is unlinked if the guard drops uncommitted.
///
/// [`commit`]: StagedFile::commit
pub(super) struct StagedFile {
    tmp_path: String,
    final_path: String,
    committed: bool,
}

impl StagedFile {
    /// The guard, and the `.tmp` opened for writing.
    pub(super) fn create(path: &str) -> Result<(Self, File), error::StorageError> {
        let tmp_path = format!("{path}{STAGING_SUFFIX}");
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(&tmp_path)?;
        let staged = StagedFile {
            tmp_path,
            final_path: path.to_owned(),
            committed: false,
        };
        Ok((staged, file))
    }

    pub(super) fn tmp_path(&self) -> &str {
        &self.tmp_path
    }

    pub(super) fn commit(mut self) -> Result<(), error::StorageError> {
        std::fs::rename(&self.tmp_path, &self.final_path)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.tmp_path);
        }
    }
}

/// Durably publish `bytes` as `<dir>/<filename>`, through a `StagedFile`.
pub fn publish_file_sync(dir: &str, filename: &str, bytes: &[u8]) -> Result<(), StorageError> {
    let (staged, file) = StagedFile::create(&format!("{dir}/{filename}"))?;
    file.write_all_at(bytes, 0)?;
    file.sync_data()?;
    staged.commit()?;
    fsync_dir(dir)
}

/// Create `dir` and any missing parent; whether this call created `dir`.
pub(crate) fn create_dir(dir: &str) -> Result<bool, StorageError> {
    let path = std::path::Path::new(dir);
    let mut made = std::fs::create_dir(path);
    if matches!(&made, Err(e) if e.kind() == std::io::ErrorKind::NotFound) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        made = std::fs::create_dir(path);
    }
    match made {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// `fsync` a directory so its entries are durable.
pub(crate) fn fsync_dir(dir: &str) -> Result<(), StorageError> {
    File::open(dir).and_then(|d| d.sync_all()).map_err(StorageError::from)
}

/// The suffix [`StagedFile`] stages under — also how startup GC names a stray
/// manifest `.tmp`.
pub(super) const STAGING_SUFFIX: &str = ".tmp";
