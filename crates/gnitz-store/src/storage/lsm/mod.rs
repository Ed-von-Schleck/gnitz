//! L3 storage LSM — the on-disk half of the storage subsystem: the in-memory
//! shard index + compaction trigger
//! (`shard_index`), the N-way compaction kernel (`compact`), the sorted run
//! (`run`) and the RAM-tier run sets built from it (`run_set`), the opaque read
//! cursor (`read_cursor`), the manifest serde (`manifest`), the filename grammar
//! (`naming`), the boot relayout (`repartition`), and the `Table` facade. The
//! shard image — its encoder, its mmap'd reader and the format rules both call —
//! lives one layer down in `repr/`, as does the WAL block codec.
//!
//! `lsm/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and re-exports the public items from these
//! submodules. The repr (L2) siblings live under `storage/repr/`; this module
//! aliases the repr submodules and the few storage-level helpers (`error`,
//! `cstr`, `StagedFile`) so the LSM files keep their `super::<mod>`
//! paths unchanged after the move under `lsm/`. The `with_*` dispatch macros
//! are not aliased here — they are reached through the aliased `columnar`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

// Re-exported from storage/mod.rs.
pub(super) mod child_dir;
pub(super) mod flush_barrier;
pub(super) mod index_gather;
pub(super) mod manifest;
pub(super) mod read_cursor;
pub(super) mod repartition;
pub(super) mod table;

// LSM-internal only.
mod compact;
mod naming;
pub(super) mod run;
mod run_set;
mod shard_index;

// Aliases so the LSM submodules keep their `super::<mod>` / `super::super::<mod>`
// paths after the move: the repr (L2) submodules plus the storage-level helpers
// that stay above `lsm/` (`error` and the `cstr` helpers, from the storage facade).
use super::repr::{batch, bloom, columnar, heap, merge, scatter, shard_file, shard_reader};
// Shard-format constants: only the LSM test modules assert against the image.
#[cfg(test)]
use super::repr::layout;
use super::{cstr, error, to_cstrings, StagedFile, STAGING_SUFFIX};
