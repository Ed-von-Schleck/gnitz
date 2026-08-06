//! L3 storage LSM — the on-disk half of the storage subsystem: mmap'd shard
//! readers (`shard_reader`), the in-memory shard index + compaction trigger
//! (`shard_index`), the N-way compaction kernel (`compact`), the sorted run
//! (`run`) and the RAM-tier run sets built from it (`run_set`), the opaque read
//! cursor (`read_cursor`), the manifest serde (`manifest`), the filename grammar
//! (`naming`), and the `Table` / `PartitionedTable` facades. The pure byte codecs
//! (`wal`, `shard_file`, `layout`) live one layer down in `repr/`.
//!
//! `lsm/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and re-exports the public items from these
//! submodules. The repr (L2) siblings live under `storage/repr/`; this module
//! aliases the repr submodules and the few storage-level helpers (`error`,
//! `cstr`/`cstr_with_tmp_suffix`, the `with_*` dispatch
//! macros) so the LSM files keep their `super::<mod>` paths unchanged after the
//! move under `lsm/`.

// Re-exported from storage/mod.rs.
pub(super) mod child_dir;
pub(super) mod flush_barrier;
pub(super) mod index_gather;
pub(super) mod manifest;
pub(super) mod partitioned_table;
pub(super) mod read_cursor;
pub(super) mod spill;
pub(super) mod table;

// LSM-internal only (`shard_reader` is storage-visible: the repr codec tests
// round-trip written shards through `MappedShard`).
mod compact;
mod naming;
pub(super) mod run;
mod run_set;
mod shard_index;
pub(in crate::storage) mod shard_reader;

// Aliases so the LSM submodules keep their `super::<mod>` / `super::super::<mod>`
// paths after the move: the repr (L2) submodules plus the storage-level helpers
// that stay above `lsm/` (`error` and the `cstr` helpers, from the storage facade).
use super::repr::{batch, bloom, columnar, heap, layout, merge, scatter, shard_file, xor8};
use super::{cstr, cstr_with_tmp_suffix, error};

/// Slot owning `key` in a sorted guard list: the last guard `≤ key`, saturating
/// to slot 0 for keys below the first guard. The read router
/// (`FLSMLevel::find_guard_idx`) routes through this; `compact::merge_and_route`
/// reproduces the same slots by binary-searching its sorted survivor buffer, and
/// the differential oracles route through here to keep the two independent.
pub(crate) fn guard_slot<T>(guards: &[T], key: u128, gk: impl Fn(&T) -> u128) -> usize {
    guards.partition_point(|g| gk(g) <= key).saturating_sub(1)
}
