//! L3 storage LSM — the on-disk half of the storage subsystem: the WAL
//! (`wal`), shard image build/write (`shard_file`), mmap'd shard readers
//! (`shard_reader`), the in-memory shard index + compaction trigger
//! (`shard_index`), the N-way compaction kernel (`compact`), the MemTable
//! (`memtable`), the opaque read cursor (`read_cursor`), the manifest serde
//! (`manifest`), the shard-format constants (`layout`), and the `Table` /
//! `PartitionedTable` facades.
//!
//! `lsm/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and re-exports the public items from these
//! submodules. The repr (L2) siblings live under `storage/repr/`; this module
//! aliases the repr submodules and the few storage-level helpers (`error`,
//! `cstr_with_tmp_suffix`, `compare_pk_bytes`, the `with_*` dispatch macros) so
//! the LSM files keep their `super::<mod>` paths unchanged after the move under
//! `lsm/`. The L2→L3 edge `batch_wire → {wal, shard_file}` lives in `repr/`, not
//! here, so `lsm/` depends only sideways within `storage`.

// Re-exported from storage/mod.rs.
pub(super) mod table;
pub(super) mod partitioned_table;
pub(super) mod read_cursor;
pub(super) mod manifest;

// `wal` + `shard_file` are reached by the documented `repr::batch_wire → lsm`
// serialization exception, so they are visible to all of `storage` (incl. repr).
pub(super) mod wal;
pub(super) mod shard_file;
// LSM-internal only.
mod compact;
mod shard_reader;
mod shard_index;
mod memtable;
mod layout;

// Aliases so the LSM submodules keep their `super::<mod>` / `super::super::<mod>`
// paths after the move: the repr (L2) submodules plus the storage-level helpers
// that stay above `lsm/`. The `with_*` macros and `compare_pk_bytes` are pulled
// from `columnar`; `error` and `cstr_with_tmp_suffix` from the storage facade.
use super::repr::{batch, bloom, columnar, heap, merge, scatter, xor8, batch_pool};
use super::repr::columnar::{with_pk_ord, with_payload_cmp, compare_pk_bytes};
use super::{error, cstr_with_tmp_suffix};
