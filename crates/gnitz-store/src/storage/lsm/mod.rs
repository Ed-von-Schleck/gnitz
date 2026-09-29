//! L3 storage LSM — the on-disk half of the storage subsystem: the in-memory
//! shard index + compaction trigger (`shard_index`), the N-way compaction kernel
//! (`compact`), the sorted run (`run`) and the RAM-tier run sets built from it
//! (`run_set`), the RAM tier's PK-probe filter (`bloom`), the opaque read cursor
//! (`read_cursor`), the manifest serde, the store directory's file names and the
//! primitives over them (`manifest`), and the `Table` facade. The
//! shard image — its encoder, its mmap'd reader and the format rules both call —
//! lives one layer down in `repr/`, as does the WAL block codec.
//!
//! `lsm/` has **no outward facade of its own** — `storage/mod.rs` curates the
//! single combined storage surface and re-exports the public items from these
//! submodules.

// Re-exported from storage/mod.rs.
pub(super) mod manifest;
pub(super) mod read_cursor;
pub(super) mod run;
pub(super) mod table;

// LSM-internal only.
mod batch_fsync;
mod bloom;
mod compact;
mod run_set;
mod shard_index;
