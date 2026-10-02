//! The storage LSM — the on-disk lifecycle of one store: the in-memory shard
//! index + compaction trigger (`shard_index`), the RAM-tier run sets
//! (`run_set`), the RAM tier's PK-probe filter (`bloom`), the manifest serde, the
//! store directory's file names and the primitives over them (`manifest`), and
//! the `Table` facade.
//!
//! The batch, the shard image, the run, the read cursor that merges runs and the
//! guard-routed merge a compaction drives are `gnitz_zset::repr`'s; what lives
//! here is their lifecycle — which runs a store holds, when they flush, compact
//! and publish a manifest.
//!
//! Nothing here is published: `relation` and `read` reach the `pub(crate) use`s
//! below, and other crates reach a store through `relation`.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

mod batch_fsync;
mod bloom;
mod manifest;
mod run_set;
mod shard_index;
mod table;

#[cfg(test)]
mod suites;

#[cfg(test)]
pub(crate) use manifest::shard_path;
pub(crate) use manifest::{link_store, manifest_path, read_at, read_intact, retire_store, shard_files};
pub use table::Cut;
pub(crate) use table::{flush_barrier, RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
