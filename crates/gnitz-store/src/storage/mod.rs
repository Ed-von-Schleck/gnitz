//! Storage subsystem: a Z-set as bytes — in memory, on the wire, on disk.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is this
//! crate's own facade. The submodules stay private, so a `pub` item inside one
//! is still dead-code checked.
//!
//! Tests no single module owns live in `suites/`, a declared `mod suites;` child
//! of this module: they reach this subsystem's surface, not any one module's
//! private items.

mod error;
mod lsm;
mod repr;
mod spill;

#[cfg(test)]
mod suites;

pub use error::StorageError;
pub use lsm::read_cursor::{PkSetGather, ReadCursor};
pub use lsm::run::StoredRow;
pub use repr::batch::{Batch, Layout, MAX_BATCH_REGIONS};
pub use repr::batch_builder::BatchBuilder;
pub use repr::batch_pool::PooledBuf;
pub use repr::batch_wire::{decode_mem_batch_from_wal_block, WireChunk};
pub use repr::merge::MemBatch;
pub use repr::scatter::route_rows_by_pk;
pub use spill::{KeyProducer, SpillSort};

pub(crate) use lsm::manifest::{link_store, manifest_path, published, retire_store};
#[cfg(test)]
pub(crate) use lsm::read_cursor::create_read_cursor;
pub(crate) use lsm::read_cursor::{empty_cursor, from_runs, SkeletonKeys};
pub(crate) use lsm::table::{flush_barrier, RecoverySource, StoreBudgets, Table, DEFAULT_RAM_TIER_BYTES};
pub(crate) use repr::batch::{range_rows, RowMark};
pub(crate) use repr::merge::{merge_consolidated, prorated_blob_cap, relocate_german_string_vec, BlobCache};
pub(crate) use repr::scatter::reset_slots;
pub(crate) use repr::seek::{pk_group_end, pk_prefix_group_end};
