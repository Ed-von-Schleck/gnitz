//! The sorted runs a trace is kept as, and the cursor that reads them back.
//!
//! - The sorted run, whatever backs it (`run`): an in-heap batch or a shard.
//! - The shard file — its format, writer, reader and compaction, the symbol
//!   table its heaps are coded under, and the storage error type they fail
//!   with (`shard`).
//! - The opaque read cursor that merges runs (`read_cursor`).
//!
//! Which runs a store holds, and when they flush, compact or publish, is
//! `gnitz-store`'s: this rung is handed runs and never asks where they came
//! from.

mod read_cursor;
mod run;
mod shard;

pub use read_cursor::{PkSetGather, ReadCursor, empty_cursor, from_runs, from_runs_at, from_runs_in_band};
pub use run::{Run, StoredRow};
pub use shard::{
    Codebook, GuardMerge, GuardRun, MappedShard, ShardDirectory, ShardWriteOpts, StorageError, guard_slot, write_shard,
};

pub(crate) use read_cursor::GroupProbe;
