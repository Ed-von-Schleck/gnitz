//! A Z-set as bytes — in memory, on the wire, on disk — and the cursor that
//! reads it back.
//!
//! - The batch: its region layout (`batch`), the row-at-a-time system-table
//!   writer over one (`batch_builder`), the direct row writer (`writer`), wire
//!   serialization (`batch_wire`), and the German-string heap (`string_heap`).
//! - The thread-local object pools that batch arenas, send buffers, cursor
//!   vectors and kernel scratch recycle through (`pool`).
//! - The kernels over batches: the OPK lower-bound search and the column
//!   pointer it reads through (`seek`), the column-first row copy (`scatter`),
//!   and the run merges, the N-way tournament they play, and single-batch
//!   consolidation (`merge`).
//! - The shard file — its format, writer, reader and compaction, and the
//!   storage error type they fail with (`shard`).
//! - Above the representation: the sorted run, whatever backs it (`run`), and
//!   the opaque read cursor that merges runs (`read_cursor`).
//!
//! The low-level WAL-block framer lives in `gnitz_wire::wal` (the one
//! definition client and engine share). Which runs a store holds, and when they
//! flush, compact or publish, is `gnitz-store`'s: this rung is handed runs and
//! never asks where they came from.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is this
//! crate's own facade. The submodules stay private, so a `pub` item inside one
//! is still dead-code checked.

mod batch;
mod batch_builder;
mod batch_wire;
mod merge;
mod pool;
mod read_cursor;
mod run;
mod scatter;
mod seek;
mod shard;
mod string_heap;
mod writer;

pub use batch::{Batch, MemBatch};
pub use batch_builder::BatchBuilder;
pub use batch_wire::WireRows;
pub use merge::merge_consolidated;
pub use pool::PooledBuf;
pub use read_cursor::{empty_cursor, from_runs, from_runs_at, from_runs_in_band, PkSetGather, ReadCursor};
pub use run::{Run, StoredRow};
pub use shard::{
    guard_slot, Codebook, GuardMerge, GuardRun, MappedShard, ShardDirectory, ShardWriteOpts, StorageError,
};

pub(crate) use batch::{range_rows, runs_where, runs_where_zip, write_to_batch};
pub(crate) use pool::{vec_bytes, with_scratch};
pub(crate) use read_cursor::{CursorRow, GroupProbe};
pub(crate) use scatter::{copy_runs, materialize_carrying, stride, stride_dispatch, with_picks};
pub(crate) use seek::pk_group_end;
pub(crate) use string_heap::{prorated_blob_cap, relocated_cell, row_long_bytes, should_relocate_blob};
pub(crate) use writer::DirectWriter;
