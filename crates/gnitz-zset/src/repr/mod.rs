//! A Z-set as bytes — in memory, on the wire, on disk — and the cursor that
//! reads it back.
//!
//! - The batch: its region layout (`batch`), the row-at-a-time system-table
//!   writer over one (`batch_builder`), the direct row writer (`writer`), wire
//!   serialization (`batch_wire`), the German-string heap (`string_heap`), and
//!   the thread-local buffer pool batch arenas and send buffers recycle through
//!   (`batch_pool`).
//! - The kernels over batches: the OPK lower-bound search and the column
//!   pointer it reads through (`seek`), the column-first row copy (`scatter`),
//!   the run merges and single-batch consolidation (`merge`), and the N-way
//!   min-merge tournament the run merge plays (`loser_tree`).
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
mod batch_pool;
mod batch_wire;
mod loser_tree;
mod merge;
mod read_cursor;
mod run;
mod scatter;
mod seek;
mod shard;
mod string_heap;
mod writer;

pub use batch::{Batch, MemBatch};
pub use batch_builder::BatchBuilder;
pub use batch_pool::PooledBuf;
pub use batch_wire::WireRows;
pub use merge::merge_consolidated;
pub use read_cursor::{
    empty_cursor, from_runs, from_runs_at, from_runs_in_band, BoundedIndexCursor, PkSetGather, ReadCursor, SourceCursor,
};
pub use run::{first_live_payload_group, Run, StoredRow};
pub use shard::{
    guard_slot, Codebook, GuardMerge, GuardRun, MappedShard, ShardDirectory, ShardWriteOpts, StorageError,
};

pub(crate) use batch::{range_rows, runs_where, runs_where_zip, write_to_batch, RowMark};
pub(crate) use read_cursor::CursorRow;
pub(crate) use scatter::{copy_runs, materialize_carrying, stride, stride_dispatch, Pick};
pub(crate) use seek::pk_group_end;
pub(crate) use string_heap::{relocated_cell, should_relocate_blob};
pub(crate) use writer::DirectWriter;
