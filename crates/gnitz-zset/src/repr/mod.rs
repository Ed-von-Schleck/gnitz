//! A Z-set as bytes in memory and on the wire.
//!
//! - The batch: its region layout (`batch`), the row-at-a-time system-table
//!   writer over one (`batch_builder`), the direct row writer (`writer`), wire
//!   serialization (`batch_wire`), the German-string heap (`string_heap`), and
//!   the FSST symbol table a heap's long values are coded under (`codebook`).
//! - The thread-local object pools that batch arenas, send buffers, cursor
//!   vectors and kernel scratch recycle through (`pool`).
//! - The kernels over batches: the OPK lower-bound search and the column
//!   pointer it reads through (`seek`), the column-first row copy (`scatter`),
//!   and the run merges, the N-way tournament they play, and single-batch
//!   consolidation (`merge`).
//!
//! The low-level WAL-block framer lives in `gnitz_wire::wal` (the one
//! definition client and engine share). The sorted runs a store keeps, the
//! shard file and the cursor that merges runs are `gnitz-trace`'s.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is this
//! crate's own facade. The submodules stay private, so a `pub` item inside one
//! is still dead-code checked.

mod batch;
mod batch_builder;
mod batch_wire;
mod codebook;
mod merge;
mod pool;
mod scatter;
mod seek;
mod string_heap;
mod writer;

pub use batch::{Batch, MemBatch};
pub use batch_builder::BatchBuilder;
pub use batch_wire::WireRows;
pub use codebook::Codebook;
pub use merge::merge_consolidated;
pub use pool::PooledBuf;

pub(crate) use batch::runs_where_zip;
pub use batch::{range_rows, runs_where};
pub use merge::{LoserTree, MergeOrder, Player, disjoint_order, run_merge};
pub use merge::{dominated, drive, merge_less};
pub use pool::{MAX_POOLED_BYTES, recycle, take};
pub use pool::{vec_bytes, with_scratch};
pub use scatter::{ColumnarSource, DecodedColumns, Pick, UnifiedSet, gather_into, gather_rows};
pub use scatter::{copy_runs, materialize_carrying, stride, stride_dispatch, with_picks};
pub use seek::pk_group_end;
pub use seek::{ColPtr, ZERO_CELL, gallop_by, lower_bound_by, seek_lower_bound, seek_pk_rows};
pub use string_heap::{BlobCache, prorated_blob_cap, relocated_cell, row_long_bytes, should_relocate_blob};
pub use string_heap::{StringDest, append_value, carried_dead, long_bytes, long_bytes_outside};
pub use writer::DirectWriter;
