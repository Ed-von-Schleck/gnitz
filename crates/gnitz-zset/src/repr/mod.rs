//! A Z-set as bytes in memory and on the wire.
//!
//! - The batch: its region layout (`batch`), the checked row-at-a-time writer
//!   over one (`batch_builder`), the direct row writer (`writer`), wire
//!   serialization (`batch_wire`), and the German-string heap, plain or coded
//!   under a store's symbol table (`string_heap`).
//! - The thread-local object pools that cursor vectors, blob caches and
//!   kernel scratch recycle through (`pool`).
//! - The kernels over batches: the column pointer every kernel reads a region
//!   through and the row sources whose regions are read so (`columnar`), the
//!   OPK lower-bound search (`seek`), the column-first row copy (`scatter`),
//!   and the run merges, the N-way tournament they play, and single-batch
//!   consolidation (`merge`).
//!
//! The low-level WAL-block framer lives in `gnitz_wire::wal` (the one
//! definition client and engine share). The sorted runs a store keeps, the
//! shard file, the symbol table a shard heap is coded under and the cursor that
//! merges runs are `gnitz-trace`'s.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is this
//! crate's own facade. The submodules stay private, so a `pub` item inside one
//! is still dead-code checked.

mod batch;
mod batch_builder;
mod batch_wire;
mod columnar;
mod merge;
mod pool;
mod scatter;
mod seek;
mod string_heap;
mod writer;

pub use batch::{Batch, MemBatch, RowOrigins, uninit_bytes};
pub use batch_builder::BatchBuilder;
pub use batch_wire::{CertifiedBlock, WireCut, WireRows};
pub use merge::merge_consolidated;

pub(crate) use batch::runs_where_zip;
pub use batch::{range_rows, runs_where};
pub use columnar::{ColPtr, ColumnarSource, DecodedColumns};
pub use merge::{RunMerge, disjoint_order, run_merge, with_spans};
pub use pool::{MAX_POOLED_BYTES, recycle, take};
pub use pool::{vec_bytes, with_scratch};
pub use scatter::{Pick, UnifiedSet, gather_into, gather_rows};
pub use scatter::{copy_runs, materialize_carrying, stride, stride_dispatch, with_picks};
pub use scatter::{span_of, span_pick};
pub use seek::pk_group_end;
pub use seek::{gallop_by, lower_bound_by, seek_lower_bound, seek_pk_rows};
pub use string_heap::{BlobCache, prorated_blob_cap, relocated_cell, row_long_bytes, should_relocate_blob};
pub use string_heap::{HeapCoder, StringDest, append_value, carried_dead, long_bytes, long_bytes_outside};
pub use writer::DirectWriter;
