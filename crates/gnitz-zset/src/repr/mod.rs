//! A Z-set as bytes — in memory, on the wire, on disk — and the cursor that
//! reads it back.
//!
//! The pure batch representation and the operations that work directly on it:
//! region layout (`batch`), wire/shard serialization (`batch_wire`), TLS buffer
//! recycling (`batch_pool`), the OPK lower-bound search (`seek`), sort-merge
//! consolidation (`merge`), the German-string heap (`string_heap`), the
//! column-first row copy (`scatter`), the N-way min-merge tournament (`loser_tree`), the
//! shard PK-probe filter (`shard_filter`), the shard-image encoder and writer
//! (`shard_file`), the shard-format constants (`layout`), and the guard-routed
//! shard merge a compaction runs (`compact`), and the bounded external sort of
//! key records (`spill`). The low-level WAL-block framer
//! lives in `gnitz_wire::wal` (the one definition client and engine share). The
//! row-at-a-time system-table writer over a batch is `batch_builder`.
//!
//! The shard *image* has one owner: `shard_file` encodes it, `shard_reader`
//! mmaps and validates it, and `layout` holds the format rules both call.
//!
//! Above the representation sit the sorted run, whatever backs it (`run`), and
//! the opaque read cursor that merges runs (`read_cursor`). Which runs a store
//! holds, and when they flush, compact or publish, is `gnitz-store`'s: this rung
//! is handed runs and never asks where they came from.
//!
//! A `pub use` below is named by another crate; a `pub(crate) use` is this
//! crate's own facade. The submodules stay private, so a `pub` item inside one
//! is still dead-code checked.

mod batch;
mod batch_builder;
mod batch_pool;
mod batch_wire;
mod compact;
mod error;
mod layout;
mod loser_tree;
mod merge;
mod read_cursor;
mod run;
mod scatter;
mod seek;
mod shard_file;
mod shard_filter;
mod shard_reader;
mod spill;
mod string_heap;

pub use batch::Batch;
pub use batch_builder::BatchBuilder;
pub use batch_pool::PooledBuf;
pub use batch_wire::{WalBlock, WireRows};
pub use compact::{guard_slot, merge_and_route, EmitGuard};
pub use error::StorageError;
pub use merge::{merge_consolidated, MemBatch};
pub use read_cursor::{
    empty_cursor, from_runs, from_runs_at, from_runs_in_band, BoundedIndexCursor, PkSetGather, ReadCursor,
    SkeletonKeys, SourceCursor,
};
pub use run::{first_live_payload_group, Run, StoredRow};
pub use seek::pk_group_end;
pub use shard_file::ShardWriteOpts;
pub use shard_reader::MappedShard;
pub use spill::{KeyProducer, SpillSort};

pub(crate) use batch::{range_rows, RowMark};
pub(crate) use scatter::materialize_carrying;
pub(crate) use seek::pk_prefix_group_end;
pub(crate) use string_heap::{copy_string_cells, prorated_blob_cap, BlobCache};
