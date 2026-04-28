//! Storage subsystem: WAL, shards, MemTable, merge, cursors, and tables.
//!
//! Only the items listed under "Public API" are part of the official surface.
//! Engine code imports from `crate::storage::{Type, fn}`.

// Internal — not accessible outside storage/
mod bloom;
mod xor8;
mod wal;
mod manifest;
mod columnar;
mod heap;
mod compact;
mod shard_file;
mod shard_reader;
mod shard_index;
mod merge;
mod batch;
mod memtable;
mod read_cursor;
mod table;
mod partitioned_table;
pub(crate) mod batch_pool;
mod error;

// ── Public API ──────────────────────────────────────────────────────────────
pub use table::Table;
pub use partitioned_table::{PartitionedTable, partition_for_key, partition_arena_size};
pub use read_cursor::CursorHandle;
pub use batch::{Batch, ConsolidatedBatch, write_to_batch, encode_multi_to_wire, wire_byte_size_multi, decode_mem_batch_from_wal_block};
pub use merge::{MemBatch, scatter_copy, scatter_multi_source};
pub use error::StorageError;

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use read_cursor::ReadCursor;
pub(crate) use columnar::compare_rows;
pub(crate) use merge::DirectWriter;

