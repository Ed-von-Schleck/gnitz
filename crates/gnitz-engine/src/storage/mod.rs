//! Storage subsystem: WAL, shards, MemTable, merge, cursors, and tables.
//!
//! Only the items listed under "Public API" are part of the official surface.
//! Engine code imports from `crate::storage::{Type, fn}`.

// Internal — not accessible outside storage/
mod bloom;
mod xor8;
pub(crate) mod wal;
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
pub use batch::{Batch, write_to_batch};
pub use merge::{MemBatch, scatter_copy, sort_and_consolidate};
pub use error::StorageError;

// ── Crate-internal: operator hot-path types (not official surface) ───────────
pub(crate) use read_cursor::{ReadCursor, create_cursor_from_snapshots};
pub(crate) use columnar::compare_rows;

