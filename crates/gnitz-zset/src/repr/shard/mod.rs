//! The shard file. Its image has one owner: `file` writes it, `reader` maps and
//! validates it, and `layout` (the framing) and `encoding` (the region
//! encodings) hold the format rules both call. `codebook` is the heap's FSST
//! symbol table, `pk_filter` the PK-probe filter, `mmap` the read-only file
//! mapping, `compact` the guard-routed shard merge a compaction runs, and `error`
//! the error type a shard read or write fails with.

mod codebook;
mod compact;
mod encoding;
mod error;
mod file;
mod layout;
mod mmap;
mod pk_filter;
mod reader;

pub use codebook::Codebook;
pub use compact::{guard_slot, merge_guard, GuardRun};
pub use error::StorageError;
pub use file::ShardWriteOpts;
pub use reader::{MappedShard, ShardDirectory};
