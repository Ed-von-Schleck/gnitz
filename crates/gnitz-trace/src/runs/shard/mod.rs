//! The shard file. Its image has one owner: `file` writes it, `reader` maps and
//! validates it, and `layout` (the framing) and `encoding` (the region
//! encodings) hold the format rules both call. `pk_filter` is the PK-probe
//! filter, `mmap` the read-only file mapping, `compact` the guard-routed shard
//! merge a compaction runs, `codebook` the FSST symbol table a store codes its
//! shard heaps under and when it trains another, and `error` the error type a
//! shard read or write fails with.

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
pub use compact::{GuardMerge, GuardRun, guard_slot};
pub use error::StorageError;
pub(crate) use file::write_shard_over;
pub use file::{ShardWriteOpts, write_shard};
pub use reader::{MappedShard, ShardDirectory};
