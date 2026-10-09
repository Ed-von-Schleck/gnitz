//! Orchestration — the master SAL dispatcher, the worker dispatch loop, the
//! single-threaded server executor, and the durable-commit batcher.
//!
//! Internal grouping, not a facade: `runtime/mod.rs` re-aliases these submodules
//! flat, so they name each other (and `protocol`/`reactor`) as
//! `crate::runtime::<mod>`.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use gnitz_wire::WireConflictMode;
use gnitz_zset::repr::Batch;

/// One decoded, shape-validated transaction family: the target `tid`, its
/// conflict `mode`, and the decoded batch. The executor decodes into these, the
/// validator borrows them, and the committer takes them by value.
///
/// It lives here rather than in any of those three siblings: the committer, the
/// only owner, already names the master module, so hosting it there would close
/// that edge into a cycle.
pub(crate) struct TxnFamily {
    pub tid: u64,
    pub mode: WireConflictMode,
    pub batch: Batch,
}

pub(super) mod committer;
pub(super) mod executor;
pub(super) mod master;
pub(super) mod peer;
pub(super) mod worker;
