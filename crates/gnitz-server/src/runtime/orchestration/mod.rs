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

pub(super) mod committer;
pub(super) mod executor;
pub(super) mod master;
pub(super) mod peer;
pub(super) mod worker;
