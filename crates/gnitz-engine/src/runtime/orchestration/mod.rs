//! L7 orchestration — the master SAL dispatcher, the worker dispatch loop, the
//! single-threaded server executor, and the durable-commit batcher.
//!
//! Internal grouping, not a facade: `runtime/mod.rs` aliases these submodules so
//! the historical `crate::runtime::<mod>` paths keep resolving across the
//! subsystem. They name each other (and `protocol`/`reactor`) through those
//! `crate::runtime::` paths.

pub(super) mod committer;
pub(super) mod executor;
pub(super) mod master;
pub(super) mod worker;
