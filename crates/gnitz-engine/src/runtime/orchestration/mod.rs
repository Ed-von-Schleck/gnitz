//! L7 orchestration — the master SAL dispatcher, the worker dispatch loop, the
//! single-threaded server executor, and the durable-commit batcher.
//!
//! Internal grouping, not a facade: `runtime/mod.rs` aliases these submodules so
//! the historical `crate::runtime::<mod>` paths keep resolving across the
//! subsystem. They name each other (and `protocol`/`reactor`) through those
//! `crate::runtime::` paths.

pub(super) mod committer;
pub(super) mod executor;
pub(super) mod lsn;
pub(super) mod master;
pub(super) mod peer;
pub(super) mod worker;

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged. Used in async handlers and the committer
/// task where a panic must not propagate (per async-invariants V.4 / V.7).
pub(crate) fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err(format!("internal server error (panic in {op})")),
    }
}
