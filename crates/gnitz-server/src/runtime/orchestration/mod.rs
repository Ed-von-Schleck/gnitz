//! L7 orchestration — the master SAL dispatcher, the worker dispatch loop, the
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
pub(super) mod lsn;
pub(super) mod master;
pub(super) mod peer;
pub(super) mod worker;

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged.
///
/// `Reactor::poll_task` does not catch unwinds, so a panic in any task
/// propagates through `tick` and takes the process down. Two kinds of call site
/// wrap against that, and they want opposite things from the `Err`: a request
/// handler returns it to the client as `STATUS_ERROR` and stays live, while a
/// site whose panic would leave master and workers inconsistent — relay
/// emission, DDL compensation, view backfill and re-stamp — turns it into
/// `gnitz_fatal_abort!`. Which one a site is, is in its `Err` arm, not here.
///
/// Debug and test builds only: release is `panic = "abort"` (`crates/Cargo.toml`),
/// where the process dies at the panic and no `Err` arm below ever runs.
pub(crate) fn guard_panic<T, F>(op: &'static str, f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(_) => Err(format!("internal server error (panic in {op})")),
    }
}
