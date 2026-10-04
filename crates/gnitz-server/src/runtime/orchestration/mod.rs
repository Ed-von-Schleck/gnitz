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

use std::future::Future;
use std::task::Poll;

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

/// Run `f` under `catch_unwind`. On panic, returns
/// `Err("internal server error (panic in <op>)")`. Otherwise the closure's
/// `Result` is returned unchanged.
///
/// `Reactor::poll_task` does not catch unwinds, so a panic in any task
/// propagates through `block_on` and takes the process down. Two kinds of call site
/// wrap against that, and they want opposite things from the `Err`: a request
/// handler returns it to the client as `WireStatus::Error` and stays live, while a
/// site whose panic would leave master and workers inconsistent — DDL
/// compensation, view backfill — turns it into
/// `gnitz_fatal_abort!`. Which one a site is, is in its `Err` arm, not here.
///
/// Debug and test builds only: an optimized build dies at the panic — by
/// `panic = "abort"` (`crates/Cargo.toml`), or by `main`'s panic hook under a
/// profile without it — and no `Err` arm below ever runs.
pub(crate) fn guard_panic<T, E: From<String>>(op: &'static str, f: impl FnOnce() -> Result<T, E>) -> Result<T, E> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|_| Err(panicked(op)))
}

/// [`guard_panic`] over a future: a panic in any poll ends it as `Err`.
pub(crate) async fn guard_panic_async<T, E: From<String>>(
    op: &'static str,
    fut: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let mut fut = std::pin::pin!(fut);
    std::future::poll_fn(|cx| {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fut.as_mut().poll(cx)))
            .unwrap_or_else(|_| Poll::Ready(Err(panicked(op))))
    })
    .await
}

fn panicked<E: From<String>>(op: &str) -> E {
    format!("internal server error (panic in {op})").into()
}
