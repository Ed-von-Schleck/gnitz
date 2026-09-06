//! The catalog applier's execution context.
//!
//! `ApplyContext` lives in its own module so its fields are invisible to the
//! sibling modules (`hooks`, `write_path`, …) that consume it: the transient
//! sub-states can only be entered through the scope helpers below, making
//! their enter/exit balance a compile-time guarantee rather than a convention.

use std::num::NonZeroU64;

use super::CatalogEngine;

/// Which of the three ways a catalog row can reach the applier is running now.
/// One value rather than a pair of booleans, whose fourth state is unreachable.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(in crate::catalog) enum ApplyMode {
    /// Boot shard replay: every row arrives as a lone `+1` at its current value,
    /// so no row carries a transition.
    Replay,
    /// First application of a client's rows.
    Live,
    /// `compensate_stage_a` replaying negated deltas to undo a failed DDL.
    Compensating,
}

/// The catalog applier's current execution context: the apply mode and the
/// DDL-zone LSN — one inspectable place that answers "what is the applier doing
/// right now".
///
/// Not `Copy`: every access goes through an accessor or the scope helper below,
/// no call site needs the whole context by value, and a stray by-value copy
/// could silently desync the mode.
pub(in crate::catalog) struct ApplyContext {
    /// `Replay` only while `open()` replays boot shards; latched to `Live` once
    /// by `go_live()`, and entered/left as `Compensating` by
    /// [`CatalogEngine::with_rollback_compensation`].
    ///
    /// It gates work a replay or a compensation must not redo; each call site
    /// states which. SAL system-table recovery runs *after* `go_live()`, so it
    /// applies as `Live` and dedupes on the per-family LSN watermark instead.
    mode: ApplyMode,
    /// LSN every write in the current DDL zone is pinned to; `None` outside
    /// a zone. Owned by the executor's DDL-zone lifecycle — `open_ddl_zone`
    /// before the mutate phase, `close_ddl_zone` on every surviving exit —
    /// so unlike the mode it needs no scope balancing. See `apply_local` for
    /// how the pin drives recovery's dedup watermark.
    ddl_zone_lsn: Option<NonZeroU64>,
}

impl ApplyContext {
    pub(super) fn new() -> Self {
        Self {
            mode: ApplyMode::Replay,
            ddl_zone_lsn: None,
        }
    }
    #[inline]
    pub(super) fn mode(&self) -> ApplyMode {
        self.mode
    }
    #[inline]
    pub(super) fn go_live(&mut self) {
        self.mode = ApplyMode::Live;
    }
    #[inline]
    pub(super) fn in_rollback(&self) -> bool {
        self.mode == ApplyMode::Compensating
    }
    #[inline]
    pub(super) fn ddl_zone_lsn(&self) -> Option<NonZeroU64> {
        self.ddl_zone_lsn
    }

    /// Open a DDL zone: pin every system-table write in the current DDL to
    /// `lsn`. The executor calls this before the mutate phase so every
    /// cascading hook sees the same value. Takes `NonZeroU64` so "no zone"
    /// cannot be smuggled in as a 0 sentinel.
    #[inline]
    pub(crate) fn open_ddl_zone(&mut self, lsn: NonZeroU64) {
        self.ddl_zone_lsn = Some(lsn);
    }

    /// Close the DDL zone after it is durably committed (or rolled back) and
    /// leave compensation. On the non-panic path the mode is already `Live`, so
    /// the reset is a no-op; after a caught forward-DDL panic (dev/test unwind
    /// builds) it restores the invariant before the next DDL — the zone close is
    /// the single point every DDL (success or compensated failure) passes
    /// through. Only `Compensating` is reset: an unconditional `Live` would also
    /// promote a `Replay` that has not reached `go_live()` yet.
    #[inline]
    pub(crate) fn close_ddl_zone(&mut self) {
        self.ddl_zone_lsn = None;
        if self.mode == ApplyMode::Compensating {
            self.mode = ApplyMode::Live;
        }
    }
}

impl CatalogEngine {
    /// Run `f` as Stage-A rollback compensation: `submit` is redirected to
    /// the no-broadcast path and backfill/cascade re-issue is skipped for its
    /// duration. Restored even on an `Err` return; a panic inside `f` leaves
    /// the mode set until `close_ddl_zone`'s transient reset (release builds
    /// abort on panic). Closure form, not a `Drop` guard: the body needs
    /// `&mut self` for the `submit_local` calls inside, which a guard
    /// borrowing the context would block.
    pub(super) fn with_rollback_compensation<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let prev = self.ctx.mode;
        self.ctx.mode = ApplyMode::Compensating;
        let r = f(self);
        self.ctx.mode = prev;
        r
    }
}
