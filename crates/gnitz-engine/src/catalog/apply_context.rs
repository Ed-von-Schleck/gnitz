//! The catalog applier's execution context.
//!
//! `ApplyContext` lives in its own module so its fields are invisible to the
//! sibling modules (`hooks`, `write_path`, …) that consume it: the transient
//! sub-states can only be entered through the scope helpers below, making
//! their enter/exit balance a compile-time guarantee rather than a convention.

use std::num::NonZeroU64;

use super::CatalogEngine;

/// The catalog applier's current execution context: the engine phase, the
/// re-entrant sub-operation (if any) the in-flight submission is part of, and
/// the DDL-zone LSN — one inspectable place that answers "what is the applier
/// doing right now".
///
/// Not `Copy`: every access goes through an accessor or a scope helper, no
/// call site needs the whole context by value, and a stray by-value copy
/// could silently desync the transient sub-states.
pub(crate) struct ApplyContext {
    /// True iff this catalog row is being applied for the FIRST time, rather
    /// than replayed from already-validated persisted state: false only while
    /// `open()` replays boot shards, latched true once by `go_live()`.
    ///
    /// So it gates work that a replay must not redo — minting a row the replay
    /// will supply anyway, or re-validating data that passed its check at
    /// original write time. Each callsite states which of the two it is. SAL
    /// system-table recovery runs *after* `go_live()`, so it applies live and
    /// relies on the per-family LSN watermark for idempotence, not this flag.
    live: bool,
    /// True while `compensate_stage_a` replays compensating deltas: `submit`
    /// is redirected to the no-broadcast path and backfill/cascade re-issue
    /// is skipped, so no compensating row is re-broadcast to workers and no
    /// side effect (backfill_index, cascade_retract_columns, hook_cascade_fk)
    /// re-runs. Never nested.
    rollback: bool,
    /// True while an owner-drop cascade is in flight: suppresses the precheck
    /// guards that exist to police a standalone user DROP — the IDX_TAB
    /// FK-target rule and the COL_TAB unpaired-`-1` reject. The owner drop
    /// already passed its own FK/view-dep precheck. During rollback precheck is
    /// bypassed by the `submit` → `submit_local` redirect, so the flag is moot
    /// there.
    cascade_drop: bool,
    /// LSN every write in the current DDL zone is pinned to; `None` outside
    /// a zone. Owned by the executor's DDL-zone lifecycle — `open_ddl_zone`
    /// before the mutate phase, `close_ddl_zone` on every surviving exit —
    /// so unlike the two transient flags it needs no scope balancing. See
    /// `apply_local` for how the pin drives recovery's dedup watermark.
    ddl_zone_lsn: Option<NonZeroU64>,
}

impl ApplyContext {
    pub(super) fn new() -> Self {
        Self {
            live: false,
            rollback: false,
            cascade_drop: false,
            ddl_zone_lsn: None,
        }
    }
    #[inline]
    pub(super) fn is_live(&self) -> bool {
        self.live
    }
    #[inline]
    pub(super) fn go_live(&mut self) {
        self.live = true;
    }
    #[inline]
    pub(super) fn in_rollback(&self) -> bool {
        self.rollback
    }
    #[inline]
    pub(super) fn in_cascade_drop(&self) -> bool {
        self.cascade_drop
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
    /// reset the transient sub-states. On the non-panic path both transients
    /// are already false, so the reset is a no-op; after a caught
    /// forward-DDL panic (dev/test unwind builds) it restores the invariant
    /// before the next DDL — the zone close is the single point every DDL
    /// (success or compensated failure) passes through. Subsequent non-DDL
    /// ingest paths use the auto-bump.
    #[inline]
    pub(crate) fn close_ddl_zone(&mut self) {
        self.ddl_zone_lsn = None;
        self.cascade_drop = false;
        self.rollback = false;
    }
}

impl CatalogEngine {
    /// Run `f` as Stage-A rollback compensation: `submit` is redirected to
    /// the no-broadcast path and backfill/cascade re-issue is skipped for its
    /// duration. Restored even on an `Err` return; a panic inside `f` leaves
    /// the flag set until `close_ddl_zone`'s transient reset (release builds
    /// abort on panic). Closure form, not a `Drop` guard: the body needs
    /// `&mut self` for the `submit_local` calls inside, which a guard
    /// borrowing the context would block.
    pub(super) fn with_rollback_compensation<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let prev = self.ctx.rollback;
        self.ctx.rollback = true;
        let r = f(self);
        self.ctx.rollback = prev;
        r
    }

    /// Run `f` as part of an owner-drop cascade: precheck's IDX_TAB FK-target
    /// guard and its unpaired-`-1` COL_TAB reject are suppressed for its
    /// duration. Closure form for the same reason as
    /// `with_rollback_compensation`.
    pub(super) fn with_cascade_drop<R>(&mut self, f: impl FnOnce(&mut Self) -> R) -> R {
        let prev = self.ctx.cascade_drop;
        self.ctx.cascade_drop = true;
        let r = f(self);
        self.ctx.cascade_drop = prev;
        r
    }
}
