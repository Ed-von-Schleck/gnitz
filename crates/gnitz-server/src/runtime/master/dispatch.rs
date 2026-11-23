//! The MasterDispatcher's writes: ACKed rounds (checkpoint halves, backfill,
//! tick), the DDL zone, and the tick ledger.

use gnitz_foundation::{gnitz_debug, gnitz_note, gnitz_warn};
use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::time::Instant;

use rustc_hash::{FxHashMap, FxHashSet};

use super::*;
use crate::catalog::{ZoneError, ZoneSink};
use crate::runtime::reactor::AckWait;
use crate::runtime::sal::Apply;
use crate::runtime::sync::{WriteGuard, chan, oneshot};
use gnitz_foundation::fault::Switch;

/// `GNITZ_INJECT_TICK_EMIT_ERROR`: fail the next tick emit that has something to
/// tick, once, as a full SAL would.
static TICK_EMIT_ERROR: Switch = Switch::new("GNITZ_INJECT_TICK_EMIT_ERROR");

/// The relations owed a tick, the rounds the ticks written took, and the waits
/// on both. `pending` grows under the SAL hold its commit groups are written
/// under, and every emit takes all of it under the hold its Tick group is
/// written under.
pub(super) struct TickLedger {
    /// The last tick round allocated, one per tid a tick group carries. Starts
    /// at 1, which is never emitted, so `after_tick = 0` names no round.
    round: Cell<u64>,
    /// Ticked relation id → the last round that ticked it.
    last_tick: RefCell<FxHashMap<u64, u64>>,
    /// Relations with a commit no emitted tick has taken, each with the row
    /// count feeding the coalesce threshold.
    pending: RefCell<FxHashMap<u64, usize>>,
    /// The SAL watermark the last emit snapshotted, or the dispatcher's
    /// construction: every commit at or below it is reflected in every view,
    /// as a read written to the SAL from now on sees it.
    last_lsn: Cell<u64>,
    /// Scratch for the tids a tick is emitted for, kept across ticks.
    tids: RefCell<Vec<u64>>,
    /// The waits for a relation to gain an un-ticked commit, each with the
    /// relations it names. An entry leaves when a relation it names becomes
    /// pending, at a [`MasterDispatcher::wake_watches`], or, once its receiver
    /// is dropped, at the next [`MasterDispatcher::watch`].
    watches: RefCell<Vec<(FxHashSet<u64>, oneshot::Sender<()>)>>,
    /// The waits for the verdict of the next emit.
    drains: RefCell<Vec<oneshot::Sender<Result<(), WireFault>>>>,
    /// The requests to the tick loop: tick whatever is pending, even nothing,
    /// by the time sent. A credit that brings a tid to `coalesce_rows` asks for
    /// a tick now; a drain asks for one by its deadline.
    requests: chan::Sender<Instant>,
    /// The receiving end of `requests`, until the tick loop takes it.
    inbox: Cell<Option<chan::Receiver<Instant>>>,
    /// The pending rows of one relation that fire an auto-tick.
    /// `GNITZ_TICK_COALESCE_ROWS` lowers it, so tests reach it on small tables.
    coalesce_rows: usize,
}

impl TickLedger {
    pub(super) fn new(last_lsn: u64) -> Self {
        let (requests, inbox) = chan::unbounded();
        TickLedger {
            round: Cell::new(1),
            last_tick: RefCell::default(),
            pending: RefCell::default(),
            last_lsn: Cell::new(last_lsn),
            tids: RefCell::default(),
            watches: RefCell::default(),
            drains: RefCell::default(),
            requests,
            inbox: Cell::new(Some(inbox)),
            coalesce_rows: gnitz_foundation::env::env_num("GNITZ_TICK_COALESCE_ROWS", 10_000),
        }
    }
}

impl MasterDispatcher {
    // -----------------------------------------------------------------------
    // ACKed rounds
    // -----------------------------------------------------------------------

    /// A checkpoint's first half: the round that flushes base and system tables.
    ///
    /// When a push to a base table was published since the last one, the round
    /// moves the base stores past what the checkpointed views integrate, so a
    /// generation bump goes ahead of it: every checkpointed view and index is
    /// invalid until the next ephemeral round restamps it. Otherwise the round
    /// moves no base store, the generation stands, and the ephemeral round
    /// republishes only the stores whose manifests changed.
    ///
    /// The round makes the system tables durable first, then writes its
    /// `Flush`; once the ACKs are in it rewinds the SAL into the next epoch
    /// under the hold the `Flush` was written under, then removes every
    /// directory no live entity owns.
    ///
    /// Every failure ends the process; a failed system flush ends it before any
    /// worker leaves the epoch.
    pub(crate) async fn checkpoint_base(&self, _alone: &WriteGuard<ApplyLock>) {
        let mut excl = self.sal.lock().await;
        if let Err(e) = self.cat_mut().flush_ahead_of_base_round(self.sal.holds_pushes()) {
            gnitz_fatal_abort!("checkpoint system flush failed: {e}");
        }
        let at = excl
            .apply(Apply::Flush)
            .map_err(WireFault::from)
            .unwrap_or_else(|e| gnitz_fatal_abort!("checkpoint Flush refused: {e}"));
        self.reactor.applied_through(at).await;
        let epoch = excl.checkpoint_reset();
        gnitz_note!("SAL checkpoint epoch={epoch}");
        // Every worker ACKed the Flush, so each has applied every DdlSync written
        // before it, and the system flush made every applied DROP durable. The
        // checkpoint's one orphan sweep: the committer holds the apply lock a
        // DDL body runs under through the ephemeral round, so no DDL lands
        // before it, and at boot `reconcile_child_dirs` has already swept.
        if let Err(e) = self.cat().registry.reclaim_orphan_relation_dirs() {
            gnitz_warn!("catalog: orphan directory sweep failed: {}", e);
        }
    }

    /// A checkpoint's second half: the round that flushes every resumable
    /// view's traces and output stores, stamped with the durable generation.
    /// It rewinds nothing: its `FlushEph` stays in the epoch it is written in —
    /// the one the base round's rewind opened, or the boot's — and the SAL hold
    /// it is written under ends before its ACKs.
    ///
    /// Once the ACKs are in, the round flushes the system tables: that puts a
    /// SERIAL reservation published since that rewind under the replay floor.
    ///
    /// A boot ends with this round alone: its workers published their base
    /// stores during recovery, at the generation a replayed push advanced
    /// pre-fork, and no push is admitted yet.
    pub(crate) async fn checkpoint_ephemeral(&self, _alone: &WriteGuard<ApplyLock>) -> Result<(), WireFault> {
        let at = {
            let mut excl = self.sal.lock().await;
            debug_assert!(
                !self.sal.holds_pushes(),
                "a push ahead of the ephemeral round in its epoch is integrated by the views the round stamps and replayed into them again at boot"
            );
            let generation = self.cat().durable_generation();
            excl.apply(Apply::FlushEph { generation })?
        };
        self.reactor.applied_through(at).await;
        self.cat_mut().flush_all_system_tables()?;
        Ok(())
    }

    /// Backfill `view_ids` as one group, in id order: a view's id exceeds that of
    /// every relation it scans.
    pub(crate) async fn backfill_views_in_dep_order(
        &self,
        _alone: &WriteGuard<ApplyLock>,
        view_ids: &[u64],
    ) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        if ordered.is_empty() {
            return Ok(());
        }
        let at = self.sal.lock().await.apply(Apply::Backfill { views: ordered.into() })?;
        self.reactor.applied_through(at).await;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // DDL zone
    // -----------------------------------------------------------------------

    /// Run catalog write `write` with its groups laid out as one SAL zone, and mark
    /// the zone applied: the write's value and the zone LSN, `0` when the write
    /// published nothing and so opened no zone. A write the catalog or
    /// the log refuses leaves the catalog as it was and its scope unpublished. A
    /// diverged catalog is one no log describes, so the process ends on it.
    /// Submits no fdatasync. The divergence abort names the caller's location.
    #[track_caller]
    pub(crate) fn emit_zone<T>(
        &self,
        excl: &mut SalExcl<'_>,
        write: impl FnOnce(&mut CatalogEngine, &mut ZoneSink<'_>) -> Result<T, ZoneError>,
    ) -> Result<(T, u64), WireFault> {
        let mut scope = excl.begin();
        // Each DDL batch is logged inside the zone, so recovery groups a DDL's
        // groups atomically.
        let written = write(&mut self.cat_mut(), &mut |family, batch| {
            scope.ddl_sync(family, batch)?;
            gnitz_debug!("ddl group tid={} rows={}", family.id(), batch.len());
            Ok(())
        });
        let value = match written {
            Ok(v) => v,
            Err(ZoneError::Refused(e)) => return Err(e),
            Err(ZoneError::Diverged(e)) => gnitz_fatal_abort!("{}: {e}", std::panic::Location::caller()),
        };
        // Only a zone has an LSN, so only a closed zone raises the replay floor.
        let Some(zone_lsn) = scope.commit() else {
            return Ok((value, 0));
        };
        self.cat_mut().mark_zone_applied(zone_lsn);
        Ok((value, zone_lsn))
    }

    // -----------------------------------------------------------------------
    // Tick ledger
    // -----------------------------------------------------------------------

    /// [`Self::owe`] `rows` to `tid` when a view scans `tid`: the credit of
    /// rows whose landing and credit are decided under one hold.
    pub(crate) fn credit(&self, laid_out: &SalExcl<'_>, tid: u64, rows: usize) {
        if self.cat().dag.is_scanned(tid) {
            self.owe(laid_out, tid, rows);
        }
    }

    /// Credit `rows` that landed above the cut while a view scanned `tid`,
    /// whether or not one still does, against `tid`'s pending-tick count. Its
    /// first commit since the emit that last took it wakes the watches naming
    /// it. A credit that brings `tid` to the coalesce threshold asks the tick
    /// loop for a tick now; below it the rows only accumulate, and are taken by
    /// the next emit, whichever task runs it. `_laid_out` is the SAL hold the
    /// rows' groups were written under.
    pub(crate) fn owe(&self, _laid_out: &SalExcl<'_>, tid: u64, rows: usize) {
        let n = match self.ticks.pending.borrow_mut().entry(tid) {
            Entry::Occupied(mut e) => {
                *e.get_mut() += rows;
                *e.get()
            }
            Entry::Vacant(e) => {
                e.insert(rows);
                let mut watches = self.ticks.watches.borrow_mut();
                for (_, wake) in watches.extract_if(.., |(relations, _)| relations.contains(&tid)) {
                    wake.send(());
                }
                rows
            }
        };
        if n >= self.ticks.coalesce_rows {
            self.ticks.requests.send(self.reactor.now());
        }
    }

    /// True when none of `relations` has a commit no emitted tick has taken.
    pub(crate) fn none_pending(&self, relations: impl IntoIterator<Item = u64>) -> bool {
        let pending = self.ticks.pending.borrow();
        pending.is_empty() || relations.into_iter().all(|s| !pending.contains_key(&s))
    }

    /// True when no relation whose ticks reach any of `targets` has a commit no
    /// emitted tick has taken, so a read of them misses nothing. A commit is
    /// pending from its layout on, ahead of its ACK, so this errs towards
    /// false, which costs one extra drain.
    pub(crate) fn all_ticked(&self, targets: impl IntoIterator<Item = u64>) -> bool {
        let cat = self.cat();
        self.none_pending(targets.into_iter().flat_map(|t| cat.dag.roots_of(t).iter().copied()))
    }

    /// Resolves when a relation whose ticks reach one of `targets` gains a
    /// commit no emitted tick has taken, or at a [`Self::wake_watches`]; `None`
    /// when one already has. The check and the registration share no await, so
    /// no commit falls between them. Dropping the receiver gives the wait up.
    pub(crate) fn watch(&self, targets: impl IntoIterator<Item = u64>) -> Option<oneshot::Receiver<()>> {
        let relations: FxHashSet<u64> = {
            let cat = self.cat();
            targets
                .into_iter()
                .flat_map(|t| cat.dag.roots_of(t).iter().copied())
                .collect()
        };
        if !self.none_pending(relations.iter().copied()) {
            return None;
        }
        let (wake, woken) = oneshot::channel();
        let mut watches = self.ticks.watches.borrow_mut();
        watches.retain(|(_, wake)| !wake.is_closed());
        watches.push((relations, wake));
        Some(woken)
    }

    /// Wake every watch. A watch names relations read off a catalog its holder
    /// has released, which a DDL may have changed.
    pub(crate) fn wake_watches(&self) {
        for (_, wake) in self.ticks.watches.take() {
            wake.send(());
        }
    }

    /// The requests the tick loop receives: tick whatever is pending by the
    /// time sent. Taken once.
    pub(crate) fn tick_requests(&self) -> chan::Receiver<Instant> {
        self.ticks.inbox.take().expect("one tick loop takes the requests")
    }

    /// The verdict of the next [`Self::emit_tick`], whichever task runs it,
    /// with a tick asked of the tick loop by `by`, so that some emit answers
    /// it.
    pub(crate) fn drain(&self, by: Instant) -> oneshot::Receiver<Result<(), WireFault>> {
        let (done, verdict) = oneshot::channel();
        self.ticks.drains.borrow_mut().push(done);
        self.ticks.requests.send(by);
        verdict
    }

    /// Tick every pending relation as one Tick group, and answer the wait for
    /// its workers' ACKs; `None` when nothing is pending. `_alone` is the
    /// caller's hold of the apply lock, which it keeps through those ACKs. A
    /// refused emit leaves every relation pending and takes no round.
    ///
    /// The tids and the watermark are read under the hold [`Self::credit`] is
    /// called under, so they are of the same commits. Every [`Self::drain`]
    /// is answered with the emit's verdict, at the emit and not its ACKs: its
    /// reader's scan follows the Tick group in the SAL. The answer shares no
    /// await with the read of what is pending, so every commit pending when the
    /// wait was registered is in the group.
    pub(crate) async fn emit_tick(&self, _alone: &WriteGuard<ApplyLock>) -> Result<Option<AckWait>, WireFault> {
        let ticks = &self.ticks;
        let emitted = async {
            if ticks.pending.borrow().is_empty() {
                ticks.last_lsn.set(self.sal.watermark());
                return Ok(None);
            }
            let mut excl = self.sal.lock().await;
            if TICK_EMIT_ERROR.take_once() {
                return Err("injected tick emit error".to_string().into());
            }
            let mut tids = ticks.tids.borrow_mut();
            tids.clear();
            tids.extend(ticks.pending.borrow().keys().copied());
            let wait = self.write_tick(&mut excl, &tids)?;
            ticks.pending.borrow_mut().clear();
            ticks.last_lsn.set(self.sal.watermark());
            Ok(Some(wait))
        }
        .await;
        let verdict = emitted.as_ref().map(|_| ()).map_err(WireFault::clone);
        for done in ticks.drains.take() {
            done.send(verdict.clone());
        }
        emitted
    }

    /// [`Self::emit_tick`], then its workers' ACKs.
    pub(crate) async fn tick(&self, alone: &WriteGuard<ApplyLock>) -> Result<(), WireFault> {
        if let Some(wait) = self.emit_tick(alone).await? {
            wait.await;
        }
        Ok(())
    }

    /// Write one Tick group for `tids` at consecutive tick rounds, and answer
    /// the wait for its ACKs. A refused write takes no round.
    fn write_tick(&self, excl: &mut SalExcl<'_>, tids: &[u64]) -> Result<AckWait, WireFault> {
        let ticks = &self.ticks;
        let first = ticks.round.get() + 1;
        let at = excl.apply(Apply::Tick { first_round: first, tids: tids.into() })?;
        ticks.round.set(first + tids.len() as u64 - 1);
        // At the write, not at the ACKs: a round written has already reached the
        // workers, and a ledger behind it would let a feed's gate answer "nothing
        // changed" over rows already in flight.
        let mut last_tick = ticks.last_tick.borrow_mut();
        for (&tid, round) in tids.iter().zip(first..) {
            last_tick.insert(tid, round);
        }
        Ok(self.reactor.applied_through(at))
    }

    /// The SAL watermark the last emit snapshotted.
    pub(super) fn last_lsn(&self) -> u64 {
        self.ticks.last_lsn.get()
    }

    /// The last round that ticked a relation `view_id` reads, directly or
    /// through the views under it, or `1` when none has been ticked — the boot
    /// round, which is never emitted and captures nothing, so a bootstrap at
    /// `after_tick = 0` always falls through to the store. May name a round
    /// that left the view no rows, one from before the view existed among them;
    /// never misses one that did.
    pub(crate) fn last_delta_round(&self, view_id: u64) -> u64 {
        let last_tick = self.ticks.last_tick.borrow();
        let cat = self.cat();
        let roots = cat.dag.roots_of(view_id);
        roots
            .iter()
            .filter_map(|s| last_tick.get(s))
            .copied()
            .max()
            .unwrap_or(1)
    }

    /// Drop a dropped relation's pending rows and last tick round.
    pub(crate) fn forget_relation(&self, id: u64) {
        self.ticks.pending.borrow_mut().remove(&id);
        self.ticks.last_tick.borrow_mut().remove(&id);
    }
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/dispatch.rs"]
mod bench;
