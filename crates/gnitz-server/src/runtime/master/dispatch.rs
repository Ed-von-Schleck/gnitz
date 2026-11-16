//! The MasterDispatcher's writes: ACKed rounds (checkpoint halves, backfill,
//! tick) and the tick ledger, the DDL zone, push groups, and the backup round.

use std::cell::Cell;
use std::collections::hash_map::Entry;

use rustc_hash::{FxHashMap, FxHashSet};

use super::route::with_routed;
use super::*;
use crate::catalog::{ZoneError, ZoneSink};
use crate::runtime::reactor::AckWait;
use crate::runtime::sal::{Apply, SalFull, SalScope};
use crate::runtime::sync::{WriteGuard, oneshot};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::PkRule;
use gnitz_zset::repr::MemBatch;

/// `GNITZ_INJECT_BACKUP_ABORT`: abort the master between the workers' copies and
/// its own, as a crash part-way through a backup would.
static BACKUP_ABORT: Seam = Seam::new("GNITZ_INJECT_BACKUP_ABORT");

/// `GNITZ_INJECT_TICK_EMIT_ERROR`: fail the next tick emit that has something to
/// tick, once, as a full SAL would.
static TICK_EMIT_ERROR: Seam = Seam::new("GNITZ_INJECT_TICK_EMIT_ERROR");

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
    /// The waits for a relation to gain an un-ticked commit. An entry leaves
    /// when a relation it names becomes pending, at a
    /// [`MasterDispatcher::wake_watches`], or with its [`Watching`].
    watches: RefCell<Vec<Watch>>,
    /// The id of the next [`Watch`].
    next_watch: Cell<u64>,
    /// The waits for the verdict of the next emit.
    drains: RefCell<Vec<oneshot::Sender<Result<(), WireFault>>>>,
}

impl TickLedger {
    pub(super) fn new(last_lsn: u64) -> Self {
        TickLedger {
            round: Cell::new(1),
            last_tick: RefCell::default(),
            pending: RefCell::default(),
            last_lsn: Cell::new(last_lsn),
            tids: RefCell::default(),
            watches: RefCell::default(),
            next_watch: Cell::new(0),
            drains: RefCell::default(),
        }
    }

    /// Wake every watch naming `relation`, which just became pending.
    fn wake(&self, relation: u64) {
        let mut watches = self.watches.borrow_mut();
        for watch in watches.extract_if(.., |w| w.relations.contains(&relation)) {
            watch.wake.send(());
        }
    }
}

struct Watch {
    id: u64,
    relations: FxHashSet<u64>,
    wake: oneshot::Sender<()>,
}

/// One wait registered by [`MasterDispatcher::watch`], given up on drop.
pub(crate) struct Watching<'a> {
    ledger: &'a TickLedger,
    id: u64,
    /// Resolves when a watched relation gains an un-ticked commit, or at a
    /// [`MasterDispatcher::wake_watches`].
    pub(crate) woken: oneshot::Receiver<()>,
}

impl Drop for Watching<'_> {
    fn drop(&mut self) {
        self.ledger.watches.borrow_mut().retain(|w| w.id != self.id);
    }
}

impl MasterDispatcher {
    /// Write `apply` to every worker, and answer the wait for all of them to
    /// have run it; they are woken at the write, so a caller may await the ACKs
    /// under the hold of `excl`. `_alone` is the caller's hold of the apply lock.
    fn write_acked(
        &self,
        _alone: &WriteGuard<ApplyLock>,
        excl: &mut SalExcl<'_>,
        apply: Apply<'_>,
    ) -> Result<AckWait, WireFault> {
        Ok(self.reactor.applied_through(excl.apply(apply)?))
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
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
    /// Once its ACKs are in, the round flushes the system tables and rewinds the
    /// SAL into the next epoch, under the hold its `Flush` was written under,
    /// then removes every directory no live entity owns.
    ///
    /// Every failure ends the process. A failed system flush leaves the log
    /// unrewound, as the log's entries are those rows' only durable copy, while
    /// the workers that ACKed the `Flush` have moved into the next epoch: no
    /// group written after it would reach them, and only a restart's replay
    /// repairs that.
    pub(crate) async fn checkpoint_base(&self, alone: &WriteGuard<ApplyLock>) {
        let mut excl = self.sal.lock().await;
        if self.sal.holds_pushes()
            && let Err(e) = self.cat_mut().advance_durable_generation()
        {
            gnitz_fatal_abort!("checkpoint generation advance failed: {e}");
        }
        let wait = self
            .write_acked(alone, &mut excl, Apply::Flush)
            .unwrap_or_else(|e| gnitz_fatal_abort!("checkpoint Flush refused: {e}"));
        wait.acks().await;
        if let Err(e) = self.cat_mut().flush_all_system_tables() {
            gnitz_fatal_abort!(
                "system flush after the checkpoint Flush failed; the workers left the epoch and the log was not rewound: {e}"
            );
        }
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
    pub(crate) async fn checkpoint_ephemeral(&self, alone: &WriteGuard<ApplyLock>) -> Result<(), WireFault> {
        let wait = {
            let mut excl = self.sal.lock().await;
            debug_assert!(
                !self.sal.holds_pushes(),
                "a push ahead of the ephemeral round in its epoch is integrated by the views the round stamps and replayed into them again at boot"
            );
            let generation = self.cat().durable_generation();
            self.write_acked(alone, &mut excl, Apply::FlushEph { generation })?
        };
        wait.acks().await;
        self.cat_mut().flush_all_system_tables()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Backfill `view_ids` as one group, in id order: a view's id exceeds that of
    /// every relation it scans.
    pub(crate) async fn backfill_views_in_dep_order(
        &self,
        alone: &WriteGuard<ApplyLock>,
        view_ids: &[u64],
    ) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        if ordered.is_empty() {
            return Ok(());
        }
        let backfill = Apply::Backfill { views: ordered.into() };
        let wait = self.write_acked(alone, &mut self.sal.lock().await, backfill)?;
        wait.acks().await;
        Ok(())
    }

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

    /// Credit `rows` against `tid`'s pending-tick count, when a view scans
    /// `tid`. Its first commit since the emit that last took it wakes the
    /// watches naming it. `_laid_out` is the SAL hold the rows' groups were
    /// written under.
    pub(crate) fn credit(&self, _laid_out: &SalExcl<'_>, tid: u64, rows: usize) {
        if !self.cat().dag.is_scanned(tid) {
            return;
        }
        match self.ticks.pending.borrow_mut().entry(tid) {
            Entry::Occupied(mut e) => *e.get_mut() += rows,
            Entry::Vacant(e) => {
                e.insert(rows);
                self.ticks.wake(tid);
            }
        }
    }

    /// True when some relation has `rows` or more pending.
    pub(crate) fn pending_reaches(&self, rows: usize) -> bool {
        self.ticks.pending.borrow().values().any(|&n| n >= rows)
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

    /// Wait for one of `relations` to gain a commit no emitted tick has taken;
    /// `None` when one already has. The check and the registration share no
    /// await, so no commit falls between them.
    pub(crate) fn watch(&self, relations: FxHashSet<u64>) -> Option<Watching<'_>> {
        if !self.none_pending(relations.iter().copied()) {
            return None;
        }
        let ticks = &self.ticks;
        let id = ticks.next_watch.get();
        ticks.next_watch.set(id + 1);
        let (wake, woken) = oneshot::channel();
        ticks.watches.borrow_mut().push(Watch { id, relations, wake });
        Some(Watching { ledger: ticks, id, woken })
    }

    /// Wake every watch. A watch names relations read off a catalog its holder
    /// has released, which a DDL may have changed.
    pub(crate) fn wake_watches(&self) {
        for watch in self.ticks.watches.take() {
            watch.wake.send(());
        }
    }

    /// The verdict of the next [`Self::emit_tick`], whichever task runs it.
    pub(crate) fn next_emit(&self) -> oneshot::Receiver<Result<(), WireFault>> {
        let (done, verdict) = oneshot::channel();
        self.ticks.drains.borrow_mut().push(done);
        verdict
    }

    /// Tick every pending relation as one Tick group, and answer the wait for
    /// its workers' ACKs; `None` when nothing is pending. `alone` is the
    /// caller's hold of the apply lock, which it keeps through those ACKs. A
    /// refused emit leaves every relation pending and takes no round.
    ///
    /// The tids and the watermark are read under the hold [`Self::credit`] is
    /// called under, so they are of the same commits. Every [`Self::next_emit`]
    /// is answered with the emit's verdict, at the emit and not its ACKs: its
    /// reader's scan follows the Tick group in the SAL. The answer shares no
    /// await with the read of what is pending, so every commit pending when the
    /// wait was registered is in the group.
    pub(crate) async fn emit_tick(&self, alone: &WriteGuard<ApplyLock>) -> Result<Option<AckWait>, WireFault> {
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
            let wait = self.write_tick(alone, &mut excl, &tids)?;
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

    /// Write one Tick group for `tids` at consecutive tick rounds, and answer
    /// the wait for its ACKs. A refused write takes no round.
    fn write_tick(
        &self,
        alone: &WriteGuard<ApplyLock>,
        excl: &mut SalExcl<'_>,
        tids: &[u64],
    ) -> Result<AckWait, WireFault> {
        let ticks = &self.ticks;
        let first = ticks.round.get() + 1;
        let tick = Apply::Tick { first_round: first, tids: tids.into() };
        let wait = self.write_acked(alone, excl, tick)?;
        ticks.round.set(first + tids.len() as u64 - 1);
        // At the write, not at the ACKs: a round written has already reached the
        // workers, and a ledger behind it would let a feed's gate answer "nothing
        // changed" over rows already in flight.
        let mut last_tick = ticks.last_tick.borrow_mut();
        for (&tid, round) in tids.iter().zip(first..) {
            last_tick.insert(tid, round);
        }
        Ok(wait)
    }

    /// The SAL watermark the last emit snapshotted.
    pub(crate) fn last_lsn(&self) -> u64 {
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

    /// Lay one push batch out as a SAL group inside `scope`, held to `rule`: the
    /// target's schema record and the rows routed to the workers that own them.
    /// Answers whether the group rides inside the zone: a base table's rows do;
    /// a stream's, which no restart recovers, ride outside. No worker ACKs it: a
    /// read or tick behind it in the SAL reaches its workers after their apply,
    /// and a worker that cannot apply it exits.
    pub(crate) fn write_commit_group(
        &self,
        scope: &mut SalScope,
        target_id: u64,
        rule: PkRule,
        batch: MemBatch<'_>,
    ) -> Result<bool, SalFull> {
        let cat = self.cat();
        let (rel, record) = cat
            .sal_target(target_id)
            .expect("a push target is registered under the catalog lock");
        let recoverable = rel.kind().is_base_table();
        with_routed(batch, rel.placement(), self.sal.num_workers(), |rows| {
            scope.push(target_id, rule, record, rows, recoverable)
        })?;
        Ok(recoverable)
    }

    /// A backup's copy, run by the committer right behind a checkpoint: every
    /// worker puts the stores it published into the data directory `dir`, then
    /// the master puts its system families', each store with the entries that
    /// reach it durable. `dir` exists; everything under it is made here.
    pub(crate) async fn backup_round(&self, dir: &str) -> Result<(), WireFault> {
        self.command(Read::Backup { dir }).await?;
        if BACKUP_ABORT.armed() {
            gnitz_fatal_abort!("injected backup abort");
        }
        self.cat().registry.link_published_into(dir)?;
        gnitz_info!("backup copied into {}", dir);
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/dispatch.rs"]
mod bench;
