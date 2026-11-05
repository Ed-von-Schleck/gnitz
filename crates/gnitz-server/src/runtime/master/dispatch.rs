//! The MasterDispatcher's writes: ACKed rounds (checkpoint halves, the boot-end
//! stamp, backfill, tick) and the tick rounds they allocate, the DDL zone, push
//! groups, the backup round, and worker reaping.

use std::time::Duration;

use super::route::with_routed;
use super::*;
use crate::catalog::{ZoneError, ZoneSink};
use crate::runtime::reactor::AckLease;
use crate::runtime::sal::{Apply, GroupTargets, SalMessageKind, SalRequest, SalScope};
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_store::relation::PkRule;
use gnitz_zset::repr::MemBatch;

/// How often a worker's death is probed for.
const WORKER_WATCH: Duration = Duration::from_millis(100);

/// `GNITZ_INJECT_BACKUP_ABORT`: abort the master between the workers' copies and
/// its own, as a crash part-way through a backup would.
static BACKUP_ABORT: Seam = Seam::new("GNITZ_INJECT_BACKUP_ABORT");

impl MasterDispatcher {
    /// Write `group` to every worker on a fresh ACK lease, and wake them now: a
    /// caller may await the ACKs under this hold.
    fn write_acked(&self, excl: &mut SalExcl<'_>, group: SalGroup<'_>) -> Result<AckLease, WireFault> {
        let lease = self.reactor.lease_acks(WorkerSet::ALL);
        let targets = GroupTargets {
            set: lease.workers(),
            reply: Some(lease.id()),
        };
        excl.write(&SalGroup { targets, ..group })?;
        excl.wake();
        Ok(lease)
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
    pub(crate) async fn checkpoint_base(&self) -> Result<(), WireFault> {
        let mut excl = self.sal.lock().await;
        if self.sal.holds_pushes() {
            self.cat_mut().advance_durable_generation()?;
        }
        self.flush_round(&mut excl, Apply::Flush).await
    }

    /// A checkpoint's second half: the round that flushes every resumable
    /// view's traces and output stores, stamped with the durable generation.
    pub(crate) async fn checkpoint_ephemeral(&self) -> Result<(), WireFault> {
        let mut excl = self.sal.lock().await;
        debug_assert!(
            !self.sal.holds_pushes(),
            "an ephemeral round's reset would discard pushes no base round flushed"
        );
        let generation = self.cat().durable_generation();
        self.flush_round(&mut excl, Apply::FlushEph { generation }).await
    }

    /// Write `round`'s group, wait for every worker's ACK, finalize.
    async fn flush_round(&self, excl: &mut SalExcl<'_>, round: Apply<'_>) -> Result<(), WireFault> {
        let lease = self.write_acked(excl, SalGroup::new(round))?;
        lease.acks().await;
        self.checkpoint_post_ack(excl)
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Backfill `view_ids` as one group, in id order: a view's id exceeds that of
    /// every relation it scans.
    pub(crate) async fn backfill_views_in_dep_order(&self, view_ids: &[u64]) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        if ordered.is_empty() {
            return Ok(());
        }
        let group = SalGroup::new(Apply::Backfill { views: ordered.into() });
        let lease = self.write_acked(&mut self.sal.lock().await, group)?;
        lease.acks().await;
        Ok(())
    }

    /// Tick every one of `tids`, in order, in one group, and await its ACKs.
    pub(crate) async fn drain_tick(&self, tids: &[u64]) -> Result<(), WireFault> {
        if tids.is_empty() {
            return Ok(());
        }
        let lease = self.emit_tick(&mut self.sal.lock().await, tids)?;
        lease.acks().await;
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
        let mut scope = excl.begin("ddl");
        let zone_lsn = scope.lsn();
        // Each DDL batch is logged inside the zone, under its one LSN, so recovery
        // groups a DDL's groups atomically.
        let written = write(&mut self.cat_mut(), &mut |family, batch| {
            scope.write(&SalGroup::ddl_sync(family, batch), true)?;
            gnitz_debug!("ddl group tid={} rows={}", family.id(), batch.len());
            Ok(())
        });
        let value = match written {
            Ok(v) => v,
            Err(ZoneError::Refused(e)) => return Err(e),
            Err(ZoneError::Diverged(e)) => gnitz_fatal_abort!("{}: {e}", std::panic::Location::caller()),
        };
        // Only a closed zone raises the replay floor: an LSN no member carries can
        // be the next scope's, whose zone boot replay would then skip.
        if !scope.commit() {
            return Ok((value, 0));
        }
        self.cat_mut().mark_zone_applied(zone_lsn);
        Ok((value, zone_lsn))
    }

    /// Write one Tick group for `tids` at consecutive tick rounds, on a fresh ACK
    /// lease. A refused write burns its rounds.
    pub(crate) fn emit_tick(&self, excl: &mut SalExcl<'_>, tids: &[u64]) -> Result<AckLease, WireFault> {
        let first = self.rounds.round.get() + 1;
        self.rounds.round.set(first + tids.len() as u64 - 1);
        self.record_delta_rounds(tids, first);
        let tick = Apply::Tick { first_round: first, tids: tids.into() };
        self.write_acked(excl, SalGroup::new(tick))
    }

    /// Raise each fed view's last-reached round to the last of `tids`' rounds
    /// (`first` onward) whose tid is in the view's source closure. Costs nothing
    /// past one registry pass on a server where no view carries a feed.
    ///
    /// At **emit** time, not at ACK time: a round emitted but not yet ACKed has
    /// already reached the view's workers, and an ACK-time update would let the
    /// gate answer "nothing changed" over a round whose rows are already in flight.
    fn record_delta_rounds(&self, tids: &[u64], first: u64) {
        let cat = self.cat();
        let mut map = self.rounds.last_delta_round.borrow_mut();
        for vid in cat.registry.delta_fed() {
            let sources = cat.dag.source_closure_of(vid);
            if let Some(i) = tids.iter().rposition(|t| sources.binary_search(t).is_ok()) {
                map.insert(vid, first + i as u64);
            }
        }
    }

    /// The last round that reached `view_id`, or `1` for a view no round has
    /// reached — the boot round, which is never emitted and captures nothing, so a
    /// bootstrap at `after_tick = 0` always falls through to the store.
    pub(crate) fn last_delta_round(&self, view_id: u64) -> u64 {
        self.rounds
            .last_delta_round
            .borrow()
            .get(&view_id)
            .copied()
            .unwrap_or(1)
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// [`worker_death`] of this dispatcher's workers.
    pub(crate) async fn worker_death(&self) -> usize {
        worker_death(&self.reactor, &self.worker_pids).await
    }

    /// `SIGKILL` every worker, then reap each: a survivor parked for W2M ring
    /// space would never read a `Shutdown`.
    pub(crate) fn kill_workers(&self) {
        for pid in &self.worker_pids {
            unsafe { libc::kill(pid.get(), libc::SIGKILL) };
        }
        self.reap_workers();
    }

    /// Broadcast `Shutdown` and reap the worker processes, blocking on each. No
    /// await separates the write from the reap, so [`Self::worker_death`] never
    /// probes workers that exited on request.
    pub(crate) async fn shutdown_workers(&self) {
        // A refusal would hang the reap below, so it must not vanish.
        if let Err(e) = self.sal.lock().await.write(&SalGroup::new(SalRequest::Shutdown)) {
            gnitz_warn!("SAL refused the Shutdown broadcast: {}; the worker reap will block", e);
        }
        self.reap_workers();
    }

    /// Block until every worker has exited.
    fn reap_workers(&self) {
        for pid in &self.worker_pids {
            let mut status: i32 = 0;
            let _ = retry_eintr(|| unsafe { libc::waitpid(pid.get(), &mut status, 0) });
        }
    }

    /// The SAL group of one push of `batch` to `target_id`, held to `rule`,
    /// addressed to the workers that own its rows and to no reply, handed to `f`
    /// with whether it rides inside the zone. A base table's rows do; a
    /// stream's, which no restart recovers, ride outside.
    fn with_commit_group<R>(
        &self,
        target_id: u64,
        rule: PkRule,
        batch: MemBatch<'_>,
        f: impl FnOnce(SalGroup<'_>, bool) -> R,
    ) -> R {
        let cat = self.cat();
        let (rel, record) = cat
            .wire_target(target_id)
            .expect("a push target is registered under the catalog lock");
        let recoverable = rel.kind().is_base_table();
        with_routed(batch, rel.placement(), self.num_workers(), |data| {
            let targets = GroupTargets { set: data.holders(), reply: None };
            f(SalGroup::push(target_id, rule, record, data, targets), recoverable)
        })
    }

    /// Lay one push batch out as a SAL group inside `scope`, held to `rule`, and
    /// answer whether the group rides inside the zone. No worker ACKs it: a read or tick behind
    /// it in the SAL reaches its workers after their apply, and a worker that
    /// cannot apply it exits.
    pub(crate) fn write_commit_group(
        &self,
        scope: &mut SalScope,
        target_id: u64,
        rule: PkRule,
        batch: MemBatch<'_>,
    ) -> Result<bool, WireFault> {
        self.with_commit_group(target_id, rule, batch, |g, recoverable| {
            scope.write(&g, recoverable)?;
            Ok(recoverable)
        })
    }

    /// Whether the commit groups of `families`, laid out back to back as
    /// [`Self::write_commit_group`] lays each out, fit a log with nothing else
    /// in its epoch: whether a checkpoint can make room for them.
    pub(crate) fn commit_fits_empty_log<'b>(
        &self,
        scope: &SalScope,
        families: impl IntoIterator<Item = (u64, MemBatch<'b>)>,
    ) -> bool {
        let bytes = (families.into_iter())
            // The rule rides in the group head, so it sizes no group.
            .map(|(target_id, batch)| {
                self.with_commit_group(target_id, PkRule::Enforce, batch, |g, _| scope.footprint(&g))
            })
            .sum();
        scope.fits_empty_log(SalMessageKind::Push, bytes)
    }

    /// Post-ACK checkpoint cleanup, run once a round's Flush ACKs are in: flush
    /// the system tables before resetting the SAL cursor (their data lives in
    /// SAL entries about to be discarded), then advance the epoch.
    ///
    /// Finalizes **both** rounds. The ephemeral one must flush too: a
    /// `reserve_serial_range` advance can land in the `sys_sequences`
    /// MemTable during the drain window, after the base round's reset, so this
    /// flush is its only durability event before the ephemeral reset makes the
    /// SAL tail useless for recovery.
    ///
    /// Returns the flush error WITHOUT resetting the SAL when the system-table
    /// flush fails: the SAL entries about to be discarded are that data's only
    /// durable copy, so resetting on a swallowed failure destroys it. The caller
    /// leaves the SAL intact and aborts or fails the boot.
    fn checkpoint_post_ack(&self, excl: &mut SalExcl<'_>) -> Result<(), WireFault> {
        let mut cat = self.cat_mut();
        cat.flush_all_system_tables()?;
        // Every worker ACKed the FLUSH, so each has applied every DdlSync written
        // before it; the flush above made every applied DROP durable.
        cat.reclaim_orphan_dirs();
        let epoch = excl.checkpoint_reset();
        gnitz_note!("SAL checkpoint epoch={epoch}");
        Ok(())
    }

    /// A backup's copy, run by the committer right behind a checkpoint: every
    /// worker puts the stores it published into the data directory `dir`, then
    /// the master puts its system families', each store with the entries that
    /// reach it durable. `dir` exists; everything under it is made here.
    pub(crate) async fn backup_round(&self, dir: &str) -> Result<(), WireFault> {
        self.command(Read::Backup { dir: dir.as_bytes().into() }).await?;
        if BACKUP_ABORT.armed() {
            gnitz_fatal_abort!("injected backup abort");
        }
        self.cat().registry.link_published_into(dir)?;
        gnitz_info!("backup copied into {}", dir);
        Ok(())
    }

    /// Boot-end checkpoint: record the launched topology, then stamp every
    /// resumable view and every index at the durable generation, which a boot
    /// that replayed a push advanced pre-fork. The workers published their base
    /// stores during recovery, and no push is admitted yet.
    pub(crate) async fn boot_checkpoint(&self) -> Result<(), WireFault> {
        {
            let mut cat = self.cat_mut();
            let width = cat.registry.slot().of;
            cat.record_topology(width)?;
        }
        self.checkpoint_ephemeral().await
    }
}

/// Resolves with the first worker of `pids` found dead, probing every
/// `WORKER_WATCH`.
pub(crate) async fn worker_death(reactor: &Reactor, pids: &[NonZero<libc::pid_t>]) -> usize {
    loop {
        reactor.sleep(WORKER_WATCH).await;
        if let Some(w) = check_workers(pids) {
            return w;
        }
    }
}

/// The first dead worker of `pids`, reported on every probe: a reaped pid
/// answers ECHILD.
fn check_workers(pids: &[NonZero<libc::pid_t>]) -> Option<usize> {
    pids.iter().position(|pid| {
        let mut status: i32 = 0;
        match retry_eintr(|| unsafe { libc::waitpid(pid.get(), &mut status, libc::WNOHANG) }) {
            Ok(0) => false, // still running
            Ok(_) => true,  // its zombie was ours to reap
            // ECHILD: reaped elsewhere, so equally dead. Any other errno is
            // unexpected and read as alive rather than as a crash.
            Err(e) => e.raw_os_error() == Some(libc::ECHILD),
        }
    })
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;
