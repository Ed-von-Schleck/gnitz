//! Master SAL dispatcher: the `MasterDispatcher` core — the ACK rounds, the
//! fan-out family (backfill / scan / index), the checkpoint rounds, the
//! tick-round counter and worker reaping.

use std::time::Duration;

use super::scatter::with_routed;
use super::*;
use crate::catalog::SysFamily;
use crate::runtime::reactor::AckLease;
use crate::runtime::sal::SalScope;
use gnitz_foundation::posix_io::retry_eintr;

/// How often a worker's death is probed for.
pub(crate) const WORKER_WATCH: Duration = Duration::from_millis(100);

/// A `u64` from the OS entropy pool for [`MasterDispatcher::delta_cursor_tag`],
/// so two back-to-back boots cannot collide.
fn boot_nonce() -> u64 {
    let mut bytes = [0u8; 8];
    let n = retry_eintr(|| unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) } as libc::c_int)
        .expect("getrandom");
    assert_eq!(n as usize, bytes.len(), "getrandom returns up to 256 bytes whole");
    u64::from_ne_bytes(bytes)
}

impl MasterDispatcher {
    pub(crate) fn new(worker_pids: Vec<i32>, catalog: *mut CatalogEngine, sal: SalWriter, reactor: Reactor) -> Self {
        debug_assert_eq!(
            sal.num_workers(),
            worker_pids.len(),
            "the SAL is written at the worker count"
        );
        MasterDispatcher {
            worker_pids,
            sal,
            reactor,
            catalog,
            unique_filters: RefCell::new(FxHashMap::default()),
            unflushed_pushes: Cell::new(false),
            tick_round: Cell::new(1),
            last_delta_round: RefCell::new(FxHashMap::default()),
            boot_nonce: boot_nonce(),
        }
    }

    /// The catalog, which outlives the dispatcher.
    pub(crate) fn cat(&self) -> &CatalogEngine {
        unsafe { &*self.catalog }
    }

    /// The catalog, exclusively: no reference from this or [`Self::cat`] may be
    /// alive across a call.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn cat_mut(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// The SAL writer: its queries, and the [`SalExcl`] every write goes through.
    pub(crate) fn sal(&self) -> &SalWriter {
        &self.sal
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// The master's event loop.
    pub(crate) fn reactor(&self) -> &Reactor {
        &self.reactor
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.sal.num_workers()
    }

    /// Resolves with the first worker found dead, probing every `WORKER_WATCH`.
    pub(crate) async fn worker_death(&self) -> usize {
        loop {
            self.reactor.sleep(WORKER_WATCH).await;
            if let Some(w) = self.check_workers() {
                return w;
            }
        }
    }

    /// Write `group` to every worker on a fresh ACK lease.
    fn write_acked(&self, excl: &SalExcl<'_>, group: DirectGroup<'_>) -> Result<AckLease, WireFault> {
        let lease = self.reactor.lease_acks(WorkerSet::ALL);
        excl.write(&DirectGroup { targets: lease.targets(), ..group })?;
        Ok(lease)
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// A checkpoint's first half: the round that flushes base and system tables.
    ///
    /// When a recoverable push group was written since the last one, the round
    /// moves the base stores past what the checkpointed views integrate, so a
    /// generation bump goes ahead of it: every checkpointed view and index is
    /// invalid until the next ephemeral round restamps it. Otherwise the round
    /// moves no base store, the generation stands, and the ephemeral round
    /// republishes only the stores whose manifests changed.
    pub(crate) async fn checkpoint_base(&self) -> Result<(), WireFault> {
        let mut excl = self.sal.lock().await;
        if self.unflushed_pushes.replace(false) {
            self.cat_mut().advance_durable_generation()?;
        }
        self.flush_round(&mut excl, Apply::Flush).await
    }

    /// A checkpoint's second half: the round that flushes every resumable
    /// view's traces and output stores, stamped with the durable generation.
    pub(crate) async fn checkpoint_ephemeral(&self) -> Result<(), WireFault> {
        let mut excl = self.sal.lock().await;
        debug_assert!(
            !self.unflushed_pushes.get(),
            "an ephemeral round's reset would discard pushes no base round flushed"
        );
        let generation = self.cat().durable_generation();
        self.flush_round(&mut excl, Apply::FlushEph { generation }).await
    }

    /// Write `round`'s group, wait for every worker's ACK, finalize.
    async fn flush_round(&self, excl: &mut SalExcl<'_>, round: Apply<'_>) -> Result<(), WireFault> {
        let lease = self.write_acked(excl, DirectGroup::new(round))?;
        // `excl` is held through the ACKs, so its drop would wake too late.
        excl.wake();
        lease.acks().await;
        self.checkpoint_post_ack(excl)
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Distributed backfill of ONE view from `source_id`; view-scoped, see
    /// [`crate::query::Drive::Backfill`].
    async fn fan_out_backfill(&self, view_id: u64, source_id: u64) -> Result<(), WireFault> {
        let group = DirectGroup::new(Apply::Backfill { source: source_id, view: view_id });
        // Two statements: the `SalExcl`'s drop is the wake.
        let lease = self.write_acked(&self.sal.lock().await, group)?;
        lease.acks().await;
        Ok(())
    }

    /// Backfill every view in `view_ids`, in dependency order, from each of its
    /// sources. The one driver that populates a view — a live CREATE VIEW bundle
    /// and the boot rebuild of generation-invalid views both run this.
    ///
    /// Ids ascend along every scan edge, so id order puts a source view before
    /// every dependent that scans it, and an upstream hidden segment is filled
    /// before a downstream view reads it.
    ///
    /// A multi-source equi-join iterates every source: each joins against the
    /// sources fed before it, so the first emits nothing, and the rest join
    /// against it. A view that runs no exchange needs no cross-worker
    /// barrier and `fan_out_backfill` accommodates that — no round completes, so
    /// each worker stops on its own drain exhaustion — which is why one driver
    /// serves every shape.
    pub(crate) async fn backfill_views_in_dep_order(&self, view_ids: &[u64]) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        for vid in ordered {
            // Owned: no catalog reference is held across the await.
            let sources = self.cat().dag.sources_of(vid).to_vec();
            for src in sources {
                self.fan_out_backfill(vid, src)
                    .await
                    .map_err(|e| e.in_context(format_args!("view={vid} source={src}")))?;
            }
        }
        Ok(())
    }

    /// Tick every one of `tids`, in order, in one group, and await its ACKs.
    pub(crate) async fn drain_tick(&self, tids: &[u64]) -> Result<(), WireFault> {
        if tids.is_empty() {
            return Ok(());
        }
        let lease = self.emit_tick(&self.sal.lock().await, tids)?;
        lease.acks().await;
        Ok(())
    }

    /// Log a DDL batch of `family` inside `scope`'s zone — one LSN across a DDL's
    /// groups, so recovery groups them atomically — and send it to every worker.
    /// `_sequences` is master state no worker reads, so its group is logged and
    /// addresses none. Publishes nothing; that is the scope's commit.
    pub(crate) fn broadcast_ddl(&self, scope: &SalScope, family: SysFamily, batch: &Batch) -> Result<(), WireFault> {
        let target_id = family.id();
        let record = self
            .cat()
            .schema_record(target_id)
            .expect("a wire target is registered under the catalog lock");
        let mut group = DirectGroup::ddl_sync(target_id, &record, batch);
        if family == SysFamily::Sequence {
            group.targets = GroupTargets {
                set: WorkerSet::EMPTY,
                ..GroupTargets::UNADDRESSED
            };
        }
        scope.write(&group, true)?;
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.len());
        Ok(())
    }

    /// Write one Tick group for `tids` at consecutive tick rounds, on a fresh ACK
    /// lease. A refused write burns its rounds.
    pub(crate) fn emit_tick(&self, excl: &SalExcl<'_>, tids: &[u64]) -> Result<AckLease, WireFault> {
        let first = self.tick_round.get() + 1;
        self.tick_round.set(first + tids.len() as u64 - 1);
        for (i, &tid) in tids.iter().enumerate() {
            self.record_delta_round(tid, first + i as u64);
        }
        let tick = Apply::Tick { first_round: first, tids: tids.into() };
        self.write_acked(excl, DirectGroup::new(tick))
    }

    /// Raise the last-reached round of every fed view in `tid`'s **forward**
    /// closure — which views this source reaches. Skipped outright when no view
    /// carries a feed, which is every server that does not use the feature.
    ///
    /// At **emit** time, not at ACK time: a round emitted but not yet ACKed has
    /// already reached the view's workers, and an ACK-time update would let the
    /// gate answer "nothing changed" over a round whose rows are already in flight.
    fn record_delta_round(&self, tid: u64, round: u64) {
        let cat = self.cat();
        if !cat.registry.any_delta_feed() {
            return;
        }
        let reached = cat.dag.dependent_closure([tid]);
        let mut map = self.last_delta_round.borrow_mut();
        for vid in reached {
            if cat.registry.relation(vid).is_some_and(|r| r.kind().has_delta_feed()) {
                map.insert(vid, round);
            }
        }
    }

    /// The last tick round this master has **emitted** — the `T` a delta reply
    /// promises, sampled under the read's own [`SalExcl`] so no tick group can
    /// land between the sample and the read group.
    pub(crate) fn last_tick_round(&self) -> u64 {
        self.tick_round.get()
    }

    /// The cursor tag a delta reply carries: distinct for every (boot, view) and,
    /// with fair odds, every spec the view is read under — so a cursor from
    /// another boot, from a dropped view, or polled under another spec is
    /// recognizably foreign. `spec` is the request's encoded bytes.
    pub(crate) fn delta_cursor_tag(&self, view_id: u64, spec: &[u8]) -> u64 {
        // splitmix64's finalizer, a bijection on u64: two views of one boot
        // never share a tag.
        let mut z = view_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.boot_nonce ^ (z ^ (z >> 31)) ^ gnitz_wire::checksum(spec)
    }

    /// The last round that reached `view_id`, or `1` for a view no round has
    /// reached — the boot round, which is never emitted and stamps nothing, so a
    /// bootstrap at `after_tick = 0` always falls through to the store.
    pub(crate) fn last_delta_round(&self, view_id: u64) -> u64 {
        self.last_delta_round.borrow().get(&view_id).copied().unwrap_or(1)
    }

    /// Drop a dropped relation's gate entry. Ids are never reused, so nothing else
    /// would ever reclaim it, and a per-DDL leak on the master is not a leak that
    /// stops.
    pub(crate) fn forget_delta_round(&self, id: u64) {
        self.last_delta_round.borrow_mut().remove(&id);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// The first dead worker, reported on every probe: a reaped pid answers
    /// ECHILD.
    pub(crate) fn check_workers(&self) -> Option<usize> {
        for (w, &pid) in self.worker_pids.iter().enumerate() {
            if pid <= 0 {
                continue;
            }
            let mut status: i32 = 0;
            let dead = match retry_eintr(|| unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) }) {
                Ok(0) => false, // still running
                Ok(_) => true,  // its zombie was ours to reap
                // ECHILD: reaped elsewhere, so equally dead. Any other errno is
                // unexpected and read as alive rather than as a crash.
                Err(e) => e.raw_os_error() == Some(libc::ECHILD),
            };
            if dead {
                return Some(w);
            }
        }
        None
    }

    /// `SIGKILL` every worker, then reap each: a survivor parked for W2M ring
    /// space would never read a `Shutdown`.
    pub(crate) fn kill_workers(&self) {
        for pid in self.live_pids() {
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
        self.reap_workers();
    }

    /// Broadcast `Shutdown` and reap the worker processes, blocking on each. No
    /// await separates the write from the reap, so [`Self::worker_death`] never
    /// probes workers that exited on request.
    pub(crate) async fn shutdown_workers(&self) {
        // A refusal would hang the reap below, so it must not vanish.
        if let Err(e) = self.sal.lock().await.write(&DirectGroup::new(SalRequest::Shutdown)) {
            gnitz_warn!("SAL refused the Shutdown broadcast: {}; the worker reap will block", e);
        }
        self.reap_workers();
    }

    fn live_pids(&self) -> impl Iterator<Item = i32> + '_ {
        self.worker_pids.iter().copied().filter(|&pid| pid > 0)
    }

    /// Block until every worker has exited.
    fn reap_workers(&self) {
        for pid in self.live_pids() {
            let mut status: i32 = 0;
            let _ = retry_eintr(|| unsafe { libc::waitpid(pid, &mut status, 0) });
        }
    }

    /// Lay one push batch out as a SAL group inside `scope`, written to the
    /// workers that own its rows, and answer the lease those workers ACK on.
    /// `recoverable` puts it inside the zone; a stream's rows ride outside. The
    /// committer commits the scope and awaits the ACKs.
    pub(crate) fn write_commit_group(
        &self,
        scope: &SalScope,
        target_id: u64,
        batch: &Batch,
        recoverable: bool,
    ) -> Result<AckLease, WireFault> {
        if recoverable {
            self.unflushed_pushes.set(true);
        }
        let cat = self.cat();
        let record = cat
            .schema_record(target_id)
            .expect("a wire target is registered under the catalog lock");
        let placement = cat
            .registry
            .relation(target_id)
            .expect("a push target is registered under the catalog lock")
            .placement();
        with_routed(batch, placement, self.num_workers(), |data| {
            // Leased before the group is laid out, so no ACK arrives unrouted.
            let lease = self.reactor.lease_acks(data.holders());
            scope.write(
                &DirectGroup::push(target_id, &record, data, lease.targets()),
                recoverable,
            )?;
            Ok(lease)
        })
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
    pub(crate) fn checkpoint_post_ack(&self, excl: &mut SalExcl<'_>) -> Result<(), WireFault> {
        let cat = self.cat_mut();
        debug_assert!(
            !cat.has_uncommitted_families(),
            "a checkpoint's system-table flush would make an uncommitted DDL durable"
        );
        cat.flush_all_system_tables()?;
        // Every worker ACKed the FLUSH, so each has applied every DdlSync written
        // before it; the flush above made every applied DROP durable.
        cat.reclaim_orphan_dirs();
        excl.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    /// Boot-end checkpoint: record the launched topology, then stamp every
    /// resumable view and every index at the durable generation, which a boot
    /// that replayed a push advanced pre-fork. The workers published their base
    /// stores during recovery, and no push is admitted yet.
    pub(crate) async fn boot_checkpoint(&self) -> Result<(), WireFault> {
        let cat = self.cat_mut();
        cat.record_topology(cat.registry.slot().of)?;
        self.checkpoint_ephemeral().await
    }
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;
