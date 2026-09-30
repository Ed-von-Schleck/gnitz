//! Master SAL dispatcher: the `MasterDispatcher` core — ack collection, the
//! fan-out family (backfill / scan / index), the checkpoint rounds, the
//! tick-round counter and worker reaping.

use std::time::{Duration, Instant};

use super::scatter::with_routed;
use super::*;
use crate::runtime::reactor::{select2, Either};
use crate::runtime::sal::SalScope;
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_store::relation::Relation;

/// One round of a checkpoint.
#[derive(Clone, Copy)]
pub(crate) enum FlushRound {
    /// Flush base and system tables.
    Base,
    /// Flush every view's traces and output stores, stamped with the durable
    /// generation.
    Ephemeral,
}

impl FlushRound {
    fn kind(self) -> SalMessageKind {
        match self {
            FlushRound::Base => SalMessageKind::Flush,
            FlushRound::Ephemeral => SalMessageKind::FlushEph,
        }
    }

    fn phase(self) -> &'static str {
        match self {
            FlushRound::Base => "checkpoint base round",
            FlushRound::Ephemeral => "checkpoint ephemeral round",
        }
    }
}

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
    /// `last_ephemeral_gen` seeds `note_flush_round`'s ordering check: the
    /// boot's recovered durable generation.
    pub(crate) fn new(
        worker_pids: Vec<i32>,
        catalog: *mut CatalogEngine,
        last_ephemeral_gen: u64,
        sal: SalWriter,
        reactor: Rc<Reactor>,
    ) -> Self {
        debug_assert_eq!(
            sal.num_workers(),
            worker_pids.len(),
            "the SAL's slot count is the worker count"
        );
        MasterDispatcher {
            worker_pids,
            sal,
            reactor,
            catalog,
            unique_filters: RefCell::new(FxHashMap::default()),
            last_ephemeral_gen: Cell::new(last_ephemeral_gen),
            unflushed_pushes: Cell::new(false),
            tick_round: Cell::new(1),
            last_delta_round: RefCell::new(FxHashMap::default()),
            boot_nonce: boot_nonce(),
        }
    }

    /// The catalog behind the raw pointer the dispatcher was constructed with.
    /// The pointer is dereferenced here and nowhere else on the master side —
    /// every other master-side name for the catalog is a wrapper around this
    /// accessor, not a second owner of the pointer. The catalog outlives the
    /// dispatcher. No reference this returns may be alive across another `cat()`
    /// call, in an argument list or in a callee.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn cat(&self) -> &mut CatalogEngine {
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
    pub(crate) fn reactor(&self) -> &Rc<Reactor> {
        &self.reactor
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.sal.num_workers()
    }

    /// Wait until every worker has answered `lease`. Fails on a worker's error
    /// ACK or death.
    pub(crate) async fn collect_round(&self, lease: &AckLease) -> Result<(), WireFault> {
        match select2(lease.acks(), self.round_failure(lease)).await {
            Either::A(r) => r,
            Either::B(e) => Err(e),
        }
    }

    /// Resolves once a worker has answered `lease` with an error or has died, probing
    /// every `WORKER_WATCH`. A worker failing before it joins a round leaves the others
    /// in their exchange wait, so an error must end the round without the other ACKs.
    async fn round_failure(&self, lease: &AckLease) -> WireFault {
        loop {
            self.reactor.timer(Instant::now() + WORKER_WATCH).await;
            if let Some(e) = lease.first_error() {
                return e;
            }
            if let Some(w) = self.check_workers() {
                return format!("worker {w} exited during {}", lease.ctx()).into();
            }
        }
    }

    /// Write the group `write` builds on a fresh ACK lease, then
    /// [`Self::collect_round`]. A refused write fails before any worker is woken,
    /// keeping its status.
    async fn broadcast_round(
        &self,
        ctx: &'static str,
        write: impl FnOnce(&SalExcl<'_>, GroupTargets) -> Result<(), WireFault>,
    ) -> Result<(), WireFault> {
        let lease = self.reactor.lease_acks(ctx);
        write(&self.sal.lock().await, GroupTargets::all(lease.id()))?;
        self.collect_round(&lease).await
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// A checkpoint's first half: a generation bump, then a base round. Every
    /// checkpointed view and index is invalid until the next ephemeral round
    /// restamps it.
    pub(crate) async fn checkpoint_base(&self) -> Result<(), WireFault> {
        self.cat().bump_checkpoint_generation()?;
        self.flush(FlushRound::Base).await
    }

    /// [`Self::checkpoint_round`] under a SAL hold of its own.
    pub(crate) async fn flush(&self, round: FlushRound) -> Result<(), WireFault> {
        self.checkpoint_round(&mut self.sal.lock().await, round).await
    }

    /// Write `round`'s flush group, wait for every worker's ACK, finalize.
    pub(crate) async fn checkpoint_round(&self, excl: &mut SalExcl<'_>, round: FlushRound) -> Result<(), WireFault> {
        let lease = self.reactor.lease_acks(round.phase());
        let generation = self.note_flush_round(round);
        excl.write(&DirectGroup {
            template: wire::WireMsg { arg0: generation, ..Default::default() },
            targets: GroupTargets::all(lease.id()),
            ..DirectGroup::new(round.kind())
        })?;
        // `excl` is held through the ACKs, so its drop would wake too late.
        excl.wake();
        self.collect_round(&lease).await?;
        self.checkpoint_post_ack(excl)
    }

    /// Record `round`, asserting the checkpoint ordering. Returns the generation
    /// workers stamp their manifests with: the durable one for an ephemeral round,
    /// 0 for a base round, which stamps none.
    fn note_flush_round(&self, round: FlushRound) -> u64 {
        let durable = self.cat().durable_generation();
        match round {
            FlushRound::Ephemeral => {
                debug_assert!(
                    !self.unflushed_pushes.get(),
                    "an ephemeral round's reset would discard pushes no base round flushed"
                );
                self.last_ephemeral_gen.set(durable);
                durable
            }
            FlushRound::Base => {
                self.unflushed_pushes.set(false);
                debug_assert!(
                    durable > self.last_ephemeral_gen.get(),
                    "base round at generation {} publishes past the last ephemeral round ({}): \
                     derived state would resume from a cut older than its base",
                    durable,
                    self.last_ephemeral_gen.get(),
                );
                0
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Distributed backfill of ONE view from `source_id`; view-scoped, see
    /// [`crate::query::Drive::Backfill`].
    async fn fan_out_backfill(&self, view_id: u64, source_id: u64) -> Result<(), WireFault> {
        // Dataless, and schema-less: the worker reads the source's schema off its
        // own catalog.
        let template = wire::WireMsg {
            target_id: source_id,
            arg0: view_id,
            ..Default::default()
        };
        self.broadcast_round("backfill", |excl, t| {
            excl.write(&DirectGroup {
                template,
                targets: t,
                ..DirectGroup::new(SalMessageKind::Backfill)
            })
        })
        .await
    }

    /// Backfill every view in `view_ids`, in dependency order, from each of its
    /// sources. The one driver that populates a view — a live CREATE VIEW bundle
    /// and the boot rebuild of generation-invalid views both run this.
    ///
    /// Ids ascend along every scan edge, so id order puts a source view before
    /// every dependent that scans it, and an upstream hidden segment is filled
    /// before a downstream view reads it.
    ///
    /// A multi-source equi-join iterates every source: the first fills its trace
    /// (joining against the still-empty other trace emits nothing), and the rest
    /// join against it. A view that runs no exchange needs no cross-worker
    /// barrier and `fan_out_backfill` accommodates that — no round completes, so
    /// each worker stops on its own drain exhaustion — which is why one driver
    /// serves every shape.
    pub(crate) async fn backfill_views_in_dep_order(&self, view_ids: &[u64]) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        for vid in ordered {
            // Owned: the loop body calls `cat()` again.
            let sources = self.cat().dag.sources_of(vid).to_vec();
            for src in sources {
                self.fan_out_backfill(vid, src).await.map_err(|e| WireFault {
                    status: e.status,
                    text: format!("view={vid} source={src}: {e}"),
                })?;
            }
        }
        Ok(())
    }

    /// Tick every one of `tids`, in order, in one [`Self::broadcast_round`].
    pub(crate) async fn drain_tick(&self, tids: &[u64]) -> Result<(), WireFault> {
        if tids.is_empty() {
            return Ok(());
        }
        self.broadcast_round("view tick drain", |excl, t| self.write_tick_group(excl, tids, t))
            .await
    }

    /// Broadcast a DDL batch to every worker inside `scope`'s zone — one LSN
    /// across a DDL's broadcasts, so recovery groups them atomically. Publishes
    /// nothing; that is the scope's commit.
    pub(crate) fn broadcast_ddl(&self, scope: &SalScope, target_id: u64, batch: &Batch) -> Result<(), WireFault> {
        let relation = wire::WireSchema::from_catalog(self.cat(), target_id);
        scope.write(&DirectGroup::ddl_sync(&relation, batch), true)?;
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.len());
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// Write one Tick group for `tids` at consecutive tick rounds. A refused
    /// write burns its rounds.
    pub(crate) fn write_tick_group(
        &self,
        excl: &SalExcl<'_>,
        tids: &[u64],
        targets: GroupTargets,
    ) -> Result<(), WireFault> {
        let first = self.tick_round.get() + 1;
        self.tick_round.set(first + tids.len() as u64 - 1);
        let mut blob = gnitz_wire::Writer::with_capacity(8 * tids.len());
        for (i, &tid) in tids.iter().enumerate() {
            self.record_delta_round(tid, first + i as u64);
            blob.u64(tid);
        }
        let blob = blob.into_vec();
        excl.write(&DirectGroup {
            template: wire::WireMsg {
                arg0: first,
                blob: &blob,
                ..Default::default()
            },
            targets,
            ..DirectGroup::new(SalMessageKind::Tick)
        })
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
        let reached = cat.dag.dependent_closure(vec![tid]);
        let mut map = self.last_delta_round.borrow_mut();
        for vid in reached {
            if cat.registry.relation(vid).is_some_and(Relation::has_delta_feed) {
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

    /// The cursor tag a delta reply carries: distinct for every (boot, view), so a
    /// cursor from another boot or from a dropped view is recognizably foreign.
    pub(crate) fn delta_cursor_tag(&self, view_id: u64) -> u64 {
        // splitmix64's finalizer: the view id is a small dense integer, and an
        // unmixed XOR would make neighbouring ids' tags differ in one bit. It is
        // a **bijection** on u64 — `x ^ (x >> k)` is invertible and both
        // multipliers are odd — so distinct view ids cannot collide within a
        // boot, which a general hash (XXH3 over 8 bytes) would not guarantee.
        let mut z = view_id.wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.boot_nonce ^ (z ^ (z >> 31))
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
    ///
    /// Probes each worker by its own pid, not `waitpid(-1)`. A per-pid `waitpid`
    /// returns ECHILD — a detected death — even if the zombie was reaped
    /// elsewhere, whereas `waitpid(-1)` returns 0 ("some child is alive") and
    /// silently misses one worker's death while others run, so it would go blind
    /// the moment a SIGCHLD/signalfd reaper or SA_NOCLDWAIT is ever added. It
    /// also names the exact dead worker for the error/log.
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

    /// Broadcast `Shutdown` and reap the worker processes, blocking on each.
    pub(crate) async fn shutdown_workers(&self) {
        // No schema block: the worker's `Shutdown` arm takes no arguments. A
        // refusal would hang the reap below, so it must not vanish.
        if let Err(e) = self.sal.lock().await.write(&DirectGroup::new(SalMessageKind::Shutdown)) {
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

    /// Lay one push batch out as a SAL group inside `scope`, every worker
    /// answering on `request_id`. `recoverable` puts it inside the zone; a
    /// stream's rows ride outside. The committer commits the scope and awaits the
    /// ACKs.
    pub(crate) fn write_commit_group(
        &self,
        scope: &SalScope,
        target_id: u64,
        batch: &Batch,
        request_id: u32,
        recoverable: bool,
    ) -> Result<(), WireFault> {
        if recoverable {
            self.unflushed_pushes.set(true);
        }
        let relation = wire::WireSchema::from_catalog(self.cat(), target_id);
        with_routed(batch, &relation, self.num_workers(), |_, data| {
            scope.write(&DirectGroup::push(&relation, data, request_id), recoverable)
        })
    }

    /// Post-ACK checkpoint cleanup, run once a round's Flush ACKs are in: flush
    /// the system tables before resetting the SAL cursor (their data lives in
    /// SAL entries about to be discarded), then advance the epoch.
    ///
    /// Finalizes **both** rounds. The ephemeral one must flush too: a
    /// `commit_serial_range_durable` advance can land in the `sys_sequences`
    /// MemTable during the drain window, after the base round's reset, so this
    /// flush is its only durability event before the ephemeral reset makes the
    /// SAL tail useless for recovery.
    ///
    /// Returns the flush error WITHOUT resetting the SAL when the system-table
    /// flush fails: the SAL entries about to be discarded are that data's only
    /// durable copy, so resetting on a swallowed failure destroys it. The caller
    /// leaves the SAL intact and aborts or fails the boot.
    pub(crate) fn checkpoint_post_ack(&self, excl: &mut SalExcl<'_>) -> Result<(), WireFault> {
        let cat = self.cat();
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

    /// Boot-end checkpoint: record the launched topology, then restamp every view
    /// and index at the generation reserved pre-fork. The workers published their
    /// base stores during recovery, and no push is admitted yet.
    pub(crate) async fn boot_checkpoint(&self) -> Result<(), WireFault> {
        let cat = self.cat();
        cat.record_topology(cat.registry.slot().of)?;
        self.flush(FlushRound::Ephemeral).await
    }

    /// The descriptor `target_id` is registered with. Panics on an unregistered
    /// id: every caller reaches it holding the catalog lock the registry's own
    /// writers take, so a miss is broken lock discipline, not a bad client id.
    pub(crate) fn schema_desc_for(&self, target_id: u64) -> SchemaDescriptor {
        self.cat()
            .registry
            .relation(target_id)
            .map(Relation::schema)
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"))
    }
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;
