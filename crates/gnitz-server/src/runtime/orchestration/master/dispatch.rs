//! Master SAL dispatcher: the `MasterDispatcher` core — ack collection, relay
//! emit, the fan-out family (backfill / scan / index), the checkpoint rounds,
//! the tick-round counter and worker reaping.

use std::time::{Duration, Instant};

use super::exchange::{ExchangeAccumulator, PendingRelay};
use super::scatter::{with_commit_indices, with_group};
use super::*;
use crate::query::RelayRoute;
use crate::runtime::orchestration::guard_panic;
use crate::runtime::peer::Peer;
use crate::runtime::reactor::{select2, Either};
use crate::runtime::sal::{SalFit, SalScope};
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_store::ops::{op_relay_broadcast, op_relay_scatter_consolidated, op_repartition_batches, ScatterSpec};
use gnitz_store::relation::Relation;

/// One round of a checkpoint.
#[derive(Clone, Copy)]
pub(crate) enum FlushRound {
    /// Flush base and system tables.
    Base,
    /// Flush every view's traces and output stores, stamped with `generation`.
    Ephemeral { generation: u64 },
}

impl FlushRound {
    fn kind(self) -> SalMessageKind {
        match self {
            FlushRound::Base => SalMessageKind::Flush,
            FlushRound::Ephemeral { .. } => SalMessageKind::FlushEph,
        }
    }

    /// The group header's `lsn`: the generation workers stamp their manifests with.
    fn lsn(self) -> u64 {
        match self {
            FlushRound::Base => 0,
            FlushRound::Ephemeral { generation } => generation,
        }
    }

    fn phase(self) -> &'static str {
        match self {
            FlushRound::Base => "checkpoint base round",
            FlushRound::Ephemeral { .. } => "checkpoint ephemeral round",
        }
    }
}

/// How often a worker's death is probed for.
pub(crate) const WORKER_WATCH: Duration = Duration::from_millis(100);

/// `GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW`: report SAL relay space as low for the
/// backfill on every non-stop round, so tests drive the reclamation protocol over
/// a small table. The steady-state relay's equivalent is `executor`'s own
/// `RELAY_SPACE_LOW`, beside the loop it perturbs.
static BACKFILL_RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW");

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

    /// `target_id`'s wire identity, off the catalog's cache: the block is built
    /// on first call and invalidated alongside col_names whenever DDL modifies
    /// the table, so the SAL write paths (commit/tick/broadcast) pay neither a
    /// block encode nor a per-column walk per group.
    fn wire_schema(&self, target_id: i64) -> wire::WireSchema {
        let descriptor = self.schema_desc_for(target_id);
        wire::WireSchema::from_catalog(self.cat(), target_id, descriptor)
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

    /// The next exchange round `lease`'s ids wait on, or `None` once all have
    /// answered. A worker ACKs a tick or backfill only after its relays are written, so
    /// `None` leaves no partial round in `acc`.
    ///
    /// At most one caller may await this at a time: the exchange queue holds a
    /// single waker.
    pub(crate) async fn next_relay(
        &self,
        lease: &Lease,
        ctx: &str,
        acc: &mut ExchangeAccumulator,
    ) -> Result<Option<PendingRelay>, WireFault> {
        let mut acks = std::pin::pin!(lease.acks(ctx));
        loop {
            match select2(acks.as_mut(), self.reactor.next_exchange()).await {
                Either::A(r) => return r.map(|()| None),
                Either::B((w, frame)) => {
                    if let Some(relay) = acc.process(w, frame) {
                        return Ok(Some(relay));
                    }
                }
            }
        }
    }

    /// Block until `lease`'s ids have answered, relaying each round inline;
    /// nothing else on the reactor runs. Fails on a worker's error ACK or death.
    ///
    /// `checkpoint_allowed`: a backfill may stamp CHECKPOINT to reclaim SAL
    /// space mid-stream (workers re-epoch inline). A drain TICK must NOT —
    /// it carries no backfill pad, so a CHECKPOINT would advance the master
    /// epoch while workers stay on the old one and wedge the cluster.
    ///
    /// `ctx` names the phase in a worker-fault or dead-worker error.
    pub(crate) fn collect_exclusive(
        &self,
        lease: &Lease,
        ctx: &str,
        checkpoint_allowed: bool,
    ) -> Result<(), WireFault> {
        self.reactor.block_on_exclusive(async {
            let collect = async {
                let mut acc = ExchangeAccumulator::new(self.num_workers());
                // Set when a round is stamped CHECKPOINT; the reset lands at the next
                // round barrier, after every worker consumed that relay — so the next
                // round is written at cursor 0 of the epoch the workers already
                // expect. A bare `checkpoint_reset`, never `checkpoint_post_ack`: a
                // mid-backfill flush would orphan unconsumed backfill groups.
                let mut pending_reset = false;
                while let Some(relay) = self.next_relay(lease, ctx, &mut acc).await? {
                    let mut excl = self.sal.lock_exclusive();
                    if std::mem::take(&mut pending_reset) {
                        excl.checkpoint_reset();
                    }
                    // Stop takes precedence: an all-pad round ends the backfill, and its
                    // leftover SAL is reclaimed by the post-backfill checkpoint. A
                    // CHECKPOINT verdict cannot rescue this round — it is written at the
                    // current cursor either way.
                    let all_pad = relay.all_pad;
                    let prep = self.prepare_relay(relay, ctx);
                    let decision = if all_pad {
                        BackfillDecision::Stop
                    } else if checkpoint_allowed
                        && (prep.with_group(BackfillDecision::Continue, |g| self.sal.fit_relay(g)) != SalFit::Fits
                            || BACKFILL_RELAY_SPACE_LOW.armed())
                    {
                        pending_reset = true;
                        BackfillDecision::Checkpoint
                    } else {
                        BackfillDecision::Continue
                    };
                    self.emit_relay(&excl, &prep, decision);
                }
                Ok::<(), WireFault>(())
            };
            match select2(collect, self.round_failure(lease, ctx)).await {
                Either::A(r) => r,
                Either::B(e) => Err(e),
            }
        })
    }

    /// Resolves once a worker has answered `lease` with an error or has died, probing
    /// every `WORKER_WATCH`. A worker failing before it joins a round leaves the others
    /// in their exchange wait, so an error must end the round without the other ACKs.
    async fn round_failure(&self, lease: &Lease, ctx: &str) -> WireFault {
        loop {
            self.reactor.timer(Instant::now() + WORKER_WATCH).await;
            if let Some(e) = lease.first_error(ctx) {
                return e;
            }
            if let Some(w) = self.check_workers() {
                return format!("worker {w} exited during {ctx}").into();
            }
        }
    }

    /// Write the group `write` builds on a fresh ACK lease, then collect exclusively.
    /// A refused write fails before any worker is woken, keeping its status.
    fn exclusive_round(
        &self,
        ctx: &str,
        checkpoint_allowed: bool,
        write: impl FnOnce(&SalExcl<'_>, GroupTargets) -> Result<(), WireFault>,
    ) -> Result<(), WireFault> {
        let lease = self.reactor.lease_acks(1, WorkerSet::ALL);
        write(&self.sal.lock_exclusive(), GroupTargets::all(lease.id(0)))?;
        self.collect_exclusive(&lease, ctx, checkpoint_allowed)
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// True while a base round has published past the last ephemeral round —
    /// i.e. every checkpointed view and index on disk is invalid until
    /// [`Self::restamp_derived`] runs.
    pub(crate) fn derived_needs_restamp(&self) -> bool {
        self.cat().durable_generation() > self.last_ephemeral_gen.get()
    }

    /// Invariant: the caller must own SAL checkpoint exclusivity — an exclusive
    /// round at boot, or in a DDL window under the catalog write lock with the
    /// committer proven idle. The *async* fan-out / tick / steady-state DDL
    /// paths must NOT call this: a concurrent Flush races the committer's own and
    /// orphans SAL writes straddling `sal.checkpoint_reset`.
    ///
    /// Publish every base table's shards and reset the SAL, invalidating
    /// checkpointed derived state first. The bump is not optional: this path
    /// makes a newer base cut durable and then discards the SAL entries that
    /// carried the difference, while every checkpointed view and index manifest
    /// still names the older cut — left generation-valid, the next boot would
    /// resume derived state behind its base with no tail left to close the gap.
    /// Durable *before* the flush round (`bump_checkpoint_generation` flushes the
    /// system tables), so a crash below leaves that state invalid rather than
    /// silently stale — the same ordering step 0 of `run_checkpoint_sequence`
    /// uses.
    ///
    /// Half a checkpoint on its own: it must be paired with `restamp_derived` in
    /// the same exclusive window.
    fn reclaim_base(&self) -> Result<(), WireFault> {
        self.cat().bump_checkpoint_generation()?;
        self.sync_round(FlushRound::Base)
    }

    /// Re-stamp the derived state a `reclaim_base` invalidated, inside the
    /// caller's exclusive window: tick every source carrying buffered deltas
    /// up to the published base cut, then persist every view trace, view output
    /// and index at the durable generation.
    ///
    /// The tick is not optional — a base round leaves `pending_deltas` in worker
    /// RAM (`handle_flush_all`), and the reclaim reset the SAL out from under
    /// them, so stamping first would mark views durable at a cut their input has
    /// not reached.
    ///
    /// `pending` is the master's set of tables with un-ticked deltas — what the
    /// committer's `Drain` covers.
    pub(crate) fn restamp_derived(&self, pending: &[i64]) -> Result<(), WireFault> {
        for &tid in pending {
            self.drain_tick_blocking(tid)?;
        }
        self.sync_round(FlushRound::Ephemeral {
            generation: self.cat().durable_generation(),
        })
    }

    /// [`Self::checkpoint_round`] on a reactor that polls no other task.
    fn sync_round(&self, round: FlushRound) -> Result<(), WireFault> {
        self.reactor
            .block_on_exclusive(async { self.checkpoint_round(&mut self.sal.lock_exclusive(), round).await })
    }

    /// Write `round`'s flush group, wait for every worker's ACK, finalize.
    pub(crate) async fn checkpoint_round(&self, excl: &mut SalExcl<'_>, round: FlushRound) -> Result<(), WireFault> {
        let ctx = round.phase();
        let lease = self.reactor.lease_acks(1, WorkerSet::ALL);
        self.note_flush_round(round);
        excl.write(&DirectGroup {
            lsn: round.lsn(),
            targets: GroupTargets::all(lease.id(0)),
            ..DirectGroup::new(round.kind())
        })?;
        // `excl` is held through the ACKs, so its drop would wake too late.
        excl.wake();
        match select2(lease.acks(ctx), self.round_failure(&lease, ctx)).await {
            Either::A(r) => r?,
            Either::B(e) => return Err(e),
        }
        self.checkpoint_post_ack(excl)
    }

    /// Check one emitted round against the ordering every base publish depends
    /// on: **no durable base-shard advance without a prior durable generation
    /// bump**, and no second base round once an ephemeral round has re-stamped
    /// derived state at the current generation.
    ///
    /// Checked on the one path that emits a round rather than left as an
    /// obligation on each call site. The committer's reclaim-only base round
    /// inside `await_servicing` rides step 0's bump without one of its own — that
    /// is the "no intervening ephemeral round" clause, encoded rather than
    /// excepted.
    fn note_flush_round(&self, round: FlushRound) {
        let durable = self.cat().durable_generation();
        if let FlushRound::Ephemeral { generation } = round {
            debug_assert_eq!(generation, durable, "ephemeral round must stamp the durable generation");
            self.last_ephemeral_gen.set(generation);
        } else {
            debug_assert!(
                durable > self.last_ephemeral_gen.get(),
                "base round at generation {} publishes past the last ephemeral round ({}): \
                 derived state would resume from a cut older than its base",
                durable,
                self.last_ephemeral_gen.get(),
            );
        }
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    /// Scatter a completed round into the group [`Self::emit_relay`] writes.
    /// Aborts on failure, naming `ctx`: every worker is parked on this relay.
    pub(crate) fn prepare_relay(&self, relay: PendingRelay, ctx: &str) -> RelayPrepared {
        guard_panic("prepare_relay", || self.build_relay(relay)).unwrap_or_else(|e: String| {
            gnitz_fatal_abort!("{ctx}: {e}; a lost relay wedges workers blocked in exchange wait")
        })
    }

    fn build_relay(&self, relay: PendingRelay) -> Result<RelayPrepared, String> {
        // `all_pad` is the backfill stop signal, read by the caller before
        // `prepare_relay`; the relay scatter itself does not depend on it.
        let PendingRelay {
            view_id,
            payloads,
            schema,
            source_id,
            all_pad: _,
        } = relay;

        let cat = self.cat();
        let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();

        // Every contributing source must be consolidated to take the merge-walk
        // scatter; a single non-consolidated one falls back to the re-sorting
        // repartition. The scatter (`op_relay_scatter_consolidated`) debug-verifies
        // each.
        let num_workers = self.num_workers();
        let scatter = |spec: ScatterSpec<'_>| {
            if sources.iter().flatten().all(|b| b.is_consolidated()) {
                op_relay_scatter_consolidated(&sources, spec, &schema, num_workers)
            } else {
                op_repartition_batches(&sources, spec, &schema, num_workers)
            }
        };

        let (dag, registry) = cat.dag_and_registry_mut();
        let Some(meta) = dag.view_meta(registry, view_id) else {
            return Err(format!(
                "view {view_id}: circuit unreadable or unroutable; source {source_id}'s delta has no \
                 scatter key"
            ));
        };
        let route = meta.relay_route(source_id);
        let dest = match route {
            RelayRoute::NoSingleKey => {
                return Err(format!(
                    "view {view_id}: source {source_id} feeds several distinct reindex keys; no single \
                     scatter key co-partitions it"
                ))
            }
            RelayRoute::Broadcast => Ok(vec![op_relay_broadcast(&sources, &schema)]),
            RelayRoute::GroupKey(cols) => scatter(ScatterSpec::GroupKey(cols)),
            RelayRoute::JoinKey(slots) => scatter(ScatterSpec::JoinKey(slots)),
        };
        // The scatter key's own reason, not a restatement: it names the column
        // and the bound it missed.
        let dest = dest.map_err(|e| format!("view {view_id}: source {source_id} relay key: {e}"))?;

        Ok(RelayPrepared {
            view: wire::WireSchema::encoded(view_id, schema),
            source_id,
            dest,
        })
    }

    /// Write a prepared relay stamped with `decision`. Aborts on failure, as
    /// [`Self::prepare_relay`] does.
    pub(crate) fn emit_relay(&self, excl: &SalExcl<'_>, prep: &RelayPrepared, decision: BackfillDecision) {
        guard_panic("emit_relay", || prep.with_group(decision, |g| excl.write(g))).unwrap_or_else(|e| {
            gnitz_fatal_abort!("emit_relay: {e}; a lost relay wedges workers blocked in exchange wait")
        })
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Distributed backfill of ONE view from `source_id`. Always view-scoped — the source may
    /// already have populated dependents (live CREATE VIEW; recovery step-4
    /// rebuild next to resumed siblings) that a closure re-drive would
    /// double-count.
    ///
    /// Reclaims SAL space before a large source; the exclusive round may further
    /// CHECKPOINT mid-stream. Both are safe on the SAL-exclusive,
    /// no-concurrent-relay paths this runs on (boot; a DDL window's exclusive
    /// rounds). A reclaim here bumps the generation, leaving every checkpointed
    /// view and index invalid until an ephemeral round re-stamps them — so a
    /// caller must run `restamp_derived` before its window closes whenever
    /// [`Self::derived_needs_restamp`] answers true afterwards.
    fn fan_out_backfill(&self, view_id: i64, source_id: i64) -> Result<(), WireFault> {
        // A backfill round cannot reclaim mid-flight — the reset reaches the
        // workers stamped on the *previous* round's relay — so round one gets
        // whatever the cursor leaves, and reclaiming first is the only lever.
        // Neither test implies the other: `GNITZ_CHECKPOINT_BYTES` is unclamped
        // and may sit below the margin.
        if self.sal.past_backfill_reclaim_threshold() || self.sal.needs_checkpoint() {
            self.reclaim_base()?;
        }
        // Dataless, but it still carries a schema block: that block is what
        // stamps `Batch.schema` on the worker side.
        let source = wire::WireSchema::encoded(source_id, self.schema_desc_for(source_id));
        let template = source.frame(wire::WireMsg {
            arg0: view_id as u64,
            ..Default::default()
        });
        self.exclusive_round("backfill relay", true, |excl, t| {
            excl.write(&DirectGroup {
                template,
                targets: t,
                ..DirectGroup::new(SalMessageKind::Backfill)
            })
        })
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
    /// barrier and `fan_out_backfill` accommodates that — no relay arrives, so
    /// each worker stops on its own drain exhaustion — which is why one driver
    /// serves every shape.
    pub(crate) fn backfill_views_in_dep_order(&self, view_ids: &[i64]) -> Result<(), WireFault> {
        let mut ordered = view_ids.to_vec();
        ordered.sort_unstable();
        ordered.dedup();
        for vid in ordered {
            let sources = {
                let (dag, registry) = self.cat().dag_and_registry_mut();
                dag.get_source_ids(registry, vid)
            };
            for src in sources {
                self.fan_out_backfill(vid, src).map_err(|e| WireFault {
                    status: e.status,
                    text: format!("view={vid} source={src}: {e}"),
                })?;
            }
        }
        Ok(())
    }

    /// Tick `source_id` in an exclusive round, relaying its exchange rounds inline.
    /// No mid-round checkpoint: a tick carries no backfill pad to re-epoch on.
    pub(crate) fn drain_tick_blocking(&self, source_id: i64) -> Result<(), WireFault> {
        self.exclusive_round("view tick drain", false, |excl, t| {
            self.write_tick_group(excl, source_id, t)
        })
    }

    /// Send a scan-shaped read of `kind` and forward the replies to `peer`;
    /// `Ok(false)` on a client disconnect.
    pub(crate) async fn fan_out_scan(
        &self,
        peer: &Peer,
        kind: SalMessageKind,
        template: wire::WireMsg<'_>,
    ) -> Result<bool, WireFault> {
        forward_scan(
            peer,
            &self.scan(DirectGroup { template, ..DirectGroup::new(kind) }).await?,
        )
        .await
    }

    /// Broadcast a DDL batch to every worker inside `scope`'s zone — one LSN
    /// across a DDL's broadcasts, so recovery groups them atomically. Publishes
    /// nothing; that is the scope's commit.
    pub(crate) fn broadcast_ddl(&self, scope: &SalScope, target_id: i64, batch: &Batch) -> Result<(), WireFault> {
        let relation = self.wire_schema(target_id);
        scope.write(
            &DirectGroup {
                template: relation.frame(wire::WireMsg::default()),
                data: GroupData::Same(wire::WireData::Whole(batch)),
                ..DirectGroup::new(SalMessageKind::DdlSync)
            },
            true,
        )?;
        gnitz_debug!("broadcast_ddl tid={} rows={}", target_id, batch.len());
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// Write a Tick group for `tid` at the next tick round, in the header's `lsn`.
    /// A refused write burns its round.
    pub(crate) fn write_tick_group(
        &self,
        excl: &SalExcl<'_>,
        tid: i64,
        targets: GroupTargets,
    ) -> Result<(), WireFault> {
        let round = self.tick_round.get() + 1;
        self.tick_round.set(round);
        self.record_delta_round(tid, round);
        excl.write(&DirectGroup {
            template: wire::WireMsg {
                target_id: tid as u64,
                ..Default::default()
            },
            lsn: round,
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
    fn record_delta_round(&self, tid: i64, round: u64) {
        let cat = self.cat();
        if !cat.registry().any_delta_feed() {
            return;
        }
        let reached = {
            let (dag, registry) = cat.dag_and_registry_mut();
            dag.dependent_closure(registry, vec![tid])
        };
        let mut map = self.last_delta_round.borrow_mut();
        for vid in reached {
            if cat.registry().relation(vid).is_some_and(Relation::has_delta_feed) {
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
    pub(crate) fn delta_cursor_tag(&self, view_id: i64) -> u64 {
        // splitmix64's finalizer: the view id is a small dense integer, and an
        // unmixed XOR would make neighbouring ids' tags differ in one bit. It is
        // a **bijection** on u64 — `x ^ (x >> k)` is invertible and both
        // multipliers are odd — so distinct view ids cannot collide within a
        // boot, which a general hash (XXH3 over 8 bytes) would not guarantee.
        let mut z = (view_id as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        self.boot_nonce ^ (z ^ (z >> 31))
    }

    /// The last round that reached `view_id`, or `1` for a view no round has
    /// reached — the boot round, which is never emitted and stamps nothing, so a
    /// bootstrap at `after_tick = 0` always falls through to the store.
    pub(crate) fn last_delta_round(&self, view_id: i64) -> u64 {
        self.last_delta_round.borrow().get(&view_id).copied().unwrap_or(1)
    }

    /// Drop a dropped relation's gate entry. Ids are never reused, so nothing else
    /// would ever reclaim it, and a per-DDL leak on the master is not a leak that
    /// stops.
    pub(crate) fn forget_delta_round(&self, id: i64) {
        self.last_delta_round.borrow_mut().remove(&id);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /// The first dead worker. A dead worker is reported on every probe until
    /// `shutdown_workers` reaps the set: a reaped pid answers ECHILD every time.
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

    /// Broadcast `Shutdown` (each worker flushes + `_exit`s) and reap the
    /// worker processes, blocking on each.
    pub(crate) async fn shutdown_workers(&self) {
        // No schema block: the worker's `Shutdown` arm takes no arguments. A
        // refusal would hang the `waitpid` below, so it must not vanish.
        if let Err(e) = self.sal.lock().await.write(&DirectGroup::new(SalMessageKind::Shutdown)) {
            gnitz_warn!("SAL refused the Shutdown broadcast: {}; the worker reap will block", e);
        }
        for &pid in &self.worker_pids {
            if pid > 0 {
                let mut status: i32 = 0;
                let _ = retry_eintr(|| unsafe { libc::waitpid(pid, &mut status, 0) });
            }
        }
    }

    /// Lay one push batch out as a SAL group inside `scope`, every worker
    /// answering on `request_id`. `recoverable` puts it inside the zone; a
    /// stream's rows ride outside. The committer commits the scope and awaits the
    /// ACKs.
    pub(crate) fn write_commit_group(
        &self,
        scope: &SalScope,
        target_id: i64,
        batch: &Batch,
        request_id: u32,
        recoverable: bool,
    ) -> Result<(), WireFault> {
        let relation = self.wire_schema(target_id);
        let group = DirectGroup {
            targets: GroupTargets::all(request_id),
            ..DirectGroup::new(SalMessageKind::Push)
        };
        // A replicated relation broadcasts: the whole batch lands in every
        // worker's ingest + SAL slot, so each worker durably logs the full table
        // and enforces uniqueness against its identical full copy.
        with_commit_indices(batch, relation.descriptor(), self.num_workers(), |worker_indices| {
            with_group(batch, worker_indices, &relation, group, |g| scope.write(g, recoverable))
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
    /// leaves the SAL intact and either retries on a later checkpoint or aborts.
    pub(crate) fn checkpoint_post_ack(&self, excl: &mut SalExcl<'_>) -> Result<(), WireFault> {
        let cat = self.cat();
        cat.flush_all_system_tables()?;
        // Every worker ACKed the FLUSH, so each has applied every DdlSync written
        // before it; the flush above made every applied DROP durable.
        cat.reclaim_orphan_dirs();
        excl.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    /// Boot-end checkpoint, as exclusive rounds: record the
    /// launched topology, then reclaim and re-stamp. The drain set is empty —
    /// recovery already drained everything and no pushes are admitted yet (the
    /// socket is not open), so `pending_deltas` is empty. Freshly backfilled
    /// views are durably checkpointed before the socket opens.
    pub(crate) fn boot_checkpoint(&self, worker_count: u32) -> Result<(), WireFault> {
        // Recorded before `reclaim_base`, whose generation bump makes it durable.
        self.cat().record_topology(worker_count);
        self.reclaim_base()?;
        self.restamp_derived(&[])
    }

    /// The descriptor `target_id` is registered with. Panics on an unregistered
    /// id: every caller reaches it holding the catalog lock the registry's own
    /// writers take, so a miss is broken lock discipline, not a bad client id.
    pub(crate) fn schema_desc_for(&self, target_id: i64) -> SchemaDescriptor {
        self.cat()
            .registry()
            .relation(target_id)
            .map(Relation::schema)
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"))
    }
}

#[cfg(test)]
#[path = "tests/dispatch.rs"]
mod tests;
