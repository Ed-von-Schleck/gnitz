//! Master SAL dispatcher: the `MasterDispatcher` core — SAL group/broadcast
//! writes, worker signalling + ack collection, relay emit, the fan-out family
//! (backfill / seek / scan / index), the checkpoint rounds, the tick-round
//! counter and worker reaping.

use std::time::{Duration, Instant};

use super::exchange::ExchangeAccumulator;
use super::*;
use crate::runtime::reactor::{select2, Either};
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io::retry_eintr;
use gnitz_store::relation::Relation;
use gnitz_wire::{low_bits_mask, BitIter};

/// A worker set rides in one word (`signal_reached`), so a worker past bit 63
/// would never be signalled.
const _: () = assert!(gnitz_wire::MAX_WORKERS <= u64::BITS as usize);

/// How often a worker's death is probed for.
pub(crate) const WORKER_WATCH: Duration = Duration::from_millis(100);

/// `GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW`: report SAL relay space as low for the
/// backfill on every non-stop round, so tests drive the reclamation protocol over
/// a small table. The steady-state relay's equivalent is `executor`'s own
/// `RELAY_SPACE_LOW`, beside the loop it perturbs.
static BACKFILL_RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW");

/// A `u64` from the OS entropy pool for [`MasterDispatcher::delta_cursor_tag`],
/// whose doc states what the tag is for. From the OS rather than from a seeded
/// generator because two back-to-back boots must not collide. A short read or an
/// unavailable pool falls back to the boot's wall clock and pid, which is weaker
/// but still distinguishes two boots of one machine.
fn boot_nonce() -> u64 {
    let mut bytes = [0u8; 8];
    let n = unsafe { libc::getrandom(bytes.as_mut_ptr().cast(), bytes.len(), 0) };
    if n == bytes.len() as isize {
        return u64::from_ne_bytes(bytes);
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    nanos ^ ((std::process::id() as u64) << 32)
}

/// A seek's answer, in the shape its fan-out produced.
pub(crate) enum SeekReply {
    /// The one answering worker's reply frame, forwarded to the client verbatim.
    Frame(W2mSlot),
    /// Every worker's rows merged into one batch, or `None` when none matched.
    Merged(Option<Box<Batch>>),
}

/// Merge every reply of a broadcast lookup into one batch, or `None` when no row
/// matched. `op` names the verb in the errors.
async fn merge_replies(
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
    op: &str,
    expected: &SchemaDescriptor,
) -> Result<Option<Batch>, WorkerFault> {
    let mut acc: Option<Batch> = None;
    let mut merged_bytes = 0usize;
    drain_index_scan(slots, scan, op, expected, |mb, frame_len| {
        // The merge goes back out as one frame, so it is bounded by what the
        // client will read (`FRAME_CAP`). Σ frame bytes ≥ that merged encode
        // size — every frame re-counts its header and the first one the schema
        // block — so capping the sum never lets an unreadable reply through, and
        // it bounds the master's merge heap on the way.
        merged_bytes += frame_len;
        if merged_bytes > crate::runtime::wire::FRAME_CAP {
            return Err(format!(
                "{op}: result exceeds the {} MiB reply cap",
                crate::runtime::wire::FRAME_CAP >> 20
            )
            .into());
        }
        let a = acc.get_or_insert_with(|| Batch::with_capacity(expected, mb.len()));
        a.append_mem_batch(mb);
        Ok(())
    })
    .await?;
    // The sink runs only for non-empty frames, so `Some` implies rows.
    Ok(acc)
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
        m2w_efds: Vec<i32>,
    ) -> Self {
        debug_assert_eq!(m2w_efds.len(), worker_pids.len(), "one wakeup eventfd per worker");
        debug_assert_eq!(
            sal.num_workers(),
            worker_pids.len(),
            "the SAL's slot count is the worker count"
        );
        MasterDispatcher {
            worker_pids: RefCell::new(worker_pids),
            sal,
            sal_writer_excl: AsyncMutex::default(),
            m2w_efds,
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
    /// accessor, not a second owner of the pointer. Sound because the master
    /// reactor is single-threaded and the catalog outlives the dispatcher.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn cat(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// The SAL-writer mutex guarding this dispatcher's `sal`. Held by whoever is
    /// writing a group; see the field's own doc for the rule.
    ///
    /// `AsyncMutex::lock` clones the `Rc` into its future and the guard owns one,
    /// so `disp.sal_excl().lock().await` holds no borrow of the dispatcher across
    /// the await.
    pub(crate) fn sal_excl(&self) -> &AsyncMutex {
        &self.sal_writer_excl
    }

    /// The boot [`SalWriter::boot_rewind`], above the same `walk_epoch` the
    /// workers were launched with. Runs once every worker has finished recovery,
    /// and before any group is written.
    pub(crate) fn boot_rewind_sal(&self, walk_epoch: u32) {
        self.sal.boot_rewind(walk_epoch);
    }

    /// `target_id`'s wire identity, off the catalog's cache: the block is built
    /// on first call and invalidated alongside col_names whenever DDL modifies
    /// the table, so the SAL write paths (commit/tick/broadcast) pay neither a
    /// block encode nor a per-column walk per group.
    fn wire_schema(&self, target_id: i64) -> wire::WireSchema {
        wire::WireSchema::from_catalog(self.cat(), target_id, self.schema_desc_for(target_id))
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Write one SAL group and nothing else — no signal, no ack collection.
    pub(crate) fn write_group(&self, g: &DirectGroup) -> Result<(), WorkerFault> {
        self.sal.write(g)
    }

    /// Write `base` with its rows PK-partitioned across the workers: each
    /// worker's slot carries only the rows of `batch` it stores — the scatter
    /// sibling of a `GroupData::Same` broadcast. `base.template` is framed by
    /// `relation`; its `data` is replaced.
    pub(super) fn write_scatter_group(
        &self,
        batch: &Batch,
        relation: &wire::WireSchema,
        base: DirectGroup<'_>,
    ) -> Result<(), WorkerFault> {
        // No reentrancy: the closure has no `.await`, so the SCATTER_INDICES
        // borrow is released before the next caller needs it.
        with_worker_indices(batch, relation.descriptor(), self.num_workers(), |worker_indices| {
            with_group(batch, worker_indices, relation, base, |g| self.sal.write(g))
        })
    }

    /// Open a SAL publication scope at `lsn` (see [`SalWriter::begin`]). The
    /// caller must hold `sal_writer_excl` and must not suspend inside it.
    pub(crate) fn begin(&self, lsn: u64, tag: &'static str) -> SalScope<'_> {
        self.sal.begin(lsn, tag)
    }

    /// Wake every worker: they see the SAL entry through the mapping's Acquire
    /// size prefix regardless, so this only ends a park.
    pub(crate) fn signal_all(&self) {
        for &efd in &self.m2w_efds {
            crate::runtime::m2w::eventfd_signal(efd);
        }
    }
    fn signal_one(&self, worker: usize) {
        crate::runtime::m2w::eventfd_signal(self.m2w_efds[worker]);
    }

    /// Wake every worker these fan-outs reached, and no other: groups written to
    /// one worker each cost one eventfd write apiece, not W.
    pub(super) fn signal_reached(&self, fanouts: impl IntoIterator<Item = super::Fanout>) {
        let all = low_bits_mask(self.num_workers());
        let mut wake: u64 = 0;
        for f in fanouts {
            wake |= match f {
                super::Fanout::Broadcast => all,
                super::Fanout::One(w) => 1 << w,
            };
        }
        for w in BitIter(wake & all) {
            self.signal_one(w);
        }
    }

    pub(crate) fn sal_fd(&self) -> i32 {
        self.sal.sal_fd()
    }

    /// The master's event loop.
    pub(crate) fn reactor(&self) -> &Rc<Reactor> {
        &self.reactor
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.sal.num_workers()
    }

    /// The next exchange round `lease`'s first `n` ids wait on, or `None` once all have
    /// answered. A worker ACKs a tick or backfill only after its relays are written, so
    /// `None` leaves no partial round in `acc`.
    ///
    /// At most one caller may await this at a time: the exchange queue holds a
    /// single waker.
    pub(crate) async fn next_relay(
        &self,
        lease: &Lease,
        n: usize,
        ctx: &str,
        acc: &mut ExchangeAccumulator,
    ) -> Result<Option<PendingRelay>, WorkerFault> {
        let mut acks = std::pin::pin!(lease.acks(n, |w, c| worker_error(w, ctx, c)));
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

    /// Block until `lease`'s first `n` ids have answered, relaying each round inline;
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
        n: usize,
        ctx: &str,
        checkpoint_allowed: bool,
    ) -> Result<(), String> {
        self.reactor.block_on_exclusive(async {
            let collect = async {
                let mut acc = ExchangeAccumulator::new(self.num_workers());
                // Set when a round is stamped CHECKPOINT; the reset lands at the next
                // round barrier, after every worker consumed that relay — so the next
                // round is written at cursor 0 of the epoch the workers already
                // expect. A bare `checkpoint_reset`, never `checkpoint_post_ack`: a
                // mid-backfill flush would orphan unconsumed backfill groups.
                let mut pending_reset = false;
                while let Some(relay) = self.next_relay(lease, n, ctx, &mut acc).await.map_err(|f| f.text)? {
                    if std::mem::take(&mut pending_reset) {
                        self.sal.checkpoint_reset();
                    }
                    // Stop takes precedence: an all-pad round ends the backfill, and its
                    // leftover SAL is reclaimed by the post-backfill checkpoint. A
                    // CHECKPOINT verdict cannot rescue this round — it is written at the
                    // current cursor either way.
                    let all_pad = relay.all_pad;
                    let prep = self.prepare_relay(relay)?;
                    let decision = if all_pad {
                        BACKFILL_DECISION_STOP
                    } else if checkpoint_allowed
                        && (self.relay_fit(prep.footprint) != SalFit::Fits || BACKFILL_RELAY_SPACE_LOW.armed())
                    {
                        pending_reset = true;
                        BACKFILL_DECISION_CHECKPOINT
                    } else {
                        BACKFILL_DECISION_CONTINUE
                    };
                    self.emit_relay_with_decision(&prep, decision)?;
                }
                Ok::<(), String>(())
            };
            match select2(collect, self.round_failure(lease, n, ctx)).await {
                Either::A(r) => r,
                Either::B(e) => Err(e),
            }
        })
    }

    /// Resolves once a worker has answered `lease` with an error or has died, probing
    /// every `WORKER_WATCH`. A worker failing before it joins a round leaves the others
    /// in their exchange wait, so an error must end the round without the other ACKs.
    async fn round_failure(&self, lease: &Lease, n: usize, ctx: &str) -> String {
        loop {
            self.reactor.timer(Instant::now() + WORKER_WATCH).await;
            if let Some(e) = lease.first_error(n, |w, c| worker_error(w, ctx, c)) {
                return e.text;
            }
            if let Some(w) = self.check_workers() {
                return format!("worker {w} exited during {ctx}");
            }
        }
    }

    /// Write the group `write` builds on a fresh ACK lease, signal, collect exclusively.
    fn exclusive_round(
        &self,
        ctx: &str,
        checkpoint_allowed: bool,
        write: impl FnOnce(GroupTargets) -> Result<(), WorkerFault>,
    ) -> Result<(), String> {
        let nw = self.num_workers();
        let lease = self.reactor.lease_acks(nw);
        write(GroupTargets::All(lease.base())).map_err(|f| f.text)?;
        self.signal_all();
        self.collect_exclusive(&lease, nw, ctx, checkpoint_allowed)
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
    fn reclaim_base(&self) -> Result<(), String> {
        self.cat().bump_checkpoint_generation()?;
        self.sync_round(0, SalMessageKind::Flush)
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
    pub(crate) fn restamp_derived(&self, pending: &[i64]) -> Result<(), String> {
        for &tid in pending {
            self.drain_tick_blocking(tid)?;
        }
        self.sync_round(self.cat().durable_generation(), SalMessageKind::FlushEph)
    }

    /// One exclusive round: emit the flush group, block for every worker's ACK,
    /// finalize. A `FlushEph` round's `lsn` IS the checkpoint generation (workers
    /// latch it via `set_resume_generation`); the base round passes 0.
    fn sync_round(&self, lsn: u64, kind: SalMessageKind) -> Result<(), String> {
        // `kind` already discriminates the two callers, so it names the phase a
        // dead worker is reported against too.
        let ctx = if kind == SalMessageKind::FlushEph {
            "checkpoint ephemeral round"
        } else {
            "checkpoint base round"
        };
        self.exclusive_round(ctx, false, |t| self.write_checkpoint_group(lsn, kind, t))?;
        self.checkpoint_post_ack()
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
    fn note_flush_round(&self, lsn: u64, kind: SalMessageKind) {
        let durable = self.cat().durable_generation();
        if kind == SalMessageKind::FlushEph {
            debug_assert_eq!(lsn, durable, "ephemeral round must stamp the durable generation");
            self.last_ephemeral_gen.set(lsn);
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

    /// Can a relay of `need` bytes be written, and if not, is a checkpoint enough
    /// to make room? [`Self::sal_space_low`] is the same rule with no group in
    /// hand.
    pub(crate) fn relay_fit(&self, need: usize) -> SalFit {
        self.sal.fit_relay(need)
    }

    /// Build the SAL group an exchange relay writes and hand it to `f`. Sizing
    /// and emission both go through here, so the bytes checked are the bytes
    /// written; `WireMsg::size` does not read `decision`.
    ///
    /// `seek_pk` echoes `source_id` back so the worker's `do_exchange_wait` can
    /// match on (view_id, source_id). Without it, a multi-source view (join over
    /// 2+ tables) can deliver the wrong source's relay to a waiting exchange and
    /// the worker demuxes against the wrong sharding columns.
    fn with_relay_group<R>(
        &self,
        view: &wire::WireSchema,
        source_id: i64,
        dest: &RelayDest,
        decision: u64,
        f: impl FnOnce(&DirectGroup) -> R,
    ) -> R {
        let template = view.frame(wire::WireMsg {
            seek_pk: source_id as u128,
            seek_col_idx: decision,
            ..Default::default()
        });
        let group = |data| DirectGroup {
            template,
            data,
            ..DirectGroup::new(SalMessageKind::ExchangeRelay)
        };
        match dest {
            RelayDest::Broadcast(b) => f(&group(GroupData::Same(wire::WireData::Whole(Some(&**b))))),
            RelayDest::PerWorker(batches) => {
                let slots: Vec<wire::WireData> = batches.iter().map(|b| wire::WireData::Whole(Some(b))).collect();
                f(&group(GroupData::PerWorker(&slots)))
            }
        }
    }

    /// CPU-only first half of exchange relay: looks up shard columns via
    /// the catalog DAG, scatters the payloads into per-worker batches, and
    /// collects column names. No SAL write yet — a steady tick relay runs this
    /// without `sal_writer_excl` so the lock covers only the synchronous
    /// SAL write in `emit_relay_with_decision`.
    pub(crate) fn prepare_relay(&self, relay: PendingRelay) -> Result<RelayPrepared, String> {
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
        // The relay treats the per-worker payloads as disjoint slices of one
        // delta and scatters them together. A replicated source breaks that: its
        // delta is broadcast, so every worker returns the same rows, and
        // scattering all W would route each row to its owner W times for the
        // post-relay consolidate to sum into one row at weight W. Take worker
        // 0's payload alone — the same single-sourcing the read gather applies
        // (`read_fanout`). Dropping the copies rather than clamping keeps
        // a genuine multiplicity in the source intact. A unary side relays under
        // `source_id == 0`, which names no relation — the guard skips a lookup
        // that cannot hit, and such a view needs no rule anyway: one whose
        // sources are all replicated is itself stamped replicated and computes
        // locally without ever reaching an exchange.
        let n_src = if source_id > 0 && cat.registry().relation(source_id).is_some_and(Relation::is_replicated) {
            1
        } else {
            payloads.len()
        };
        let sources: Vec<Option<&Batch>> = payloads[..n_src].iter().map(|o| o.as_ref()).collect();

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
            .map(RelayDest::PerWorker)
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
            RelayRoute::Broadcast => Ok(RelayDest::Broadcast(Box::new(op_relay_broadcast(&sources, &schema)))),
            RelayRoute::GroupKey(cols) => scatter(ScatterSpec::GroupKey(cols)),
            RelayRoute::JoinKey(slots) => scatter(ScatterSpec::JoinKey(slots)),
        };
        // The scatter key's own reason, not a restatement: it names the column
        // and the bound it missed.
        let dest = dest.map_err(|e| format!("view {view_id}: source {source_id} relay key: {e}"))?;

        // Encoded once and carried forward: `emit_relay_with_decision` runs the
        // same group again — twice more when a reclaim barrier forces a retry —
        // and every pass would otherwise rebuild these identical bytes.
        let view = wire::WireSchema::encoded(view_id, schema);

        // Size the group here, outside `sal_writer_excl`: the batches are in
        // hand, so the fit check under the lock is a comparison rather than a
        // sizing pass.
        let footprint = self.with_relay_group(&view, source_id, &dest, BACKFILL_DECISION_CONTINUE, |g| {
            self.sal.footprint(g)
        });

        Ok(RelayPrepared { view, source_id, dest, footprint })
    }

    /// Synchronous second half of a relay: writes the ExchangeRelay group to
    /// SAL and signals workers, stamping the round `decision` (a
    /// `BACKFILL_DECISION_*`) onto the relay's `seek_col_idx`. No awaits inside.
    ///
    /// The caller must exclude every other SAL writer, by either of the two means
    /// this codebase has: a steady tick relay holds `sal_writer_excl` across the
    /// call, while an exclusive round polls no task, so none can reach the SAL.
    pub(crate) fn emit_relay_with_decision(&self, prep: &RelayPrepared, decision: u64) -> Result<(), String> {
        self.with_relay_group(&prep.view, prep.source_id, &prep.dest, decision, |g| {
            self.write_group(g)
        })
        .map_err(|f| f.text)?;
        self.signal_all();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Distributed backfill of ONE view from `source_id`; `view_id` rides to
    /// the worker in the frame's `seek_pk`. Always view-scoped — the source may
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
    fn fan_out_backfill(&self, view_id: i64, source_id: i64) -> Result<(), String> {
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
            seek_pk: view_id as u128,
            ..Default::default()
        });
        self.exclusive_round("backfill relay", true, |t| {
            self.write_group(&DirectGroup {
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
    /// `order_by_view_deps` is the one owner of that ordering — a source view
    /// precedes every dependent that scans it, so an upstream hidden segment is
    /// filled before a downstream view reads it.
    ///
    /// A multi-source equi-join iterates every source: the first fills its trace
    /// (joining against the still-empty other trace emits nothing), and the rest
    /// join against it. A view that runs no exchange needs no cross-worker
    /// barrier and `fan_out_backfill` accommodates that — no relay arrives, so
    /// each worker stops on its own drain exhaustion — which is why one driver
    /// serves every shape.
    pub(crate) fn backfill_views_in_dep_order(&self, view_ids: &[i64]) -> Result<(), String> {
        let (dag, registry) = self.cat().dag_and_registry_mut();
        for vid in dag.order_by_view_deps(registry, view_ids) {
            let sources = dag.get_source_ids(registry, vid);
            for src in sources {
                self.fan_out_backfill(vid, src)
                    .map_err(|e| format!("view={vid} source={src}: {e}"))?;
            }
        }
        Ok(())
    }

    /// Synchronously drain one source's pending ticks in an exclusive round: emit a
    /// Tick, signal, and collect each worker's ACK while relaying its exchange
    /// dependents inline. The `handle_ddl_txn` caller holds the catalog write lock
    /// and the tick gate with the committer idle, so no other tick races this.
    /// Checkpointing is forced off: a tick carries no backfill pad, so a
    /// CHECKPOINT verdict would advance only the master's epoch and wedge the
    /// cluster (see `collect_exclusive`).
    ///
    /// Writes under no SAL mutex, so `fn` rather than `async fn` is what keeps its
    /// group out of `dispatch_scan_multi_fanout`'s sampled round: making it `async`
    /// lets the reactor interleave it and breaks that read-freshness contract.
    pub(crate) fn drain_tick_blocking(&self, source_id: i64) -> Result<(), String> {
        self.exclusive_round("view tick drain", false, |t| self.write_tick_group(source_id, t))
    }

    /// Point lookup of `pk`. A key-routed relation's rows sit on the worker the
    /// key hashes to, and a replicated one's on every worker, so one worker's
    /// single reply frame is handed back verbatim. A `Local` relation's rows sit on
    /// whichever worker produced them — a view key's group spans workers — so every
    /// worker is asked and their replies are merged.
    pub(crate) async fn fan_out_seek(
        &self,
        target_id: i64,
        pk: u128,
        seek_pk_extra: &[u8],
        client_version: u16,
    ) -> Result<SeekReply, WorkerFault> {
        let num_workers = self.num_workers();
        let schema = self.schema_desc_for(target_id);
        // Decode the wire pair to the OPK bytes (width-universal), then route off
        // the distribution prefix via the shared `worker_for_pk`. A Seek
        // always carries the full PK and the prefix ⊆ the PK, so a full-PK seek
        // of a key-routed relation pins exactly one worker. Hashing the native
        // value instead of the OPK bytes would misroute signed and compound PKs.
        let opk = gnitz_store::schema::key::seek_opk_bytes(&schema, pk, seek_pk_extra)
            .map_err(|e| format!("seek: table {target_id}: {e}"))?;
        let fanout = match schema.placement().is_key_routed() {
            true => Fanout::One(schema.worker_for_pk(opk.pk_bytes(), num_workers)),
            false => read_fanout(self, target_id),
        };
        let (mut slots, scan) = dispatch_scan_fanout(self, fanout, |targets| {
            self.write_group(&DirectGroup {
                template: wire::WireMsg {
                    target_id: target_id as u64,
                    // A unicast reply goes to the client verbatim, so it carries
                    // the version the client already HAS and the worker omits the
                    // block on a hit — where `handle_scan` stamps the one the
                    // client *will* have, its prelim frame having just sent it. A
                    // merge is re-encoded here, and negotiates on the way out.
                    flags: match fanout {
                        Fanout::One(_) => gnitz_wire::wire_flags_set_schema_version(0, client_version),
                        Fanout::Broadcast => 0,
                    },
                    seek_pk: pk,
                    seek_pk_extra,
                    ..Default::default()
                },
                targets,
                ..DirectGroup::new(SalMessageKind::Seek)
            })
        })
        .await?;
        match fanout {
            Fanout::One(worker) => {
                let slot = slots.pop().expect("unicast fan-out returns one slot");
                // A point seek's reply must fit one frame; a train would be
                // forwarded truncated, so reject it rather than silently drop the
                // remainder.
                expect_single_frame(&slot, worker, "seek")?;
                Ok(SeekReply::Frame(slot))
            }
            Fanout::Broadcast => merge_replies(slots, &scan, "seek", &schema)
                .await
                .map(|merged| SeekReply::Merged(merged.map(Box::new))),
        }
    }

    /// Fan out a SCAN to the workers `unicast` names and forward every response
    /// frame straight to the client, continuation chunks included. `Ok(false)` on
    /// a mid-stream client disconnect.
    ///
    /// A plain scan negotiates its schema block through `wire_flags`; a `ReadSpec`
    /// read carries its request blob in `seek_pk_extra`, or one per worker in `extras`.
    ///
    /// `forward_scan_slots` returns on the FIRST worker fault, decode error or
    /// client disconnect, without draining the doomed trains: the scan's lease
    /// drops on return and the drain releases every undrained frame at the ring
    /// boundary, so a still-streaming worker cannot wedge in
    /// `W2mWriter::send_msg`. The fault frame can therefore reach a client that
    /// already read earlier data frames; its reply accumulator discards those.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn fan_out_scan(
        &self,
        unicast: Fanout,
        target_id: i64,
        client_id: u64,
        peer: &Peer,
        kind: SalMessageKind,
        wire_flags: u64,
        seek_pk_extra: &[u8],
        extras: Option<&[Vec<u8>]>,
    ) -> Result<bool, WorkerFault> {
        let (slots, scan) = dispatch_scan_fanout(self, unicast, |targets| {
            self.write_group(&DirectGroup {
                template: wire::WireMsg {
                    target_id: target_id as u64,
                    client_id,
                    flags: wire_flags,
                    seek_pk_extra,
                    ..Default::default()
                },
                extras,
                targets,
                ..DirectGroup::new(kind)
            })
        })
        .await?;
        forward_scan_slots(peer, slots, &scan).await
    }

    /// Broadcast a DDL batch to every worker inside `scope`'s zone — one LSN
    /// across a DDL's broadcasts, so recovery groups them atomically. Publishes
    /// nothing and signals nobody; both are the scope's commit and the caller's.
    pub(crate) fn broadcast_ddl(&self, scope: &SalScope, target_id: i64, batch: &Batch) -> Result<(), WorkerFault> {
        let relation = self.wire_schema(target_id);
        scope.write(
            &DirectGroup {
                template: relation.frame(wire::WireMsg::default()),
                data: GroupData::Same(wire::WireData::Whole(Some(batch))),
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

    /// Write a Tick group for `tid`. Does NOT signal: the caller writes
    /// every group in the batch, then calls `signal_all` once — a signal is a
    /// wake, not framing, so batching it changes no outcome. No schema block:
    /// `handle_tick` looks the target's schema up in its own catalog.
    ///
    /// **The tick round is allocated here**, because this is the one writer of a
    /// tick group — anywhere else and the allocation would have to enumerate the
    /// paths that emit one. It also makes the idle-poll map complete for free:
    /// the round, the record of which views it reaches, and the group write are
    /// one await-free body, so a poll that sees the counter advanced also sees
    /// the round recorded.
    ///
    /// The round rides in the group header's `lsn`, free there because every
    /// command verb but the ephemeral flush round passes `0`, and a non-zero
    /// `lsn` alone does not make a group a zone on the replay walk.
    ///
    /// A failed emit **burns** its round rather than reusing it, since rounds
    /// must be strictly increasing. That gates nothing away: no rows exist at the
    /// burnt round, and the re-queued tid's next tick raises the map past it.
    pub(crate) fn write_tick_group(&self, tid: i64, targets: GroupTargets) -> Result<(), WorkerFault> {
        let round = self.tick_round.get() + 1;
        self.tick_round.set(round);
        self.record_delta_round(tid, round);
        self.write_group(&DirectGroup {
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
    /// promises, sampled by the read's own fan-out under `sal_writer_excl` so no
    /// tick group can land between the sample and the read group.
    pub(crate) fn last_tick_round(&self) -> u64 {
        self.tick_round.get()
    }

    /// The cursor tag a delta reply carries: `boot_nonce ^ splitmix64(view_id)`.
    ///
    /// One field answers two questions because they are the same question. A round
    /// number alone identifies nothing — it is meaningless across a restart,
    /// because the counter starts over, and meaningless across a
    /// `DROP VIEW v; CREATE VIEW v …`, because the recreated `v` takes a fresh id
    /// whose rounds are numbered from the same global counter as the old one's. A
    /// client that does not recognise the tag discards its copy and re-reads at
    /// `after_tick = 0`, which is the right answer to both.
    ///
    /// A per-view first round recorded on the master is the obvious alternative and
    /// cannot be made exact: a subscriber that bootstraps a view immediately after
    /// its creation is handed the counter's current value as `T` — the same value a
    /// cursor from the dropped view can hold — so any threshold that refuses the
    /// stale cursor also refuses the fresh one, and the client loops. The tag has no
    /// such overlap because it compares identities, not magnitudes.
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
        for w in 0..self.num_workers() {
            let pid = self.worker_pids.borrow()[w];
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
    pub(crate) fn shutdown_workers(&self) {
        // No schema block: the worker's `Shutdown` arm takes no arguments. A
        // refusal would hang the `waitpid` below, so it must not vanish.
        if let Err(e) = self.write_group(&DirectGroup::new(SalMessageKind::Shutdown)) {
            gnitz_warn!("SAL refused the Shutdown broadcast: {}; the worker reap will block", e);
        }
        self.signal_all();
        for w in 0..self.num_workers() {
            let pid = std::mem::replace(&mut self.worker_pids.borrow_mut()[w], 0);
            if pid > 0 {
                let mut status: i32 = 0;
                let _ = retry_eintr(|| unsafe { libc::waitpid(pid, &mut status, 0) });
            }
        }
    }

    /// Lay one push batch out as a SAL group inside `scope`, worker `w` answering
    /// on `base + w`. `recoverable` puts it inside the zone; a stream's rows ride
    /// outside. The committer commits the scope, signals and awaits the ACKs.
    pub(crate) fn write_commit_group(
        &self,
        scope: &SalScope,
        target_id: i64,
        batch: &Batch,
        base: u64,
        recoverable: bool,
    ) -> Result<(), WorkerFault> {
        let relation = self.wire_schema(target_id);
        // Identical scatter for both routings; only the per-worker index fill
        // differs (full broadcast vs PK-partitioned). One `with_group` call site
        // keeps the atomic-zone framing, LSN, ACK accounting, and the committer's
        // single `fdatasync` shared between them.
        let group = DirectGroup {
            targets: GroupTargets::All(base),
            ..DirectGroup::new(SalMessageKind::Push)
        };
        // A replicated relation broadcasts: the whole batch lands in every
        // worker's ingest + SAL slot, so each worker durably logs the full table
        // and enforces uniqueness against its identical full copy.
        with_commit_indices(batch, relation.descriptor(), self.num_workers(), |worker_indices| {
            with_group(batch, worker_indices, &relation, group, |g| scope.write(g, recoverable))
        })
    }

    /// Write a checkpoint flush group ([`SalMessageKind::Flush`] base round or
    /// [`SalMessageKind::FlushEph`] ephemeral round). Does NOT sync/signal. Caller signals,
    /// then either awaits the replies `targets` names or reads the rings. `lsn` is supplied by the
    /// caller — the ephemeral round passes the checkpoint generation there, which
    /// workers read to stamp view manifests (the base round passes 0).
    ///
    /// Every worker gets a bare control block: `handle_flush_all` takes neither a
    /// schema nor a batch, and reads the generation the worker latched into its
    /// catalog off this round's header.
    pub(crate) fn write_checkpoint_group(
        &self,
        lsn: u64,
        kind: SalMessageKind,
        targets: GroupTargets,
    ) -> Result<(), WorkerFault> {
        self.note_flush_round(lsn, kind);
        self.write_group(&DirectGroup { lsn, targets, ..DirectGroup::new(kind) })
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
    pub(crate) fn checkpoint_post_ack(&self) -> Result<(), String> {
        let cat = self.cat();
        cat.flush_all_system_tables()?;
        // Every worker ACKed the FLUSH, so each has applied every DdlSync written
        // before it; the flush above made every applied DROP durable.
        cat.reclaim_orphan_dirs();
        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    /// Boot-end checkpoint, as exclusive rounds: record the
    /// launched topology, then reclaim and re-stamp. The drain set is empty —
    /// recovery already drained everything and no pushes are admitted yet (the
    /// socket is not open), so `pending_deltas` is empty. Freshly backfilled
    /// views are durably checkpointed before the socket opens.
    pub(crate) fn boot_checkpoint(&self, worker_count: u32) -> Result<(), String> {
        // The topology row's durability rides the gen bump's system-table flush
        // (both are `_sequences` rows), so a manifest stamped at a generation
        // implies the topology row for that layout is durable.
        self.cat()
            .record_topology(worker_count)
            .map_err(|e| format!("topology record failed: {e}"))?;
        self.reclaim_base()?;
        self.restamp_derived(&[])
    }

    /// Accessor for the committer. True when a checkpoint is warranted — see
    /// [`SalWriter::needs_checkpoint`].
    pub(crate) fn sal_needs_checkpoint(&self) -> bool {
        self.sal.needs_checkpoint()
    }

    /// Accessor for the committer and the watchdog. True when less than the
    /// reclaim margin is free, so the next relay-sized group would be refused.
    pub(crate) fn sal_space_low(&self) -> bool {
        self.sal.below_reclaim_margin()
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
