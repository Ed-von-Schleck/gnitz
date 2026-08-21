//! Master SAL dispatcher: the `MasterDispatcher` core — SAL group/broadcast
//! writes, worker signalling + ack collection, relay emit, the fan-out family
//! (seek / scan / index / gather), checkpointing, and the scan-fanout helpers.

use super::*;
use crate::runtime::sal::{FLAG_ZONE_START, MAX_WORKERS};
use gnitz_engine::foundation::fault::Seam;

/// Yields the set bit positions of a worker mask, lowest first.
struct BitIter(u64);

impl Iterator for BitIter {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        if self.0 == 0 {
            return None;
        }
        let w = self.0.trailing_zeros() as usize;
        self.0 &= self.0 - 1;
        Some(w)
    }
}

/// Route a ScanSpec read, as the [`Fanout`] every fan-out helper speaks.
///
/// A replicated relation is a full identical copy on every worker, so worker 0
/// answers it alone; otherwise the read goes to the single worker owning its PK
/// range when [`confined_worker`] can prove one, and is broadcast when it cannot.
pub(crate) fn scan_spec_route(disp: &MasterDispatcher, target_id: i64, spec: &[u8]) -> Fanout {
    match replicated_unicast(disp, target_id) {
        Fanout::One(w) => Fanout::One(w),
        Fanout::Broadcast => confined_worker(disp, target_id, spec).map_or(Fanout::Broadcast, Fanout::One),
    }
}

/// The one worker that can answer this read, or `None` when nothing proves one —
/// a relation whose rows are not key-placed, a non-`PkRange` bound, a forged
/// descriptor (left for the worker to reject at the trust boundary), or a range
/// spanning workers. Takes the already-unpacked `ReadSpec`, not the whole
/// `seek_pk_extra` blob: `handle_scan_spec` unpacks it once for the gate and the
/// routing both. An `IndexRange` bound is never confined: a secondary index
/// is one unpartitioned table per worker, so nothing derives its owner from the
/// key.
///
/// Only a key-routed relation names an owner, and the catalog — not the master's
/// own stores — answers whether a relation is: the master holds no store at all
/// after the fork, so its own handles cannot speak for the workers'.
fn confined_worker(disp: &MasterDispatcher, target_id: i64, spec: &[u8]) -> Option<usize> {
    let cat = disp.cat();
    let schema = cat.get_schema_desc(target_id)?;
    // For anything but `Keyed` no key names an owner, so nothing can be routed
    // against a placement the write path scatters differently.
    if !schema.placement().is_key_routed() {
        return None;
    }
    let desc = gnitz_wire::peek_pk_range(spec)?;
    gnitz_engine::catalog::scan_spec_worker(&schema, &desc, disp.num_workers)
}

/// Timeout for the synchronous `W2mReceiver::wait_for` fallback in the two
/// reactor-driven-but-sometimes-parked collect loops below.
///
/// `wait_for` returns the instant a worker bumps `reader_seq` *as long as the
/// futex wake reaches it*. At boot (no reactor) and in the steady state nothing
/// competes for that wake, so the loop is woken promptly and this value is just
/// an unhit ceiling. But the reactor-parked CREATE-VIEW backfill runs these
/// loops while the reactor's `FUTEX_WAITV` SQE is still armed on the same
/// `reader_seq` words: the kernel can deliver a worker's wake to that op (whose
/// CQE then sits unprocessed — the reactor is parked here) instead of to this
/// `futex_wait`, which would otherwise sleep the full timeout while the reply
/// already sits in the ring. A short timeout caps that stolen-wake stall (the
/// loop re-`try_read`s every iteration and finds the reply) at a few ms instead
/// of ~1 s, with negligible extra polling on the never-stalled paths.
const W2M_SYNC_WAIT_MS: i32 = 10;

/// `GNITZ_INJECT_RELAY_SPACE_LOW` / `GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW`:
/// report SAL relay space as low — for the steady-state relay until the next
/// checkpoint bumps the epoch, for the backfill on every non-stop round. Lets
/// tests drive the SAL reclamation protocol (worker re-epoch, master
/// `checkpoint_reset`, epoch advancing) over a small table that would never
/// approach the 1 GiB mmap.
static RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_RELAY_SPACE_LOW");
static BACKFILL_RELAY_SPACE_LOW: Seam = Seam::new("GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW");

/// `GNITZ_INJECT_TICK_EMIT_ERROR=<table>`: fail the named table's next tick
/// emit, once, as a full SAL would.
static TICK_EMIT_ERROR: Seam = Seam::new("GNITZ_INJECT_TICK_EMIT_ERROR");

/// The per-worker request ids a *synchronous* collect writes into its group.
/// `collect_acks` / `collect_acks_and_relay` read the W2M rings directly and
/// never route by req_id, so zeros are correct rather than merely unused.
const SYNC_COLLECT_REQ_IDS: [u64; MAX_WORKERS] = [0; MAX_WORKERS];

/// A `u64` from the OS entropy pool, mixed into every delta reply's cursor tag.
///
/// From the OS rather than from a seeded generator because two back-to-back
/// boots must not collide: a client holding a cursor across a restart has to see
/// a tag it does not recognise, discard its copy and re-read at `after_tick = 0`.
/// A short read or an unavailable pool falls back to the boot's wall clock and
/// pid, which is weaker but still distinguishes two boots of one machine.
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

impl MasterDispatcher {
    pub fn new(
        num_workers: usize,
        worker_pids: Vec<i32>,
        catalog: *mut CatalogEngine,
        sal: SalWriter,
        w2m: Rc<W2mReceiver>,
        m2w_efds: Vec<i32>,
    ) -> Self {
        debug_assert_eq!(m2w_efds.len(), num_workers, "one wakeup eventfd per worker",);
        MasterDispatcher {
            num_workers,
            worker_pids: RefCell::new(worker_pids),
            sal,
            m2w_efds,
            w2m,
            catalog,
            unique_filters: RefCell::new(FxHashMap::default()),
            check_batch_pool: RefCell::new(FxHashMap::default()),
            // Seeded from the recovered generation: `recovery_start_generation_bump`
            // has already pushed the durable one past whatever the last completed
            // checkpoint stamped, so every base round below must bump again first.
            // The unit tests that construct a null catalog never emit a round.
            last_ephemeral_gen: Cell::new(if catalog.is_null() {
                0
            } else {
                unsafe { (*catalog).durable_generation() }
            }),
            tick_round: Cell::new(1),
            last_delta_round: RefCell::new(FxHashMap::default()),
            boot_nonce: boot_nonce(),
        }
    }

    /// The catalog behind the raw pointer the dispatcher was constructed with.
    /// Every `&self` method reaches the catalog through here, so the pointer is
    /// dereferenced in one place. Same accessor the two sibling owners of this
    /// pointer have (`executor::Shared::cat`, `WorkerProcess::cat`): the master
    /// reactor is single-threaded and the catalog outlives the dispatcher.
    #[allow(clippy::mut_from_ref)]
    pub(super) fn cat(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// Boot-time SAL reset: sentinel prefix cleared, cursor 0, `epoch` (the
    /// recovered walk epoch plus one, so epochs stay monotone across boots and a
    /// previous boot's leftover can never be read as current). Sole caller is
    /// `server_main` after all workers finish recovery.
    pub fn reset_sal(&self, epoch: u32) {
        self.sal.boot_reset(epoch);
    }

    /// Return the schema descriptor, a cached prebuilt schema wire block,
    /// and the derived `(wire_safe, wire_row_fixed_stride)` for `target_id`.
    /// The block is built lazily on first call and stored in the catalog
    /// cache; it is invalidated alongside col_names whenever DDL modifies
    /// the table. Used by SAL write paths (commit/tick/broadcast) to skip
    /// per-call `build_schema_wire_block` allocations and per-column
    /// iteration in `scatter_wire_group`.
    pub(super) fn cached_schema_block(&self, target_id: i64) -> (SchemaDescriptor, Rc<Vec<u8>>, bool, u32) {
        let schema = self.schema_desc_for(target_id);
        let cat = self.cat();
        let e = crate::runtime::wire::get_or_build_schema_wire_block(cat, target_id, &schema);
        (schema, e.block, e.wire_safe, e.wire_row_fixed_stride)
    }

    pub(super) fn pool_pop_batch(&self, slot: super::preflight::PoolSlot) -> Option<Batch> {
        self.check_batch_pool.borrow_mut().get_mut(&slot).and_then(|v| v.pop())
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Write one command group with per-worker request ids: no rows and no
    /// schema block, so each worker's slot is a bare control block. Every
    /// command verb's worker arm resolves the schema it needs from its own
    /// catalog, so no block is sent.
    ///
    /// `seek_pk`/`seek_col_idx` carry the verb's two scalar operands and
    /// `seek_pk_extra` its verbatim payload. `lsn` is 0 for every verb — a lost
    /// command needs no crash recovery — except the checkpoint rounds, where
    /// FlushEph's lsn IS the checkpoint generation.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_command_group(
        &self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: Fanout,
        client_id: u64,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        self.sal.write_group_direct(
            &DirectGroup {
                template: wire::WireMsg {
                    target_id: target_id as u64,
                    client_id,
                    flags: wire_flags,
                    seek_pk,
                    seek_col_idx,
                    seek_pk_extra,
                    ..Default::default()
                },
                worker_data: &[],
                req_ids,
                unicast_worker: match unicast_worker {
                    Fanout::Broadcast => None,
                    Fanout::One(w) => Some(w),
                },
            },
            lsn,
            sal_flags,
        )
    }

    /// One group carrying rows, broadcast with per-worker request ids. The two
    /// data-bearing fan-outs: the exchange relay (whose schema block is what
    /// stamps `Batch.schema` on the worker side) and the `FLAG_HAS_PK` unique
    /// check.
    ///
    /// The schema block carries no column names: `decode_schema_block` parses
    /// only the col_idx / type / flags regions, and these groups never leave the
    /// master→worker SAL.
    fn data_group<'a>(
        target_id: i64,
        worker_data: &'a [wire::WireData<'a>],
        schema: &'a SchemaDescriptor,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &'a [u64],
    ) -> DirectGroup<'a> {
        DirectGroup {
            template: wire::WireMsg {
                target_id: target_id as u64,
                seek_pk,
                seek_col_idx,
                schema: Some(schema),
                ..Default::default()
            },
            worker_data,
            req_ids,
            unicast_worker: None,
        }
    }

    /// Emit a [`data_group`](Self::data_group). LSN 0 — both verbs are
    /// command-scoped; durable writers (`scatter_wire_group`,
    /// `write_broadcast_direct`, the commit sentinel) carry caller-supplied LSNs
    /// on their own paths.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_data_group(
        &self,
        target_id: i64,
        sal_flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
    ) -> Result<(), String> {
        let worker_data: Vec<wire::WireData> = worker_batches.iter().map(|b| wire::WireData::Whole(*b)).collect();
        let group = Self::data_group(target_id, &worker_data, schema, seek_pk, seek_col_idx, req_ids);
        self.sal.write_group_direct(&group, 0, sal_flags)
    }

    /// Write one group whose rows are PK-partitioned across the workers: each
    /// worker's slot carries only the rows it stores. The scatter sibling of
    /// `write_data_group`, and the only shape the probe/gather paths need — the
    /// durable commit scatter (`write_commit_group`) carries a schema block and
    /// wire props of its own, so it calls `scatter_wire_group` directly.
    pub(super) fn write_scatter_group(
        &self,
        batch: &Batch,
        schema: &SchemaDescriptor,
        target_id: i64,
        sal_flags: u32,
        seek_col_idx: u64,
        req_ids: &[u64],
    ) -> Result<(), String> {
        // No reentrancy: the closure has no `.await`, so the SCATTER_INDICES
        // borrow is released before the next caller needs it.
        with_worker_indices(batch, schema, self.num_workers, |worker_indices| {
            self.sal.scatter_wire_group(
                batch,
                worker_indices,
                schema,
                target_id as u32,
                0,
                sal_flags,
                0,
                seek_col_idx,
                req_ids,
                None,
                None,
            )
        })
    }

    /// Write one scan group. The worker's `scan_family` resolves and returns the
    /// relation's schema from its own catalog, so the group carries no rows and
    /// no schema block.
    ///
    /// A plain scan passes `sal_flags = 0` and the schema-version bits in
    /// `wire_flags` (plus `FLAG_SCAN_FIFO_REPLY` for a multi-scan). A ScanSpec
    /// (`ReadSpec`) read passes `FLAG_SCAN_SPEC` and the client's bundled spec +
    /// reply-schema blob **verbatim** in `seek_pk_extra`, with `wire_flags = 0`:
    /// its reply carries no schema block, so there is no version to negotiate.
    /// The master reads only the bound header out of the spec, to route
    /// (`confined_worker`); the worker is the sole `ReadSpec`/OPK decoder.
    ///
    /// `seek_pk` carries the last tick round the master had emitted when it wrote
    /// this group, which is what cuts a delta read's interval above. It was
    /// hard-coded `0` before, and read by nothing in either scan arm, so the two
    /// callers with nothing to say there still pass `0`.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_scan_group(
        &self,
        target_id: i64,
        sal_flags: u32,
        wire_flags: u64,
        req_ids: &[u64],
        unicast_worker: Fanout,
        client_id: u64,
        seek_pk_extra: &[u8],
        seek_pk: u128,
    ) -> Result<(), String> {
        self.write_command_group(
            target_id,
            0,
            sal_flags,
            wire_flags,
            seek_pk,
            0,
            req_ids,
            unicast_worker,
            client_id,
            seek_pk_extra,
        )
    }

    /// Replicate one dataless control slot to every worker, then signal (no
    /// fdatasync). `lsn` is the caller's: a DDL zone LSN (`broadcast_ddl`), the
    /// checkpoint generation (FlushEph round), or 0 for command-only groups.
    ///
    /// A schema block built here carries no column names: `decode_schema_block`
    /// parses only the col_idx / type / flags regions, and the SAL never leaves
    /// the master→worker path.
    fn send_broadcast(
        &self,
        target_id: i64,
        lsn: u64,
        flags: u32,
        schema: Option<&SchemaDescriptor>,
        seek_pk: u128,
    ) -> Result<(), String> {
        self.sal.write_group_direct(
            &DirectGroup {
                template: wire::WireMsg {
                    target_id: target_id as u64,
                    seek_pk,
                    schema,
                    ..Default::default()
                },
                worker_data: &[],
                req_ids: &SYNC_COLLECT_REQ_IDS[..self.num_workers],
                unicast_worker: None,
            },
            lsn,
            flags,
        )?;
        self.signal_all();
        Ok(())
    }

    /// Wake every worker: they see the SAL entry through the mapping's Acquire
    /// size prefix regardless, so this only ends a park.
    pub(crate) fn signal_all(&self) {
        for &efd in &self.m2w_efds {
            crate::runtime::posix::eventfd_signal(efd);
        }
    }
    pub(super) fn signal_one(&self, worker: usize) {
        crate::runtime::posix::eventfd_signal(self.m2w_efds[worker]);
    }

    pub(crate) fn sal_fd(&self) -> i32 {
        self.sal.sal_fd()
    }

    /// A handle on the W2M receiver for the reactor, which reads the same rings.
    /// Both sides hold a clone: the dispatcher's synchronous collect helpers stay
    /// usable during the reactor-parked stop-the-world CREATE-VIEW backfill,
    /// where the reactor (the only other reader) is parked inside that call.
    pub fn w2m_receiver(&self) -> Rc<W2mReceiver> {
        Rc::clone(&self.w2m)
    }

    /// Liveness gate for the pre-reactor bootstrap wait loops. A crashed worker
    /// (panic / OOM-kill / SIGKILL) leaves its `reader_seq` frozen, so `wait_for`
    /// only ever times out and the wait loop would spin forever. Callers probe
    /// before parking and surface the dead worker as an error instead of hanging
    /// the master; `context` names the bootstrap phase ("before completing
    /// recovery sync", "during backfill relay"). On these paths workers stay
    /// alive after acking, so a reaped worker has not published the awaited
    /// frame — no ack is lost.
    fn fail_if_worker_dead(&self, context: &str) -> Result<(), String> {
        let dead = self.check_workers();
        if dead >= 0 {
            return Err(format!("worker {dead} exited {context}"));
        }
        Ok(())
    }

    /// Wait for one ACK from each worker, driving each ring synchronously via
    /// `W2mReceiver::wait_for` (FUTEX_WAIT on `reader_seq`). The tail-chasing
    /// ring self-maintains — no reset needed.
    ///
    /// Two callers, matching `collect_acks_and_relay`: boot, before the reactor
    /// is up, and the reactor-parked stop-the-world CREATE VIEW window (a
    /// backfill whose `checkpoint_before_backfill` fires reaches this
    /// post-handoff).
    pub fn collect_acks(&self) -> Result<(), String> {
        for w in 0..self.num_workers {
            loop {
                match self.w2m.try_read(w) {
                    Some(decoded) => {
                        if let Some(e) = worker_error(w, "recovery sync", &decoded.control) {
                            return Err(e.text);
                        }
                        break;
                    }
                    None => {
                        self.fail_if_worker_dead("before completing recovery sync")?;
                        let _ = self.w2m.wait_for(w, W2M_SYNC_WAIT_MS);
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Collect ACKs from all workers, relaying exchange messages inline by
    /// walking each ring serially with `W2mReceiver::wait_for` and a private
    /// `ExchangeAccumulator`. Two callers, both holding SAL-writer exclusivity
    /// with no concurrent relay to service:
    /// - **Boot** (`fan_out_backfill`), before the reactor is up.
    /// - **Live CREATE VIEW**, the reactor-parked stop-the-world DDL window:
    ///   `handle_ddl_txn` holds the catalog write lock (so the async
    ///   `relay_loop`, which would take the read lock, cannot run and cannot
    ///   deadlock) and drives the backfill/drain inline.
    ///
    /// `checkpoint_allowed`: a backfill may stamp CHECKPOINT to reclaim SAL
    /// space mid-stream (workers re-epoch inline). A drain TICK must NOT —
    /// it carries no backfill pad, so a CHECKPOINT would advance the master
    /// epoch while workers stay on the old one and wedge the cluster; pass
    /// `false` to force CONTINUE.
    fn collect_acks_and_relay(&self, checkpoint_allowed: bool) -> Result<(), String> {
        let nw = self.num_workers;
        // One bit per worker still owing its ACK; `MAX_WORKERS` is 64.
        let mut pending_mask: u64 = if nw == 64 { u64::MAX } else { (1u64 << nw) - 1 };
        let mut acc = crate::runtime::reactor::ExchangeAccumulator::new(nw);
        // Armed when a round is stamped CHECKPOINT; the actual SAL reset is
        // deferred to the next round barrier (see the decision block below).
        let mut pending_reset = false;

        while pending_mask != 0 {
            // One full pass over the still-pending workers per iteration. If a
            // pass makes no progress, wait on all of them. Exchange replies from
            // any worker may trigger further SAL writes + replies, so we loop
            // broadly.
            let mut progressed = false;
            for w in BitIter(pending_mask) {
                let Some(decoded) = self.w2m.try_read(w) else {
                    continue;
                };
                progressed = true;
                if decoded.control.flags & FLAG_EXCHANGE != 0 {
                    if let Some(relay) = acc.process(w, decoded) {
                        // A round just completed. If a prior round was stamped
                        // CHECKPOINT, every worker has now consumed that relay
                        // — a worker issues its next round only after consuming
                        // the prior relay and bumping its read epoch inline, so
                        // this round's `num_workers` reports prove it. Reclaim
                        // the SAL write side NOW, before writing this round, so
                        // this round lands at write_cursor 0 in the new epoch the
                        // workers already expect. Direct checkpoint_reset only —
                        // never checkpoint_post_ack / FLAG_FLUSH, which a
                        // mid-backfill flush would race, orphaning unconsumed
                        // backfill groups and hanging boot.
                        if pending_reset {
                            self.sal.checkpoint_reset();
                            pending_reset = false;
                        }
                        // Decide this round's collective verdict, stamped onto
                        // its relay. Stop takes precedence: an all-pad round ends
                        // the backfill and its leftover SAL is reclaimed by the
                        // normal post-backfill checkpoint. Otherwise, when space
                        // runs short against this round's own size, stamp
                        // CHECKPOINT (continue + tell workers to re-epoch inline)
                        // and arm the reset for the next round barrier. That
                        // cannot rescue this round — the reset only lands at the
                        // next barrier, so this round is written at the current
                        // cursor either way.
                        let all_pad = relay.all_pad;
                        let prep = self.prepare_relay(relay)?;
                        let decision = if all_pad {
                            BACKFILL_DECISION_STOP
                        } else if checkpoint_allowed
                            && (!self.sal_has_room_for(prep.footprint) || BACKFILL_RELAY_SPACE_LOW.armed())
                        {
                            pending_reset = true;
                            BACKFILL_DECISION_CHECKPOINT
                        } else {
                            BACKFILL_DECISION_CONTINUE
                        };
                        self.emit_relay_with_decision(&prep, decision)?;
                    }
                } else {
                    if let Some(e) = worker_error(w, "backfill relay", &decoded.control) {
                        return Err(e.text);
                    }
                    pending_mask &= !(1u64 << w);
                }
            }
            if !progressed {
                self.fail_if_worker_dead("during backfill relay")?;
                // Wait on ALL still-pending workers at once: any could be the next
                // to publish, and a single-word wait would miss a wake on a
                // different worker's reader_seq.
                let mut pending = [0usize; MAX_WORKERS];
                let mut np = 0;
                for w in BitIter(pending_mask) {
                    pending[np] = w;
                    np += 1;
                }
                if np > 0 {
                    let _ = self.w2m.wait_any(&pending[..np], W2M_SYNC_WAIT_MS);
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// Reclaim before a backfill whose first round could be arbitrarily large.
    /// A backfill round cannot reclaim mid-flight — the reset must reach the
    /// workers stamped on the *previous* round's relay, so round one gets
    /// whatever the cursor happens to leave, and starting from a reclaimed SAL
    /// is the only lever there is. An idle server past neither line pays
    /// nothing.
    fn checkpoint_before_backfill(&self) -> Result<(), String> {
        if self.sal.cursor() > self.relay_margin() || self.sal.needs_checkpoint() {
            self.reclaim_base()?;
        }
        Ok(())
    }

    /// Invariant: callers must live on a path that owns SAL checkpoint
    /// exclusivity. Today that is the bootstrap backfill and boot checkpoint, and
    /// the reactor-parked stop-the-world CREATE-VIEW window (`handle_ddl_txn`
    /// holds the catalog write lock with the committer proven idle and the
    /// reactor parked, so no other SAL writer exists — the same exclusivity boot
    /// has). The *async* fan-out / tick / steady-state DDL paths must NOT call
    /// this — a concurrent FLAG_FLUSH races the committer's own and orphans SAL
    /// writes straddling `sal.checkpoint_reset`.
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
    /// Half a checkpoint on its own. Both callers pair it with `restamp_derived`
    /// in the same reactor-parked span.
    fn reclaim_base(&self) -> Result<u64, String> {
        let gen = self.bump_checkpoint_generation();
        self.sync_round(0, FLAG_FLUSH)?;
        Ok(gen)
    }

    /// Re-stamp the derived state a `reclaim_base` invalidated, inside the
    /// caller's reactor-parked window: tick every source carrying buffered deltas
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
        self.sync_round(self.cat().durable_generation(), FLAG_FLUSH_EPH)
    }

    /// One synchronous round (pre-reactor W2M path, or a reactor-parked window):
    /// emit the flush group, block for every worker's ACK, finalize.
    /// A `FLAG_FLUSH_EPH` round's `lsn` IS the checkpoint generation (workers
    /// latch it via `set_committed_generation`); the base round passes 0.
    fn sync_round(&self, lsn: u64, flags: u32) -> Result<(), String> {
        self.note_flush_round(lsn, flags);
        // No schema block: `handle_flush_all` takes neither a schema nor a batch.
        self.send_broadcast(0, lsn, flags, None, 0)?;
        self.collect_acks()?;
        self.checkpoint_post_ack()
    }

    /// Check one emitted round against the ordering every base publish depends
    /// on: **no durable base-shard advance without a prior durable generation
    /// bump**, and no second base round once an ephemeral round has re-stamped
    /// derived state at the current generation.
    ///
    /// Both master-side emitters funnel through here (`sync_round` and
    /// `write_checkpoint_group`), so the rule is checked in one place rather
    /// than left as an obligation on each call site. The committer's
    /// reclaim-only base round inside `await_servicing` rides step 0's bump
    /// without one of its own — that is the "no intervening ephemeral round"
    /// clause, encoded rather than excepted.
    fn note_flush_round(&self, lsn: u64, flags: u32) {
        let durable = self.cat().durable_generation();
        if flags & FLAG_FLUSH_EPH != 0 {
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

    fn seam_armed_epoch() -> &'static std::sync::atomic::AtomicU32 {
        static ARMED: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(u32::MAX);
        &ARMED
    }

    /// Raw SAL relay-space threshold: at least 1/8 of the mmap still free.
    /// Seam-free — the boot backfill relay checks this directly because it
    /// must keep failing-on-low-space without observing the relay_loop test
    /// seam (which would spuriously fail an in-progress backfill). The
    /// watchdog's reclaim trigger reads it for the same reason: a timer
    /// honouring the seam would checkpoint the armed epoch away before
    /// `relay_loop` reached its low-space branch.
    pub(crate) fn sal_relay_space_ok_raw(&self) -> bool {
        self.sal_has_room_for(0)
    }

    /// The proactive reclaim watermark: 1/8 of the mapping. A relay is refused
    /// below it even when it would fit, so the reclaim happens on the relay that
    /// still has room to spare rather than on the one that has run out.
    fn relay_margin(&self) -> u64 {
        self.sal.mmap_size() >> 3
    }

    /// Free SAL bytes, against both the reclaim watermark and a caller's own
    /// requirement. `need` 0 asks about the watermark alone.
    fn sal_has_room_for(&self, need: usize) -> bool {
        let free = self.sal.mmap_size() - self.sal.cursor();
        free >= std::cmp::max(self.relay_margin(), need as u64)
    }

    /// The per-worker slots a relay emits.
    fn relay_data<'a>(&self, dest: &'a RelayDest) -> Vec<wire::WireData<'a>> {
        match dest {
            RelayDest::PerWorker(batches) => batches
                .iter()
                .map(|b| wire::WireData::Whole((b.count > 0).then_some(b)))
                .collect(),
            RelayDest::Broadcast(b) => {
                vec![wire::WireData::Whole((b.count > 0).then_some(&**b)); self.num_workers]
            }
        }
    }

    /// The SAL group an exchange relay writes. `prepare_relay` sizes it and
    /// `emit_relay_with_decision` emits it, so the bytes checked are the bytes
    /// written — `decision` is the only field that differs between the two, and
    /// `WireMsg::size` does not read it.
    ///
    /// `seek_pk` echoes `source_id` back so the worker's `do_exchange_wait` can
    /// match on (view_id, source_id). Without it, a multi-source view (join over
    /// 2+ tables) can deliver the wrong source's relay to a waiting exchange and
    /// the worker demuxes against the wrong sharding columns.
    fn relay_group<'a>(
        &self,
        view_id: i64,
        source_id: i64,
        schema: &'a SchemaDescriptor,
        worker_data: &'a [wire::WireData<'a>],
        decision: u64,
    ) -> DirectGroup<'a> {
        Self::data_group(
            view_id,
            worker_data,
            schema,
            source_id as u128,
            decision,
            &SYNC_COLLECT_REQ_IDS[..self.num_workers],
        )
    }

    /// Whether a group of `need` bytes fits the SAL, and if not, whether a
    /// checkpoint could make it fit.
    pub(crate) fn sal_fit(&self, need: usize) -> SalFit {
        self.sal.fit(need)
    }

    /// True when the SAL is above the reclaim watermark. Checked *before*
    /// consuming a relay so a low-space condition can be resolved (checkpoint)
    /// rather than silently discarding the relay and deadlocking blocked
    /// workers. While the debug seam is armed, reports low until the next
    /// checkpoint bumps the SAL epoch.
    pub(crate) fn sal_has_relay_space(&self) -> bool {
        if RELAY_SPACE_LOW.armed()
            && Self::seam_armed_epoch().load(std::sync::atomic::Ordering::Relaxed) == self.sal.epoch()
        {
            return false;
        }
        self.sal_relay_space_ok_raw()
    }

    /// relay_loop's variant: the first call arms the seam at the current epoch
    /// (one-shot: the CAS from the u32::MAX sentinel succeeds once per process),
    /// then defers to sal_has_relay_space() so relay_loop and the committer see
    /// the same verdict until a checkpoint bumps the epoch and disarms it.
    pub(crate) fn sal_has_relay_space_arming(&self) -> bool {
        if RELAY_SPACE_LOW.armed() {
            let _ = Self::seam_armed_epoch().compare_exchange(
                u32::MAX,
                self.sal.epoch(),
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            );
        }
        self.sal_has_relay_space()
    }

    /// CPU-only first half of exchange relay: looks up shard columns via
    /// the catalog DAG, scatters the payloads into per-worker batches, and
    /// collects column names. No SAL write yet — `relay_loop` runs this
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
        // scattering all W would route each row to its owner W times for
        // `consolidate_exchanged` to sum into one row at weight W. Take worker
        // 0's payload alone — the same single-sourcing the read gather applies
        // (`replicated_unicast`). Dropping the copies rather than clamping keeps
        // a genuine multiplicity in the source intact. A unary side relays under
        // `source_id == 0`, which names no relation — the guard skips a lookup
        // that cannot hit, and such a view needs no rule anyway: one whose
        // sources are all replicated is itself stamped replicated and computes
        // locally without ever reaching an exchange.
        let n_src = if source_id > 0 && cat.dag().relation_is_replicated(source_id) {
            1
        } else {
            payloads.len()
        };
        let sources: Vec<Option<&Batch>> = payloads[..n_src].iter().map(|o| o.as_ref()).collect();

        // A join-shard scatter (cols from a reindex chain) must route by the
        // reindex key so a row lands on the worker that owns its `_join_pk`
        // partition; a GROUP BY / set-op exchange scatter routes by the group
        // key (consistent with op_reduce's output PK). See `RouteMode`.
        // A join-shard scatter carries a per-slot promotion target; a GROUP BY /
        // set-op scatter has none, and only the join arm ever consults one.
        let meta = cat.dag_mut().view_meta(view_id);
        // A source whose scan feeds several distinct reindex keys has no single
        // pack key: routing by any one of them, or by the view's `shard_cols`,
        // would put the delta on a worker the trace side never stored under, and
        // the join would silently drop matches. Refuse the round instead.
        if let Some(None) = meta.join_shard_map.get(&source_id) {
            return Err(format!(
                "view {view_id}: source {source_id} feeds several distinct reindex keys; no single \
                 scatter key co-partitions it"
            ));
        }
        let join_key = (source_id > 0)
            .then(|| meta.join_shard_map.get(&source_id))
            .flatten()
            .and_then(|k| k.as_ref())
            .filter(|k| !k.cols.is_empty());
        let is_join = join_key.is_some();
        let shard_cols: &[u32] = match join_key {
            Some(k) => &k.cols,
            // A view with no `ExchangeShard` shards on `∅` — every row to
            // partition 0's owner — the same route a global aggregate takes.
            None => meta.shard_cols.as_deref().unwrap_or(&[]),
        };

        // A range-join INPUT relay (source_id > 0, is_join over a DeltaTraceRange
        // view): `view_range_join_n_eq` reads the equality-conjunct count straight
        // off the join node. The trace-side reindex key is [eq cols…, range col]
        // (len n_eq + 1). A band join (n_eq ≥ 1) scatters by the eq PREFIX — route
        // by the first n_eq slots, dropping the trailing range slot, so equal
        // eq-values co-partition both sides and the range probe is partition-local.
        // A pure range join (n_eq == 0) has no eq prefix: its matches are spread
        // over the whole key space, so it BROADCASTS the full delta and each worker
        // trims to its owned slice (WorkerFilter) before integrating. The output
        // relay (source_id == 0) is NOT a join relay (is_join is false there) and
        // keeps the GroupKey scatter.
        let range_n_eq = if is_join { meta.range_join_n_eq } else { None };

        let dest = if range_n_eq == Some(0) {
            // Pure range join: broadcast the full delta to every worker.
            RelayDest::Broadcast(Box::new(op_relay_broadcast(&sources, &schema)))
        } else {
            // Scatter. Band join (range_n_eq == Some(n_eq ≥ 1)): route by the eq
            // prefix shard_cols[..n_eq]. Equi-join: full shard cols. Both
            // JoinPromote. GROUP BY / set-op: full shard cols, GroupKey.
            let route_len = range_n_eq.map_or(shard_cols.len(), |n_eq| n_eq as usize);
            debug_assert!(
                range_n_eq.is_none_or(|n_eq| shard_cols.len() == n_eq as usize + 1),
                "range-join reindex key = [eq…, range]: len must be n_eq + 1"
            );
            let col_indices = &shard_cols[..route_len];
            // A GroupKey scatter promotes nothing; a join's target list is as long
            // as its key, so slice it to the routing prefix.
            let route_tcs: &[u8] = match join_key {
                Some(k) => &k.target_tcs[..route_len],
                None => &[],
            };
            let mode = if is_join {
                RouteMode::JoinPromote
            } else {
                RouteMode::GroupKey
            };
            // Every contributing source must be consolidated to take the
            // merge-walk scatter; a single non-consolidated source falls back to
            // the re-sorting repartition. The scatter
            // (`op_relay_scatter_consolidated_mode`) debug-verifies each.
            RelayDest::PerWorker(if sources.iter().flatten().all(|b| b.is_consolidated()) {
                op_relay_scatter_consolidated_mode(&sources, col_indices, route_tcs, &schema, self.num_workers, mode)
            } else {
                op_repartition_batches_mode(&sources, col_indices, route_tcs, &schema, self.num_workers, mode)
            })
        };

        // Size the group here, outside `sal_writer_excl`: the batches are in
        // hand, so the fit check under the lock is a comparison rather than a
        // sizing pass.
        let worker_data = self.relay_data(&dest);
        let footprint = self.sal.group_footprint_direct(&self.relay_group(
            view_id,
            source_id,
            &schema,
            &worker_data,
            BACKFILL_DECISION_CONTINUE,
        ));
        drop(worker_data);

        Ok(RelayPrepared {
            view_id,
            source_id,
            dest,
            schema,
            footprint,
        })
    }

    /// Synchronous second half of a relay: writes the FLAG_EXCHANGE_RELAY group to
    /// SAL and signals workers, stamping the round `decision` (a
    /// `BACKFILL_DECISION_*`) onto the relay's `seek_col_idx`. Caller holds
    /// `sal_writer_excl` for the duration; no awaits inside.
    ///
    /// CONTINUE == 0 is the value a plain steady-state relay's `seek_col_idx` has
    /// always carried. `collect_acks_and_relay` is the sole STOP/CHECKPOINT
    /// stamper (it terminates a backfill's chunked exchange on an all-pad round);
    /// the reactor's `relay_loop` serves only steady-state tick exchanges and
    /// always passes CONTINUE.
    pub(crate) fn emit_relay_with_decision(&self, prep: &RelayPrepared, decision: u64) -> Result<(), String> {
        let worker_data = self.relay_data(&prep.dest);
        let group = self.relay_group(prep.view_id, prep.source_id, &prep.schema, &worker_data, decision);
        self.sal.write_group_direct(&group, 0, FLAG_EXCHANGE_RELAY)?;
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
    /// Reclaims SAL space before a large source; the collect loop may further
    /// CHECKPOINT mid-stream. Both are safe on the SAL-exclusive,
    /// no-concurrent-relay paths this runs on (boot; the reactor-parked DDL
    /// window). A reclaim here bumps the generation, leaving every checkpointed
    /// view and index invalid until an ephemeral round re-stamps them — so a
    /// caller that observes the bump must run `restamp_derived` before its window
    /// closes.
    pub fn fan_out_backfill(&self, view_id: i64, source_id: i64) -> Result<(), String> {
        self.checkpoint_before_backfill()?;
        let schema = self.schema_desc_for(source_id);
        self.send_broadcast(source_id, 0, FLAG_BACKFILL, Some(&schema), view_id as u128)?;
        self.collect_acks_and_relay(true)
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
    pub fn backfill_views_in_dep_order(&self, view_ids: &[i64]) -> Result<(), String> {
        for vid in self.cat().dag_mut().order_by_view_deps(view_ids) {
            let sources = self.cat().dag_mut().get_source_ids(vid);
            for src in sources {
                self.fan_out_backfill(vid, src)
                    .map_err(|e| format!("view={vid} source={src}: {e}"))?;
            }
        }
        Ok(())
    }

    /// Synchronously drain one source's pending ticks during the reactor-parked
    /// CREATE-VIEW window: emit a FLAG_TICK, signal, and collect each worker's
    /// ACK while relaying its exchange dependents inline. The `handle_ddl_txn`
    /// caller holds the catalog write lock and the tick gate with the committer
    /// idle, so the async `relay_loop` cannot run (no deadlock) and no other tick
    /// races this. Checkpointing is forced off: a tick carries no backfill pad,
    /// so a CHECKPOINT verdict would advance only the master's epoch and wedge
    /// the cluster (see `collect_acks_and_relay`).
    pub(crate) fn drain_tick_blocking(&self, source_id: i64) -> Result<(), String> {
        let nw = self.num_workers;
        self.write_tick_group(source_id, &SYNC_COLLECT_REQ_IDS[..nw])?;
        self.signal_all();
        self.collect_acks_and_relay(false)
    }

    // The async fan-outs below are free functions over `&MasterDispatcher`, not
    // methods: other reactor tasks re-enter the dispatcher across their awaits,
    // so no exclusive borrow may span one.

    pub async fn fan_out_seek(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        target_id: i64,
        pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<W2mSlot, String> {
        let num_workers = disp.num_workers;
        let schema = disp
            .cat()
            .get_schema_desc(target_id)
            .ok_or_else(|| format!("seek: table {target_id} not found"))?;
        // Decode the wire pair to the OPK bytes (width-universal), then route off
        // the distribution prefix via the shared `worker_for_pk`. A FLAG_SEEK
        // always carries the full PK and the prefix ⊆ the PK, so a full-PK seek
        // pins exactly one worker — no broadcast clause. Hashing the native value
        // instead of the OPK bytes would misroute signed and compound PKs.
        let opk = gnitz_engine::schema::key::seek_opk_bytes(&schema, pk, seek_pk_extra)
            .map_err(|e| format!("seek: table {target_id}: {e}"))?;
        let worker = schema.worker_for_pk(opk.pk_bytes(), num_workers);
        let (mut slots, _req_ids, _lease) = dispatch_scan_fanout(
            disp,
            reactor,
            sal_excl,
            Fanout::One(worker),
            |disp, req_ids, unicast| {
                disp.write_command_group(target_id, 0, FLAG_SEEK, 0, pk, 0, req_ids, unicast, 0, seek_pk_extra)
            },
        )
        .await?;
        let slot = slots.pop().expect("unicast fan-out returns one slot");
        // A point seek's reply must fit one frame; a train would be forwarded
        // truncated, so reject it rather than silently drop the remainder.
        expect_single_frame(&slot, worker, "seek").map_err(|f| f.text)?;
        Ok(slot)
    }

    /// Index lookup: fan one frame out to ALL workers and MERGE every matching
    /// base row into one batch via the train drain (an oversized worker reply
    /// arrives as a chunked train; a single-frame reply is a length-1 train).
    /// Returns the merged base rows, or `None` when no row matches.
    ///
    /// The sole index-seek fan-out — a secondary index is one unpartitioned
    /// table per worker, so nothing derives the owning worker from an indexed
    /// value and every arity, unique or not, must ask all of them. A unique
    /// index matches at most one row, so merging one is correct.
    ///
    /// The master forwards the client's wire payload verbatim — it never
    /// decodes seek_pk_extra into u128s and re-encodes them (the worker is
    /// the sole OPK encoder). seek_col_idx carries pack_pk_cols(col_indices),
    /// already validated by the caller.
    /// `_lease` held across the full drain: its workers stream, so releasing
    /// the gate before every train is consumed (or the drain errors and the
    /// lease drop discards the rest) risks a discarded late frame.
    pub async fn fan_out_seek_by_index_collect(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        target_id: i64,
        seek_col_idx: u64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<Option<Batch>, String> {
        const OP: &str = "seek_by_index";
        // `expected` is captured inside the fan-out closure so the reply
        // guard is definitionally the schema the request was built from; a
        // separate pre-fanout catalog read could diverge across the
        // `sal_excl` await if a DDL interleaves, failing healthy replies.
        // Single-source a REPLICATED owner: a broadcast-and-merge would append
        // each matching row `nw` times (weights copied verbatim, no consolidation),
        // handing the client `nw` duplicates of every row. Hashed owners keep
        // the fan-out (matches scatter by PK).
        let unicast = replicated_unicast(disp, target_id);
        let mut expected: Option<SchemaDescriptor> = None;
        let (slots, req_ids, _lease) =
            dispatch_scan_fanout(disp, reactor, sal_excl, unicast, |disp, req_ids, unicast| {
                // The schema is the master's own reply guard, not something the
                // group carries: the worker's `seek_by_index` arm resolves its
                // own from its own catalog.
                expected = Some(disp.schema_desc_for(target_id));
                disp.write_command_group(
                    target_id,
                    0,
                    FLAG_SEEK_BY_INDEX,
                    0,
                    seek_pk,
                    seek_col_idx,
                    req_ids,
                    unicast,
                    0,
                    seek_pk_extra,
                )
            })
            .await?;
        let expected = expected.expect("fan-out closure ran");

        let mut acc: Option<Batch> = None;
        let mut merged_bytes = 0usize;
        drain_index_scan(slots, &req_ids, reactor, OP, &expected, |mb, frame_len| {
            // The merge goes back out as one frame, so it is bounded by what the
            // client will read (`FRAME_CAP`). Σ frame bytes ≥ that merged encode
            // size — every frame re-counts its header and the first one the schema
            // block — so capping the sum never lets an unreadable reply through,
            // and it bounds the master's merge heap on the way.
            merged_bytes += frame_len;
            if merged_bytes > crate::runtime::wire::FRAME_CAP {
                return Err(format!(
                    "{OP}: result exceeds the {} MiB reply cap; add a tighter \
                     predicate or LIMIT",
                    crate::runtime::wire::FRAME_CAP >> 20
                )
                .into());
            }
            let a = acc.get_or_insert_with(|| Batch::with_capacity(expected, mb.count));
            a.append_mem_batch(mb);
            Ok(())
        })
        .await
        .map_err(|f| f.text)?;
        // The sink runs only for non-empty frames, so `Some` implies rows.
        Ok(acc)
    }

    /// Fan out a SCAN to the workers `unicast` names — [`Fanout`] states what
    /// each shape costs. Forwards every response frame directly to the client, continuation
    /// chunks included, and returns `Ok(true)` when all drained trains finish,
    /// `Ok(false)` if the client disconnects mid-stream, `Err` on a worker
    /// error.
    ///
    /// `sal_flags`/`wire_flags`/`seek_pk_extra` select the read shape, as
    /// `write_scan_group` documents: a plain scan passes the client's schema
    /// version in `wire_flags` so workers can decide whether to include a schema
    /// block; a ScanSpec read passes `FLAG_SCAN_SPEC` and the client's verbatim
    /// spec blob, and negotiates no version (its reply carries no schema block —
    /// the client decodes against the schema it authored). `unicast` comes from
    /// [`replicated_unicast`] or [`scan_spec_route`], which own the routing
    /// policy (replicated relations to worker 0, confinable ranges to their
    /// owner).
    ///
    /// `forward_scan_slots` returns on the FIRST worker fault, decode error, or
    /// client disconnect: `_lease` drops on return and `route_scan_slot` discards
    /// every undrained frame at the ring boundary, advancing `consume_cursor`, so
    /// a still-streaming worker cannot wedge in `send_encoded` — draining the
    /// doomed trains would be pure waste. The client may already have received
    /// earlier workers' data frames when the fault surfaces, so the fault frame
    /// `finish_scan_fanout` emits can arrive mid-stream:
    /// `Connection::drain_reply_train` (gnitz-core), the one reply reassembler,
    /// returns on the first failing status and discards the batch it had
    /// accumulated, so no partial rows surface.
    ///
    /// **It also returns the tick round it sampled**, and samples it *inside* the
    /// closure `dispatch_scan_fanout` runs under `sal_writer_excl` — the same
    /// window the read's group is written in. That is what makes the round a
    /// property of the SAL prefix rather than of a tick's success: every worker
    /// sees the read group after exactly the tick groups of rounds `<= T` and
    /// before any group of a later round.
    ///
    /// No tick group can land between the sample and the write, and the mutex is
    /// only half of why — which matters because the other half is the fragile
    /// half. `run_tick` writes its groups under it; the three `drain_tick_blocking`
    /// callers write theirs under **no** mutex at all. What covers all four paths
    /// is that the reactor is single-threaded and each of them is await-free, so
    /// two of them cannot interleave. The mutex earns its place against the
    /// *committer*, which holds it across an fsync and would otherwise put a read
    /// group inside a commit zone. Stated that way, the first `drain_tick_blocking`
    /// caller to grow an `await` is a visible break rather than a silent one.
    ///
    /// The round is written into the group's `seek_pk` as well as returned, so the
    /// worker cuts the delta interval at it rather than walking to the end of its
    /// store. `seek_pk` is genuinely free on a scan group — hard-coded `0` before
    /// this and read by nothing in either scan arm.
    ///
    /// Sampling unconditionally is a `Cell` read under a lock the function already
    /// holds, so it costs the other callers nothing; the plain scan and the
    /// multi-scan drop the payload explicitly at their own call sites.
    #[allow(clippy::too_many_arguments)]
    pub async fn fan_out_scan(
        disp: &MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex>,
        unicast: Fanout,
        target_id: i64,
        client_id: u64,
        peer: &Peer,
        sal_flags: u32,
        wire_flags: u64,
        seek_pk_extra: &[u8],
    ) -> Result<(bool, u64), WorkerFault> {
        // `_lease` held across the entire continuation drain: every worker
        // streams a multi-frame train, and a cancelled drain (client disconnect)
        // must keep the ids active until the lease drops so the gate discards —
        // not parks — late frames.
        let sampled = Cell::new(0u64);
        let (slots, req_ids, _lease) =
            dispatch_scan_fanout(disp, reactor, sal_excl, unicast, |disp, req_ids, unicast| {
                let round = disp.last_tick_round();
                sampled.set(round);
                disp.write_scan_group(
                    target_id,
                    sal_flags,
                    wire_flags,
                    req_ids,
                    unicast,
                    client_id,
                    seek_pk_extra,
                    round as u128,
                )
            })
            .await?;
        let ok = forward_scan_slots(reactor, peer, slots, &req_ids, unicast).await?;
        Ok((ok, sampled.get()))
    }

    /// Await + forward one already-dispatched scan relation: the await+drain
    /// half of `fan_out_scan`, factored out for the multi-scan path
    /// (`handle_scan_multi`), which writes every relation's group up front under
    /// one SAL cut via `dispatch_scan_multi_fanout` and then drains each relation
    /// here, in request order. Awaits every worker's first reply frame (join_all
    /// across workers for a broadcast, the single slot for a unicast), then
    /// forwards each worker's train to the client in ascending worker order.
    ///
    /// `Ok(false)` on client disconnect, `Err` on a worker fault / malformed
    /// train. The caller holds the relation's `ScanLease` for the whole call, so
    /// a `false`/`Err` return drops it and discards the undrained remainder at
    /// the ring boundary. Draining relation `i` fully before relation `i+1` is
    /// the FIFO invariant's supported usage: with `FLAG_SCAN_FIFO_REPLY`, each
    /// worker streams the relations in request order, so relation `i`'s frames
    /// sit at the front of every ring with a live consumer.
    pub(crate) async fn await_and_drain_scan_relation(
        reactor: &crate::runtime::reactor::Reactor,
        peer: &Peer,
        unicast: Fanout,
        req_ids: &[u64],
        nw: usize,
    ) -> Result<bool, WorkerFault> {
        let slots = await_scan_slots(reactor, unicast, req_ids, nw).await;
        forward_scan_slots(reactor, peer, slots, req_ids, unicast).await
    }

    /// Broadcast a DDL batch to every worker. `lsn` is the caller's zone
    /// LSN — one LSN across all broadcasts of a DDL so recovery can group
    /// them as an atomic zone. `zone_start` marks this as the zone's first group,
    /// which is what gives the zone a byte span recovery can attribute damage to.
    pub fn broadcast_ddl(&self, target_id: i64, batch: &Batch, lsn: u64, zone_start: bool) -> Result<(), String> {
        let (schema, schema_block, _safe, _stride) = self.cached_schema_block(target_id);
        let worker_data = vec![wire::WireData::Whole(Some(batch)); self.num_workers];
        self.sal.write_group_direct(
            &DirectGroup {
                template: wire::WireMsg {
                    target_id: target_id as u64,
                    schema: Some(&schema),
                    prebuilt_schema_block: Some(schema_block.as_slice()),
                    ..Default::default()
                },
                worker_data: &worker_data,
                req_ids: &SYNC_COLLECT_REQ_IDS[..self.num_workers],
                unicast_worker: None,
            },
            lsn,
            FLAG_DDL_SYNC | if zone_start { FLAG_ZONE_START } else { 0 },
        )?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={} lsn={}", target_id, batch.count, lsn);
        Ok(())
    }

    /// Close an atomic zone at `lsn`: write the empty commit sentinel
    /// and signal workers. All preceding groups at this LSN belong to
    /// the zone; recovery applies them only when this sentinel reaches
    /// disk before the crash.
    pub fn commit_zone(&self, lsn: u64) -> Result<(), String> {
        self.sal.write_commit_sentinel(lsn)?;
        self.signal_all();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// The one-shot tick-emit latch, but only for the table the seam names;
    /// `None` when the seam is unset or `tid` is some other table. Checked before
    /// the catalog lookup, so an unset seam costs no name resolution.
    fn injected_tick_emit_latch(&self, tid: i64) -> Option<&'static std::sync::atomic::AtomicBool> {
        static ARMED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        let name = TICK_EMIT_ERROR.names()?;
        let (_, table) = self.cat().get_qualified_name(tid)?;
        (table == name).then_some(&ARMED)
    }

    /// Arm half: a committed non-empty push to the named table arms the one-shot
    /// tick-emit failure.
    ///
    /// Arming on a push rather than on "the next tick anywhere" is what makes the
    /// seam land where the scenario it models does: a `CREATE VIEW` drives one
    /// tick of its source to seed the view, well before any test read reaches the
    /// tick loop, so an unconditional latch would always be spent by the CREATE.
    /// That seeding push carries no rows, hence the row-count test.
    fn arm_injected_tick_emit_error(&self, target_id: i64, rows: usize) {
        if rows > 0 {
            if let Some(armed) = self.injected_tick_emit_latch(target_id) {
                armed.store(true, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    /// Fire half: fail the armed table's next `write_tick_group` before it writes
    /// anything — what a full SAL does to a tick. One-shot, so the follow-up read
    /// can watch the re-queued tid tick and the view converge.
    fn take_injected_tick_emit_error(&self, tid: i64) -> bool {
        let fires = self
            .injected_tick_emit_latch(tid)
            .is_some_and(|armed| armed.swap(false, std::sync::atomic::Ordering::Relaxed));
        if fires {
            gnitz_debug!("injected tick emit error fires (tid={})", tid);
        }
        fires
    }

    /// Write a FLAG_TICK group for `tid` with per-worker req_ids. Does NOT
    /// signal: the caller writes every group in the batch, then calls
    /// `signal_all` once. A signal is a wake, not framing — a worker reaches a
    /// published group through the mapping either way — so batching the wake
    /// changes no outcome and saves `nw` eventfd writes per group beyond the
    /// first. Per-worker slots each carry
    /// the corresponding req_id from `req_ids[w]`. No schema block:
    /// `handle_tick` looks the target's schema up in its own catalog.
    ///
    /// **This is where the tick round is allocated**, because this is the one
    /// writer of a tick group: `run_tick` and `drain_tick_blocking` — itself the
    /// CREATE-VIEW drain, the boot recovery sweep and the post-reclaim restamp —
    /// all funnel here. Allocating anywhere else means enumerating those paths,
    /// and an enumeration is wrong the day a fifth appears. It also makes the
    /// idle-poll map complete for free: the round, the record of which views it
    /// reaches, and the group write are one synchronous await-free body, so a poll
    /// that sees the counter advanced also sees the round recorded.
    ///
    /// The round rides in the group header's `lsn`, which is free for it: every
    /// command verb passes `0` there except the ephemeral flush round, which
    /// carries the checkpoint generation, and the replay walk already says a
    /// non-zero `lsn` alone does not make a group a zone. So the round costs no
    /// SAL bytes and changes no recovery behaviour.
    ///
    /// The counter is advanced — and the map written — **before** the
    /// injected-failure seam fires, so a failed emit burns a round rather than
    /// reusing it: rounds must be strictly increasing, and the re-queued tid ticks
    /// again under a later one. Recording a round that never reached a worker
    /// gates away nothing: no rows exist at it, and the re-tick raises the map
    /// past it, so a cursor sitting at the burnt round falls through again.
    pub(crate) fn write_tick_group(&self, tid: i64, req_ids: &[u64]) -> Result<(), String> {
        let round = self.tick_round.get() + 1;
        self.tick_round.set(round);
        self.record_delta_round(tid, round);
        if self.take_injected_tick_emit_error(tid) {
            return Err(format!("injected tick emit error (tid={tid})"));
        }
        self.write_command_group(tid, round, FLAG_TICK, 0, 0, 0, req_ids, Fanout::Broadcast, 0, &[])
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
        if !cat.dag().any_delta_feed() {
            return;
        }
        let reached = cat.dag_mut().dependent_closure(vec![tid]);
        let mut map = self.last_delta_round.borrow_mut();
        for vid in reached {
            if cat.dag().relation_has_delta_feed(vid) {
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
        // unmixed XOR would make neighbouring ids' tags differ in one bit.
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

    pub fn check_workers(&self) -> i32 {
        // Probe each worker by its own pid, not `waitpid(-1)`. A per-pid
        // `waitpid` returns ECHILD — a detected death — even if the zombie was
        // reaped elsewhere, whereas `waitpid(-1)` returns 0 ("some child is
        // alive") and silently misses one worker's death while others run, so it
        // would go blind the moment a SIGCHLD/signalfd reaper or SA_NOCLDWAIT is
        // ever added. It also names the exact dead worker for the error/log.
        for w in 0..self.num_workers {
            let pid = self.worker_pids.borrow()[w];
            if pid <= 0 {
                continue;
            }
            let mut status: i32 = 0;
            loop {
                let rpid = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                if rpid > 0 {
                    self.worker_pids.borrow_mut()[w] = 0; // reaped — never waitpid it again
                    return w as i32;
                }
                if rpid == 0 {
                    break; // still running
                }
                // rpid == -1
                let err = crate::runtime::posix::errno();
                if err == libc::EINTR {
                    continue; // signal, not death — retry
                }
                if err == libc::ECHILD {
                    self.worker_pids.borrow_mut()[w] = 0; // already gone
                    return w as i32;
                }
                break; // unexpected errno: treat as alive
            }
        }
        -1
    }

    /// Broadcast `FLAG_SHUTDOWN` (each worker flushes + `_exit`s) and reap the
    /// worker processes.
    pub fn shutdown_workers(&self) {
        // No schema block: the worker's `Shutdown` arm takes no arguments.
        let _ = self.send_broadcast(0, 0, FLAG_SHUTDOWN, None, 0);
        for w in 0..self.num_workers {
            let pid = self.worker_pids.borrow()[w];
            if pid > 0 {
                let mut status: i32 = 0;
                unsafe {
                    libc::waitpid(pid, &mut status, 0);
                }
            }
        }
        // All workers reaped: no process can race a removal. Reclaim any dirs
        // still gated (dropped entities whose gating checkpoint never arrived).
        self.cat().drain_checkpoint_gated_deletions();
    }

    /// Whether a transaction's family groups fit the SAL, and if not, whether a
    /// checkpoint could make them fit. The committer's whole space question in
    /// one call — it never sees the cursor or the capacity rule itself.
    pub(crate) fn txn_fit(&self, families: &[(i64, &Batch)]) -> SalFit {
        self.sal.fit(self.txn_zone_footprint(families))
    }

    /// The exact SAL footprint (bytes) of a transaction's family groups — the sum
    /// over families of each family group, partitioned the way
    /// `write_commit_group` will emit it (broadcast for a replicated schema, else
    /// PK-partitioned).
    ///
    /// Each family is measured as the [`DirectGroup`] `scatter_wire_group` builds
    /// for it, so the fit check and the emission read one set of slot sizes. They
    /// must not drift: the committer fail-stops the node when a family fails to
    /// fit after an earlier family already wrote to the SAL.
    fn txn_zone_footprint(&self, families: &[(i64, &Batch)]) -> usize {
        let nw = self.num_workers;
        let mut total = 0usize;
        for &(tid, batch) in families {
            let (schema, block, wire_safe, wire_row_stride) = self.cached_schema_block(tid);
            let sal = &self.sal;
            total += with_commit_indices(batch, &schema, nw, |wi| {
                sal.scatter_group_footprint(
                    batch,
                    wi,
                    &schema,
                    tid as u32,
                    Some(block.as_slice()),
                    Some((wire_safe, wire_row_stride)),
                )
            });
        }
        total
    }

    /// Write one push batch as a SAL group at the caller's `lsn`, with a
    /// per-worker request id. Called from the committer task, which signals,
    /// closes the zone, and awaits fsync + the per-worker ACKs itself.
    pub(crate) fn write_commit_group(
        &self,
        target_id: i64,
        lsn: u64,
        batch: &Batch,
        mode: WireConflictMode,
        req_ids: &[u64],
        zone_start: bool,
    ) -> Result<(), String> {
        self.arm_injected_tick_emit_error(target_id, batch.count);
        let (schema, schema_block, wire_safe, wire_row_stride) = self.cached_schema_block(target_id);
        let nw = self.num_workers;
        let wire_flags = wire_flags_set_conflict_mode(0, mode);
        // Identical scatter for both routings; only the per-worker index fill
        // differs (full broadcast vs PK-partitioned). One `scatter_wire_group`
        // call site keeps the atomic-zone framing, LSN, ACK accounting, and the
        // committer's single `fdatasync` shared between them.
        let scatter = |worker_indices: &[Vec<u32>]| {
            self.sal.scatter_wire_group(
                batch,
                worker_indices,
                &schema,
                target_id as u32,
                lsn,
                FLAG_PUSH | if zone_start { FLAG_ZONE_START } else { 0 },
                wire_flags,
                0,
                req_ids,
                Some(schema_block.as_slice()),
                Some((wire_safe, wire_row_stride)),
            )
        };
        // A replicated relation broadcasts: the whole batch lands in every
        // worker's ingest + SAL slot, so each worker durably logs the full table
        // and enforces uniqueness against its identical full copy.
        with_commit_indices(batch, &schema, nw, scatter)
    }

    /// Write a checkpoint flush group (`FLAG_FLUSH` base round or
    /// `FLAG_FLUSH_EPH` ephemeral round) with per-worker req_ids. Does NOT
    /// sync/signal. Caller signals + awaits replies. `lsn` is supplied by the
    /// caller — the ephemeral round passes the checkpoint generation there, which
    /// workers read to stamp view manifests (the base round passes 0).
    ///
    /// Every worker gets a bare control block: `handle_flush_all` takes neither a
    /// schema nor a batch, and reads the generation from
    /// `worker_ctx::committed_generation()`.
    pub(crate) fn write_checkpoint_group(&self, lsn: u64, flags: u32, req_ids: &[u64]) -> Result<(), String> {
        self.note_flush_round(lsn, flags);
        self.write_command_group(0, lsn, flags, 0, 0, 0, req_ids, Fanout::Broadcast, 0, &[])
    }

    /// Post-ACK checkpoint cleanup: flush system tables before resetting
    /// the SAL cursor (their data lives in SAL entries about to be
    /// discarded), then advance the epoch. Called by both the synchronous
    /// `sync_round` and the async committer after it collects FLAG_FLUSH ACKs.
    ///
    /// Finalizes **both** checkpoint rounds — base and ephemeral. The ephemeral
    /// round must flush too: a `commit_serial_range_durable` advance can land in
    /// the `sys_sequences` MemTable during the drain window (after the base
    /// round's reset), so this flush is its only durability event before the
    /// ephemeral reset makes the SAL tail useless for recovery. The
    /// gated-deletion drain is a no-op in the ephemeral position (DDL is
    /// barrier-deferred through the whole checkpoint, so nothing enqueues after
    /// the base round drained).
    ///
    /// Returns the flush error WITHOUT resetting the SAL when the system-table
    /// flush fails: the SAL entries about to be discarded are that data's only
    /// durable copy, so resetting on a swallowed failure destroys it — the same
    /// hazard the boot path guards via `flush_all_system_tables`. Callers leave
    /// the SAL intact and retry on a later checkpoint (committer) or abort boot
    /// (`sync_round`).
    pub(crate) fn checkpoint_post_ack(&self) -> Result<(), String> {
        let cat = self.cat();
        cat.flush_all_system_tables()?;
        // Now safe: every worker ACKed the FLUSH, so all have consumed past any
        // DROP that gated a directory — hence finished the matching CREATE.
        cat.drain_checkpoint_gated_deletions();
        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    /// Bump the committed checkpoint generation (step 0 of the sequence).
    /// Delegates to the catalog: durably records the seq-4 row and publishes the
    /// new value to `worker_ctx`. Returns the new generation.
    pub(crate) fn bump_checkpoint_generation(&self) -> u64 {
        self.cat().bump_checkpoint_generation()
    }

    /// Synchronous boot-end checkpoint (pre-reactor W2M path): record the
    /// launched topology, then reclaim and re-stamp. The drain set is empty —
    /// recovery already drained everything and no pushes are admitted yet (the
    /// socket is not open), so `pending_deltas` is empty. Freshly backfilled
    /// views are durably checkpointed before the socket opens.
    pub(crate) fn boot_checkpoint(&self, worker_count: u32) -> Result<(), String> {
        // The topology row's durability rides the gen bump's system-table flush
        // (both are `_sequences` rows), so a manifest stamped at a generation
        // implies the topology row for that layout is durable.
        self.cat().record_topology(worker_count);
        self.reclaim_base()?;
        self.restamp_derived(&[])
    }

    /// Accessor for the committer. True when the SAL write cursor has
    /// crossed the configured checkpoint threshold.
    pub fn sal_needs_checkpoint(&self) -> bool {
        self.sal.needs_checkpoint()
    }

    /// Get the schema descriptor for a target_id. Panics if the table
    /// has no schema (committer should only see tables that validated).
    pub fn schema_desc_for(&self, target_id: i64) -> SchemaDescriptor {
        self.cat()
            .get_schema_desc(target_id)
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"))
    }
}

/// Await each dispatched worker's first reply slot for a scan group: the single
/// slot under `Fanout::One` (keyed on `req_ids[0]`), or all `nw` in worker
/// order under `Fanout::Broadcast`. The one owner of the unicast/broadcast await
/// shape, shared by `dispatch_scan_fanout`, `fan_out_scan`, and
/// `await_and_drain_scan_relation`.
pub(crate) async fn await_scan_slots(
    reactor: &crate::runtime::reactor::Reactor,
    unicast: Fanout,
    req_ids: &[u64],
    nw: usize,
) -> Vec<W2mSlot> {
    if unicast != Fanout::Broadcast {
        vec![reactor.await_scan_slot(req_ids[0] as u32).await]
    } else {
        crate::runtime::reactor::join_all_unpin(req_ids[..nw].iter().map(|&id| reactor.await_scan_slot(id as u32)))
            .await
    }
}

#[cfg(test)]
mod worker_liveness_tests {
    use super::*;
    use crate::runtime::w2m_ring;
    use gnitz_engine_testkit::SharedRegion;

    const RING_CAP: usize = 64 * 1024;

    // Build an inert dispatcher for the pre-reactor liveness-probe paths: real
    // but empty W2M rings so the bootstrap wait loops can `try_read` (always
    // None here, so they reach the park / no-progress arm that probes), a null
    // SAL and catalog (untouched on the no-frame path), and the given worker
    // pids. Returns the ring regions so they outlive the dispatcher
    // (W2mReceiver holds the raw ptrs but does not own them).
    fn probe_dispatcher(worker_pids: Vec<i32>) -> (MasterDispatcher, Vec<SharedRegion>) {
        let nw = worker_pids.len();
        let mut rings = Vec::with_capacity(nw);
        for _ in 0..nw {
            let region = SharedRegion::new(RING_CAP);
            unsafe { w2m_ring::init_region_for_tests(region.ptr(), RING_CAP as u64) };
            rings.push(region);
        }
        // `-1` eventfds: nothing parks on them here, and `eventfd_signal`
        // discards a failed write (the counter is only a wake hint).
        let disp = MasterDispatcher::new(
            nw,
            worker_pids,
            std::ptr::null_mut(),
            SalWriter::new(std::ptr::null_mut(), -1, 0, nw),
            Rc::new(W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect())),
            vec![-1; nw],
        );
        (disp, rings)
    }

    // Fork a child that exits immediately, then block-reap it. The pid is now a
    // confirmed non-child, so a later `waitpid` on it yields ECHILD — a
    // deterministic "dead" verdict with no race against the probe.
    fn spawn_and_reap_dead() -> i32 {
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let mut status = 0;
        unsafe { libc::waitpid(pid, &mut status, 0) };
        pid
    }

    #[test]
    fn check_workers_reports_neg1_for_live_worker() {
        // Child blocks in pause() so it is unambiguously alive across the probe.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe {
                libc::pause();
                libc::_exit(0);
            }
        }
        let (disp, _rings) = probe_dispatcher(vec![pid]);
        assert_eq!(disp.check_workers(), -1, "a live worker must not be reported dead");
        assert_eq!(
            disp.worker_pids.borrow()[0],
            pid,
            "a live worker's pid must be retained"
        );
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(pid, &mut status, 0);
        }
        drop(disp);
    }

    #[test]
    fn check_workers_reaps_and_zeroes_then_does_not_re_report() {
        // An exited child becomes a zombie; the detecting `waitpid(WNOHANG)`
        // reaps it (rpid > 0) and must zero the slot so a second probe does not
        // re-`waitpid` a non-child and re-report the same worker as dead.
        let pid = unsafe { libc::fork() };
        assert!(pid >= 0, "fork failed");
        if pid == 0 {
            unsafe { libc::_exit(0) };
        }
        let (disp, _rings) = probe_dispatcher(vec![pid]);
        // Bounded poll until the zombie is reaped by the probe (the child exits
        // near-instantly). The bound keeps a regression from hanging the suite.
        let mut detected = -1;
        for _ in 0..2000 {
            detected = disp.check_workers();
            if detected >= 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(detected, 0, "the exited worker must be detected dead");
        assert_eq!(disp.worker_pids.borrow()[0], 0, "a reaped pid must be zeroed");
        assert_eq!(disp.check_workers(), -1, "a zeroed worker must not be re-reported");
        drop(disp);
    }

    #[test]
    fn collect_acks_errors_when_worker_dies_before_acking() {
        // Boot-recovery path: wait_all_workers finds an empty ring and reaches
        // the park arm, whose liveness probe must surface the dead worker as a
        // clean error instead of looping on `wait_for` forever.
        let dead = spawn_and_reap_dead();
        let (disp, _rings) = probe_dispatcher(vec![dead]);
        let err = disp.collect_acks().expect_err("a dead worker must fail ack collection");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("recovery sync"),
            "error identifies the recovery path: {err}"
        );
        drop(disp);
    }

    #[test]
    fn collect_acks_and_relay_errors_when_worker_dies_mid_backfill() {
        // Backfill path: collect_acks_and_relay makes no progress on an empty
        // ring and reaches the !progressed arm, whose probe must surface the
        // dead worker.
        let dead = spawn_and_reap_dead();
        let (disp, _rings) = probe_dispatcher(vec![dead]);
        let err = disp
            .collect_acks_and_relay(true)
            .expect_err("a dead worker must fail the backfill relay");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("backfill relay"),
            "error identifies the backfill path: {err}"
        );
        drop(disp);
    }
}

#[cfg(test)]
mod checkpoint_finalize_tests {
    use super::*;
    use crate::runtime::sal::SalWriter;
    use crate::runtime::w2m::W2mReceiver;
    use gnitz_engine::catalog::{CatalogEngine, FIRST_USER_TABLE_ID, SEQ_TAB_ID};
    use gnitz_engine_testkit::SharedRegion;

    const SAL_SIZE: usize = 4096;

    fn finalize_temp_dir(name: &str) -> String {
        gnitz_engine_testkit::scratch_dir("checkpoint_finalize", name)
    }

    /// The checkpoint finalizer must flush system tables before resetting the SAL:
    /// a `commit_serial_range_durable` advance can land in the `sys_sequences`
    /// MemTable during the drain window (bypassing the committer barrier and the SAL
    /// reset that already ran in the base round), so the finalizer is its only
    /// durability event before the ephemeral reset makes the SAL log useless for
    /// recovery. Strip the flush and a crash right after (no `close()`) loses it.
    #[test]
    fn checkpoint_post_ack_flushes_memtable_only_sequence_advance() {
        let dir = finalize_temp_dir("seq_survives_reset");
        let user_seq = FIRST_USER_TABLE_ID + 3;
        {
            let mut engine = CatalogEngine::open(&dir, 1).unwrap();

            // Reserve + ingest straight into the catalog — no SAL involved, so the
            // advance lands ONLY in the sys_sequences MemTable (mirrors
            // test_user_sequence_durable_roundtrip's setup).
            let (base, delta, _lsn) = engine.reserve_user_sequence(user_seq, 64);
            assert_eq!(base, 1);
            engine.ingest_to_family(SEQ_TAB_ID, &delta).unwrap();
            assert_eq!(engine.user_sequence(user_seq), Some(64));

            // A fake but real, writable SAL region: checkpoint_reset() stores 0 at
            // the base pointer, so it must not be null.
            let sal_region = SharedRegion::new(SAL_SIZE);
            let sal_writer = SalWriter::new(sal_region.ptr(), -1, SAL_SIZE as u64, 0);
            let catalog_ptr = &mut engine as *mut CatalogEngine;
            let disp = MasterDispatcher::new(
                0,
                Vec::new(),
                catalog_ptr,
                sal_writer,
                Rc::new(W2mReceiver::new(Vec::new())),
                Vec::new(),
            );

            // The finalizer under guard: must durably flush sys_sequences before the
            // reset.
            disp.checkpoint_post_ack().unwrap();
            drop(disp);

            // Crash semantics: no engine.close(). No Drop impl, so only flushed data survives.
            drop(engine);
        }

        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(
            engine.user_sequence(user_seq),
            Some(64),
            "sys_sequences high-water must survive a crash right after the checkpoint finalize"
        );
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `reclaim_base` owns the generation bump that invalidates every
    /// checkpointed view and index before it publishes a newer base cut: exactly
    /// one per call, and `boot_checkpoint` consumes that one rather than adding
    /// its own.
    ///
    /// Zero workers: every round's ACK collection is an empty loop, so the two
    /// rounds and the finalizer run for real without a forked cluster.
    #[test]
    fn reclaim_base_bumps_once_and_boot_checkpoint_consumes_it() {
        let dir = finalize_temp_dir("reclaim_base_bumps");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        assert_eq!(engine.durable_generation(), 0, "fresh DB starts at generation 0");

        let sal_region = SharedRegion::new(SAL_SIZE);
        let sal_writer = SalWriter::new(sal_region.ptr(), -1, SAL_SIZE as u64, 0);
        let catalog_ptr = &mut engine as *mut CatalogEngine;
        let disp = MasterDispatcher::new(
            0,
            Vec::new(),
            catalog_ptr,
            sal_writer,
            Rc::new(W2mReceiver::new(Vec::new())),
            Vec::new(),
        );

        // Epoch 0 is the empty-slot sentinel, so the region needs a boot reset
        // before any group is written — what `server_main` does after worker ACKs.
        disp.reset_sal(1);

        assert_eq!(disp.reclaim_base().unwrap(), 1, "the base round bumps G → G+1");
        assert_eq!(disp.cat().durable_generation(), 1);
        assert_eq!(disp.reclaim_base().unwrap(), 2, "and exactly once per call");

        // The boot checkpoint's base round bumps 2 → 3; its ephemeral round stamps
        // 3 (`note_flush_round`'s `debug_assert_eq!` is what pins that here).
        disp.boot_checkpoint(1).unwrap();
        assert_eq!(
            disp.cat().durable_generation(),
            3,
            "boot_checkpoint advances by exactly one",
        );

        drop(disp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The pair `reclaim_base` + `restamp_derived` is a whole checkpoint: the
    /// ephemeral round re-stamps at exactly the generation the base round
    /// invalidated everything at, so nothing is left rebuild-on-boot.
    #[test]
    fn restamp_derived_stamps_the_durable_generation() {
        let dir = finalize_temp_dir("restamp_stamps_durable_gen");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();

        let sal_region = SharedRegion::new(SAL_SIZE);
        let sal_writer = SalWriter::new(sal_region.ptr(), -1, SAL_SIZE as u64, 0);
        let catalog_ptr = &mut engine as *mut CatalogEngine;
        let disp = MasterDispatcher::new(
            0,
            Vec::new(),
            catalog_ptr,
            sal_writer,
            Rc::new(W2mReceiver::new(Vec::new())),
            Vec::new(),
        );
        disp.reset_sal(1);

        let gen = disp.reclaim_base().unwrap();
        assert_ne!(
            disp.last_ephemeral_gen.get(),
            gen,
            "the base round alone leaves every derived manifest behind the new generation"
        );

        // Empty drain set: zero workers hold no pending deltas.
        disp.restamp_derived(&[]).unwrap();
        assert_eq!(
            disp.last_ephemeral_gen.get(),
            disp.cat().durable_generation(),
            "the ephemeral round re-validates the derived state at the durable generation"
        );

        drop(disp);
        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
