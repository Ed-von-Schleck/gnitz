//! Master SAL dispatcher: the `MasterDispatcher` core — SAL group/broadcast
//! writes, worker signalling + ack collection, relay emit, the fan-out family
//! (seek / scan / index / gather), checkpointing, and the scan-fanout helpers.

use super::*;

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

impl MasterDispatcher {
    pub fn new(
        num_workers: usize,
        worker_pids: Vec<i32>,
        catalog: *mut CatalogEngine,
        sal: SalWriter,
        w2m: W2mReceiver,
    ) -> Self {
        MasterDispatcher {
            num_workers,
            worker_pids,
            sal,
            w2m: Some(w2m),
            w2m_ptr: std::ptr::null(),
            catalog,
            router: PartitionRouter::new(),
            unique_filters: FxHashMap::default(),
            check_batch_pool: FxHashMap::default(),
            next_lsn: 0,
        }
    }

    pub fn reset_sal(&mut self, write_cursor: u64, epoch: u32) {
        self.sal.reset(write_cursor, epoch);
    }

    /// Allocate the next monotonic LSN. Single allocator seam — after Phase 1,
    /// every SAL group LSN flows from here. Phases 3 and 6 will replace this
    /// with caller-supplied zone LSNs for DDL and push paths.
    pub fn next_lsn(&mut self) -> u64 {
        let v = self.next_lsn;
        self.next_lsn += 1;
        v
    }

    pub(super) fn get_schema_and_names(&mut self, target_id: i64) -> (SchemaDescriptor, Rc<Vec<Vec<u8>>>) {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat
            .get_schema_desc(target_id)
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"));
        let names = cat.get_col_names_bytes(target_id);
        (schema, names)
    }

    /// Return the schema descriptor, a cached prebuilt schema wire block,
    /// and the derived `(wire_safe, wire_row_fixed_stride)` for `target_id`.
    /// The block is built lazily on first call and stored in the catalog
    /// cache; it is invalidated alongside col_names whenever DDL modifies
    /// the table. Used by SAL write paths (commit/tick/broadcast) to skip
    /// per-call `build_schema_wire_block` allocations and per-column
    /// iteration in `scatter_wire_group`.
    fn cached_schema_block(&mut self, target_id: i64) -> (SchemaDescriptor, Rc<Vec<u8>>, bool, u32) {
        let cat = unsafe { &mut *self.catalog };
        let schema = cat
            .get_schema_desc(target_id)
            .unwrap_or_else(|| panic!("master: no schema for target_id={target_id}"));
        if let Some(cached) = cat.get_cached_schema_wire_block(target_id) {
            return (schema, cached.block, cached.wire_safe, cached.wire_row_fixed_stride);
        }
        let names = cat.get_col_names_bytes(target_id);
        let (name_refs, n) = col_names_as_refs(&names);
        let block = Rc::new(crate::runtime::wire::build_schema_wire_block(
            &schema,
            &name_refs[..n],
            target_id as u32,
        ));
        let (wire_safe, wire_row_stride) = crate::runtime::sal::compute_wire_props(&schema);
        cat.set_schema_wire_block(target_id, block.clone(), wire_safe, wire_row_stride);
        (schema, block, wire_safe, wire_row_stride)
    }

    pub(super) fn pool_pop_batch(&mut self, id: i64) -> Option<Batch> {
        self.check_batch_pool.get_mut(&id).and_then(|v| v.pop())
    }

    // -----------------------------------------------------------------------
    // Core send/receive helpers
    // -----------------------------------------------------------------------

    /// Encode per-worker data directly into SAL mmap.
    /// Does NOT fdatasync or signal. `lsn` is supplied by the caller.
    ///
    /// Sync callers pass `request_id=0` (or some single id) which is
    /// replicated across all workers. Async callers use
    /// `write_group_with_req_ids` to thread distinct per-worker ids.
    #[allow(clippy::too_many_arguments)]
    fn write_group(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
        unicast_worker: i32,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        let nw = self.num_workers;
        let mut ids = [0u64; crate::runtime::sal::MAX_WORKERS];
        for item in ids.iter_mut().take(nw) {
            *item = request_id;
        }
        self.write_group_with_req_ids(
            target_id,
            lsn,
            sal_flags,
            0,
            worker_batches,
            schema,
            col_names,
            seek_pk,
            seek_col_idx,
            &ids[..nw],
            unicast_worker,
            0,
            None,
            seek_pk_extra,
        )
    }

    /// Encode per-worker data with per-worker request ids. Used by async
    /// fan-outs that need distinct ids per worker for reply routing.
    /// `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, must be paired with empty
    /// `col_names` (computing names only to discard them negates the savings).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn write_group_with_req_ids(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
        client_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        let (name_refs, n) = col_names_as_refs(col_names);
        let col_names_opt = if n == 0 || prebuilt_schema_block.is_some() {
            None
        } else {
            Some(&name_refs[..n])
        };

        self.sal.write_group_direct(
            target_id as u32,
            lsn,
            sal_flags,
            wire_flags,
            worker_batches,
            schema,
            col_names_opt,
            seek_pk,
            seek_col_idx,
            req_ids,
            unicast_worker,
            client_id,
            prebuilt_schema_block,
            seek_pk_extra,
        )
    }

    /// Encode batch once directly into SAL mmap, replicate to all workers.
    /// `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, must be paired with empty
    /// `col_names` (computing names only to discard them negates the savings).
    #[allow(clippy::too_many_arguments)]
    fn write_broadcast(
        &mut self,
        target_id: i64,
        lsn: u64,
        sal_flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
    ) -> Result<(), String> {
        let (name_refs, n) = col_names_as_refs(col_names);
        let col_names_opt = if n == 0 || prebuilt_schema_block.is_some() {
            None
        } else {
            Some(&name_refs[..n])
        };

        self.sal.write_broadcast_direct(
            target_id as u32,
            lsn,
            sal_flags,
            0,
            batch,
            schema,
            col_names_opt,
            seek_pk,
            seek_col_idx,
            request_id,
            prebuilt_schema_block,
        )
    }

    /// Encode once, write to all workers, signal (no fdatasync).
    /// `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    fn send_broadcast(
        &mut self,
        target_id: i64,
        lsn: u64,
        flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.write_broadcast(
            target_id,
            lsn,
            flags,
            batch,
            schema,
            col_names,
            seek_pk,
            seek_col_idx,
            request_id,
            None,
        )?;
        self.signal_all();
        Ok(())
    }

    pub(crate) fn signal_all(&self) {
        self.sal.signal_all();
    }
    fn signal_one(&self, worker: usize) {
        self.sal.signal_one(worker);
    }

    pub(crate) fn sal_fd(&self) -> i32 {
        self.sal.sal_fd()
    }

    /// Write per-worker message group + signal all workers (no fdatasync).
    /// `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    fn send_to_workers(
        &mut self,
        target_id: i64,
        lsn: u64,
        flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names: &[Vec<u8>],
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.write_group(
            target_id,
            lsn,
            flags,
            worker_batches,
            schema,
            col_names,
            seek_pk,
            seek_col_idx,
            request_id,
            -1,
            &[],
        )?;
        self.signal_all();
        Ok(())
    }

    /// Transfer ownership of the W2M receiver to the reactor.
    /// Called once by the executor after bootstrap; panics if called twice.
    pub fn take_w2m(&mut self) -> W2mReceiver {
        self.w2m.take().expect("take_w2m called twice")
    }

    /// Record a pointer to the reactor-owned receiver (a stable `OnceCell` slot
    /// for the reactor's lifetime). After this, `w2m()` resolves through the
    /// pointer, keeping the synchronous collect helpers usable by the
    /// reactor-parked stop-the-world CREATE-VIEW backfill — the sole post-handoff
    /// reader, running while the reactor (the only other reader) is parked in
    /// that same call. Called once by the executor right after `take_w2m`.
    pub fn set_w2m_receiver_ptr(&mut self, p: *const W2mReceiver) {
        self.w2m_ptr = p;
    }

    fn w2m(&self) -> &W2mReceiver {
        // After handoff the receiver lives in the reactor; borrow it via the
        // recorded pointer. Before handoff (boot) the dispatcher owns it.
        if !self.w2m_ptr.is_null() {
            return unsafe { &*self.w2m_ptr };
        }
        self.w2m.as_ref().expect("W2mReceiver already handed off to reactor")
    }

    /// Liveness gate for the pre-reactor bootstrap wait loops. A crashed worker
    /// (panic / OOM-kill / SIGKILL) leaves its `reader_seq` frozen, so `wait_for`
    /// only ever times out and the wait loop would spin forever. Callers probe
    /// before parking and surface the dead worker as an error instead of hanging
    /// the master; `context` names the bootstrap phase ("before completing
    /// recovery sync", "during backfill relay"). On these paths workers stay
    /// alive after acking, so a reaped worker has not published the awaited
    /// frame — no ack is lost.
    fn fail_if_worker_dead(&mut self, context: &str) -> Result<(), String> {
        let dead = self.check_workers();
        if dead >= 0 {
            return Err(format!("worker {dead} exited {context}"));
        }
        Ok(())
    }

    /// Wait for one response from each worker. Bootstrap-only: runs
    /// before the reactor is up, so we drive each worker's ring via
    /// `W2mReceiver::wait_for` (sync FUTEX_WAIT on `reader_seq`). The
    /// tail-chasing ring self-maintains — no reset needed.
    #[allow(clippy::needless_range_loop)]
    fn wait_all_workers(&mut self) -> Result<Vec<Option<DecodedWire>>, String> {
        let nw = self.num_workers;
        let mut results: Vec<Option<DecodedWire>> = (0..nw).map(|_| None).collect();
        for w in 0..nw {
            loop {
                match self.w2m().try_read(w) {
                    Some(decoded) => {
                        if decoded.control.status != 0 {
                            let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                            return Err(format!("worker {w}: {msg}"));
                        }
                        results[w] = Some(decoded);
                        break;
                    }
                    None => {
                        self.fail_if_worker_dead("before completing recovery sync")?;
                        let _ = self.w2m().wait_for(w, W2M_SYNC_WAIT_MS);
                    }
                }
            }
        }
        Ok(results)
    }

    pub fn collect_acks(&mut self) -> Result<(), String> {
        self.wait_all_workers()?;
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
    ///   `handle_system_dml` holds the catalog write lock (so the async
    ///   `relay_loop`, which would take the read lock, cannot run and cannot
    ///   deadlock) and drives the backfill/drain inline. `w2m()` resolves
    ///   through the reactor's receiver there (see `set_w2m_receiver_ptr`).
    ///
    /// `checkpoint_allowed`: a backfill may stamp CHECKPOINT to reclaim SAL
    /// space mid-stream (workers re-epoch inline). A drain TICK must NOT —
    /// it carries no backfill pad, so a CHECKPOINT would advance the master
    /// epoch while workers stay on the old one and wedge the cluster; pass
    /// `false` to force CONTINUE.
    #[allow(clippy::needless_range_loop)]
    fn collect_acks_and_relay(&mut self, checkpoint_allowed: bool) -> Result<(), String> {
        let nw = self.num_workers;
        let mut collected = vec![false; nw];
        let mut remaining = nw;
        let mut acc = crate::runtime::reactor::ExchangeAccumulator::new(nw);
        // Armed when a round is stamped CHECKPOINT; the actual SAL reset is
        // deferred to the next round barrier (see the decision block below).
        let mut pending_reset = false;

        while remaining > 0 {
            // One full pass over all workers per iteration. If a pass
            // makes no progress, we wait_for on the first still-active
            // worker. Exchange replies from any worker may trigger
            // further SAL writes + replies, so we loop broadly.
            let mut progressed = false;
            for w in 0..nw {
                if collected[w] {
                    continue;
                }
                let Some(decoded) = self.w2m().try_read(w) else {
                    continue;
                };
                progressed = true;
                if (decoded.control.flags as u32) & FLAG_EXCHANGE != 0 {
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
                        // normal post-backfill checkpoint. Otherwise, when SAL
                        // space is low, stamp CHECKPOINT (continue + tell workers
                        // to re-epoch inline) and arm the reset for the next round
                        // barrier; this round's relay is still written at the high
                        // cursor (a single round fits the 1/8 reserve).
                        let decision = if relay.all_pad {
                            BACKFILL_DECISION_STOP
                        } else if checkpoint_allowed
                            && (!self.sal_relay_space_ok_raw() || Self::inject_backfill_reclaim())
                        {
                            pending_reset = true;
                            BACKFILL_DECISION_CHECKPOINT
                        } else {
                            BACKFILL_DECISION_CONTINUE
                        };
                        let prep = self.prepare_relay(relay)?;
                        self.emit_relay_with_decision(prep, decision)?;
                    }
                } else {
                    if decoded.control.status != 0 {
                        let msg = String::from_utf8_lossy(&decoded.control.error_msg);
                        return Err(format!("worker {w}: {msg}"));
                    }
                    collected[w] = true;
                    remaining -= 1;
                }
            }
            if !progressed {
                self.fail_if_worker_dead("during backfill relay")?;
                // Wait on ALL still-active workers at once: any could be the next to
                // publish, and a single-word wait would miss a wake on a different
                // worker's reader_seq.
                let mut pending = [0usize; crate::runtime::sal::MAX_WORKERS];
                let mut np = 0;
                for w in 0..nw {
                    if !collected[w] {
                        pending[np] = w;
                        np += 1;
                    }
                }
                if np > 0 {
                    let _ = self.w2m().wait_any(&pending[..np], W2M_SYNC_WAIT_MS);
                }
            }
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // SAL Checkpoint
    // -----------------------------------------------------------------------

    /// Invariant: callers must live on a path that owns SAL checkpoint
    /// exclusivity. Today that is the bootstrap backfill, the committer task,
    /// and the reactor-parked stop-the-world CREATE-VIEW backfill
    /// (`handle_system_dml` holds the catalog write lock with the committer
    /// proven idle and the reactor parked, so no other SAL writer exists —
    /// the same exclusivity boot has). The *async* fan-out / tick / steady-state
    /// DDL paths must NOT call this — a concurrent FLAG_FLUSH races the
    /// committer's own and orphans SAL writes straddling `sal.checkpoint_reset`.
    /// See async-invariants.md §III.3a.
    pub(crate) fn maybe_checkpoint(&mut self) -> Result<(), String> {
        if !self.sal.needs_checkpoint() {
            return Ok(());
        }
        self.do_checkpoint()
    }

    fn do_checkpoint(&mut self) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        let lsn = self.next_lsn();
        self.send_broadcast(0, lsn, FLAG_FLUSH, None, &schema, &[], 0, 0, 0)?;
        self.collect_acks()?;
        self.checkpoint_post_ack()
    }

    // -----------------------------------------------------------------------
    // Exchange relay
    // -----------------------------------------------------------------------

    #[cfg(debug_assertions)]
    fn seam_armed_epoch() -> &'static std::sync::atomic::AtomicU32 {
        static ARMED: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(u32::MAX);
        &ARMED
    }

    /// Debug-only backfill seam: when `GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW` is
    /// set, `collect_acks_and_relay` treats SAL space as low on every non-stop
    /// round, forcing a per-round CHECKPOINT + reset. Lets tests exercise the SAL
    /// reclamation protocol (worker re-epoch, master `checkpoint_reset`, epoch
    /// advancing many times) over a small table that would otherwise never
    /// approach the 1 GiB mmap. Release builds compile this to `false`.
    #[cfg(debug_assertions)]
    fn inject_backfill_reclaim() -> bool {
        // The env var can't change mid-boot; read it once. `collect_acks_and_relay`
        // calls this once per round, and the seam's whole purpose is to drive the
        // round count up — so an uncached read would re-allocate per round.
        use std::sync::OnceLock;
        static ARMED: OnceLock<bool> = OnceLock::new();
        *ARMED.get_or_init(|| std::env::var_os("GNITZ_INJECT_BACKFILL_RELAY_SPACE_LOW").is_some())
    }
    #[cfg(not(debug_assertions))]
    fn inject_backfill_reclaim() -> bool {
        false
    }

    /// Raw SAL relay-space threshold: at least 1/8 of the mmap still free.
    /// Seam-free — the boot backfill relay checks this directly because it
    /// must keep failing-on-low-space without observing the relay_loop test
    /// seam (which would spuriously fail an in-progress backfill).
    fn sal_relay_space_ok_raw(&self) -> bool {
        self.sal.mmap_size() - self.sal.cursor() >= (self.sal.mmap_size() >> 3)
    }

    /// True when enough SAL space remains for a relay write (>= 1/8 of the
    /// mmap). Checked *before* consuming a relay so a low-space condition
    /// can be resolved (checkpoint) rather than silently discarding the
    /// relay and deadlocking blocked workers. While the debug seam is armed,
    /// reports low until the next checkpoint bumps the SAL epoch.
    pub(crate) fn sal_has_relay_space(&self) -> bool {
        #[cfg(debug_assertions)]
        if Self::seam_armed_epoch().load(std::sync::atomic::Ordering::Relaxed) == self.sal.epoch() {
            return false;
        }
        self.sal_relay_space_ok_raw()
    }

    /// relay_loop's variant: with GNITZ_INJECT_RELAY_SPACE_LOW set, the first
    /// call arms the seam at the current epoch (one-shot: the CAS from the
    /// u32::MAX sentinel succeeds once per process), then defers to
    /// sal_has_relay_space() so relay_loop and the committer see the same
    /// verdict until a checkpoint bumps the epoch and disarms it.
    pub(crate) fn sal_has_relay_space_arming(&self) -> bool {
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_RELAY_SPACE_LOW").is_ok() {
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
    /// SAL write in `emit_relay`.
    pub(crate) fn prepare_relay(&mut self, relay: PendingRelay) -> Result<RelayPrepared, String> {
        // `all_pad` is the backfill stop signal, read by the caller before
        // `prepare_relay`; the relay scatter itself does not depend on it.
        let PendingRelay {
            view_id,
            payloads,
            schema,
            source_id,
            all_pad: _,
        } = relay;

        let cat = unsafe { &mut *self.catalog };
        // A join-shard scatter (cols from a reindex chain) must route by the
        // reindex key so a row lands on the worker that owns its `_join_pk`
        // partition; a GROUP BY / set-op exchange scatter routes by the group
        // key (consistent with op_reduce's output PK). See `RouteMode`.
        // A join-shard scatter carries (reindex col, carried promotion target tc)
        // pairs; a GROUP BY / set-op scatter carries plain shard cols (no
        // promotion). Split the pairs into a column list + a parallel target-tc
        // list for the scatter packer.
        let (shard_cols, target_tcs, is_join): (Vec<i32>, Vec<u8>, bool) = if source_id > 0 {
            let pairs = cat.dag.get_join_shard_cols(view_id, source_id);
            if pairs.is_empty() {
                (cat.dag.get_shard_cols(view_id), Vec::new(), false)
            } else {
                let cols = pairs.iter().map(|&(c, _)| c).collect();
                let tcs = pairs.iter().map(|&(_, t)| t).collect();
                (cols, tcs, true)
            }
        } else {
            (cat.dag.get_shard_cols(view_id), Vec::new(), false)
        };

        // A range-join INPUT relay (source_id > 0, is_join over a DeltaTraceRange
        // view): `view_range_join_n_eq` reads the equality-conjunct count straight
        // off the join node. The trace-side reindex key is [eq cols…, range col]
        // (len n_eq + 1). A band join (n_eq ≥ 1) scatters by the eq PREFIX — route
        // by the first n_eq slots, dropping the trailing range slot, so equal
        // eq-values co-partition both sides and the range probe is partition-local.
        // A pure range join (n_eq == 0) has no eq prefix: its matches are spread
        // over the whole key space, so it BROADCASTS the full delta and each worker
        // trims to its owned slice (PartitionFilter) before integrating. The output
        // relay (source_id == 0) is NOT a join relay (is_join is false there) and
        // keeps the GroupKey scatter.
        let range_n_eq = if is_join {
            cat.dag.view_range_join_n_eq(view_id)
        } else {
            None
        };

        let dest_batches = if range_n_eq == Some(0) {
            // Pure range join: broadcast the full delta to every worker.
            let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
            op_relay_broadcast(&sources, &schema, self.num_workers)
        } else {
            // Scatter. Band join (range_n_eq == Some(n_eq ≥ 1)): route by the eq
            // prefix shard_cols[..n_eq]. Equi-join: full shard cols. Both
            // JoinPromote. GROUP BY / set-op: full shard cols, GroupKey.
            let route_len = range_n_eq.map_or(shard_cols.len(), |n_eq| n_eq as usize);
            debug_assert!(
                range_n_eq.is_none_or(|n_eq| shard_cols.len() == n_eq as usize + 1),
                "range-join reindex key = [eq…, range]: len must be n_eq + 1"
            );
            let col_indices: Vec<u32> = shard_cols[..route_len].iter().map(|&c| c as u32).collect();
            // target_tcs is EMPTY for a GroupKey scatter (no promotion) and has
            // length shard_cols.len() for any join; slice it to the routing prefix
            // when promoting, empty otherwise.
            let route_tcs: &[u8] = if is_join { &target_tcs[..route_len] } else { &[] };
            let mode = if is_join {
                RouteMode::JoinPromote
            } else {
                RouteMode::GroupKey
            };
            let consolidated_sources: Option<Vec<Option<&ConsolidatedBatch>>> = payloads
                .iter()
                .map(|opt| match opt {
                    None => Some(None),
                    Some(b) => ConsolidatedBatch::from_batch_ref(b).map(Some),
                })
                .collect();
            match consolidated_sources {
                Some(sources) => op_relay_scatter_consolidated_mode(
                    &sources,
                    &col_indices,
                    route_tcs,
                    &schema,
                    self.num_workers,
                    mode,
                ),
                None => {
                    let sources: Vec<Option<&Batch>> = payloads.iter().map(|o| o.as_ref()).collect();
                    op_repartition_batches_mode(&sources, &col_indices, route_tcs, &schema, self.num_workers, mode)
                }
            }
        };

        let (_, name_bytes) = self.get_schema_and_names(view_id);

        Ok(RelayPrepared {
            view_id,
            source_id,
            dest_batches,
            schema,
            name_bytes,
        })
    }

    /// Synchronous second half of a steady-state relay: writes the
    /// FLAG_EXCHANGE_RELAY group to SAL and signals workers, with no backfill
    /// coordination. Caller holds `sal_writer_excl` for the duration; no awaits
    /// inside. CONTINUE == 0 is the value a non-backfill relay's `seek_col_idx`
    /// has always carried, so this is byte-identical to the pre-backfill relay.
    pub(crate) fn emit_relay(&mut self, prep: RelayPrepared) -> Result<(), String> {
        self.emit_relay_with_decision(prep, BACKFILL_DECISION_CONTINUE)
    }

    /// As `emit_relay`, but stamps the boot backfill round `decision` (a
    /// `BACKFILL_DECISION_*`) onto the relay's `seek_col_idx`. Only the boot
    /// backfill collect-loop (`collect_acks_and_relay`) needs the non-CONTINUE
    /// values; the steady-state reactor path goes through `emit_relay`.
    pub(crate) fn emit_relay_with_decision(&mut self, prep: RelayPrepared, decision: u64) -> Result<(), String> {
        let RelayPrepared {
            view_id,
            source_id,
            dest_batches,
            schema,
            name_bytes,
        } = prep;
        let refs: Vec<Option<&Batch>> = dest_batches
            .iter()
            .map(|b| if b.count > 0 { Some(b) } else { None })
            .collect();
        let lsn = self.next_lsn();
        // Echo `source_id` back via `seek_pk` so the worker's `do_exchange_wait`
        // can match on (view_id, source_id). Without this, a multi-source view
        // (join over 2+ tables) can deliver the wrong source's relay to a
        // waiting exchange and the worker demuxes against the wrong sharding
        // columns. `seek_col_idx` carries the backfill round `decision`
        // (BACKFILL_DECISION_*); CONTINUE == 0 is the steady-state value, so a
        // non-backfill relay is byte-identical to before.
        self.send_to_workers(
            view_id,
            lsn,
            FLAG_EXCHANGE_RELAY,
            &refs,
            &schema,
            &name_bytes,
            source_id as u128,
            decision,
            0,
        )
    }

    fn record_index_routing(
        &mut self,
        target_id: i64,
        schema: &SchemaDescriptor,
        source_batch: &Batch,
        worker_indices: &[Vec<u32>],
    ) {
        let cat = unsafe { &mut *self.catalog };
        let n_idx = cat.get_index_circuit_count(target_id);
        if n_idx == 0 {
            return;
        }

        for ci in 0..n_idx {
            // The routing cache is keyed (table, col, u128); only SINGLE-COLUMN
            // unique circuits populate it. A composite unique seek
            // broadcasts-and-merges instead — widening the cache's unbounded
            // per-distinct-value map to composite `PkBuf` keys would grow every
            // entry ~5× for the dominant single-column population — so its
            // routing is never recorded.
            let col_idx = match cat.unique_index_circuit_cols(target_id, ci) {
                Some(c) if c.len() == 1 => c[0],
                _ => continue,
            };
            for (w, wi) in worker_indices[..self.num_workers].iter().enumerate() {
                if !wi.is_empty() {
                    self.router.record_routing_from_source(
                        source_batch,
                        wi,
                        schema,
                        target_id as u32,
                        col_idx,
                        w as u32,
                    );
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Fan-out operations
    // -----------------------------------------------------------------------

    /// Distributed backfill from `source_id`. `view_id` selects the scope,
    /// carried to the worker in the frame's `seek_pk`:
    /// - `view_id == 0` — boot mode: drive `source_id`'s whole dependent
    ///   closure (every view starts empty at boot, so this is correct and
    ///   re-derives each view once).
    /// - `view_id != 0` — live CREATE-VIEW mode: drive ONLY that view. The
    ///   source already has populated existing dependents that a closure
    ///   re-drive would double-count.
    ///
    /// May `maybe_checkpoint` to reclaim SAL space before a large source; the
    /// collect loop may further CHECKPOINT mid-stream. Both are safe on the
    /// SAL-exclusive, no-concurrent-relay paths this runs on (boot; the
    /// reactor-parked DDL window).
    pub fn fan_out_backfill(&mut self, view_id: i64, source_id: i64) -> Result<(), String> {
        self.maybe_checkpoint()?;
        let (schema, col_names) = self.get_schema_and_names(source_id);
        let lsn = self.next_lsn();
        self.send_broadcast(
            source_id,
            lsn,
            FLAG_BACKFILL,
            None,
            &schema,
            &col_names,
            view_id as u128,
            0,
            0,
        )?;
        self.collect_acks_and_relay(true)
    }

    /// Synchronously drain one source's pending ticks during the reactor-parked
    /// CREATE-VIEW window: emit a FLAG_TICK, signal, and collect each worker's
    /// ACK while relaying its exchange dependents inline. The `handle_system_dml`
    /// caller holds the catalog write lock and the tick gate with the committer
    /// idle, so the async `relay_loop` cannot run (no deadlock) and no other tick
    /// races this. Checkpointing is forced off: a tick carries no backfill pad,
    /// so a CHECKPOINT verdict would advance only the master's epoch and wedge
    /// the cluster (see `collect_acks_and_relay`).
    pub(crate) fn drain_tick_blocking(&mut self, source_id: i64) -> Result<(), String> {
        let nw = self.num_workers;
        // The synchronous collect reads W2M rings directly and routes neither by
        // req_id; any value works. (Boot's `fan_out_backfill` likewise passes 0.)
        let req_ids = [0u64; crate::runtime::sal::MAX_WORKERS];
        let lsn = self.next_lsn();
        self.write_tick_group(source_id, lsn, &req_ids[..nw])?;
        self.signal_all();
        self.collect_acks_and_relay(false)
    }

    // Async fan-outs take `*mut Self` instead of `&mut self` because the
    // exclusive borrow must end before `.await`: other reactor tasks
    // re-enter the dispatcher in the meantime. Only call from inside a
    // reactor task driven by `block_until_idle`.

    pub async fn fan_out_seek_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<W2mSlot, String> {
        let num_workers = unsafe { (*disp_ptr).num_workers };
        let schema = unsafe {
            (*(*disp_ptr).catalog)
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("seek: table {target_id} not found"))?
        };
        // `stride` (full PK width) drives `assemble_wide_pk`'s reassembly; the
        // shared `partition_for_pk` then selects the worker off the distribution
        // prefix. A FLAG_SEEK always carries the full PK and the prefix ⊆ the PK,
        // so a full-PK seek still pins exactly one worker — no broadcast clause.
        let stride = schema.pk_stride() as usize;
        let worker = if schema.pk_is_wide() {
            let full_pk_buf = crate::schema::assemble_wide_pk(&schema, pk, seek_pk_extra, stride)
                .map_err(|e| format!("seek: table {target_id}: {e}"))?;
            worker_for_partition(schema.partition_for_pk(&full_pk_buf), num_workers)
        } else {
            // Narrow path: `pk` is the native seek key, but ingestion routes on the
            // OPK bytes (partition_for_key(get_pk) == partition_for_pk_bytes(opk)).
            // Encode native → OPK and route those bytes; hashing the native value
            // misroutes signed/compound narrow PKs.
            let (opk, _) = crate::storage::opk_key(&schema, pk);
            worker_for_partition(schema.partition_for_pk(&opk), num_workers)
        };
        single_worker_async(
            disp_ptr,
            reactor,
            sal_excl,
            target_id,
            FLAG_SEEK,
            worker,
            pk,
            0,
            "seek",
            seek_pk_extra,
        )
        .await
    }

    pub async fn fan_out_seek_by_index_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        col_idx: u32,
        key: u128,
    ) -> Result<W2mSlot, String> {
        // The routing cache is keyed by `extract_col_key` (OPK-widened for
        // integer columns, XXH3 for STRING/BLOB), but `key` is the native seek
        // value. Transform it into the stored representation before probing;
        // a raw native query always misses for signed integers (and could
        // spuriously hit a different value's OPK-widened key).
        let cached = unsafe {
            let schema = (*(*disp_ptr).catalog).get_schema_desc(target_id);
            match schema.and_then(|s| index_route_key(&s, col_idx, key)) {
                Some(rk) => (*disp_ptr).router.worker_for_index_key(target_id as u32, col_idx, rk),
                None => -1,
            }
        };
        if cached >= 0 {
            let worker = cached as usize;
            return single_worker_async(
                disp_ptr,
                reactor,
                sal_excl,
                target_id,
                FLAG_SEEK_BY_INDEX,
                worker,
                key,
                col_idx as u64,
                "seek_by_index",
                &[],
            )
            .await;
        }

        // Cache miss: broadcast to all workers with per-worker req_ids and
        // forward the slot whose worker found a row (or slot 0 if none).
        // `_lease` keeps the scan active across the single-frame inspection
        // below; its workers also stream, so dropping it early would let the
        // gate discard a late frame.
        let (mut slots, _req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            disp.write_group_with_req_ids(
                target_id,
                lsn,
                FLAG_SEEK_BY_INDEX,
                0,
                &[],
                &schema,
                &col_names,
                key,
                col_idx as u64,
                req_ids,
                -1,
                0,
                None,
                &[],
            )
        })
        .await?;

        let mut data_idx = None;
        for (w, slot) in slots.iter().enumerate() {
            let ctrl = peek_control_block(slot.bytes()).map_err(|e| format!("seek_by_index: worker {w}: {e}"))?;
            if ctrl.status != 0 {
                return Err(format!(
                    "worker {}: seek_by_index: {}",
                    w,
                    String::from_utf8_lossy(&ctrl.error_msg)
                ));
            }
            // This path inspects-and-forwards exactly one slot per worker; a
            // chunked train here would be forwarded truncated, its remainder
            // silently discarded by the lease drop. A unique single-column
            // seek returns at most one row, so a train means the invariant
            // broke (e.g. a shrunken GNITZ_REPLY_FRAME_BUDGET) — fail loudly.
            if ctrl.flags & FLAG_CONTINUATION != 0 {
                return Err(format!(
                    "worker {w}: seek_by_index: unexpected chunked reply on a \
                     single-frame path"
                ));
            }
            if ctrl.flags & FLAG_HAS_DATA != 0 {
                data_idx = Some(w);
            }
        }
        Ok(slots.swap_remove(data_idx.unwrap_or(0)))
    }

    /// SELECT-path index lookup: broadcast to ALL workers and MERGE every
    /// matching base row into one batch.
    ///
    /// A non-unique indexed value matches rows scattered across workers (the
    /// per-key routing cache is only populated for unique indexes), so unlike
    /// `fan_out_seek_by_index_async` (which returns a single worker's slot, used
    /// by the UPSERT identity check where the index is unique) this must
    /// aggregate. Returns the merged base rows, or `None` when no row matches.
    pub async fn fan_out_seek_by_index_collect_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        seek_col_idx: u64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<Option<Batch>, String> {
        Self::fan_out_index_collect_common(
            disp_ptr,
            reactor,
            sal_excl,
            target_id,
            FLAG_SEEK_BY_INDEX,
            seek_pk,
            seek_col_idx,
            seek_pk_extra,
            "seek_by_index",
        )
        .await
    }

    /// SELECT-path ordered range scan over a secondary index: broadcast the range
    /// descriptor to ALL workers and MERGE every matching base row into one batch.
    ///
    /// Differs from `fan_out_seek_by_index_collect_async` only in the
    /// master→worker leg: the `u32` SAL dispatch flag
    /// `FLAG_SEEK_BY_INDEX_RANGE_SAL` (so the worker classifies it as
    /// `SeekByIndexRange`, not a point seek), and the descriptor riding
    /// `seek_pk_extra` (arbitrary length — this leg is not `PkTuple`-bound; the
    /// worker is the sole OPK encoder, so the descriptor is forwarded verbatim).
    /// A range's matches scatter by source PK, so broadcast-and-merge is the
    /// correct, in-tree mechanism — no range-aware exchange is needed.
    pub async fn fan_out_seek_by_index_range_collect_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        seek_col_idx: u64,
        seek_pk_extra: &[u8],
    ) -> Result<Option<Batch>, String> {
        Self::fan_out_index_collect_common(
            disp_ptr,
            reactor,
            sal_excl,
            target_id,
            FLAG_SEEK_BY_INDEX_RANGE_SAL,
            0,
            seek_col_idx,
            seek_pk_extra,
            "seek_by_index_range",
        )
        .await
    }

    /// Shared skeleton of the two broadcast-and-merge index seeks above:
    /// fan one frame out to ALL workers under `sal_flag` and merge every
    /// worker's matching base rows into one batch via the train drain
    /// (an oversized worker reply arrives as a chunked train; a single-frame
    /// reply is a length-1 train).
    ///
    /// The master forwards the client's wire payload verbatim — it never
    /// decodes seek_pk_extra into u128s and re-encodes them (the worker is
    /// the sole OPK encoder). seek_col_idx carries pack_pk_cols(col_indices),
    /// already validated by the caller.
    /// `_lease` held across the full drain: its workers stream, so releasing
    /// the gate before every train is consumed (or the drain errors and the
    /// lease drop discards the rest) risks a discarded late frame.
    #[allow(clippy::too_many_arguments)]
    async fn fan_out_index_collect_common(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        sal_flag: u32,
        seek_pk: u128,
        seek_col_idx: u64,
        seek_pk_extra: &[u8],
        op: &str,
    ) -> Result<Option<Batch>, String> {
        // `expected` is captured inside the fan-out closure so the reply
        // guard is definitionally the schema the request was built from; a
        // separate pre-fanout catalog read could diverge across the
        // `sal_excl` await if a DDL interleaves, failing healthy replies.
        let mut expected: Option<SchemaDescriptor> = None;
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            expected = Some(schema);
            disp.write_group_with_req_ids(
                target_id,
                lsn,
                sal_flag,
                0,
                &[],
                &schema,
                &col_names,
                seek_pk,
                seek_col_idx,
                req_ids,
                -1,
                0,
                None,
                seek_pk_extra,
            )
        })
        .await?;
        let expected = expected.expect("fan-out closure ran");

        let mut acc: Option<Batch> = None;
        let mut merged_bytes = 0usize;
        drain_index_scan(slots, &req_ids, reactor, op, &expected, |mb, frame_len| {
            // Σ frame bytes ≥ the merged single-frame encode size (every frame
            // re-counts its header and the first one the schema block), so this
            // cap can never let a reply through that the client would reject
            // (MAX_FRAME_PAYLOAD_CLIENT) — and it bounds the master's merge heap.
            merged_bytes += frame_len;
            if merged_bytes > gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT {
                return Err(format!(
                    "{op}: result exceeds the {} MiB reply cap; add a tighter \
                     predicate or LIMIT",
                    gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT >> 20
                ));
            }
            let a = acc.get_or_insert_with(|| Batch::with_schema(expected, mb.count));
            a.append_mem_batch_range(mb, 0, mb.count, crate::storage::WeightFill::Copy);
            Ok(())
        })
        .await?;
        // The sink runs only for non-empty frames, so `Some` implies rows.
        Ok(acc)
    }

    /// Resolve the committed holder of a unique index value: seek the index by
    /// the value's native per-column keys and return the holder's source PK (or
    /// `None`). Used by the UPSERT verify, which must confirm a colliding
    /// committed value is held by the same row (or a row releasing it in this
    /// batch); the caller decodes the OPK span to `natives` via
    /// `span_to_natives`.
    ///
    /// Arity gates the routing: a single-column unique seek keeps the unicast
    /// routing-cache fast path (the common same-PK upsert whose value is
    /// unchanged lands here, so it is hot); a composite unique seek
    /// broadcasts-and-merges (the routing cache stays single-column — see
    /// `record_index_routing`), with the trailing native values riding
    /// `seek_pk_extra` as 16-byte LE slots, the exact wire form the worker's
    /// SeekByIndex handler reassembles. A unique index yields at most one
    /// holder, so merging one row is correct.
    pub(super) async fn seek_unique_holder(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        col_indices: PkColList,
        natives: [u128; gnitz_wire::PK_LIST_MAX_COLS],
    ) -> Result<Option<PkBuf>, String> {
        let cols = col_indices.as_slice();
        if cols.len() == 1 {
            // single-column unique: unicast to the one owning worker (routing cache).
            let slot =
                Self::fan_out_seek_by_index_async(disp_ptr, reactor, sal_excl, target_id, cols[0], natives[0]).await?;
            let ctrl = peek_control_block(slot.bytes()).map_err(|e| e.to_string())?;
            if ctrl.flags & FLAG_HAS_DATA == 0 {
                return Ok(None);
            }
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl.block_size, ctrl, None)
                .map_err(|e| e.to_string())?;
            Ok(zc
                .data_batch
                .filter(|b| b.count > 0)
                .map(|b| PkBuf::from_bytes(b.get_pk_bytes(0))))
        } else {
            // composite unique: broadcast-and-merge. natives[0] → seek_pk;
            // natives[1..] → 16-byte LE slots in seek_pk_extra.
            let mut extra = [0u8; (gnitz_wire::PK_LIST_MAX_COLS - 1) * 16];
            for (slot, &v) in extra.chunks_exact_mut(16).zip(&natives[1..cols.len()]) {
                slot.copy_from_slice(&v.to_le_bytes());
            }
            let batch = Self::fan_out_seek_by_index_collect_async(
                disp_ptr,
                reactor,
                sal_excl,
                target_id,
                gnitz_wire::pack_pk_cols(cols),
                natives[0],
                &extra[..(cols.len() - 1) * 16],
            )
            .await?;
            Ok(batch.map(|b| PkBuf::from_bytes(b.get_pk_bytes(0))))
        }
    }

    /// Fan out a SCAN to all workers, forward every response frame directly
    /// to `fd` (including continuation chunks), and return `Ok(true)` when
    /// all workers finish. Returns `Ok(false)` if the client disconnects
    /// mid-stream; returns `Err` on a worker error.
    ///
    /// Each slot is dropped (advancing `consume_cursor`) before the next
    /// continuation frame is awaited to prevent W2M ring deadlock.
    pub async fn fan_out_scan_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        client_id: u64,
        fd: i32,
        client_version: u16,
    ) -> Result<bool, String> {
        // `_lease` held across the entire continuation drain: every worker
        // streams a multi-frame train, and a cancelled drain (client
        // disconnect) must keep the ids active until the lease drops so the
        // gate discards — not parks — late frames.
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, req_ids| {
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let lsn = disp.next_lsn();
            // Embed client_version in wire_flags bits 24-39 so workers can
            // decide whether to include the schema block in their response.
            let wire_flags = gnitz_wire::wire_flags_set_schema_version(0, client_version);
            disp.write_group_with_req_ids(
                target_id,
                lsn,
                0,
                wire_flags,
                &[],
                &schema,
                &col_names,
                0,
                0,
                req_ids,
                -1,
                client_id,
                None,
                &[],
            )
        })
        .await?;

        // Return on the FIRST worker fault, decode error, or client
        // disconnect: `_lease` drops on return and `route_scan_slot` discards
        // every undrained frame at the ring boundary, advancing
        // `consume_cursor`, so a still-streaming worker cannot wedge in
        // `send_encoded` — draining the doomed trains would be pure waste. On
        // a fault the client sees its data frames followed by a STATUS_ERROR
        // frame, which `recv_scan_response` handles mid-stream.
        for (w, slot) in slots.into_iter().enumerate() {
            if !drain_scan_train(reactor, fd, slot, req_ids[w] as u32, w).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Single-source SCAN: send the SCAN to **one** worker and forward its
    /// continuation train to the client. Used for a **replicated** relation,
    /// whose full copy lives on every worker — `fan_out_scan_async` would
    /// concatenate W identical copies. Worker 0 always exists, and replicated
    /// tables are exempt from the bootstrap trim, so it holds the full copy at
    /// partition 0. Same train/lease contract as `fan_out_scan_async`; the only
    /// differences are the unicast write and draining a single worker's train.
    #[allow(clippy::too_many_arguments)]
    pub async fn fan_out_scan_single_worker_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        worker: usize,
        client_id: u64,
        fd: i32,
        client_version: u16,
    ) -> Result<bool, String> {
        let req_id = {
            let _guard = sal_excl.lock().await;
            unsafe {
                let disp = &mut *disp_ptr;
                let (schema, col_names) = disp.get_schema_and_names(target_id);
                let req_id = reactor.alloc_scan_request_id();
                let lsn = disp.next_lsn();
                let wire_flags = gnitz_wire::wire_flags_set_schema_version(0, client_version);
                let nw = disp.num_workers;
                // write_group_direct keys replies by worker slot; under unicast
                // only `worker`'s slot is written and replies, all on this req_id.
                let ids = [req_id; crate::runtime::sal::MAX_WORKERS];
                disp.write_group_with_req_ids(
                    target_id,
                    lsn,
                    0,
                    wire_flags,
                    &[],
                    &schema,
                    &col_names,
                    0,
                    0,
                    &ids[..nw],
                    worker as i32,
                    client_id,
                    None,
                    &[],
                )?;
                disp.signal_one(worker);
                req_id
            }
        };
        // Hold the scan id active across the whole continuation drain (see
        // `fan_out_scan_async`): a cancelled drain must keep the id active so the
        // gate discards — not parks — late frames.
        let _lease = reactor.scan_lease(&[req_id as u32]);
        let slot = reactor.await_scan_slot(req_id as u32).await;
        drain_scan_train(reactor, fd, slot, req_id as u32, worker).await
    }

    /// Async version of `execute_pipeline`. Writes each check with
    /// per-worker req_ids, signals once, and joins all replies.
    ///
    /// `sal_excl` is held only for the synchronous write + signal phase;
    /// see `dispatch_scan_fanout` for the rationale.
    pub(super) async fn execute_pipeline_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        checks: &mut [PipelinedCheck],
    ) -> Result<Vec<FxHashSet<PkBuf>>, String> {
        let num_checks = checks.len();
        if num_checks == 0 {
            return Ok(Vec::new());
        }

        let (nw, all_req_ids): (usize, Vec<u64>) = {
            let _guard = sal_excl.lock().await;
            unsafe {
                let disp = &mut *disp_ptr;
                let nw = disp.num_workers;
                let mut rids: Vec<u64> = Vec::with_capacity(num_checks * nw);
                for _ in 0..(num_checks * nw) {
                    rids.push(reactor.alloc_request_id());
                }
                for (idx, check) in checks.iter().enumerate() {
                    let lsn = disp.next_lsn();
                    let req_slice = &rids[idx * nw..(idx + 1) * nw];
                    match check.payload.as_ref().expect("payload consumed") {
                        CheckPayload::Broadcast(batch) => {
                            let refs: Vec<Option<&Batch>> = (0..nw).map(|_| Some(batch)).collect();
                            disp.write_group_with_req_ids(
                                check.target_id,
                                lsn,
                                check.flags,
                                0,
                                &refs,
                                &check.schema,
                                &[],
                                0,
                                check.col_hint,
                                req_slice,
                                -1,
                                0,
                                None,
                                &[],
                            )?;
                        }
                        CheckPayload::ScatterSource { source } => {
                            // Routing is always by the schema PK; compute the
                            // per-worker indices on the fly. No reentrancy: this
                            // loop body has no `.await`, so the SCATTER_INDICES
                            // borrow is released before the next iteration.
                            let pk_cols = check.schema.pk_indices();
                            with_worker_indices(source, pk_cols, &check.schema, nw, |worker_indices| {
                                disp.sal.scatter_wire_group(
                                    source,
                                    worker_indices,
                                    &check.schema,
                                    None,
                                    check.target_id as u32,
                                    lsn,
                                    check.flags,
                                    0,
                                    0,
                                    check.col_hint,
                                    req_slice,
                                    -1,
                                    None,
                                    None,
                                )
                            })?;
                        }
                    }
                }
                disp.signal_all();
                (nw, rids)
            }
        };

        let decoded_vec: Vec<DecodedWire> =
            crate::runtime::reactor::join_all_unpin(all_req_ids.iter().map(|&id| reactor.await_reply(id))).await;

        let mut results: Vec<FxHashSet<PkBuf>> = checks
            .iter()
            .map(|check| {
                let cap = match check.payload.as_ref().expect("payload consumed") {
                    CheckPayload::Broadcast(b) => b.count,
                    CheckPayload::ScatterSource { source } => source.count,
                };
                FxHashSet::with_capacity_and_hasher(cap, Default::default())
            })
            .collect();

        if let Some(err) = first_worker_error("pipeline", &decoded_vec) {
            return Err(err);
        }
        for check_idx in 0..num_checks {
            for w in 0..nw {
                let decoded = &decoded_vec[check_idx * nw + w];
                if let Some(ref batch) = decoded.data_batch {
                    for j in 0..batch.count {
                        if batch.get_weight(j) == 1 {
                            results[check_idx].insert(PkBuf::from_bytes(batch.get_pk_bytes(j)));
                        }
                    }
                }
            }
        }
        Ok(results)
    }

    /// Batched stored-row gather. Scatters `pks` to their owning workers (one
    /// group, partitioned by the parent PK columns so each worker only reads
    /// rows it stores), each worker reads the committed rows for its PKs and
    /// replies with them projected to `project` (the referenced parent column
    /// indices). Returns a `pk → projected values` map: each value is the
    /// promoted index key, or `None` for a NULL referenced value; PKs whose
    /// committed row is absent are omitted entirely. Each row's values are
    /// aligned to `project`.
    ///
    /// This is the `O(num_workers)`-round-trip replacement for the per-row
    /// serial single-key seek loop used by FK RESTRICT on non-PK UNIQUE
    /// targets. It is a sibling of `execute_pipeline_async` (which returns only
    /// existence) rather than a modification of it: the has-pk pipeline echoes
    /// the caller's payload (`filter_by_pk`), so it structurally cannot return
    /// a stored column the caller does not already hold.
    ///
    /// Replies arrive as reply trains (an oversized gather reply chunks; a
    /// single-frame reply is a length-1 train), so the fan-out uses scan
    /// request ids and the train drain. The expected projected schema guards
    /// each train's first frame — a worker whose catalog lags a DDL would
    /// otherwise hand back rows the master mis-decodes.
    pub(super) async fn execute_gather_async(
        disp_ptr: *mut MasterDispatcher,
        reactor: &crate::runtime::reactor::Reactor,
        sal_excl: &Rc<AsyncMutex<()>>,
        target_id: i64,
        mut pks: Vec<PkBuf>,
        project: &[u8],
    ) -> Result<GatherMap, String> {
        if pks.is_empty() {
            return Ok(GatherMap::default());
        }
        // Sort so each worker's sublist reaches `gather_family` ascending:
        // `removed`/updated PKs are extracted from an FxHashMap (arbitrary
        // order) and `scatter_wire_group` preserves per-worker relative order,
        // so a globally sorted input yields per-worker-sorted sublists.
        pks.sort_unstable_by(|a, b| a.pk_bytes().cmp(b.pk_bytes()));
        let col_mask = pack_gather_cols(project).ok_or("gather: more than 8 projected columns")?;

        let parent_schema = unsafe {
            (&*(*disp_ptr).catalog)
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("gather: no schema for table {target_id}"))?
        };
        // The exact constructor the worker uses for its reply schema, so a
        // matching reply validates by construction.
        let expected = crate::catalog::project_schema(&parent_schema, project);

        // `_lease` held across the full drain below (see `dispatch_scan_fanout`).
        let (slots, req_ids, _lease) = dispatch_scan_fanout(disp_ptr, reactor, sal_excl, |disp, rids| {
            let nw = disp.num_workers;
            let pk_cols = parent_schema.pk_indices();
            let pooled = disp.pool_pop_batch(target_id);
            let batch = build_check_batch_pkbuf(&parent_schema, &pks, pooled);
            let lsn = disp.next_lsn();
            with_worker_indices(&batch, pk_cols, &parent_schema, nw, |worker_indices| {
                disp.sal.scatter_wire_group(
                    &batch,
                    worker_indices,
                    &parent_schema,
                    None,
                    target_id as u32,
                    lsn,
                    FLAG_GATHER,
                    /* wire_flags */ 0,
                    /* seek_pk */ 0,
                    /* seek_col_idx */ col_mask,
                    rids,
                    /* unicast_worker */ -1,
                    None,
                    None,
                )
            })?;
            // The scatter batch is fully consumed by the synchronous
            // scatter_wire_group above; return it to the pool.
            recycle_check_batch(disp, target_id, batch);
            Ok(())
        })
        .await?;

        // Precompute (type_code, col_size) per projected column from the parent
        // schema; the reply's projected payload index k corresponds to project[k].
        let proj_meta: Vec<(u8, usize)> = project
            .iter()
            .map(|&p| {
                let col = parent_schema.columns[p as usize];
                (col.type_code, col.size() as usize)
            })
            .collect();

        let mut out = GatherMap::new(proj_meta.len());
        drain_index_scan(slots, &req_ids, reactor, "gather", &expected, |b, _| {
            // The column slices are invariant across a frame's rows; derive
            // each once per frame instead of once per (row × column). The
            // arity is hard-capped by `pack_gather_cols` (one u8 per u64 byte).
            let mut col_slices: [&[u8]; 8] = [&[]; 8];
            for (k, &(_, col_size)) in proj_meta.iter().enumerate() {
                col_slices[k] = b.col_data(k, col_size);
            }
            for j in 0..b.count {
                let null_word = b.get_null_word(j);
                out.push_row(
                    PkBuf::from_bytes(b.get_pk_bytes(j)),
                    proj_meta.iter().enumerate().map(|(k, &(col_type, col_size))| {
                        if null_word & (1u64 << k) != 0 {
                            None
                        } else {
                            Some(payload_native_key(col_slices[k], j * col_size, col_size, col_type))
                        }
                    }),
                );
            }
            Ok(())
        })
        .await?;
        Ok(out)
    }

    /// Broadcast a DDL batch to every worker. `lsn` is supplied by the
    /// caller — Phase 3 uses one zone-LSN across all broadcasts in a DDL
    /// so recovery can group them as an atomic zone.
    pub fn broadcast_ddl(&mut self, target_id: i64, batch: &Batch, lsn: u64) -> Result<(), String> {
        let (schema, schema_block, _safe, _stride) = self.cached_schema_block(target_id);
        self.write_broadcast(
            target_id,
            lsn,
            FLAG_DDL_SYNC,
            Some(batch),
            &schema,
            &[],
            0,
            0,
            0,
            Some(schema_block.as_slice()),
        )?;
        self.signal_all();
        gnitz_debug!("broadcast_ddl tid={} rows={} lsn={}", target_id, batch.count, lsn);
        Ok(())
    }

    /// Close an atomic zone at `lsn`: write the empty commit sentinel
    /// and signal workers. All preceding groups at this LSN belong to
    /// the zone; recovery applies them only when this sentinel reaches
    /// disk before the crash.
    pub fn commit_zone(&mut self, lsn: u64) -> Result<(), String> {
        self.sal.write_commit_sentinel(lsn)?;
        self.signal_all();
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tick group writer (used by the async tick task in executor.rs)
    // -----------------------------------------------------------------------

    /// Write a FLAG_TICK group for `tid` with per-worker req_ids. Does
    /// NOT signal — the caller batches multiple `write_tick_group` calls
    /// followed by a single `signal_all` (IV.6). The underlying SAL
    /// encoder reuses `write_group_with_req_ids`; per-worker slots all
    /// carry the corresponding req_id from `req_ids[w]`. `lsn` is
    /// supplied by the caller.
    pub(crate) fn write_tick_group(&mut self, tid: i64, lsn: u64, req_ids: &[u64]) -> Result<(), String> {
        let (schema, schema_block, _safe, _stride) = self.cached_schema_block(tid);
        self.write_group_with_req_ids(
            tid,
            lsn,
            FLAG_TICK,
            0,
            &[],
            &schema,
            &[],
            0,
            0,
            req_ids,
            -1,
            0,
            Some(schema_block.as_slice()),
            &[],
        )
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    pub fn check_workers(&mut self) -> i32 {
        // Probe each worker by its own pid, not `waitpid(-1)`. A per-pid
        // `waitpid` returns ECHILD — a detected death — even if the zombie was
        // reaped elsewhere, whereas `waitpid(-1)` returns 0 ("some child is
        // alive") and silently misses one worker's death while others run, so it
        // would go blind the moment a SIGCHLD/signalfd reaper or SA_NOCLDWAIT is
        // ever added. It also names the exact dead worker for the error/log.
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid <= 0 {
                continue;
            }
            let mut status: i32 = 0;
            loop {
                let rpid = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
                if rpid > 0 {
                    self.worker_pids[w] = 0; // reaped — never waitpid it again
                    return w as i32;
                }
                if rpid == 0 {
                    break; // still running
                }
                // rpid == -1
                let err = crate::foundation::syscall::errno();
                if err == libc::EINTR {
                    continue; // signal, not death — retry
                }
                if err == libc::ECHILD {
                    self.worker_pids[w] = 0; // already gone
                    return w as i32;
                }
                break; // unexpected errno: treat as alive
            }
        }
        -1
    }

    pub fn shutdown_workers(&mut self) {
        let schema = SchemaDescriptor::minimal_u64();
        let lsn = self.next_lsn();
        let _ = self.send_broadcast(0, lsn, FLAG_SHUTDOWN, None, &schema, &[], 0, 0, 0);
        for w in 0..self.num_workers {
            let pid = self.worker_pids[w];
            if pid > 0 {
                let mut status: i32 = 0;
                unsafe {
                    libc::waitpid(pid, &mut status, 0);
                }
            }
        }
        // All workers reaped: no process can race a removal. Reclaim any dirs
        // still gated (dropped entities whose gating checkpoint never arrived).
        unsafe { &mut *self.catalog }.drain_checkpoint_gated_deletions();
    }

    /// Commit N push batches as a single SAL group write. Called from
    /// the committer task. Returns (groups, req_ids, fsync_id) — the
    /// caller awaits fsync + per-worker req_ids separately so they can
    /// `join!` them. `lsn` is supplied by the caller.
    pub(crate) fn write_commit_group(
        &mut self,
        target_id: i64,
        lsn: u64,
        batch: &Batch,
        mode: WireConflictMode,
        req_ids: &[u64],
    ) -> Result<(), String> {
        let (schema, schema_block, wire_safe, wire_row_stride) = self.cached_schema_block(target_id);
        let nw = self.num_workers;
        let pk_col = schema.pk_indices();
        let wire_flags = wire_flags_set_conflict_mode(0, mode);
        // Identical scatter for both routings; only the per-worker index fill
        // differs (full broadcast vs PK-partitioned). One `scatter_wire_group`
        // call site keeps the atomic-zone framing, LSN, ACK accounting, and the
        // committer's single `fdatasync` shared between them.
        let scatter = |worker_indices: &[Vec<u32>]| {
            self.record_index_routing(target_id, &schema, batch, worker_indices);
            self.sal.scatter_wire_group(
                batch,
                worker_indices,
                &schema,
                None,
                target_id as u32,
                lsn,
                FLAG_PUSH,
                wire_flags,
                0,
                0,
                req_ids,
                -1,
                Some(schema_block.as_slice()),
                Some((wire_safe, wire_row_stride)),
            )
        };
        if schema.replicated() {
            // Replicated: the whole batch lands in every worker's ingest + SAL
            // slot, so each worker durably logs the full table and enforces
            // uniqueness against its identical full copy.
            with_broadcast_indices(batch, nw, scatter)
        } else {
            with_worker_indices(batch, pk_col, &schema, nw, scatter)
        }
    }

    /// Write a FLAG_FLUSH checkpoint group with per-worker req_ids.
    /// Does NOT sync/signal. Caller signals + awaits replies. `lsn` is
    /// supplied by the caller.
    pub(crate) fn write_checkpoint_group(&mut self, lsn: u64, req_ids: &[u64]) -> Result<(), String> {
        let schema = SchemaDescriptor::minimal_u64();
        // One "slot" per worker with empty batch — each worker replies
        // after flushing its system tables and advancing its epoch.
        let refs: Vec<Option<&Batch>> = (0..self.num_workers).map(|_| None).collect();
        self.write_group_with_req_ids(
            0,
            lsn,
            FLAG_FLUSH,
            0,
            &refs,
            &schema,
            &[],
            0,
            0,
            req_ids,
            -1,
            0,
            None,
            &[],
        )
    }

    /// Post-ACK checkpoint cleanup: flush system tables before resetting
    /// the SAL cursor (their data lives in SAL entries about to be
    /// discarded), then advance the epoch. Called by both the bootstrap
    /// sync path (`do_checkpoint`) and the async committer after it
    /// collects FLAG_FLUSH ACKs.
    ///
    /// Returns the flush error WITHOUT resetting the SAL when the system-table
    /// flush fails: the SAL entries about to be discarded are that data's only
    /// durable copy, so resetting on a swallowed failure destroys it — the same
    /// hazard the boot path guards via `flush_all_system_tables`. Callers leave
    /// the SAL intact and retry on a later checkpoint (committer) or abort boot
    /// (`do_checkpoint`).
    pub(crate) fn checkpoint_post_ack(&mut self) -> Result<(), String> {
        let cat = unsafe { &mut *self.catalog };
        cat.flush_all_system_tables()?;
        // Now safe: every worker ACKed the FLUSH, so all have consumed past any
        // DROP that gated a directory — hence finished the matching CREATE.
        cat.drain_checkpoint_gated_deletions();
        self.sal.checkpoint_reset();
        gnitz_info!("SAL checkpoint epoch={}", self.sal.epoch());
        Ok(())
    }

    /// Accessor for the committer. True when the SAL write cursor has
    /// crossed the configured checkpoint threshold.
    pub fn sal_needs_checkpoint(&self) -> bool {
        self.sal.needs_checkpoint()
    }

    /// Get the schema descriptor for a target_id. Panics if the table
    /// has no schema (committer should only see tables that validated).
    pub fn schema_desc_for(&mut self, target_id: i64) -> SchemaDescriptor {
        self.get_schema_and_names(target_id).0
    }

    pub(super) fn get_col_name(&mut self, target_id: i64, col_idx: usize) -> String {
        let cat = unsafe { &mut *self.catalog };
        cat.get_column_names(target_id)
            .get(col_idx)
            .cloned()
            .unwrap_or_else(|| "?".to_string())
    }

    pub(super) fn get_qualified_name_owned(&mut self, table_id: i64) -> (String, String) {
        let cat = unsafe { &mut *self.catalog };
        cat.get_qualified_name(table_id)
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .unwrap_or_default()
    }
}

/// Forward one worker's SCAN continuation train to the client: send each frame
/// to `fd` (dropping it before awaiting the next, per the W2M ring contract) and
/// loop until the train header reports no more frames. `slot` is the first,
/// already-awaited frame. Returns `Ok(false)` if the client disconnects
/// mid-stream and `Err` on a malformed train header. Shared by
/// `fan_out_scan_async` (one call per worker) and
/// `fan_out_scan_single_worker_async` (one call for the single source worker).
async fn drain_scan_train(
    reactor: &crate::runtime::reactor::Reactor,
    fd: i32,
    mut slot: W2mSlot,
    req_id: u32,
    worker: usize,
) -> Result<bool, String> {
    loop {
        let (_, has_more) = parse_train_header(&slot, worker, "scan")?;
        let rc = reactor.send_slot(fd, slot).await;
        if rc < 0 {
            return Ok(false);
        }
        if !has_more {
            break;
        }
        slot = reactor.await_scan_slot(req_id).await;
    }
    Ok(true)
}

/// Common body for every single-worker async fan-out. Submits the SAL
/// message, signals the worker, yields, and returns the raw `W2mSlot` so the
/// caller can forward it to the client without an intermediate decode/copy.
/// The closures in the public wrappers compute the worker; everything else
/// is here so there is a single place to maintain the unsafe
/// borrow-and-release pattern.
///
/// `sal_excl` is held only for the synchronous write + signal phase;
/// see `dispatch_scan_fanout` for the rationale.
#[allow(clippy::too_many_arguments)]
async fn single_worker_async(
    disp_ptr: *mut MasterDispatcher,
    reactor: &crate::runtime::reactor::Reactor,
    sal_excl: &Rc<AsyncMutex<()>>,
    target_id: i64,
    flags: u32,
    worker: usize,
    seek_pk: u128,
    seek_col_idx: u64,
    op_name: &'static str,
    seek_pk_extra: &[u8],
) -> Result<W2mSlot, String> {
    let req_id = {
        let _guard = sal_excl.lock().await;
        unsafe {
            let disp = &mut *disp_ptr;
            let (schema, col_names) = disp.get_schema_and_names(target_id);
            let req_id = reactor.alloc_scan_request_id();
            let lsn = disp.next_lsn();
            disp.write_group(
                target_id,
                lsn,
                flags,
                &[],
                &schema,
                &col_names,
                seek_pk,
                seek_col_idx,
                req_id,
                worker as i32,
                seek_pk_extra,
            )?;
            disp.signal_one(worker);
            req_id
        }
    };
    // Hold the scan active across the await; without this the gate in
    // route_scan_slot discards the reply and the await never resolves. Dropped
    // on return (nothing to purge) or on cancellation (freeing any in-flight
    // slot). One frame, no continuation train, so the lease lives in this body.
    let _lease = reactor.scan_lease(&[req_id as u32]);
    let slot = reactor.await_scan_slot(req_id as u32).await;
    let ctrl = peek_control_block(slot.bytes()).expect("W2M ctrl corrupt in single_worker_async");
    if ctrl.status != 0 {
        let msg = String::from_utf8_lossy(&ctrl.error_msg);
        return Err(format!("worker {worker}: {op_name}: {msg}"));
    }
    // The slot is forwarded as a complete reply; a chunked train here would
    // be truncated to its first frame, the remainder silently discarded by
    // the lease drop. Callers only route requests whose replies fit one frame
    // (e.g. a unique point seek), so a train means that invariant broke
    // (e.g. a shrunken GNITZ_REPLY_FRAME_BUDGET) — fail loudly instead.
    if ctrl.flags & FLAG_CONTINUATION != 0 {
        return Err(format!(
            "worker {worker}: {op_name}: unexpected chunked reply on a \
             single-frame path"
        ));
    }
    Ok(slot)
}

#[cfg(test)]
mod worker_liveness_tests {
    use super::*;
    use crate::runtime::sal::SalWriter;
    use crate::runtime::w2m::W2mReceiver;
    use crate::runtime::w2m_ring;

    const RING_CAP: usize = 64 * 1024;

    // Build an inert dispatcher for the pre-reactor liveness-probe paths: real
    // but empty W2M rings so the bootstrap wait loops can `try_read` (always
    // None here, so they reach the park / no-progress arm that probes), a null
    // SAL and catalog (untouched on the no-frame path), and the given worker
    // pids. Returns the ring pointers so the caller can munmap after dropping
    // the dispatcher (W2mReceiver holds the raw ptrs but does not own them).
    fn probe_dispatcher(worker_pids: Vec<i32>) -> (MasterDispatcher, Vec<*mut u8>) {
        let nw = worker_pids.len();
        let mut ptrs = Vec::with_capacity(nw);
        for _ in 0..nw {
            let p = unsafe {
                let p = libc::mmap(
                    std::ptr::null_mut(),
                    RING_CAP,
                    libc::PROT_READ | libc::PROT_WRITE,
                    libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                    -1,
                    0,
                ) as *mut u8;
                assert!(!p.is_null(), "mmap failed");
                std::ptr::write_bytes(p, 0, RING_CAP);
                w2m_ring::init_region_for_tests(p, RING_CAP as u64);
                p
            };
            ptrs.push(p);
        }
        let disp = MasterDispatcher::new(
            nw,
            worker_pids,
            std::ptr::null_mut(),
            SalWriter::new(std::ptr::null_mut(), -1, 0, Vec::new()),
            W2mReceiver::new(ptrs.clone()),
        );
        (disp, ptrs)
    }

    fn free_rings(ptrs: &[*mut u8]) {
        for &p in ptrs {
            unsafe {
                libc::munmap(p as *mut libc::c_void, RING_CAP);
            }
        }
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
        let (mut disp, ptrs) = probe_dispatcher(vec![pid]);
        assert_eq!(disp.check_workers(), -1, "a live worker must not be reported dead");
        assert_eq!(disp.worker_pids[0], pid, "a live worker's pid must be retained");
        unsafe {
            libc::kill(pid, libc::SIGKILL);
            let mut status = 0;
            libc::waitpid(pid, &mut status, 0);
        }
        drop(disp);
        free_rings(&ptrs);
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
        let (mut disp, ptrs) = probe_dispatcher(vec![pid]);
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
        assert_eq!(disp.worker_pids[0], 0, "a reaped pid must be zeroed");
        assert_eq!(disp.check_workers(), -1, "a zeroed worker must not be re-reported");
        drop(disp);
        free_rings(&ptrs);
    }

    #[test]
    fn collect_acks_errors_when_worker_dies_before_acking() {
        // Boot-recovery path: wait_all_workers finds an empty ring and reaches
        // the park arm, whose liveness probe must surface the dead worker as a
        // clean error instead of looping on `wait_for` forever.
        let dead = spawn_and_reap_dead();
        let (mut disp, ptrs) = probe_dispatcher(vec![dead]);
        let err = disp.collect_acks().expect_err("a dead worker must fail ack collection");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("recovery sync"),
            "error identifies the recovery path: {err}"
        );
        drop(disp);
        free_rings(&ptrs);
    }

    #[test]
    fn collect_acks_and_relay_errors_when_worker_dies_mid_backfill() {
        // Backfill path: collect_acks_and_relay makes no progress on an empty
        // ring and reaches the !progressed arm, whose probe must surface the
        // dead worker.
        let dead = spawn_and_reap_dead();
        let (mut disp, ptrs) = probe_dispatcher(vec![dead]);
        let err = disp
            .collect_acks_and_relay(true)
            .expect_err("a dead worker must fail the backfill relay");
        assert!(err.contains("worker 0"), "error names the dead worker: {err}");
        assert!(
            err.contains("backfill relay"),
            "error identifies the backfill path: {err}"
        );
        drop(disp);
        free_rings(&ptrs);
    }
}
