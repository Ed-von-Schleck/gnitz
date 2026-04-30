//! Worker process event loop.
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::schema::SchemaDescriptor;
use crate::dag::ExchangeCallback;
use crate::storage::{BlobCacheGuard, partition_for_key};
use crate::ops::worker_for_partition;
use crate::runtime::wire::{self as ipc, STATUS_OK, STATUS_ERROR, FLAG_CONTINUATION};
use crate::runtime::sal::{
    SAL_MMAP_SIZE, FLAG_EXCHANGE, FLAG_CHECKPOINT,
    SalReader, SalMessageKind,
};
use crate::runtime::w2m::W2mWriter;
use crate::storage::Batch;

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// Lookup target for FLAG_HAS_PK requests.
enum HasPkLookup {
    /// Check the table's primary-key store.
    PrimaryKey,
    /// Check a unique secondary index on `col_idx` (0-indexed).
    UniqueIndex(u32),
}

impl HasPkLookup {
    fn from_wire(seek_col_idx: u64) -> Self {
        if seek_col_idx > 0 {
            HasPkLookup::UniqueIndex((seek_col_idx - 1) as u32)
        } else {
            HasPkLookup::PrimaryKey
        }
    }
}

/// A DDL_SYNC message received during an exchange wait, decoded eagerly.
struct DeferredDdl {
    target_id: i64,
    batch: Batch,
}

// ---------------------------------------------------------------------------
// DispatchContext + DispatchOutcome
//
// `dispatch` is the single source of truth for the inline-vs-defer matrix
// (see the doc above `WorkerProcess::dispatch`). The context tells the
// dispatcher whether the worker is at top-level draining the SAL or
// blocked inside `do_exchange_wait`; the same `(context, kind)` pair
// always maps to the same decision so behavior cannot drift between the
// two call sites.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum DispatchContext {
    /// The worker is draining the SAL from its main run loop.
    TopLevel,
    /// The worker is blocked in `do_exchange_wait`, awaiting an
    /// EXCHANGE_RELAY whose `(view_id, source_id)` matches `want_key`.
    /// `schema` is the outgoing batch's schema; used to fabricate an
    /// empty relay payload if the master sends a header-only relay.
    InsideExchangeWait { want_key: (i64, i64), schema: Option<SchemaDescriptor> },
}

/// Result of a single `dispatch` call.
enum DispatchOutcome {
    /// The dispatcher handled the message; the caller should keep
    /// draining the SAL.
    Continue,
    /// The worker is shutting down; unwind out of the loops.
    Shutdown,
    /// `InsideExchangeWait` saw the relay it was waiting for; return
    /// this batch to the DAG.
    RelayMatched(Batch),
}

struct WorkerExchangeHandler {
    stash: HashMap<i64, Batch>,
    deferred: Vec<DeferredDdl>,
    /// FLAG_TICK messages encountered inside `do_exchange_wait` while waiting
    /// for a FLAG_EXCHANGE_RELAY. Stashed here and replayed (via
    /// `replay_deferred_ticks`) after the current tick's ACK is sent, so the
    /// master observes ACKs in SAL arrival order and a later tick cannot
    /// re-enter `view_id` while an outer exchange for the same view is still
    /// awaiting its relay. The req_id is the original FLAG_TICK request id
    /// so the replayed ACK is routable.
    deferred_ticks: Vec<(i64, u64)>,
    /// FLAG_EXCHANGE_RELAY messages whose `(view_id, source_id)` doesn't
    /// match the active exchange wait. Keyed by the tuple so a stashed
    /// relay for one source never satisfies a wait for a different source
    /// of the same view (which would drive the inline DAG re-entry with
    /// the wrong sharding columns).
    pending_relays: HashMap<(i64, i64), Batch>,
}

impl WorkerExchangeHandler {
    /// Stash a `FLAG_PRELOADED_EXCHANGE` batch for `view_id`.
    ///
    /// **Invariant: at most one preloaded batch per `view_id` per epoch.**
    /// The stash is keyed by `view_id` only — a second insert for the same
    /// view (typical for joins where the master emits preloaded data per
    /// input source) would silently overwrite the first. The debug assert
    /// turns that latent bug into a panic in dev/test builds; the master
    /// must batch all preloaded data for a view into a single SAL group
    /// before any consumer (`do_exchange_wait`) drains it.
    ///
    /// As of this writing the master does not emit `FLAG_PRELOADED_EXCHANGE`
    /// at all; this code path is dead and the assert documents the
    /// constraint that any future emitter must respect. If multi-source
    /// preloaded groups become a real protocol need, re-key `stash` to
    /// `(view_id, source_id)` (mirroring `pending_relays`) and update both
    /// `dispatch_top_level`'s prelude drain and `do_exchange_wait`'s stash
    /// lookup.
    fn stash_preloaded(&mut self, view_id: i64, batch: Batch) {
        debug_assert!(
            !self.stash.contains_key(&view_id),
            "stash_preloaded: view_id={} already has a preloaded batch — \
             multi-source preloaded for the same view would silently overwrite",
            view_id,
        );
        self.stash.insert(view_id, batch);
    }
}

/// Bridges the DAG's `ExchangeCallback` requirement to `WorkerProcess`.
/// Holds a mutable reference to the worker so `do_exchange` can re-enter
/// the worker's handlers (`handle_push`, `handle_tick`) inline when those
/// messages arrive mid-wait. `tick_request_id` is the id of the message
/// that kicked off this DAG evaluation; echoed on FLAG_EXCHANGE so the
/// master's accumulator stays routable.
struct WorkerExchangeCtx<'a> {
    worker: &'a mut WorkerProcess,
    tick_request_id: u64,
}

impl<'a> ExchangeCallback for WorkerExchangeCtx<'a> {
    fn do_exchange(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
    ) -> Batch {
        self.worker.do_exchange_wait(view_id, batch, source_id, self.tick_request_id)
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    worker_id: u32,
    num_workers: usize,
    master_pid: i32,
    catalog: *mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    exchange: WorkerExchangeHandler,
    pending_deltas: HashMap<i64, Batch>,
    read_cursor: u64,
    expected_epoch: u32,
    // Per-target schema cache: skip the 512-byte schema block parse on hot paths.
    // Populated lazily from decoded.schema on first non-DDL message per target.
    schema_cache: HashMap<i64, SchemaDescriptor>,
    // Per-target encoded schema wire block (no col_names on the worker path).
    // Keyed by target_id; value = (num_columns, pk_index, block) so a cached
    // table block is not reused when send_response is called with an index
    // schema for the same target_id (FLAG_HAS_PK UniqueIndex path). On
    // fingerprint mismatch the block is built inline without evicting the cache.
    schema_wire_block_cache: HashMap<i64, (u32, u32, Rc<Vec<u8>>)>,
}

fn filter_by_pk(
    batch: &Option<Batch>,
    schema: SchemaDescriptor,
    n: usize,
    mut has_pk: impl FnMut(u128) -> bool,
) -> Batch {
    let mut result = Batch::with_schema(schema, n);
    if let Some(ref b) = batch {
        let mut blob_cache = BlobCacheGuard::acquire(&schema, n);
        for i in 0..n {
            let pk = b.get_pk(i);
            if has_pk(pk) {
                result.append_row_from_source(pk, 1, b, i, blob_cache.get_mut());
            }
        }
    }
    result
}

impl WorkerProcess {
    pub fn new(
        worker_id: u32,
        num_workers: usize,
        master_pid: i32,
        catalog: *mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
    ) -> Self {
        WorkerProcess {
            worker_id,
            num_workers,
            master_pid,
            catalog,
            sal_reader,
            w2m_writer,
            exchange: WorkerExchangeHandler {
                stash: HashMap::new(),
                deferred: Vec::new(),
                deferred_ticks: Vec::new(),
                pending_relays: HashMap::new(),
            },
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    // ── Main event loop ────────────────────────────────────────────────

    pub fn run(&mut self) -> i32 {
        // Startup ACK is unsolicited; request_id=0 is the reserved untagged slot.
        self.send_ack(0, 0, 0);

        loop {
            let ready = self.sal_reader.wait(1000);
            if ready == 0 {
                let ppid = unsafe { libc::getppid() };
                if ppid != self.master_pid {
                    self.shutdown();
                    return 0;
                }
                continue;
            }
            if ready < 0 {
                continue;
            }

            if self.drain_sal() {
                return 0;
            }
        }
    }

    /// Process all pending SAL message groups. Returns true on shutdown.
    fn drain_sal(&mut self) -> bool {
        while let Some((kind, target_id, wire)) = self.next_sal_message() {
            match self.dispatch_top_level(kind, target_id, wire) {
                DispatchOutcome::Continue => {
                    // Replay ticks deferred during any exchange wait now that the
                    // outer tick's ACK has been sent. Pushes are handled inline in
                    // `do_exchange_wait` (safe because user-table push only
                    // appends to `pending_deltas`) so we don't defer them.
                    if !self.exchange.deferred_ticks.is_empty() {
                        self.replay_deferred_ticks();
                    }
                }
                DispatchOutcome::Shutdown => return true,
                DispatchOutcome::RelayMatched(_) => {
                    // RelayMatched is only produced inside `do_exchange_wait`;
                    // the top-level dispatcher classifies ExchangeRelay as a
                    // protocol bug and never emits this outcome.
                    debug_assert!(false, "RelayMatched at top-level drain_sal");
                }
            }
        }
        false
    }

    /// Drain one SAL group, advancing `read_cursor` only on a clean read.
    ///
    /// `expected_epoch` is re-read on each call: a FLAG_FLUSH dispatched
    /// inline bumps it, and a stale snapshot would cause every
    /// post-checkpoint message to fail the epoch check forever (spinning
    /// at an unchanged cursor). The cursor is advanced *only* on a
    /// successful epoch match — a post-epoch group remains parked at the
    /// cursor until the cursor is reset by checkpoint.
    fn next_sal_message(&mut self)
        -> Option<(SalMessageKind, i64, Option<&'static [u8]>)>
    {
        if self.read_cursor + 8 >= SAL_MMAP_SIZE as u64 {
            return None;
        }
        let (msg, new_cursor) = self.sal_reader.try_read(self.read_cursor)?;
        if msg.epoch != self.expected_epoch {
            return None;
        }
        self.read_cursor = new_cursor;
        Some((
            msg.kind,
            msg.target_id as i64,
            msg.wire_data,
        ))
    }

    /// Dispatch a SAL message at the top level — drains any preloaded
    /// prelude groups before handing the underlying message to the
    /// unified dispatcher.
    fn dispatch_top_level(
        &mut self,
        mut kind: SalMessageKind,
        mut target_id: i64,
        mut wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        // Drain a (possibly chained) preloaded-exchange prelude before
        // processing the underlying message. Each preloaded group stashes
        // a batch keyed by view_id; the next group at the same epoch
        // becomes the message we actually dispatch.
        while kind == SalMessageKind::PreloadedExchange {
            let _ = self.dispatch(DispatchContext::TopLevel, kind, target_id, wire);
            match self.next_sal_message() {
                Some((k, t, w)) => { kind = k; target_id = t; wire = w; }
                None => return DispatchOutcome::Continue,
            }
        }
        self.dispatch(DispatchContext::TopLevel, kind, target_id, wire)
    }

    /// Decode wire bytes for dispatch.
    ///
    /// At top-level we go through the per-target schema cache — the wire
    /// for hot kinds (PUSH/TICK/SEEK) carries the same data schema for
    /// every group with that `target_id`, so re-skipping the encoded
    /// schema block on cache hit is a worthwhile optimization. DDL_SYNC
    /// (catalog schema) and HAS_PK (index or parent-table schema) bypass
    /// the cache: their schema blocks may differ from the cached
    /// user-table schema for the same `target_id`.
    ///
    /// Inside an exchange wait we always do a full decode — same reason
    /// as DDL_SYNC/HAS_PK — and the schema cache is intentionally
    /// untouched there.
    fn decode_for_dispatch(
        &mut self,
        ctx: DispatchContext,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&[u8]>,
    ) -> Option<ipc::DecodedWire> {
        let data = wire?;
        match ctx {
            DispatchContext::TopLevel => {
                let needs_full = matches!(kind,
                    SalMessageKind::DdlSync | SalMessageKind::HasPk);
                if !needs_full {
                    if let Some(&schema) = self.schema_cache.get(&target_id) {
                        return ipc::decode_wire_with_schema(data, &schema).ok();
                    }
                    let d = ipc::decode_wire(data).ok();
                    if let Some(ref d) = d {
                        if let Some(s) = d.schema {
                            self.schema_cache.insert(target_id, s);
                            // Invalidate the prebuilt wire block so the next
                            // send_response rebuilds it from the new schema.
                            self.schema_wire_block_cache.remove(&target_id);
                        }
                    }
                    d
                } else {
                    ipc::decode_wire(data).ok()
                }
            }
            DispatchContext::InsideExchangeWait { .. } => ipc::decode_wire(data).ok(),
        }
    }

    /// The single source of truth for the inline-vs-defer matrix.
    /// Match exhaustiveness (`match (ctx, kind)`) means a new
    /// `SalMessageKind` variant cannot be added without explicitly
    /// deciding its behavior in both contexts. The walk-the-matrix
    /// test (`tests::test_dispatch_matrix_*`) makes the *behavioral*
    /// spec for each non-trivial cell explicit.
    ///
    /// | Kind              | TopLevel               | InsideExchangeWait                    |
    /// |-------------------|------------------------|---------------------------------------|
    /// | Shutdown          | inline                 | inline                                |
    /// | Flush             | inline (resets cursor) | inline (same)                         |
    /// | DdlSync           | apply via cat().ddl_sync | defer to exchange.deferred          |
    /// | ExchangeRelay     | (unreachable, warn)    | match want_key OR park in pending_relays |
    /// | PreloadedExchange | drained by dispatch_top_level prelude | stash by view_id    |
    /// | Backfill          | inline                 | inline                                |
    /// | HasPk             | inline                 | inline                                |
    /// | Push              | inline (must)          | inline (must — sal_writer_excl deadlock) |
    /// | Tick              | inline + replay defer  | defer to exchange.deferred_ticks      |
    /// | SeekByIndex       | inline                 | inline                                |
    /// | Seek              | inline                 | inline                                |
    /// | Scan              | inline                 | inline                                |
    ///
    /// Reasons for the non-trivial rules (each cites the bug or
    /// invariant that fixing the rule violates):
    ///
    /// * **Tick defer inside exchange.** An inline tick eval would
    ///   re-enter `view_id` with a different source and produce
    ///   schema-mismatched relays. Defer + replay after the outer
    ///   tick's ACK so the master observes ACKs in SAL arrival order.
    ///   See `WorkerExchangeHandler::deferred_ticks` and the SAL
    ///   exchange-interleaving fix (a later tick re-entering an
    ///   in-flight view exposed the original bug).
    ///
    /// * **DdlSync defer inside exchange.** Applying a catalog mutation
    ///   inline would race in-flight DAG eval and create schema
    ///   inconsistency across nested exchanges. Stage the batch in
    ///   `exchange.deferred` and apply it after the DAG returns
    ///   (`dispatch_deferred`).
    ///
    /// * **Push inline (mandatory in both contexts).** The master
    ///   committer holds `sal_writer_excl` for the entire push group's
    ///   write+signal+fsync window while awaiting the push ACK; the
    ///   relay_loop needs the same mutex to publish this exchange's
    ///   relay, so deferring the push would deadlock the worker
    ///   against the committer. See `committer::Shared::sal_writer_excl`
    ///   and `commit_pushes` for the lock discipline.
    ///
    /// * **ExchangeRelay key match.** `(view_id, source_id)` — keying by
    ///   view alone breaks when a join view has multiple input tables:
    ///   a relay for source A would satisfy a wait for source B and
    ///   drive the inline DAG with the wrong sharding columns. Mismatched
    ///   relays are parked in `pending_relays` and picked up by the next
    ///   nested wait that asks for the right pair.
    ///
    /// * **PreloadedExchange stash.** Keyed by `view_id` only on the
    ///   stash side (`WorkerExchangeHandler::stash`). See `stash_preloaded`
    ///   for the single-source-per-view invariant.
    fn dispatch(
        &mut self,
        ctx: DispatchContext,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        // Unicast kinds without a per-worker payload aren't for us.
        if wire.is_none() && !kind.is_broadcast() {
            return DispatchOutcome::Continue;
        }

        match (ctx, kind) {
            // ── Tick: inline at top-level, defer inside exchange wait ──
            (DispatchContext::TopLevel, SalMessageKind::Tick) => {
                self.run_via_dispatch_inner(ctx, kind, target_id, wire)
            }
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::Tick) => {
                let req_id = wire
                    .and_then(|d| ipc::decode_wire(d).ok())
                    .map(|d| d.control.request_id)
                    .unwrap_or(0);
                self.exchange.deferred_ticks.push((target_id, req_id));
                DispatchOutcome::Continue
            }

            // ── DdlSync: apply at top-level, defer inside ──────────────
            (DispatchContext::TopLevel, SalMessageKind::DdlSync) => {
                self.run_via_dispatch_inner(ctx, kind, target_id, wire)
            }
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::DdlSync) => {
                if let Some(data) = wire {
                    if let Ok(decoded) = ipc::decode_wire(data) {
                        if let Some(batch) = decoded.data_batch {
                            self.exchange.deferred.push(DeferredDdl { target_id, batch });
                        }
                    }
                }
                DispatchOutcome::Continue
            }

            // ── ExchangeRelay: unreachable at top, match-or-queue inside
            (DispatchContext::TopLevel, SalMessageKind::ExchangeRelay) => {
                gnitz_warn!(
                    "W{} unexpected ExchangeRelay at top-level dispatch tid={}",
                    self.worker_id, target_id,
                );
                DispatchOutcome::Continue
            }
            (DispatchContext::InsideExchangeWait { want_key, schema },
                SalMessageKind::ExchangeRelay) => {
                // source_id is echoed back via seek_pk_lo (see
                // master::relay_exchange). Decode once, before taking
                // the batch out.
                let decoded = wire.and_then(|d| ipc::decode_wire(d).ok());
                let relay_source_id = decoded.as_ref()
                    .map(|d| d.control.seek_pk as i64)
                    .unwrap_or(0);
                let relay_batch = decoded
                    .and_then(|d| d.data_batch)
                    .unwrap_or_else(|| {
                        let empty_schema = schema.unwrap_or_default();
                        Batch::with_schema(empty_schema, 0)
                    });
                let relay_key = (target_id, relay_source_id);
                if relay_key == want_key {
                    return DispatchOutcome::RelayMatched(relay_batch);
                }
                self.exchange.pending_relays.insert(relay_key, relay_batch);
                DispatchOutcome::Continue
            }

            // ── PreloadedExchange: stash in both contexts ──────────────
            // (At TopLevel, dispatch_top_level's prelude loop calls us
            // for each chained preloaded group before the underlying
            // message; the stash side-effect is the same in both
            // contexts.)
            (_, SalMessageKind::PreloadedExchange) => {
                if let Some(data) = wire {
                    if let Ok(decoded) = ipc::decode_wire(data) {
                        if let Some(batch) = decoded.data_batch {
                            self.exchange.stash_preloaded(target_id, batch);
                        } else if let Some(s) = decoded.schema {
                            self.exchange.stash_preloaded(target_id, Batch::with_schema(s, 0));
                        }
                    }
                }
                DispatchOutcome::Continue
            }

            // ── All others: identical inline behavior in both contexts ─
            (_, SalMessageKind::Shutdown)
            | (_, SalMessageKind::Flush)
            | (_, SalMessageKind::Backfill)
            | (_, SalMessageKind::HasPk)
            | (_, SalMessageKind::Push)
            | (_, SalMessageKind::SeekByIndex)
            | (_, SalMessageKind::Seek)
            | (_, SalMessageKind::Scan) => {
                self.run_via_dispatch_inner(ctx, kind, target_id, wire)
            }
        }
    }

    /// Decode (with the context-appropriate schema cache policy) and
    /// dispatch through `dispatch_inner`, mapping its `DispatchResult`
    /// into a `DispatchOutcome`. Errors are sent on the W2M ring with
    /// the inbound request_id so the master reactor can route the
    /// failure back to the original caller.
    fn run_via_dispatch_inner(
        &mut self,
        ctx: DispatchContext,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        let decoded = self.decode_for_dispatch(ctx, kind, target_id, wire);
        let request_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
        match self.dispatch_inner(kind, target_id, decoded, request_id) {
            DispatchResult::Continue => DispatchOutcome::Continue,
            DispatchResult::Error(msg) => {
                self.send_error(&msg, request_id);
                DispatchOutcome::Continue
            }
            DispatchResult::Shutdown => DispatchOutcome::Shutdown,
        }
    }

    fn dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        decoded: Option<ipc::DecodedWire>,
        request_id: u64,
    ) -> DispatchResult {
        // Extract control fields before consuming decoded
        let seek_pk = decoded.as_ref().map(|d| d.control.seek_pk).unwrap_or(0);
        let seek_col_idx = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);
        let client_id = decoded.as_ref().map(|d| d.control.client_id).unwrap_or(0);

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch);

        match kind {
            SalMessageKind::Shutdown => {
                self.shutdown();
                DispatchResult::Shutdown
            }

            SalMessageKind::Flush => {
                self.read_cursor = 0;
                self.expected_epoch += 1;
                match self.handle_flush_all() {
                    Ok(()) => self.send_ack(0, FLAG_CHECKPOINT, request_id),
                    Err(msg) => self.send_error(&msg, request_id),
                }
                DispatchResult::Continue
            }

            SalMessageKind::DdlSync => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        if let Err(msg) = self.cat().ddl_sync(target_id, batch) {
                            return DispatchResult::Error(msg);
                        }
                        gnitz_debug!("W{} ddl_sync tid={}", self.worker_id, target_id);
                    }
                }
                DispatchResult::Continue
            }

            SalMessageKind::Backfill => {
                if let Err(msg) = self.handle_backfill(target_id, request_id) {
                    return DispatchResult::Error(msg);
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::HasPk => {
                let lookup = HasPkLookup::from_wire(seek_col_idx);
                if let Err(msg) = self.handle_has_pk(target_id, batch, lookup, request_id) {
                    return DispatchResult::Error(msg);
                }
                DispatchResult::Continue
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        if let Err(msg) = self.handle_push(target_id, batch, request_id) {
                            return DispatchResult::Error(msg);
                        }
                    }
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::Tick => {
                if let Err(msg) = self.handle_tick(target_id, request_id) {
                    return DispatchResult::Error(msg);
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::SeekByIndex => {
                let col_idx = seek_col_idx as u32;
                match self.cat().seek_by_index(target_id, col_idx, seek_pk) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id);
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Seek => {
                match self.cat().seek_family(target_id, seek_pk) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_response(target_id as u64, result.as_ref(), schema.as_ref(), request_id);
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Scan => {
                match self.cat().scan_family(target_id) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_scan_response(target_id as u64, Some(&*result), schema.as_ref(), request_id, client_id);
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            // ExchangeRelay only ever arrives while the worker is blocked
            // inside `do_exchange_wait`; that loop consumes it directly
            // and never returns through `dispatch_inner`. PreloadedExchange
            // is drained by `dispatch_top_level` before reaching here.
            // Either reaching dispatch_inner is a master-side bug.
            SalMessageKind::ExchangeRelay | SalMessageKind::PreloadedExchange => {
                gnitz_warn!(
                    "W{} unexpected {:?} at top-level dispatch_inner tid={}",
                    self.worker_id, kind, target_id,
                );
                DispatchResult::Continue
            }
        }
    }

    // ── W2M response helpers ───────────────────────────────────────────

    fn send_ack(&self, target_id: u64, flags: u32, request_id: u64) {
        let sz = ipc::wire_size(STATUS_OK, &[], None, None, None, None);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, target_id, 0, flags as u64,
                0u128, 0, request_id, STATUS_OK, &[], None, None, None, None,
            );
        });
    }

    fn send_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
    ) {
        let tid_key = target_id as i64;
        // Rc::clone releases the mutable borrow on schema_wire_block_cache
        // before w2m_writer is accessed below. On fingerprint mismatch (index
        // schema vs cached table schema for the same target_id) build inline
        // without overwriting — the cached table block remains valid for SEEK/SCAN.
        let prebuilt_rc: Option<Rc<Vec<u8>>> = schema.map(|s| {
            let nc = s.num_columns;
            let pi = s.pk_index;
            if let Some(entry) = self.schema_wire_block_cache.get(&tid_key) {
                if entry.0 == nc && entry.1 == pi {
                    return Rc::clone(&entry.2);
                }
                return Rc::new(ipc::build_schema_wire_block(s, &[], target_id as u32));
            }
            let block = Rc::new(ipc::build_schema_wire_block(s, &[], target_id as u32));
            self.schema_wire_block_cache.insert(tid_key, (nc, pi, block.clone()));
            block
        });
        let prebuilt: Option<&[u8]> = prebuilt_rc.as_deref().map(|v| v.as_slice());
        let sz = ipc::wire_size(STATUS_OK, &[], schema, None, result, prebuilt);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, target_id, 0, 0,
                0u128, 0, request_id, STATUS_OK, &[],
                schema, None, result, prebuilt,
            );
        });
    }

    // Unlike send_response, uses encode_wire_into (checksummed, client-shape) and
    // the catalog-level cache that includes col_names.
    fn send_scan_response(
        &mut self,
        target_id: u64,
        result: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        request_id: u64,
        client_id: u64,
    ) {
        let tid_key = target_id as i64;
        let prebuilt_rc: Option<Rc<Vec<u8>>> = schema.map(|s| {
            if let Some(block) = self.cat().get_cached_schema_wire_block(tid_key) {
                return block;
            }
            let col_names = self.cat().get_col_names_bytes(tid_key);
            let (name_refs, n) = ipc::col_names_as_refs(&col_names);
            let block = Rc::new(ipc::build_schema_wire_block(s, &name_refs[..n], target_id as u32));
            self.cat().set_schema_wire_block(tid_key, block.clone());
            block
        });
        let prebuilt: Option<&[u8]> = prebuilt_rc.as_deref().map(|v| v.as_slice());
        let sz = ipc::wire_size(STATUS_OK, &[], schema, None, result, prebuilt);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into(
                buf, 0, target_id, client_id, FLAG_CONTINUATION,
                0u128, 0, 0, // request_id=0: routing uses internal_req_id from ring prefix
                STATUS_OK, &[],
                schema, None, result, prebuilt,
            );
        });
    }

    fn send_error(&self, error_msg: &str, request_id: u64) {
        let msg = error_msg.as_bytes();
        let sz = ipc::wire_size(STATUS_ERROR, msg, None, None, None, None);
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, 0, 0, 0,
                0u128, 0, request_id, STATUS_ERROR, msg, None, None, None, None,
            );
        });
    }

    // ── Request handlers ───────────────────────────────────────────────

    /// Flush a family to disk, logging on failure without propagating — the
    /// data remains safe in the memtable until the next checkpoint flush.
    fn flush_family_best_effort(&mut self, target_id: i64) {
        if let Err(rc) = self.cat().flush_family(target_id) {
            gnitz_info!("W{} flush_family tid={} rc={} (data in memtable)", self.worker_id, target_id, rc);
        }
    }

    fn handle_push(
        &mut self, target_id: i64, batch: Batch, _request_id: u64,
    ) -> Result<(), String> {
        // Filter to only rows belonging to this worker's partitions.
        let batch = self.filter_my_partition(batch);
        if batch.count == 0 {
            return Ok(());
        }
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            // Master never sends FLAG_PUSH for system tables; system-table
            // changes arrive via FLAG_DDL_SYNC → ddl_sync. Reaching here
            // means a protocol invariant was violated.
            return Err(format!(
                "W{}: FLAG_PUSH for system table_id={}; expected DDL_SYNC",
                self.worker_id, target_id
            ));
        } else {
            let effective = self.cat().ingest_returning_effective(target_id, batch)?;
            self.flush_family_best_effort(target_id);
            if let Some(existing) = self.pending_deltas.get_mut(&target_id) {
                existing.append_batch(&effective, 0, effective.count);
            } else {
                self.pending_deltas.insert(target_id, effective);
            }
        }
        gnitz_debug!("W{} push tid={} rows={}", self.worker_id, target_id, row_count);
        Ok(())
    }

    /// Filter a broadcast batch to only rows belonging to this worker's partitions.
    fn filter_my_partition(&self, batch: Batch) -> Batch {
        let schema = match batch.schema {
            Some(s) => s,
            None => return batch,
        };
        let n = batch.count;
        if n == 0 {
            return batch;
        }

        // Single pass: collect matching row indices.
        let wid = self.worker_id as usize;
        let nw = self.num_workers;
        let mut indices: Vec<u32> = Vec::new();
        for i in 0..n {
            if worker_for_partition(partition_for_key(batch.get_pk(i)), nw) == wid {
                indices.push(i as u32);
            }
        }
        if indices.len() == n {
            return batch;
        }
        if indices.is_empty() {
            return Batch::with_schema(schema, 0);
        }

        // Vectorized scatter: one bulk copy per column, no per-row overhead.
        let mb = batch.as_mem_batch();
        Batch::from_indexed_rows(&mb, &indices, &schema)
    }

    fn handle_tick(&mut self, target_id: i64, request_id: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.pending_deltas.remove(&target_id) {
            d
        } else {
            if !self.cat().has_id(target_id) {
                return Ok(());
            }
            let schema = self.cat().get_schema_desc(target_id)
                .ok_or_else(|| format!("no schema for tid={}", target_id))?;
            Batch::with_schema(schema, 0)
        };
        self.evaluate_dag(target_id, delta, request_id);
        Ok(())
    }

    fn handle_backfill(&mut self, source_tid: i64, request_id: u64) -> Result<(), String> {
        if !self.cat().has_id(source_tid) {
            return Ok(());
        }
        let local_batch = self.cat().scan_family(source_tid)?;
        let owned = Arc::try_unwrap(local_batch).unwrap_or_else(|a| (*a).clone());
        self.evaluate_dag(source_tid, owned, request_id);
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<Batch>,
        lookup: HasPkLookup,
        request_id: u64,
    ) -> Result<(), String> {
        let n = batch.as_ref().map(|b| b.count).unwrap_or(0);

        let (schema, result) = match lookup {
            HasPkLookup::UniqueIndex(col_idx) => {
                let index_handle = self.cat().get_index_store_handle(target_id, col_idx);
                if index_handle.is_null() {
                    return Err(format!(
                        "No unique index on column {} for table {}", col_idx, target_id
                    ));
                }
                let schema = batch.as_ref()
                    .and_then(|b| b.schema)
                    .or_else(|| self.cat().get_schema_desc(target_id))
                    .ok_or_else(|| format!("no schema for tid={}", target_id))?;
                let table = unsafe { &mut *index_handle };
                (schema, filter_by_pk(&batch, schema, n, |pk| table.has_pk(pk)))
            }
            HasPkLookup::PrimaryKey => {
                let schema = self.cat().get_schema_desc(target_id)
                    .ok_or_else(|| format!("no schema for tid={}", target_id))?;
                let ptable_handle = self.cat().get_ptable_handle(target_id);
                let result = filter_by_pk(&batch, schema, n, |pk| {
                    ptable_handle.is_some_and(|pt_ptr| unsafe { &mut *pt_ptr }.has_pk(pk))
                });
                (schema, result)
            }
        };
        self.send_response(target_id as u64, Some(&result), Some(&schema), request_id);
        Ok(())
    }

    fn handle_flush_all(&mut self) -> Result<(), String> {
        self.pending_deltas.clear();
        let ids = self.cat().iter_user_table_ids();
        for tid in ids {
            self.cat()
                .flush_family(tid)
                .map_err(|e| format!("flush_family tid={} rc={}", tid, e))?;
        }
        Ok(())
    }

    /// Run multi-worker DAG evaluation with the exchange context.
    /// `request_id` is the master's request id of the message that
    /// triggered this evaluation (FLAG_TICK / FLAG_PUSH / FLAG_BACKFILL);
    /// echoed by `do_exchange_wait` so the master accumulator's wakers
    /// stay routable.
    fn evaluate_dag(&mut self, source_id: i64, delta: Batch, request_id: u64) {
        let dag = self.cat().get_dag_ptr();
        let mut ctx = WorkerExchangeCtx {
            worker: self,
            tick_request_id: request_id,
        };
        unsafe { &mut *dag }.evaluate_dag_multi_worker(source_id, delta, &mut ctx);
        // Apply DDL_SYNC messages deferred during exchange waits.
        self.dispatch_deferred();
    }

    fn dispatch_deferred(&mut self) {
        for ddl in std::mem::take(&mut self.exchange.deferred) {
            let _ = self.cat().ddl_sync(ddl.target_id, ddl.batch);
        }
    }

    /// Send FLAG_EXCHANGE to the master and block until its FLAG_EXCHANGE_RELAY
    /// for `view_id` comes back on the SAL. Messages that arrive mid-wait are
    /// dispatched inline — handle_push, handle_tick — so ACKs flow back through
    /// the master reactor in their natural arrival order, routed by req_id.
    /// Relays whose view_id does not match the innermost wait are parked in
    /// `pending_relays`; the next nested wait to ask for them will pick them
    /// up without re-reading the SAL.
    fn do_exchange_wait(
        &mut self,
        view_id: i64,
        batch: &Batch,
        source_id: i64,
        tick_request_id: u64,
    ) -> Batch {
        if let Some(stashed) = self.exchange.stash.remove(&view_id) {
            return stashed;
        }

        let schema = batch.schema;
        let sz = ipc::wire_size(STATUS_OK, &[], schema.as_ref(), None, Some(batch), None);
        self.w2m_writer.send_encoded(sz, tick_request_id as u32, |buf| {
            ipc::encode_wire_into_ipc(
                buf, 0, view_id as u64, 0, FLAG_EXCHANGE as u64,
                source_id as u128, 0, tick_request_id, STATUS_OK, &[],
                schema.as_ref(), None, Some(batch), None,
            );
        });

        let master_pid = self.master_pid;
        let want_key = (view_id, source_id);
        let ctx = DispatchContext::InsideExchangeWait { want_key, schema };

        loop {
            if let Some(b) = self.exchange.pending_relays.remove(&want_key) {
                return b;
            }

            self.sal_reader.wait(30000);

            // If the master died (killed or gnitz_fatal_abort) while we
            // were waiting, exit like the main run loop does.
            if master_pid != 0 && unsafe { libc::getppid() } != master_pid {
                unsafe { libc::_exit(0); }
            }

            loop {
                if let Some(b) = self.exchange.pending_relays.remove(&want_key) {
                    return b;
                }

                let (kind, target_id, wire) = match self.next_sal_message() {
                    Some(v) => v,
                    None => break, // no more entries — back to outer wait
                };

                match self.dispatch(ctx, kind, target_id, wire) {
                    DispatchOutcome::Continue => {}
                    DispatchOutcome::RelayMatched(batch) => return batch,
                    DispatchOutcome::Shutdown => {
                        // dispatch_inner's Shutdown arm already called
                        // libc::_exit — unreachable in practice.
                        return Batch::with_schema(
                            schema.unwrap_or_default(), 0,
                        );
                    }
                }
            }
        }
    }

    /// Replay FLAG_TICK messages deferred during an exchange wait. Runs
    /// after the current tick's ACK has been sent so the master observes
    /// ACKs in SAL arrival order. Each replayed tick may itself trigger
    /// more exchanges and defer more ticks, so loop until the queue is
    /// empty.
    fn replay_deferred_ticks(&mut self) {
        while !self.exchange.deferred_ticks.is_empty() {
            let ticks: Vec<(i64, u64)> =
                std::mem::take(&mut self.exchange.deferred_ticks);
            for (target_id, req_id) in ticks {
                match self.handle_tick(target_id, req_id) {
                    Ok(()) => self.send_ack(target_id as u64, 0, req_id),
                    Err(e) => self.send_error(&e, req_id),
                }
            }
        }
    }


    fn shutdown(&mut self) {
        let _ = self.handle_flush_all();
        unsafe { libc::_exit(0); }
    }
}

enum DispatchResult {
    Continue,
    Shutdown,
    Error(String),
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaDescriptor;

    fn test_schema() -> SchemaDescriptor {
        use crate::schema::{SchemaColumn, type_code};
        let mut sd = SchemaDescriptor::default();
        sd.num_columns = 2;
        sd.pk_index = 0;
        sd.columns[0] = SchemaColumn::new(type_code::U64, 0);
        sd.columns[1] = SchemaColumn::new(type_code::U64, 0);
        sd
    }

    fn make_handler() -> WorkerExchangeHandler {
        WorkerExchangeHandler {
            stash: HashMap::new(),
            deferred: Vec::<DeferredDdl>::new(),
            deferred_ticks: Vec::new(),
            pending_relays: HashMap::new(),
        }
    }

    #[test]
    fn test_exchange_handler_stash_and_retrieve() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch = Batch::with_schema(schema, 0);
        handler.stash_preloaded(42, batch);
        assert!(handler.stash.contains_key(&42));
        let retrieved = handler.stash.remove(&42);
        assert!(retrieved.is_some());
        assert!(!handler.stash.contains_key(&42));
    }

    /// Locks the single-source-per-view invariant on `stash_preloaded`.
    /// A second insert for the same `view_id` while the slot is still
    /// occupied is forbidden — see the doc comment on `stash_preloaded`.
    /// (If the master ever needs to send preloaded data from multiple
    /// sources for the same view, the stash must be re-keyed to
    /// `(view_id, source_id)` to match `pending_relays`.)
    #[test]
    #[should_panic(expected = "already has a preloaded batch")]
    fn test_exchange_handler_stash_rejects_double_insert() {
        let mut handler = make_handler();
        let schema = test_schema();
        let batch1 = Batch::with_schema(schema, 0);
        let batch2 = Batch::with_schema(schema, 0);
        handler.stash_preloaded(42, batch1);
        handler.stash_preloaded(42, batch2);
    }

    #[test]
    fn test_pending_deltas_accumulation() {
        let schema = test_schema();
        let mut pending: HashMap<i64, Batch> = HashMap::new();

        let mut b1 = Batch::with_schema(schema, 1);
        b1.extend_pk(1u128);
        b1.extend_weight(&1i64.to_le_bytes());
        b1.extend_null_bmp(&0u64.to_le_bytes());
        b1.extend_col(0, &10u64.to_le_bytes());
        b1.count = 1;

        let mut b2 = Batch::with_schema(schema, 1);
        b2.extend_pk(2u128);
        b2.extend_weight(&1i64.to_le_bytes());
        b2.extend_null_bmp(&0u64.to_le_bytes());
        b2.extend_col(0, &20u64.to_le_bytes());
        b2.count = 1;

        // First insert
        pending.insert(100, b1);
        // Accumulate
        if let Some(existing) = pending.get_mut(&100) {
            existing.append_batch(&b2, 0, b2.count);
        }
        assert_eq!(pending[&100].count, 2);
    }

    // -- filter_my_partition tests ----------------------------------------

    /// Build a batch with the given PK values (pk_lo only, pk_hi=0).
    fn make_batch(schema: SchemaDescriptor, pks: &[u64]) -> Batch {
        let mut b = Batch::with_schema(schema, pks.len());
        for &pk in pks {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &pk.to_le_bytes());
            b.count += 1;
        }
        b
    }

    #[test]
    fn test_filter_my_partition_all_workers_cover_all_rows() {
        let schema = test_schema();
        let pks: Vec<u64> = (0..200).collect();
        let batch = make_batch(schema, &pks);
        let nw = 4usize;

        let mut total = 0usize;
        for wid in 0..nw as u32 {
            let wp = WorkerProcess {
                worker_id: wid,
                num_workers: nw,
                master_pid: 0,
                catalog: std::ptr::null_mut(),
                sal_reader: unsafe { std::mem::zeroed() },
                w2m_writer: unsafe { std::mem::zeroed() },
                exchange: make_handler(),
                pending_deltas: HashMap::new(),
                read_cursor: 0,
                expected_epoch: 1,
                schema_cache: HashMap::new(),
                schema_wire_block_cache: HashMap::new(),
            };
            let filtered = wp.filter_my_partition(batch.clone_batch());
            // Every row in the filtered batch must belong to this worker.
            for i in 0..filtered.count {
                let part = partition_for_key(filtered.get_pk(i));
                assert_eq!(worker_for_partition(part, nw), wid as usize);
            }
            total += filtered.count;
        }
        // No rows lost or duplicated.
        assert_eq!(total, 200);
    }

    #[test]
    fn test_filter_my_partition_empty_batch() {
        let schema = test_schema();
        let batch = Batch::with_schema(schema, 0);
        let wp = WorkerProcess {
            worker_id: 0,
            num_workers: 4,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };
        let result = wp.filter_my_partition(batch);
        assert_eq!(result.count, 0);
    }

    #[test]
    fn test_filter_my_partition_no_schema_passthrough() {
        let schema = test_schema();
        let mut batch = Batch::with_schema(schema, 1);
        batch.extend_pk(1u128);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.extend_col(0, &1u64.to_le_bytes());
        batch.count = 1;
        // filter_my_partition short-circuits when schema is None.
        batch.schema = None;
        let wp = WorkerProcess {
            worker_id: 0,
            num_workers: 4,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };
        let result = wp.filter_my_partition(batch);
        assert_eq!(result.count, 1);
    }

    // -- pending_relays tests ------------------------------------------------

    /// Relays arriving for a different (view_id, source_id) while an outer
    /// exchange waits are parked in `pending_relays`; the next nested wait
    /// to ask for that exact pair pulls them from the queue instead of
    /// re-reading the SAL.
    #[test]
    fn test_pending_relays_queue_and_drain() {
        let mut h = make_handler();
        let schema = test_schema();
        let mut batch_b = Batch::with_schema(schema, 0);
        batch_b.count = 7;
        let mut batch_c = Batch::with_schema(schema, 0);
        batch_c.count = 9;
        h.pending_relays.insert((200, 0), batch_b);
        h.pending_relays.insert((300, 0), batch_c);

        // Nested wait for view 200 finds its relay without touching the SAL.
        let b = h.pending_relays.remove(&(200, 0)).expect("view 200 relay queued");
        assert_eq!(b.count, 7);
        assert!(!h.pending_relays.contains_key(&(200, 0)));
        // Unrelated view 300 remains parked for its own wait.
        assert!(h.pending_relays.contains_key(&(300, 0)));
    }

    /// Two exchange rounds for the *same* view_id but different
    /// source_ids (typical for a join view with multiple input tables)
    /// must stash independently. Keying by view_id alone was the
    /// root-cause bug — a relay for source A satisfied a wait for
    /// source B.
    #[test]
    fn test_pending_relays_keyed_by_view_and_source() {
        let mut h = make_handler();
        let schema = test_schema();
        let mut batch_a = Batch::with_schema(schema, 0);
        batch_a.count = 3;
        let mut batch_b = Batch::with_schema(schema, 0);
        batch_b.count = 11;

        // Same view_id=100, different source_ids 10 and 20.
        h.pending_relays.insert((100, 10), batch_a);
        h.pending_relays.insert((100, 20), batch_b);

        // Retrieving one does NOT retrieve the other.
        let a = h.pending_relays.remove(&(100, 10)).expect("(100,10) queued");
        assert_eq!(a.count, 3);
        assert!(h.pending_relays.contains_key(&(100, 20)),
            "retrieving (100,10) must leave (100,20) in place");
        let b = h.pending_relays.remove(&(100, 20)).expect("(100,20) queued");
        assert_eq!(b.count, 11);
    }

    /// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
    /// `send_response`, `send_error`) must echo the inbound request_id back
    /// on the W2M region so the master reactor can route it. We fake out the
    /// W2M writer with a real anonymous mmap, fire each helper with a
    /// distinct id, then read the messages back through `decode_wire` and
    /// assert the ids round-trip.
    #[test]
    fn test_send_helpers_echo_request_id() {
        use crate::runtime::wire as ipc;
        use crate::runtime::w2m_ring;
        // Use the production-sized region — the mmap reservation is
        // lazy-populated, so the 1 GiB backing is cheap.
        let region_size = w2m_ring::W2M_REGION_SIZE;
        let region_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                region_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            )
        };
        assert_ne!(region_ptr, libc::MAP_FAILED);
        let region_ptr = region_ptr as *mut u8;
        unsafe { w2m_ring::init_region(region_ptr, region_size as u64); }

        let w2m_writer = W2mWriter::new(region_ptr, region_size as u64);

        let mut wp = WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, 0, req_ack);
        let schema = test_schema();
        wp.send_response(8, None, Some(&schema), req_resp);
        wp.send_error("boom", req_err);

        // Decode the three messages back from the ring via try_consume.
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(region_ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (data_ptr, sz, new_rc, _req_id) = unsafe {
                w2m_ring::try_consume(hdr, region_ptr as *const u8, rc)
                    .expect("expected a message")
            };
            let data = unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) };
            let decoded = ipc::decode_wire(data).expect("decode_wire");
            decoded_ids.push(decoded.control.request_id);
            rc = new_rc;
        }
        assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);

        unsafe {
            libc::munmap(region_ptr as *mut libc::c_void, region_size);
        }
    }

    #[test]
    fn from_wire_zero_is_primary_key() {
        assert!(matches!(HasPkLookup::from_wire(0), HasPkLookup::PrimaryKey));
    }

    #[test]
    fn from_wire_one_is_unique_index_col0() {
        assert!(matches!(HasPkLookup::from_wire(1), HasPkLookup::UniqueIndex(0)));
    }

    #[test]
    fn from_wire_two_is_unique_index_col1() {
        assert!(matches!(HasPkLookup::from_wire(2), HasPkLookup::UniqueIndex(1)));
    }

    #[test]
    fn from_wire_large_value_is_unique_index_not_primary_key() {
        // Before the fix, (u64::MAX as i32) = -1 < 0 would return PrimaryKey.
        assert!(matches!(HasPkLookup::from_wire(u64::MAX), HasPkLookup::UniqueIndex(_)));
        assert!(matches!(HasPkLookup::from_wire(i32::MAX as u64 + 1), HasPkLookup::UniqueIndex(_)));
    }

    // -- Walk-the-matrix dispatch tests ---------------------------------------

    /// Build a worker that's safe for `dispatch` calls whose behavior
    /// does not enter the catalog (Tick/DdlSync/ExchangeRelay/PreloadedExchange
    /// inside an exchange wait, plus ExchangeRelay/PreloadedExchange at
    /// top-level which warn-or-stash without touching the catalog).
    ///
    /// The W2M ring is unused by these arms; sal_reader is also unused
    /// because we drive `dispatch` directly.
    fn make_worker_for_matrix() -> WorkerProcess {
        WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        }
    }

    /// Tick inside an exchange wait MUST defer to `deferred_ticks`,
    /// not run inline. Cited bug: an inline tick eval re-enters `view_id`
    /// with a different source and produces schema-mismatched relays.
    #[test]
    fn test_dispatch_matrix_tick_defers_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let ctx = DispatchContext::InsideExchangeWait {
            want_key: (100, 5),
            schema: Some(test_schema()),
        };
        assert!(wp.exchange.deferred_ticks.is_empty());
        let outcome = wp.dispatch(ctx, SalMessageKind::Tick, 999, None);
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert_eq!(wp.exchange.deferred_ticks.len(), 1);
        assert_eq!(wp.exchange.deferred_ticks[0].0, 999,
            "Tick target_id must be carried into deferred_ticks");
    }

    /// ExchangeRelay inside an exchange wait whose `(view_id, source_id)`
    /// matches `want_key` returns `RelayMatched(batch)`; a non-matching
    /// pair is parked in `pending_relays`. Driving with `Some(&[])`
    /// gives a decode failure → relay_source_id=0, empty batch.
    #[test]
    fn test_dispatch_matrix_exchange_relay_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let schema = test_schema();
        let want_key = (100, 0);
        let ctx = DispatchContext::InsideExchangeWait { want_key, schema: Some(schema) };

        // Mismatched view: parked.
        let empty: &'static [u8] = &[];
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 200, Some(empty));
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert!(wp.exchange.pending_relays.contains_key(&(200, 0)),
            "non-matching relay must be parked in pending_relays");

        // Matching key: returned via RelayMatched.
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 100, Some(empty));
        assert!(matches!(outcome, DispatchOutcome::RelayMatched(_)),
            "key-matching relay must short-circuit with RelayMatched");
    }

    /// ExchangeRelay at TopLevel is a protocol bug — it can only arrive
    /// while the worker is blocked in `do_exchange_wait`. The dispatcher
    /// warns and continues; no observable state change.
    #[test]
    fn test_dispatch_matrix_exchange_relay_top_level_warns_and_continues() {
        let mut wp = make_worker_for_matrix();
        let empty: &'static [u8] = &[];
        let outcome = wp.dispatch(
            DispatchContext::TopLevel,
            SalMessageKind::ExchangeRelay,
            100,
            Some(empty),
        );
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert!(wp.exchange.pending_relays.is_empty(),
            "TopLevel must NOT park relays — they belong to do_exchange_wait");
    }

    /// PreloadedExchange returns Continue in both contexts and
    /// (with a non-decoding empty wire) makes no state change.
    /// The decoded-payload path is exercised by the existing
    /// `test_exchange_handler_stash_*` tests.
    #[test]
    fn test_dispatch_matrix_preloaded_exchange_both_contexts() {
        for ctx in [
            DispatchContext::TopLevel,
            DispatchContext::InsideExchangeWait {
                want_key: (1, 1),
                schema: Some(test_schema()),
            },
        ] {
            let mut wp = make_worker_for_matrix();
            let empty: &'static [u8] = &[];
            let outcome = wp.dispatch(ctx, SalMessageKind::PreloadedExchange, 7, Some(empty));
            assert!(matches!(outcome, DispatchOutcome::Continue));
        }
    }

    /// Walk every `SalMessageKind` and assert a *behavior contract*
    /// for the InsideExchangeWait context. Kinds that defer must NOT
    /// touch the catalog; kinds that go inline through `dispatch_inner`
    /// are not exercised here (they need a real catalog) — the contract
    /// here is that the *defer* decisions are correctly wired.
    ///
    /// Adding a new variant to `SalMessageKind` and forgetting to slot
    /// it into `dispatch`'s `match (ctx, kind)` is already a compile
    /// error (exhaustiveness). This test catches the next-most-likely
    /// regression: routing a should-defer kind to the inline path.
    #[test]
    fn test_dispatch_matrix_walk_kinds_defer_decisions() {
        // Kinds the InsideExchangeWait context MUST defer.
        let must_defer = [SalMessageKind::Tick, SalMessageKind::DdlSync];
        // ExchangeRelay is its own special case (RelayMatched / park).
        // PreloadedExchange stashes (no defer queue).
        // The rest go through dispatch_inner inline.

        for kind in SalMessageKind::ALL {
            if !must_defer.contains(&kind) { continue; }

            let mut wp = make_worker_for_matrix();
            let ctx = DispatchContext::InsideExchangeWait {
                want_key: (0, 0),
                schema: Some(test_schema()),
            };

            let before_ticks = wp.exchange.deferred_ticks.len();
            let before_ddl = wp.exchange.deferred.len();
            let _ = wp.dispatch(ctx, kind, 42, None);
            let after_ticks = wp.exchange.deferred_ticks.len();
            let after_ddl = wp.exchange.deferred.len();

            match kind {
                SalMessageKind::Tick => {
                    assert_eq!(after_ticks, before_ticks + 1,
                        "Tick must defer to deferred_ticks inside exchange wait");
                    assert_eq!(after_ddl, before_ddl,
                        "Tick must NOT touch the deferred (DDL) queue");
                }
                SalMessageKind::DdlSync => {
                    // DdlSync requires a decodable wire to actually defer
                    // (the deferred batch is taken from decoded.data_batch).
                    // With wire=None the dispatch arm is reached and runs
                    // its (trivially empty) effect: this test asserts that
                    // it returns Continue and doesn't panic, and that the
                    // deferred queue did not gain a synthetic entry
                    // (None wire ⇒ no data_batch ⇒ no push).
                    assert_eq!(after_ddl, before_ddl,
                        "DdlSync with no wire data must not synthesize a deferred entry");
                }
                _ => unreachable!(),
            }
        }
    }

    // -- next_sal_message invariant tests -------------------------------------

    /// Drive `next_sal_message` against a hand-built SAL with mixed-epoch
    /// entries. The invariant:
    ///
    /// 1. A successful read advances `read_cursor`.
    /// 2. An epoch mismatch returns `None` and leaves `read_cursor`
    ///    untouched, so a future epoch bump (via FLAG_FLUSH inline
    ///    handling) will pick the same group up.
    /// 3. The expected_epoch is re-read on every call — bumping it
    ///    between calls flips a previously-rejected group into a
    ///    consumable one without rewinding the cursor.
    #[test]
    fn test_next_sal_message_epoch_gating() {
        use crate::runtime::sal::{
            sal_write_group, SalReader, FLAG_PUSH, FLAG_DDL_SYNC,
        };

        const SAL_SIZE: usize = 1 << 20;
        let sal_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                SAL_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1, 0,
            )
        };
        assert_ne!(sal_ptr, libc::MAP_FAILED);
        let sal_ptr = sal_ptr as *mut u8;
        unsafe { std::ptr::write_bytes(sal_ptr, 0, SAL_SIZE); }

        // Single worker; each group puts a one-byte payload at slot 0.
        let nw = 1u32;
        let payload = [0u8; 1];
        let ptrs: Vec<*const u8> = vec![payload.as_ptr()];
        let sizes: Vec<u32> = vec![1];

        // Group 1: epoch=1, FLAG_PUSH, target=42
        let r1 = unsafe {
            sal_write_group(
                sal_ptr, 0, nw, 42, 100, FLAG_PUSH, 1, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r1.status, 0);
        // Group 2: epoch=1, FLAG_DDL_SYNC, target=43
        let r2 = unsafe {
            sal_write_group(
                sal_ptr, r1.new_cursor, nw, 43, 101, FLAG_DDL_SYNC, 1, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r2.status, 0);
        // Group 3: epoch=2 — must be skipped while expected_epoch==1.
        let r3 = unsafe {
            sal_write_group(
                sal_ptr, r2.new_cursor, nw, 44, 102, FLAG_PUSH, 2, SAL_SIZE as u64,
                ptrs.as_ptr(), sizes.as_ptr(),
            )
        };
        assert_eq!(r3.status, 0);

        let sal_reader = SalReader::new(sal_ptr as *const u8, 0, SAL_SIZE, -1);
        let mut wp = WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog: std::ptr::null_mut(),
            sal_reader,
            w2m_writer: unsafe { std::mem::zeroed() },
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            read_cursor: 0,
            expected_epoch: 1,
            schema_cache: HashMap::new(),
            schema_wire_block_cache: HashMap::new(),
        };

        // First call: epoch 1 group 1 — consumed.
        let cursor_before = wp.read_cursor;
        let (kind, target_id, _wire) = wp.next_sal_message().expect("group 1");
        assert_eq!(kind, SalMessageKind::Push);
        assert_eq!(target_id, 42);
        assert!(wp.read_cursor > cursor_before, "cursor must advance on success");

        // Second call: epoch 1 group 2 — consumed.
        let cursor_before = wp.read_cursor;
        let (kind, target_id, _wire) = wp.next_sal_message().expect("group 2");
        assert_eq!(kind, SalMessageKind::DdlSync);
        assert_eq!(target_id, 43);
        assert!(wp.read_cursor > cursor_before);

        // Third call: epoch 2 group 3 — REJECTED while expected_epoch==1.
        // Must return None and leave the cursor untouched.
        let cursor_at_epoch_fence = wp.read_cursor;
        assert!(wp.next_sal_message().is_none(),
            "epoch-mismatched group must be rejected");
        assert_eq!(wp.read_cursor, cursor_at_epoch_fence,
            "cursor must NOT advance on epoch mismatch (group remains parked)");

        // Bumping expected_epoch makes the same group consumable on the
        // next call — this is the load-bearing invariant cited by the
        // doc comment in `next_sal_message`. (Cursor was unchanged, so
        // we re-read the same group.)
        wp.expected_epoch = 2;
        let (kind, target_id, _wire) = wp.next_sal_message()
            .expect("group 3 should be consumable after epoch bump");
        assert_eq!(kind, SalMessageKind::Push);
        assert_eq!(target_id, 44);
        assert!(wp.read_cursor > cursor_at_epoch_fence);

        // No more groups.
        assert!(wp.next_sal_message().is_none());

        unsafe { libc::munmap(sal_ptr as *mut libc::c_void, SAL_SIZE); }
    }

}
