//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::foundation::fault::Seam;
use crate::query::ExchangeCallback;
use crate::runtime::sal::{
    SalMessageKind, SalReader, BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_STOP, BACKFILL_PAD_BIT, FLAG_EXCHANGE,
};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{self as ipc, FLAG_CONTINUATION, FLAG_SCAN_LAST, STATUS_ERROR, STATUS_OK};
use crate::schema::key::PkBuf;
use crate::schema::SchemaDescriptor;
use crate::storage::{flush_barrier, BlobCacheGuard, FlushRound, Table};
use crate::storage::{schema_wire_safe, Batch};

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// Lookup target for FLAG_HAS_PK requests.
enum HasPkLookup {
    /// Check the table's primary-key store.
    PrimaryKey,
    /// Check a secondary index on the carried column list (single- or
    /// multi-column; a composite index is located by its exact list). Unique and
    /// non-unique alike — rule F2 probes a child's FK auto-index, which is never
    /// unique.
    SecondaryIndex {
        cols: gnitz_wire::PkColList,
        /// Answer each match with the matched STORED entry key
        /// (`[span ‖ holder PK]`) instead of echoing the probe key — see
        /// `HAS_PK_WANT_HOLDER`.
        want_holder: bool,
    },
}

impl HasPkLookup {
    /// A zero column-list word → PrimaryKey. Otherwise the word carries
    /// `pack_pk_cols(col_indices)`, whose packed flag (bit 63) is always set, so
    /// a real index check is never 0 and never collides with the PK sentinel.
    /// The column list is validated against the table's schema by
    /// `validate_index_cols` at the dispatch arm.
    fn from_wire(seek_col_idx: u64) -> Self {
        match gnitz_wire::pk_cols_word(seek_col_idx) {
            0 => HasPkLookup::PrimaryKey,
            cols_word => HasPkLookup::SecondaryIndex {
                cols: gnitz_wire::unpack_pk_cols(cols_word),
                want_holder: seek_col_idx & gnitz_wire::HAS_PK_WANT_HOLDER != 0,
            },
        }
    }
}

/// A DDL_SYNC message received during an exchange wait, decoded eagerly.
struct DeferredDdl {
    target_id: i64,
    batch: Batch,
}

/// A FLAG_TICK deferred out of a *blocking* evaluation poll (an exchange wait),
/// replayed at the next top-level drain in SAL arrival order. Replay needs only
/// the view id and the original request id (so the replayed ACK is routable) —
/// decoded EAGERLY at defer time: `Flush` runs inline in `InEval` and resets the
/// SAL, so no raw wire pointer may be stashed across the wait (the `DeferredDdl`
/// discipline).
struct DeferredTick {
    target_id: i64,
    req_id: u64,
}

/// Per-chunk collective decision the master stamps onto a distributed-backfill
/// relay (in `seek_col_idx`), recorded into `WorkerExchangeHandler::
/// backfill_signal` and read once per chunk by `handle_backfill`.
/// `BACKFILL_DECISION_CHECKPOINT` is folded into `Continue` after its inline SAL
/// re-epoch is applied (see `consume_backfill_decision`), so the slot only ever
/// holds the loop verdict.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BackfillRound {
    Stop,
    Continue,
}

// ---------------------------------------------------------------------------
// DispatchContext
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
    /// The worker is draining the SAL from its main run loop — no DAG
    /// evaluation is in flight.
    TopLevel,
    /// The worker is blocked in `do_exchange_wait` inside an in-flight DAG
    /// evaluation (a tick's exchange or a backfill's), polling the SAL for the
    /// `EXCHANGE_RELAY` matching `relay_wait = (view_id, source_id)`: a matching
    /// relay unblocks it, a non-matching one parks.
    ///
    /// Maintenance (`Tick`) and `DdlSync` defer — running them inline would
    /// re-enter or alias the live evaluation — while live point reads/writes stay
    /// inline so ingestion never stalls.
    InEval { relay_wait: (i64, i64) },
}

struct WorkerExchangeHandler {
    deferred: Vec<DeferredDdl>,
    /// A maintenance `Tick` encountered inside a *blocking* evaluation poll (an
    /// exchange wait) re-enters the DAG, so it is stashed (decoded, see
    /// [`DeferredTick`]) and replayed at the next top-level drain
    /// (`replay_deferred_ticks`) in SAL arrival order, after the current tick's
    /// ACK is sent, so the master observes ACKs in SAL arrival order and a later
    /// tick cannot re-enter `view_id` while an outer exchange for the same view is
    /// still awaiting its relay.
    deferred_ticks: Vec<DeferredTick>,
    /// FLAG_EXCHANGE_RELAY messages whose `(view_id, source_id)` doesn't
    /// match the active exchange wait. Keyed by the tuple so a stashed
    /// relay for one source never satisfies a wait for a different source
    /// of the same view (which would drive the inline DAG re-entry with
    /// the wrong sharding columns). The `u64` is the relay's backfill decision
    /// (`seek_col_idx`), applied when the relay is later un-parked and consumed
    /// — so a parked CHECKPOINT/STOP is never lost (in practice backfill runs in
    /// lockstep and never parks, but carrying it keeps the path correct).
    pending_relays: HashMap<(i64, i64), (Batch, u64)>,
    /// `Some(pad)` while a distributed backfill drains this worker's source
    /// partition: `do_exchange_wait` stamps the pad bit onto every outbound
    /// FLAG_EXCHANGE and relay consumption acts on the master's stamped decision.
    /// `None` outside backfill, so steady-state exchanges keep a 0 pad bit and
    /// ignore the (also-0) relay decision.
    backfill_pad: Option<bool>,
    /// The master's per-chunk stop/continue verdict for the current backfill
    /// chunk (last relay of the chunk wins; every round of a chunk carries the
    /// same verdict). `take`n once per chunk by `handle_backfill`.
    backfill_signal: Option<BackfillRound>,
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
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch {
        self.worker
            .do_exchange_wait(view_id, batch, source_id, self.tick_request_id)
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    master_pid: i32,
    catalog: *mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    exchange: WorkerExchangeHandler,
    pending_deltas: HashMap<i64, Batch>,
    /// FIFO queue of in-progress chunked reply trains. Two clients can run two
    /// large requests concurrently (connections are independent reactor tasks
    /// and `handle_scan` holds only the catalog read lock), so a scalar slot
    /// would let the second train overwrite the first and hang its master-side
    /// drain forever.
    ///
    /// Trains drain strictly FIFO — the front train finishes before the next
    /// starts. Do NOT interleave streams round-robin: the master drains one
    /// request's train at a time, so an interleaved second train's frames
    /// would sit parked in the master's scan queue holding un-released ring slots;
    /// `consume_cursor` (released in ring order, `w2m.rs`) could then never
    /// pass them, the ring fills, the worker blocks in `send_encoded`, and the
    /// cluster deadlocks. FIFO is deadlock-free: every fan-out writes its
    /// group to all workers under `sal_writer_excl`, so all worker queues
    /// share one global request order; each master task drains workers in
    /// ascending index order; the earliest-ordered awaited train always has
    /// its frames at the front of some worker's queue with a live consumer.
    ///
    /// Chunks are emitted ONLY from `drain_sal` / `run` — never from
    /// `do_exchange_wait`'s inline dispatch loop. That loop can ENQUEUE trains
    /// (the Scan/seek/gather arms dispatch inline in both contexts); they must
    /// stay queued until the exchange completes. Emitting there would let
    /// `send_encoded` block on a full W2M ring — full because the queued
    /// train's master-side consumer paces a slow client TCP connection — while
    /// the `ExchangeRelay` this worker is waiting for sits unread in the SAL:
    /// the join would stall indefinitely on an unrelated slow client.
    /// Queued-but-unemitted is safe; the relay does not depend on any train
    /// draining.
    pending_streams: VecDeque<PendingScan>,
    /// Per-frame wire budget for chunked reply trains (`send_scan_response`,
    /// `stream_batch_response`, `emit_pending_scan_chunk`): [`ipc::FRAME_CAP`]
    /// in production, since every chunk reaches the client as one frame. Debug
    /// builds may shrink it via `GNITZ_REPLY_FRAME_BUDGET` (read once at
    /// construction) so e2e tests exercise multi-frame trains with small tables;
    /// a larger value is ignored. The master parks a full train per ring while
    /// draining another worker, but `InFlightState` grows to track it, so the
    /// train length an override produces is bounded only by the ring's byte
    /// capacity — there is no per-train frame-count ceiling.
    ///
    /// This budgets only the chunk split point; single-frame paths that cannot
    /// chunk (non-wire-safe STRING replies) check `FRAME_CAP` directly.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

pub(crate) use reply::send_unique_preflight_keys;
use reply::PendingScan;
#[cfg(test)]
use reply::PendingScanKind;

/// `GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR`: fail the pre-flight on every worker so
/// tests can assert the master surfaces the fault, drains the fan-out, and
/// leaves the catalog and unique-filter state untouched.
static UNIQUE_PREFLIGHT_ERROR: Seam = Seam::new("GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR");

/// Debug-only size knobs: shrink a worker's reply frame / pre-flight frame so a
/// small table still exercises the multi-frame paths.
static REPLY_FRAME_BUDGET: Seam = Seam::new("GNITZ_REPLY_FRAME_BUDGET");
static PREFLIGHT_KEYS_PER_FRAME: Seam = Seam::new("GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME");

/// Append-or-insert one base table's effective delta into a `pending_deltas`
/// map — the single buffering shape shared by the live push path
/// (`handle_push`) and boot SAL replay (`recover_from_sal`), which must agree
/// so the recovery tick sweep drains exactly what a live tick would.
pub(crate) fn buffer_pending_delta(pending: &mut HashMap<i64, Batch>, tid: i64, delta: Batch) {
    if let Some(existing) = pending.get_mut(&tid) {
        existing.append_batch(&delta, 0, delta.count);
    } else {
        pending.insert(tid, delta);
    }
}

/// Which schema wire block a worker reply carries, declared by the dispatch arm
/// that knows where the descriptor came from. Resolved by `reply_schema_block`;
/// the descriptor never reaches the encoder (a block, when one is emitted,
/// supersedes it).
///
/// `Table` is the target table's own schema: the reply may serve (and
/// populate) the table's cached schema wire block. `OneOff` is a projected or
/// synthetic schema — Gather's projection, HasPk UniqueIndex's index schema —
/// whose block is built fresh per reply: serving the table's cached block for
/// those would make the master decode the frames with the table's row stride,
/// and caching them would poison the table's block. `ClientAuthored` is the
/// ScanSpec reply schema the client built and shipped in `seek_pk_extra`: the
/// client decodes the reply against its own copy, so no block is emitted, and
/// the descriptor answers only `schema_wire_safe` — `ReplySchema::None` would
/// claim wire-safe unconditionally and send a STRING projection down the
/// chunking path.
#[derive(Clone, Copy)]
enum ReplySchema<'a> {
    None,
    Table(&'a SchemaDescriptor),
    OneOff(&'a SchemaDescriptor),
    ClientAuthored(&'a SchemaDescriptor),
}

/// What one probe row resolved to. The reply key is named by the variant rather
/// than inferred from the scratch buffer's length, so both arms below read as
/// what they answer with.
enum Resolved {
    /// No match — the row is dropped.
    Absent,
    /// Matched; answer with the probe key itself (a pure existence check).
    ProbeKey,
    /// Matched; answer with the key the closure wrote into its scratch.
    Scratch,
}

/// Filter a check-batch to the rows `resolve` matches, copying each matched row
/// into the result under the key `resolve` names. Keys on verbatim OPK bytes,
/// correct for every PK width. `resolve` receives the row's raw OPK PK bytes and
/// a scratch key it may write the answer into.
fn filter_by_pk_bytes(
    batch: Option<&Batch>,
    schema: SchemaDescriptor,
    mut resolve: impl FnMut(&[u8], &mut PkBuf) -> Resolved,
) -> Batch {
    let n = batch.map_or(0, |b| b.count);
    let mut result = Batch::with_capacity(schema, n);
    if let Some(b) = batch {
        let mut blob_cache = BlobCacheGuard::acquire(&schema, n);
        let mut scratch = PkBuf::zeroed(0);
        for i in 0..n {
            let pkb = b.get_pk_bytes(i);
            let key = match resolve(pkb, &mut scratch) {
                Resolved::Absent => continue,
                Resolved::ProbeKey => pkb,
                Resolved::Scratch => scratch.pk_bytes(),
            };
            result.append_row_from_source_bytes(key, 1, b, i, blob_cache.get_mut());
        }
    }
    result
}

impl WorkerProcess {
    pub fn new(
        master_pid: i32,
        catalog: *mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
        // Effective base-table deltas buffered during SAL replay (the
        // un-checkpointed tail of every base feeding ≥1 view). The master's
        // post-reset recovery tick sweep drains these into the views via
        // `handle_tick`; not cleared on checkpoint, so it survives boot → sweep.
        pending_deltas: HashMap<i64, Batch>,
    ) -> Self {
        // Worker rank/count (and role) are latched in the fork child before any
        // catalog work — see `server_main`, not here: boot-compiled plans
        // would otherwise carry rank 0 / num_workers 1.
        WorkerProcess {
            master_pid,
            catalog,
            sal_reader,
            w2m_writer,
            exchange: WorkerExchangeHandler {
                deferred: Vec::new(),
                deferred_ticks: Vec::new(),
                pending_relays: HashMap::new(),
                backfill_pad: None,
                backfill_signal: None,
            },
            pending_deltas,
            pending_streams: VecDeque::new(),
            reply_frame_budget: REPLY_FRAME_BUDGET
                .count()
                .map(|n| n as usize)
                .filter(|&n| n <= ipc::FRAME_CAP)
                .unwrap_or(ipc::FRAME_CAP),
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// Reject a column list that is malformed or names a column outside
    /// `target_id`'s schema, before it reaches the catalog. The worker-side gate
    /// for every arm carrying a `pack_pk_cols` word; the master applies the same
    /// rule in `validated_index_cols`.
    fn validate_index_cols(&mut self, target_id: i64, cols: &gnitz_wire::PkColList, op: &str) -> Result<(), String> {
        match self.cat().get_schema_desc(target_id) {
            Some(s) if s.cols_in_range(cols) => Ok(()),
            _ => Err(format!("{op}: invalid column list for table {target_id}")),
        }
    }

    // ── Main event loop ────────────────────────────────────────────────

    pub fn run(&mut self, boot_error: Option<String>) -> i32 {
        if let Some(e) = boot_error {
            // Master's wait_all_workers turns this nonzero status into a boot
            // abort BEFORE the SAL sentinel is zeroed — the replayed data's
            // only durable copy survives for the next boot.
            self.send_error(&e, 0);
            return 1;
        }
        // Startup ACK is unsolicited; request_id=0 is the reserved untagged slot.
        self.send_ack(0, 0);

        loop {
            // Skip the SAL wait while a chunked reply train is in progress: the
            // queued state drives the next drain_sal to emit the next chunk
            // immediately.
            if self.pending_streams.is_empty() {
                let ready = self.sal_reader.wait(1000);
                if ready == 0 {
                    if self.master_is_gone() {
                        self.shutdown();
                    }
                    continue;
                }
                if ready < 0 {
                    continue;
                }
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups. Shutdown `_exit`s inline.
    fn drain_sal(&mut self) {
        // Emit the next chunk of the FRONT pending train before draining new
        // SAL messages. One chunk per drain_sal pass; send_encoded provides
        // backpressure. Single-frame replies for other requests still go out
        // immediately between chunks (distinct ring-prefix request ids; the
        // master reactor routes per id).
        if !self.pending_streams.is_empty() {
            self.emit_pending_scan_chunk();
        }
        while let Some((kind, target_id, wire)) = self.next_sal_message() {
            // Only an exchange wait can match a relay; the top-level dispatcher
            // classifies ExchangeRelay as a protocol bug and returns nothing.
            let matched = self.dispatch(DispatchContext::TopLevel, kind, target_id, wire);
            debug_assert!(matched.is_none(), "relay matched at top-level drain_sal");
            // Replay ticks deferred during any exchange wait now that the outer
            // tick's ACK has been sent. Pushes are handled inline in
            // `do_exchange_wait` (safe because a user-table push only appends to
            // `pending_deltas`), so they are never deferred.
            if !self.exchange.deferred_ticks.is_empty() {
                self.replay_deferred_ticks();
            }
        }
    }

    /// Replay ticks deferred inside a blocking evaluation poll, at top level in
    /// SAL arrival order. Drained into a scratch vec first — a replayed tick may
    /// itself reach an exchange wait and defer more ticks into a fresh
    /// `deferred_ticks` — and looped until the queue stays empty.
    fn replay_deferred_ticks(&mut self) {
        while !self.exchange.deferred_ticks.is_empty() {
            for DeferredTick { target_id, req_id } in std::mem::take(&mut self.exchange.deferred_ticks) {
                match self.handle_tick(target_id, req_id) {
                    Ok(()) => self.send_ack(target_id as u64, req_id),
                    Err(e) => self.send_error(&e, req_id),
                }
            }
        }
    }

    /// The next SAL group to dispatch. The single SAL-read choke point — both
    /// the top-level loop and the inside-exchange-wait loop funnel through here.
    fn next_sal_message(&mut self) -> Option<(SalMessageKind, i64, Option<&'static [u8]>)> {
        let msg = self.sal_reader.next()?;
        // The ephemeral flush round carries the checkpoint generation in the
        // group header's `lsn` field. Latch it into `worker_ctx` before dispatch
        // so `manifest_header` stamps every view manifest this round publishes
        // with it.
        if msg.kind == SalMessageKind::FlushEph {
            crate::foundation::worker_ctx::set_committed_generation(msg.lsn);
        }
        Some((msg.kind, msg.target_id as i64, msg.wire_data))
    }

    /// The single source of truth for the inline-vs-defer matrix. Match
    /// exhaustiveness (`match (ctx, kind)`) means a new `SalMessageKind` variant
    /// cannot be added without explicitly deciding its behavior in both
    /// contexts; the walk-the-matrix tests pin the non-trivial cells.
    ///
    /// Nearly every kind runs inline in both contexts. Only four cells differ,
    /// and each is here because getting it wrong broke something:
    ///
    /// * **Tick defers inside an evaluation.** An inline tick eval would
    ///   re-enter `view_id` with a different source and produce
    ///   schema-mismatched relays. Defer + replay after the outer
    ///   tick's ACK so the master observes ACKs in SAL arrival order.
    ///   See `WorkerExchangeHandler::deferred_ticks`.
    ///
    /// * **DdlSync defers inside an evaluation.** Applying a catalog mutation
    ///   inline would race in-flight DAG eval and create schema
    ///   inconsistency across nested exchanges. Stage the batch in
    ///   `exchange.deferred` and apply it after the DAG returns
    ///   (`dispatch_deferred`).
    ///
    /// * **Push stays inline in both contexts.** The master
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
    fn dispatch(
        &mut self,
        ctx: DispatchContext,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> Option<Batch> {
        // Unicast kinds without a per-worker payload aren't for us.
        if wire.is_none() && !kind.is_broadcast() {
            return None;
        }

        match (ctx, kind) {
            // ── Tick (maintenance): inline at top-level; defer inside an
            //    in-flight evaluation — an inline tick would re-enter the DAG
            //    with a different source and emit schema-mismatched relays.
            (DispatchContext::TopLevel, SalMessageKind::Tick) => self.run_via_dispatch_inner(kind, target_id, wire),
            (DispatchContext::InEval { .. }, SalMessageKind::Tick) => {
                // Only the request id is needed; peek the control block instead of
                // decoding the whole frame.
                let req_id = wire
                    .and_then(|d| ipc::peek_client_control(d).ok())
                    .map(|c| c.request_id)
                    .unwrap_or(0);
                self.exchange.deferred_ticks.push(DeferredTick { target_id, req_id });
                None
            }

            // ── DdlSync (catalog mutation): apply at top-level; defer inside —
            //    an inline catalog mutation races in-flight DAG eval.
            (DispatchContext::TopLevel, SalMessageKind::DdlSync) => self.run_via_dispatch_inner(kind, target_id, wire),
            (DispatchContext::InEval { .. }, SalMessageKind::DdlSync) => {
                if let Some(data) = wire {
                    match ipc::decode_wire(data) {
                        Ok(decoded) => {
                            if let Some(batch) = decoded.data_batch {
                                self.exchange.deferred.push(DeferredDdl { target_id, batch });
                            }
                        }
                        Err(e) => {
                            // A dropped DDL permanently diverges this worker's
                            // catalog from the master — silently wrong results.
                            self.fatal_shutdown(&format!("failed to decode deferred DDL for tid={target_id}: {e}"))
                        }
                    }
                }
                None
            }

            // ── ExchangeRelay: unreachable at top-level; inside an evaluation,
            //    deliver it to a matching relay wait or park it.
            (DispatchContext::TopLevel, SalMessageKind::ExchangeRelay) => {
                gnitz_warn!("unexpected ExchangeRelay at top-level dispatch tid={}", target_id);
                None
            }
            (DispatchContext::InEval { relay_wait }, SalMessageKind::ExchangeRelay) => {
                // source_id is echoed back via seek_pk; the backfill round
                // decision rides in seek_col_idx. A decode failure here (WAL-block
                // checksum mismatch / truncation) must NOT silently default to
                // source_id=0 + an empty batch: a unary exchange's want_key has
                // source_id=0, so the defaulted relay would match, unblock the
                // wait, and drop the whole partition's exchanged data — silent
                // wrong results. Fail-stop, honoring the checksum decode_wire just
                // verified.
                let Some(data) = wire else {
                    // ExchangeRelay is unicast, so the wire==None guard at the
                    // top of `dispatch` already filtered a missing payload.
                    unreachable!("ExchangeRelay with no payload (filtered by dispatch's None guard)")
                };
                let decoded = match ipc::decode_wire(data) {
                    Ok(decoded) => decoded,
                    Err(e) => self.fatal_shutdown(&format!("failed to decode ExchangeRelay for tid={target_id}: {e}")),
                };
                let relay_source_id = decoded.control.seek_pk as i64;
                let relay_decision = decoded.control.seek_col_idx;
                // Header-only relay: the master stamps a schema block onto every
                // slot of a relay group, so an empty batch built from the relayed
                // schema is the correct payload. A schema-less relay would leave
                // no way to build it without silently guessing the shape.
                let relay_batch = decoded.data_batch.unwrap_or_else(|| match decoded.schema {
                    Some(s) => Batch::empty_with_schema(&s),
                    None => self.fatal_shutdown(&format!("ExchangeRelay for tid={target_id} carries no schema")),
                });
                let relay_key = (target_id, relay_source_id);
                // Delivered only to the wait blocked on exactly this relay.
                if relay_wait == relay_key {
                    // Consumed for the active wait: act on the decision (record
                    // the slot, apply any inline checkpoint) before returning.
                    self.consume_backfill_decision(relay_decision);
                    return Some(relay_batch);
                }
                // Not the relay we're blocked on: park it (with its decision) for
                // a later wait. During backfill the cluster runs in lockstep and
                // this never fires, but carrying the decision keeps it correct if
                // it ever does.
                self.exchange
                    .pending_relays
                    .insert(relay_key, (relay_batch, relay_decision));
                None
            }

            // ── All others: identical inline behavior in both contexts ─
            (_, SalMessageKind::Shutdown)
            | (_, SalMessageKind::Flush)
            | (_, SalMessageKind::FlushEph)
            | (_, SalMessageKind::Backfill)
            | (_, SalMessageKind::HasPk)
            | (_, SalMessageKind::Gather)
            | (_, SalMessageKind::UniquePreflight)
            | (_, SalMessageKind::Push)
            | (_, SalMessageKind::SeekByIndex)
            | (_, SalMessageKind::Seek)
            | (_, SalMessageKind::ScanSpec)
            | (_, SalMessageKind::Scan) => self.run_via_dispatch_inner(kind, target_id, wire),
        }
    }

    /// Decode (with the context-appropriate schema cache policy) and
    /// dispatch through `dispatch_inner`. Errors are sent on the W2M ring
    /// with the inbound request_id so the master reactor can route the
    /// failure back to the original caller.
    fn run_via_dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> Option<Batch> {
        // A frame that fails to decode is fail-stop, not a silent no-op: the
        // dropped message would diverge this worker from the master (the same
        // reasoning the deferred-DdlSync and ExchangeRelay decode paths use), and
        // a dropped Push would be ACKed as a successful ingest.
        let decoded = match wire {
            None => None,
            Some(data) => match ipc::decode_wire(data) {
                Ok(d) => Some(d),
                Err(e) => self.fatal_shutdown(&format!("failed to decode {kind:?} for tid={target_id}: {e}")),
            },
        };
        let request_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
        if let Err(msg) = self.dispatch_inner(kind, target_id, decoded, request_id) {
            self.send_error(&msg, request_id);
            if kind == SalMessageKind::DdlSync {
                // DDL application failure on trusted master→worker IPC means
                // memory corruption or an engine bug; continuing would leave
                // this worker with a permanently stale catalog.
                self.fatal_shutdown(&format!("DdlSync application failed for tid={target_id}: {msg}"));
            }
        }
        None
    }

    fn dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        mut decoded: Option<ipc::DecodedWire>,
        request_id: u64,
    ) -> Result<(), String> {
        // Extract control fields before consuming decoded
        let seek_pk = decoded.as_ref().map(|d| d.control.seek_pk).unwrap_or(0);
        let seek_col_idx = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);
        let client_id = decoded.as_ref().map(|d| d.control.client_id).unwrap_or(0);
        let ctrl_wire_flags = decoded.as_ref().map(|d| d.control.flags).unwrap_or(0);
        let client_version = gnitz_wire::wire_flags_get_schema_version(ctrl_wire_flags);
        // Wide-PK seek key tail (bytes 16..stride); empty for narrow PKs. Taken
        // (not cloned) — nothing reads the control block after this point — and
        // extracted before `decoded` is consumed by the `data_batch` take below.
        let seek_pk_extra: Vec<u8> = decoded
            .as_mut()
            .map(|d| std::mem::take(&mut d.control.seek_pk_extra))
            .unwrap_or_default();

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch);

        match kind {
            SalMessageKind::Shutdown => self.shutdown(),

            SalMessageKind::Flush => {
                self.sal_reader.checkpoint_reset();
                self.handle_flush_all()?;
                self.send_ack(0, request_id);
                Ok(())
            }

            // Ephemeral-state flush round: persist every view's operator-trace
            // tables and output stores, stamped with the checkpoint generation
            // already latched into `worker_ctx` at the classify site.
            SalMessageKind::FlushEph => {
                self.sal_reader.checkpoint_reset();
                self.handle_flush_all_ephemeral()?;
                self.send_ack(0, request_id);
                Ok(())
            }

            SalMessageKind::DdlSync => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        self.cat().ddl_sync(target_id, batch)?;
                        // Drop hooks queue the entity's directory, but the master
                        // (which shares this on-disk tree) physically removes it
                        // after the DDL zone is durable. Discard the worker's
                        // redundant queue so it cannot grow unbounded.
                        self.cat().discard_pending_dir_deletions();
                        // A DROP retracts the table/view's catalog row, so its id is
                        // no longer live. If a push landed between its last tick and
                        // the drop, its pending_deltas entry would never tick again
                        // (the master's tick loop filters dropped ids), so GC any
                        // now-dead ids here — this is a dead entry's only reaper.
                        // Raw reborrow: `self.cat()` would borrow all of self and
                        // conflict with the `pending_deltas` field borrow.
                        let cat = unsafe { &*self.catalog };
                        self.pending_deltas.retain(|tid, _| cat.has_id(*tid));
                        gnitz_debug!("ddl_sync tid={}", target_id);
                    }
                }
                Ok(())
            }

            SalMessageKind::Backfill => {
                // `target_id` is the source table; `seek_pk` carries the view to
                // drive. Stop-the-world (the DDL parks the reactor): no yield.
                self.handle_backfill(target_id, seek_pk as i64, request_id)?;
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::HasPk => {
                let lookup = HasPkLookup::from_wire(seek_col_idx);
                if let HasPkLookup::SecondaryIndex { cols, .. } = &lookup {
                    self.validate_index_cols(target_id, cols, "has_pk")?;
                }
                self.handle_has_pk(target_id, batch, lookup, request_id, client_id, seek_pk)
            }

            SalMessageKind::Gather => {
                // The projected column mask rides in `seek_col_idx`. The PK
                // batch arrives in `data_batch` (a worker with an empty
                // sublist still replies — the master joins one reply per
                // worker). PKs come pre-sorted from the master's global sort
                // (scatter preserves per-worker order), aiding the cursor.
                let project: Vec<u8> = crate::runtime::sal::unpack_gather_cols(seek_col_idx).collect();
                // The batch PK region holds verbatim OPK bytes (the master packs
                // them via `extend_pk_bytes`), so lend them to the seek directly,
                // at every PK width. Round-tripping a narrow key back through
                // `get_pk` → `opk_key` would re-OPK-encode it (double sign-flip
                // for signed; scrambled compound bytes), probing a key that
                // matches no stored row.
                let result = match batch.as_ref() {
                    Some(b) => {
                        let keys = (0..b.count).map(|i| b.get_pk_bytes(i));
                        self.cat().gather_family_bytes(target_id, keys, &project)?
                    }
                    // A worker with an empty sublist still replies — the master
                    // joins one reply per worker.
                    None => self
                        .cat()
                        .gather_family_bytes(target_id, std::iter::empty(), &project)?,
                };
                // The projected reply schema is synthetic — never the
                // table's cached block.
                let schema = result.schema;
                self.stream_batch_response(
                    target_id as u64,
                    Some(result),
                    schema.as_ref().map_or(ReplySchema::None, ReplySchema::OneOff),
                    request_id,
                    client_id,
                    0,
                )
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        self.handle_push(target_id, batch, request_id)?;
                    }
                }
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::Tick => {
                self.handle_tick(target_id, request_id)?;
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::SeekByIndex => {
                let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
                self.validate_index_cols(target_id, &cols, "seek_by_index")?;
                // A prefix seek supplies fewer values than the index's arity.
                let keys = gnitz_wire::unpack_index_key_slots(seek_pk, &seek_pk_extra, cols.as_slice().len())?;
                let (result, schema) = self.cat().seek_by_index(target_id, cols.as_slice(), keys.as_slice())?;
                self.stream_batch_response(
                    target_id as u64,
                    result,
                    ReplySchema::Table(&schema),
                    request_id,
                    client_id,
                    seek_pk,
                )
            }

            SalMessageKind::Seek => {
                // The full seek key arrives as the wire pair seek_pk (low ≤16
                // native bytes) + seek_pk_extra (the 16..stride suffix, empty for
                // narrow PKs). `seek_family` decodes it through `seek_opk_bytes`
                // at every width — user and system tables alike, no width fork.
                let (result, schema) = self.cat().seek_family(target_id, seek_pk, &seek_pk_extra)?;
                // One frame, and a view key names its whole PK group — so this
                // reply has no size bound of its own. `send_response` rejects one
                // too large for the client rather than emitting it.
                self.send_response(
                    target_id as u64,
                    result.as_ref(),
                    ReplySchema::Table(&schema),
                    request_id,
                    client_id,
                    seek_pk,
                )
            }

            SalMessageKind::Scan => {
                let (result, schema) = self.cat().scan_family(target_id)?;
                // A multi-scan group carries FLAG_SCAN_FIFO_REPLY in its control
                // block: route this relation's reply through `pending_streams`
                // so ring order equals request order (the master drains a
                // multi-scan's relations one train at a time, in request order).
                let force_fifo = ctrl_wire_flags & gnitz_wire::FLAG_SCAN_FIFO_REPLY != 0;
                self.send_scan_response(
                    target_id as u64,
                    result,
                    ReplySchema::Table(&schema),
                    request_id,
                    client_id,
                    client_version,
                    force_fifo,
                )
            }

            SalMessageKind::ScanSpec => {
                // The control block's `seek_pk_extra` bundles the encoded `ReadSpec`
                // and the client's reply-schema wire block. Decoding the block both
                // validates it and gives `scan_spec_family` its output shape; it goes
                // no further, since the client decodes the reply against its own copy.
                let (spec_bytes, reply_block) =
                    gnitz_wire::unpack_scan_spec_extra(&seek_pk_extra).map_err(|e| format!("scan_spec: {e}"))?;
                let spec = gnitz_wire::ReadSpec::decode(spec_bytes).map_err(|e| format!("scan_spec: {e}"))?;
                let reply_schema = ipc::decode_schema_block(reply_block, true)
                    .map_err(|e| format!("scan_spec: reply schema block: {e}"))?;
                let keeper = self.cat().scan_spec_family(target_id, &spec, &reply_schema)?;
                self.send_scan_response(
                    target_id as u64,
                    Rc::new(keeper),
                    ReplySchema::ClientAuthored(&reply_schema),
                    request_id,
                    client_id,
                    0,
                    false,
                )
            }

            SalMessageKind::UniquePreflight => {
                // CREATE UNIQUE INDEX global pre-flight: project this worker's
                // committed partition of `target_id` to OPK leading-key spans for
                // the column list in `seek_col_idx` (packed via pack_pk_cols),
                // sort them, and stream the sorted spans back for the master's
                // k-way merge. An error here surfaces as the terminal fault frame
                // the master's merge expects (send_error in run_via_dispatch_inner).
                let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
                self.validate_index_cols(target_id, &cols, "unique pre-flight")?;
                self.handle_unique_preflight(target_id, cols.as_slice(), request_id)?;
                Ok(())
            }

            // `dispatch` consumes ExchangeRelay itself in both contexts and
            // never routes it here.
            SalMessageKind::ExchangeRelay => unreachable!("ExchangeRelay never reaches dispatch_inner"),
        }
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: i64, batch: Batch, _request_id: u64) -> Result<(), String> {
        // Master pre-partitions FLAG_PUSH rows in `scatter_wire_group`,
        // so every slot already contains only this worker's rows. A second
        // partition-hash filter here would be pure overhead.
        if batch.count == 0 {
            return Ok(());
        }
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            // Master never sends FLAG_PUSH for system tables; system-table
            // changes arrive via FLAG_DDL_SYNC → ddl_sync. Reaching here
            // means a protocol invariant was violated.
            return Err(format!("FLAG_PUSH for system table_id={target_id}; expected DDL_SYNC"));
        }
        let effective = self.cat().ingest_returning_effective(target_id, batch)?;
        buffer_pending_delta(&mut self.pending_deltas, target_id, effective);
        gnitz_debug!("push tid={} rows={}", target_id, row_count);
        Ok(())
    }

    fn handle_tick(&mut self, target_id: i64, request_id: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.pending_deltas.remove(&target_id) {
            d
        } else {
            if !self.cat().has_id(target_id) {
                return Ok(());
            }
            let schema = self
                .cat()
                .get_schema_desc(target_id)
                .ok_or_else(|| format!("no schema for tid={target_id}"))?;
            Batch::empty_with_schema(&schema)
        };
        self.evaluate_dag(target_id, delta, request_id);
        Ok(())
    }

    /// Distributed CREATE-VIEW backfill, worker side. Streams this worker's
    /// committed slice of `source_tid` through the incremental plan one chunk at
    /// a time (peak RAM ~O(chunk), not O(slice)), driving an exchange round
    /// per chunk per exchanging view across the cross-worker barrier.
    ///
    /// All workers must issue the SAME number of rounds, but slices are
    /// unequal — so a worker that has drained its own keeps issuing EMPTY
    /// (pad) rounds to stay in lockstep, until the master signals stop. The stop
    /// decision is collective: each worker stamps a per-chunk pad bit onto every
    /// FLAG_EXCHANGE it issues (`do_exchange_wait`), the master ANDs them and
    /// stamps the verdict back onto each relay, and the worker records it into a
    /// single per-chunk slot read here.
    ///
    /// A view that runs NO exchange has no barrier: no relay arrives, the slot
    /// stays `None`, and the worker self-terminates on local drain exhaustion.
    ///
    /// **View-scoped.** Drives ONLY `view_id` (`backfill_view_step`), never the
    /// source's whole dependent closure: the source may already have populated
    /// dependents (live CREATE VIEW over a source with prior views; recovery
    /// step-4 rebuild next to resumed siblings) that a closure re-drive would
    /// double-count. A view backfill runs stop-the-world (the DDL parks the
    /// reactor), so it never yields to live traffic between chunks.
    fn handle_backfill(&mut self, source_tid: i64, view_id: i64, request_id: u64) -> Result<(), String> {
        // Recovery step-4: the FIRST backfill command for an invalid view resets
        // its output store + operator scratch on THIS worker before any fill,
        // so the rebuild starts from an empty, well-formed store (the tick sweep
        // may have polluted its tentatively-loaded state). Gated on the
        // COW-inherited `invalid_views` set and self-clearing, so a multi-source
        // join resets once (on its first source) and the remaining sources fill
        // the just-reset store.
        if self.cat().invalid_views.contains(&view_id) {
            self.cat().reset_view_output_for_rebuild(view_id)?;
            self.cat().invalid_views.remove(&view_id);
        }
        let chunk_rows = self.cat().ddl_scan_chunk_rows;
        // Needed to synthesize empty pad chunks. An unregistered source is a
        // fail-stop: DDL_SYNC applies in SAL order, so a worker that cannot see
        // the source has diverged from the catalog.
        let schema = self
            .cat()
            .get_schema_desc(source_tid)
            .ok_or_else(|| format!("backfill: no schema for source {source_tid}"))?;
        let mut handle = self.cat().open_source_cursor(view_id, source_tid);
        let mut produced_any = false;

        loop {
            // `None` ⇒ partition exhausted: this round is an empty PAD. The
            // master ANDs the pad bit across workers and stamps the collective
            // stop/continue/checkpoint decision back onto each relay.
            let drained = handle.as_mut().and_then(|h| h.drain_chunk(chunk_rows));
            let pad = drained.is_none();
            let chunk = drained.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            self.exchange.backfill_pad = Some(pad);
            produced_any |= self.backfill_view_step(view_id, source_tid, chunk, request_id);
            // do_exchange_wait applied any inline CHECKPOINT per relay and folded
            // it into Continue; the slot now holds the chunk's stop/continue
            // verdict, or `None` if this chunk issued no exchange (a non-barrier
            // source). Stop on the master's verdict, or — with no barrier (no
            // relay, so no signal) — on local drain exhaustion.
            let signal = self.exchange.backfill_signal.take();
            if signal == Some(BackfillRound::Stop) || (signal.is_none() && pad) {
                break;
            }
        }

        // Steady-state ticks must keep passing a 0 pad bit (see do_exchange_wait).
        self.exchange.backfill_pad = None;
        // Release the last chunk's pinned delta registers. This loop drives only
        // `view_id`, so that is the one regfile it can have pinned.
        self.cat().dag.clear_view_regfile_deltas(view_id);
        // `backfill_view_step` bypasses the closure driver's per-view flush, so
        // flush the view's output trace once after the final chunk. Only when it
        // produced rows (the first source of a join produces none — it just
        // fills its trace).
        if produced_any {
            // On abort the master's watchdog turns the dead worker into a
            // cluster abort, and restart re-derives the view.
            self.cat().dag.flush_view_or_abort(view_id);
        }
        Ok(())
    }

    /// View-scoped backfill of one chunk: run only `view_id`'s epoch over a
    /// chunk of `source_id` (through the exchange ctx, so its scatter/relay
    /// round runs across the worker barrier) and ingest the output. Returns
    /// whether the view produced rows. The worker analogue of `evaluate_dag`
    /// but for a single view rather than the source's whole closure.
    fn backfill_view_step(&mut self, view_id: i64, source_id: i64, delta: Batch, request_id: u64) -> bool {
        let dag = self.cat().get_dag_ptr();
        let mut ctx = WorkerExchangeCtx {
            worker: self,
            tick_request_id: request_id,
        };
        let produced = unsafe { &mut *dag }.backfill_view_step_multi_worker(view_id, source_id, delta, &mut ctx);
        // Apply DDL_SYNC messages deferred during exchange waits (mirrors
        // `evaluate_dag`).
        self.dispatch_deferred();
        produced
    }

    /// Act on the backfill decision a master stamped onto a relay's
    /// `seek_col_idx`, the moment that relay is consumed for its matching wait.
    /// No-op outside a backfill (steady-state relays carry CONTINUE == 0 and
    /// there is no loop reading the slot).
    fn consume_backfill_decision(&mut self, decision: u64) {
        if self.exchange.backfill_pad.is_none() {
            return;
        }
        // CHECKPOINT is a CONTINUE that also applies the relay-driven half of a
        // SAL checkpoint inline: advance the read epoch so post-reset groups (the
        // master writes them at `write_cursor == 0` in the bumped epoch) are
        // accepted and any pre-reset group parks via `next_sal_message`'s epoch
        // check. Deliberately NOT the FLAG_FLUSH arm — no `handle_flush_all`, no
        // flush ACK; the master's consumption proof is the next round's
        // FLAG_EXCHANGE report, which a flush ACK would be misread as a
        // terminal ACK that retires the worker.
        if decision == BACKFILL_DECISION_CHECKPOINT {
            self.sal_reader.checkpoint_reset();
        }
        self.exchange.backfill_signal = Some(if decision == BACKFILL_DECISION_STOP {
            BackfillRound::Stop
        } else {
            BackfillRound::Continue
        });
    }

    /// CREATE UNIQUE INDEX pre-flight, worker side: project every
    /// positive-weight, non-null row of this worker's committed partition of
    /// `owner_id` to the OPK leading-key span of `col_indices` (the same
    /// `IndexKeySpec::key_bytes` contract the master's filter warmup and merge use),
    /// and stream the spans back byte-lexicographically sorted. A consolidated
    /// row at weight ≥ 2 emits its span twice — it IS that many live instances of
    /// the key, and the duplicate must be visible to the merge as an adjacent
    /// pair. No local dedup and no within-partition duplicate check: the master's
    /// adjacent-equal merge subsumes both.
    ///
    /// Sorting is the bounded external merge sort (`storage::SpillSort`), so
    /// peak RAM is the spill budget, not the partition size — a
    /// whole-partition in-RAM sort OOM-kills the worker on a large table. All
    /// fallible spill I/O completes before the first frame is sent, so a fault
    /// returns `Err` (surfaced to the master as a clean pre-flight fault via
    /// `send_error`), never a truncated train.
    ///
    /// MUST observe the same snapshot `backfill_index` will later project:
    /// the master sends this command inside the DDL critical section
    /// (committer barrier drained, catalog write lock held), before the
    /// IDX_TAB +1 broadcast, so no concurrent INSERT can interleave.
    fn handle_unique_preflight(&mut self, owner_id: i64, col_indices: &[u32], request_id: u64) -> Result<(), String> {
        if UNIQUE_PREFLIGHT_ERROR.armed() {
            return Err("injected unique pre-flight fault".to_string());
        }
        let schema = self
            .cat()
            .get_schema_desc(owner_id)
            .ok_or_else(|| format!("unique pre-flight: no schema for table {owner_id}"))?;
        // The index circuit is not registered until this pre-flight succeeds, so
        // build its schema from the owner schema + column list — identical inputs
        // to the master's own build, so the reply frame layout agrees by
        // construction. `make_index_schema` also bounds-checks the columns (a
        // protocol-level mismatch rather than a user error) and yields the
        // promoted per-column types/sizes for the span.
        let idx_schema = crate::schema::make_index_schema(col_indices, &schema)?;
        let spec = crate::schema::IndexKeySpec::new(col_indices, &schema, &idx_schema);
        let frame_schema = crate::runtime::sal::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // The spill file is an anonymous inode on the owner table's own data
        // disk, so it never leaks and shares the table's filesystem.
        let stride = spec.key_size();
        let dir = self
            .cat()
            .table_directory(owner_id)
            .ok_or_else(|| format!("unique pre-flight: no directory for table {owner_id}"))?
            .to_string();
        let chunk_rows = self.cat().ddl_scan_chunk_rows;

        // Stream the partition chunk-wise, projecting each row to its span and
        // feeding it to the external sort. Peak RAM is the spill budget, not the
        // partition. `key_bytes` keeps the single column→span definition shared
        // with the filter warmup and the master merge.
        let mut sorter = crate::storage::SpillSort::new(&dir, stride, unique_preflight_spill_bytes());
        let mut keybuf = PkBuf::zeroed(0);
        if let Some(mut handle) = self.cat().open_store_cursor(owner_id) {
            while let Some(chunk) = handle.drain_chunk(chunk_rows) {
                let mb = chunk.as_mem_batch();
                for row in 0..chunk.count {
                    let w = chunk.get_weight(row);
                    if w <= 0 {
                        continue;
                    }
                    if !spec.key_bytes(&mb, row, &mut keybuf) {
                        continue;
                    }
                    sorter.push(keybuf.pk_bytes())?;
                    // Chunks are consolidated: weight ≥ 2 is the same row w
                    // times; one extra copy suffices to put an adjacent equal
                    // pair in the sorted stream for the master's merge.
                    if w > 1 {
                        sorter.push(keybuf.pk_bytes())?;
                    }
                }
            }
        }

        // `finish` runs the final spill + merge setup (the last fallible I/O);
        // the returned producer then lends the globally-sorted spans
        // infallibly, one at a time, into the frame train.
        let mut producer = sorter.finish()?;
        send_unique_preflight_keys(
            &self.w2m_writer,
            owner_id as u64,
            &frame_schema,
            request_id,
            unique_preflight_keys_per_frame(),
            &mut producer,
        );
        Ok(())
    }

    fn handle_has_pk(
        &mut self,
        target_id: i64,
        batch: Option<Batch>,
        lookup: HasPkLookup,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
    ) -> Result<(), String> {
        // Each arm resolves the probe and names the schema its reply carries;
        // the single exit below frames both the same way.
        let (result, schema, one_off) = match lookup {
            HasPkLookup::SecondaryIndex { cols, want_holder } => {
                let index_handle = self.cat().get_index_store_handle(target_id, cols.as_slice());
                if index_handle.is_null() {
                    return Err(format!(
                        "No index on columns {:?} for table {}",
                        cols.as_slice(),
                        target_id
                    ));
                }
                // The check target is the unique INDEX table, whose schema is
                // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                // `idx_key_size` and the PK-byte reads below come from it, so an
                // owner-table schema would compute the wrong prefix width.
                let schema = batch
                    .as_ref()
                    .and_then(|b| b.schema)
                    .or_else(|| self.cat().get_index_schema_by_cols(target_id, cols.as_slice()))
                    .ok_or_else(|| format!("no index schema for tid={} cols={:?}", target_id, cols.as_slice()))?;
                // Index layout: PK = (indexed-key field, src_pk_cols). Any
                // positive-weight match means the value is already in the index.
                // `open_cursor` keeps a compaction Io/InvalidShard failure from
                // silently turning a present key into "absent".
                let table = unsafe { &*index_handle };
                let mut cursor = table.open_cursor();
                // The check batch's PK is the OPK index composite
                // `(indexed-value…, src_pk_cols)`; the leading `idx_key_size`
                // bytes are the OPK-encoded indexed value(s). Prefix-match that
                // whole leading span — OPK puts the distinguishing bytes last, so
                // a source-width prefix would match only the zero high bytes.
                let idx_key_size = schema.leading_key_size(cols.as_slice().len());
                let result = filter_by_pk_bytes(batch.as_ref(), schema, |pkb, holder| {
                    if !cursor.seek_first_positive_with_prefix(&pkb[..idx_key_size]) {
                        return Resolved::Absent;
                    }
                    if !want_holder {
                        return Resolved::ProbeKey;
                    }
                    // `[span ‖ holder PK]` verbatim: `IndexKeySpec::write_entry`
                    // wrote the source PK at `idx_key_size`, so the caller splits
                    // it back out without decoding anything.
                    holder.set_from(cursor.current_pk_bytes());
                    Resolved::Scratch
                });
                // The index schema is not table `target_id`'s own — one-off block.
                (result, schema, true)
            }
            HasPkLookup::PrimaryKey => {
                let schema = self
                    .cat()
                    .get_schema_desc(target_id)
                    .ok_or_else(|| format!("no schema for tid={target_id}"))?;
                let store = self.cat().get_store_handle(target_id);
                // Route on verbatim OPK bytes for every PK width: feeding `get_pk`
                // (OPK-widened) to `has_pk(u128)` would re-OPK-encode it, a double
                // sign-flip that misses signed PKs.
                let result = filter_by_pk_bytes(batch.as_ref(), schema, |pkb, _| {
                    if store.as_ref().is_some_and(|t| t.has_pk_bytes(pkb)) {
                        Resolved::ProbeKey
                    } else {
                        Resolved::Absent
                    }
                });
                (result, schema, false)
            }
        };

        self.send_response(
            target_id as u64,
            Some(&result),
            if one_off {
                ReplySchema::OneOff(&schema)
            } else {
                ReplySchema::Table(&schema)
            },
            request_id,
            client_id,
            seek_pk,
        )
    }

    /// Base checkpoint round: flush every user relation's store + index
    /// circuits (`SalReplay` publishes; rederived tables fold to RAM inline).
    fn handle_flush_all(&mut self) -> Result<(), String> {
        // pending_deltas is intentionally NOT cleared here. A checkpoint can
        // fire before buffered effective deltas are ticked into their views;
        // discarding them would leave views diverged from the base tables until a
        // restart rebuilds them. The checkpoint only persists base-table data —
        // views are re-derived — so the buffered deltas must survive to be ticked
        // by the next auto-tick or the scan barrier. Live entries drain on the
        // next tick (bounded by the 10k-row auto-tick); a dropped table's entry is
        // GC'd in the DdlSync arm (retain(has_id)).
        let tables = self.cat().dag.collect_base_flush_tables();
        flush_barrier(tables, FlushRound::Base).map_err(|e| format!("base flush: {e}"))
    }

    /// Ephemeral checkpoint round: force-persist every view's operator-trace
    /// tables and output stores, stamped with the generation latched at the
    /// classify site. Two global passes — traces first, then outputs — satisfy
    /// the flush-ordering invariant (any output@G ⟹ that view's own traces
    /// durable@G) and batch better than per-view interleaving.
    fn handle_flush_all_ephemeral(&mut self) -> Result<(), String> {
        let (traces, outputs) = self.cat().dag.collect_ephemeral_flush_tables();
        // Stamp every manifest with the generation latched at the classify site
        // (`set_committed_generation` on the FlushEph message).
        let generation = crate::foundation::worker_ctx::committed_generation();
        let pass = |tables: Vec<*mut Table>, what: &str| {
            flush_barrier(tables, FlushRound::Ephemeral(generation)).map_err(|e| format!("ephemeral {what} flush: {e}"))
        };
        pass(traces, "trace")?; // Pass 1: all traces fully durable FIRST
        pass(outputs, "output") // Pass 2: all output stores
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

    /// Whether the master process has exited (killed, or `gnitz_fatal_abort`).
    /// The fallback to `PR_SET_PDEATHSIG`; `master_pid == 0` in unit tests, which
    /// have no master to outlive.
    fn master_is_gone(&self) -> bool {
        self.master_pid != 0 && unsafe { libc::getppid() } != self.master_pid
    }

    fn shutdown(&mut self) -> ! {
        let _ = self.handle_flush_all();
        unsafe { libc::_exit(0) }
    }

    /// Unrecoverable worker fault: log, flush, `_exit`. The master's
    /// watchdog turns the dead worker into a cluster abort.
    fn fatal_shutdown(&mut self, msg: &str) -> ! {
        gnitz_warn!("FATAL: {}. Shutting down.", msg);
        self.shutdown()
    }
}

// ---------------------------------------------------------------------------
// Unique pre-flight key stream
// ---------------------------------------------------------------------------

/// Keys per W2M frame for the unique pre-flight stream. The per-key wire size is
/// `idx_key_size + 16` (the OPK leading-key span + 8 B weight + 8 B null word): a
/// single ≤8-byte column promotes to an 8-byte (U64/I64) span → 24 B/key; a
/// composite span can reach `MAX_PK_BYTES` (80 B) → ~96 B/key, so a full frame is
/// ~24–96 MiB — comfortably under `MAX_W2M_MSG` (256 MiB) at this key count.
///
/// The count is a pure throughput/memory knob: larger frames amortize the
/// per-frame wire and park/drain overhead over more keys. It has no correctness
/// floor — the W2M ring back-pressures a slow client by bytes, and
/// `InFlightState` grows to track however many frames a ring holds, so any frame
/// size is safe.
const UNIQUE_PREFLIGHT_KEYS_PER_FRAME: usize = 1 << 20;

/// Frame size for the unique pre-flight stream. Debug builds may shrink it via
/// GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME so tests exercise multi-frame trains
/// with small tables; any value is safe now that `InFlightState` grows with the
/// parked depth (see UNIQUE_PREFLIGHT_KEYS_PER_FRAME).
fn unique_preflight_keys_per_frame() -> usize {
    PREFLIGHT_KEYS_PER_FRAME
        .count()
        .map_or(UNIQUE_PREFLIGHT_KEYS_PER_FRAME, |n| n as usize)
}

/// Default in-RAM key-byte budget before the pre-flight sort spills a run (128 MiB).
const UNIQUE_PREFLIGHT_SPILL_BYTES: usize = 128 * 1024 * 1024;

/// Byte budget of accumulated key spans before the pre-flight's external sort
/// spills a sorted run to disk. Read from `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`
/// (a production memory lever, honoured in every build), default 128 MiB. Peak
/// worker RAM during the pre-flight is roughly this budget plus the sort index
/// and one reorder buffer — bounded regardless of partition size.
fn unique_preflight_spill_bytes() -> usize {
    crate::foundation::env::env_usize("GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES", UNIQUE_PREFLIGHT_SPILL_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::w2m_ring;
    use crate::schema::SchemaDescriptor;
    use crate::test_support::col_def;

    fn test_schema() -> SchemaDescriptor {
        use crate::schema::{type_code, SchemaColumn};
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        )
    }

    /// A one-row batch of `schema` with the given PK and payload value.
    fn one_row_batch(schema: &SchemaDescriptor, pk: u128, v: u64) -> Batch {
        let mut b = Batch::with_capacity(*schema, 1);
        b.extend_pk(pk);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        b.extend_col(0, &v.to_le_bytes());
        b.count = 1;
        b
    }

    fn make_handler() -> WorkerExchangeHandler {
        WorkerExchangeHandler {
            deferred: Vec::<DeferredDdl>::new(),
            deferred_ticks: Vec::new(),
            pending_relays: HashMap::new(),
            backfill_pad: None,
            backfill_signal: None,
        }
    }

    /// `buffer_pending_delta` appends into an existing entry rather than
    /// replacing it — the shape the live push path and boot SAL replay share.
    #[test]
    fn test_pending_deltas_accumulation() {
        let schema = test_schema();
        let mut pending: HashMap<i64, Batch> = HashMap::new();

        buffer_pending_delta(&mut pending, 100, one_row_batch(&schema, 1, 10));
        assert_eq!(pending[&100].count, 1);
        buffer_pending_delta(&mut pending, 100, one_row_batch(&schema, 2, 20));
        assert_eq!(pending[&100].count, 2, "a second delta appends to the same table");
        buffer_pending_delta(&mut pending, 200, one_row_batch(&schema, 3, 30));
        assert_eq!(pending[&200].count, 1, "a different table gets its own entry");
    }

    /// Stage 0 wire-protocol contract: every reply helper (`send_ack`,
    /// `send_response`, `send_error`) must echo the inbound request_id back
    /// on the W2M region so the master reactor can route it. We fake out the
    /// W2M writer with a real anonymous mmap, fire each helper with a
    /// distinct id, then read the messages back through `decode_wire` and
    /// assert the ids round-trip.
    #[test]
    fn test_send_helpers_echo_request_id() {
        use crate::runtime::w2m_ring;
        use crate::runtime::wire as ipc;
        // Use the production-sized region — the mmap reservation is
        // lazy-populated, so the 1 GiB backing is cheap.
        let region_size = w2m_ring::W2M_REGION_SIZE;
        let region = crate::test_support::SharedRegion::new(region_size);
        let region_ptr = region.ptr();
        unsafe {
            w2m_ring::init_region(region_ptr, region_size as u64);
        }

        let w2m_writer = W2mWriter::new(region_ptr, region_size as u64);

        let mut wp = make_test_worker(std::ptr::null_mut(), w2m_writer);

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, req_ack);
        // Pass ReplySchema::None: send_response consults the catalog only
        // when a schema is present, and this test uses a null catalog pointer.
        // The id round-trip is the assertion of interest.
        wp.send_response(8, None, ReplySchema::None, req_resp, 0, 0u128)
            .unwrap();
        wp.send_error("boom", req_err);

        // Decode the three messages back from the ring via try_consume.
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(region_ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (data_ptr, sz, new_rc, _req_id) =
                unsafe { w2m_ring::try_consume(hdr, region_ptr as *const u8, rc).expect("expected a message") };
            let data = unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) };
            let decoded = ipc::decode_wire_ipc(data).expect("decode_wire_ipc");
            decoded_ids.push(decoded.control.request_id);
            rc = new_rc;
        }
        assert_eq!(decoded_ids, vec![req_ack, req_resp, req_err]);
    }

    #[test]
    fn from_wire_zero_is_primary_key() {
        assert!(matches!(HasPkLookup::from_wire(0), HasPkLookup::PrimaryKey));
    }

    /// `seek_col_idx` carries `pack_pk_cols(cols)` — the packed flag (bit 63) is
    /// always set, so it is never 0 and never collides with the PK sentinel —
    /// optionally OR'd with the holder directive on bit 62, which must survive
    /// the round trip without disturbing the column list.
    #[test]
    fn from_wire_decodes_the_column_list_with_and_without_the_holder_directive() {
        for cols in [&[0u32][..], &[3][..], &[63][..], &[1, 4][..], &[0, 2, 5, 7][..]] {
            let packed = gnitz_wire::pack_pk_cols(cols);
            assert_ne!(packed, 0);
            for want in [false, true] {
                let word = packed | if want { gnitz_wire::HAS_PK_WANT_HOLDER } else { 0 };
                match HasPkLookup::from_wire(word) {
                    HasPkLookup::SecondaryIndex {
                        cols: decoded,
                        want_holder,
                    } => {
                        assert_eq!(decoded.as_slice(), cols);
                        assert_eq!(want_holder, want);
                    }
                    HasPkLookup::PrimaryKey => panic!("packed list must decode to SecondaryIndex"),
                }
            }
        }
    }

    // -- Walk-the-matrix dispatch tests ---------------------------------------

    /// The one test constructor for `WorkerProcess`. Fields a test does not
    /// exercise stay null/zeroed/default; pre-seeded state (`sal_reader`,
    /// `pending_streams`, `reply_frame_budget`) is assigned after construction.
    fn make_test_worker(catalog: *mut CatalogEngine, writer: W2mWriter) -> WorkerProcess {
        WorkerProcess {
            master_pid: 0,
            catalog,
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_streams: VecDeque::new(),
            reply_frame_budget: ipc::FRAME_CAP,
        }
    }

    /// Build a worker that's safe for `dispatch` calls whose behavior
    /// does not enter the catalog (Tick/DdlSync/ExchangeRelay inside an
    /// exchange wait, plus ExchangeRelay at top-level which warns
    /// without touching the catalog).
    ///
    /// The W2M ring is unused by these arms; sal_reader is also unused
    /// because we drive `dispatch` directly.
    fn make_worker_for_matrix() -> WorkerProcess {
        make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() })
    }

    /// Tick inside an exchange wait MUST defer to `deferred_ticks`,
    /// not run inline. Cited bug: an inline tick eval re-enters `view_id`
    /// with a different source and produces schema-mismatched relays.
    #[test]
    fn test_dispatch_matrix_tick_defers_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let ctx = DispatchContext::InEval { relay_wait: (100, 5) };
        assert!(wp.exchange.deferred_ticks.is_empty());
        assert!(wp.dispatch(ctx, SalMessageKind::Tick, 999, None).is_none());
        assert_eq!(wp.exchange.deferred_ticks.len(), 1);
        assert_eq!(
            wp.exchange.deferred_ticks[0].target_id, 999,
            "Tick target_id must be carried into deferred_ticks"
        );
    }

    /// Encode a header-only ExchangeRelay wire frame (schema, no data batch)
    /// whose control block echoes `source_id` via `seek_pk`, as the master's
    /// `emit_relay_with_decision` does. Leaked to `'static` for `dispatch`.
    /// A DECODABLE frame is now required — a corrupt/undecodable relay fail-stops
    /// the worker (mirrors the DdlSync decode arm) rather than defaulting to an
    /// empty batch, so tests can no longer feed `&[]`.
    fn encode_relay_frame(target_id: u64, source_id: u128, schema: &SchemaDescriptor) -> &'static [u8] {
        // No data batch — a header-only relay. `seek_pk` echoes the source_id the
        // waiter matches on; `seek_col_idx` 0 is BACKFILL_DECISION_CONTINUE.
        let msg = ipc::WireMsg {
            target_id,
            seek_pk: source_id,
            schema: Some(schema),
            ..Default::default()
        };
        Box::leak(msg.encode_to_vec().into_boxed_slice())
    }

    /// Encode a wire frame carrying `batch` under `schema`. Leaked to `'static`
    /// for `dispatch`, which takes its payload from the SAL mapping.
    fn encode_data_frame(target_id: u64, schema: &SchemaDescriptor, batch: &Batch) -> &'static [u8] {
        let msg = ipc::WireMsg {
            target_id,
            schema: Some(schema),
            data: ipc::WireData::Whole(Some(batch)),
            ..Default::default()
        };
        Box::leak(msg.encode_to_vec().into_boxed_slice())
    }

    /// ExchangeRelay inside an exchange wait whose `(view_id, source_id)`
    /// matches `want_key` returns `RelayMatched(batch)`; a non-matching
    /// pair is parked in `pending_relays`.
    #[test]
    fn test_dispatch_matrix_exchange_relay_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let schema = test_schema();
        let want_key = (100, 0);
        let ctx = DispatchContext::InEval { relay_wait: want_key };

        // Mismatched view (target_id=200 ≠ want 100): parked under (200, 0).
        let frame = encode_relay_frame(200, 0, &schema);
        assert!(wp
            .dispatch(ctx, SalMessageKind::ExchangeRelay, 200, Some(frame))
            .is_none());
        assert!(
            wp.exchange.pending_relays.contains_key(&(200, 0)),
            "non-matching relay must be parked in pending_relays"
        );

        // Matching key (target_id=100, source_id=0 == want_key): returns the batch.
        let frame = encode_relay_frame(100, 0, &schema);
        assert!(
            wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 100, Some(frame))
                .is_some(),
            "a key-matching relay must short-circuit out of dispatch with its batch"
        );
    }

    /// ExchangeRelay at TopLevel is a protocol bug — it can only arrive
    /// while the worker is blocked in `do_exchange_wait`. The dispatcher
    /// warns and continues; no observable state change.
    #[test]
    fn test_dispatch_matrix_exchange_relay_top_level_warns_and_continues() {
        let mut wp = make_worker_for_matrix();
        let empty: &'static [u8] = &[];
        assert!(wp
            .dispatch(
                DispatchContext::TopLevel,
                SalMessageKind::ExchangeRelay,
                100,
                Some(empty)
            )
            .is_none());
        assert!(
            wp.exchange.pending_relays.is_empty(),
            "TopLevel must NOT park relays — they belong to do_exchange_wait"
        );
    }

    /// DdlSync inside an exchange wait MUST stage its batch in
    /// `exchange.deferred` rather than applying it: an inline catalog mutation
    /// races the in-flight DAG evaluation. (Adding a `SalMessageKind` without
    /// deciding both of its cells is already a compile error — `dispatch`
    /// matches exhaustively on `(ctx, kind)` — so what needs a test is the
    /// defer decision itself.)
    #[test]
    fn test_dispatch_matrix_ddl_sync_defers_inside_exchange() {
        let mut wp = make_worker_for_matrix();
        let schema = test_schema();
        let ctx = DispatchContext::InEval { relay_wait: (0, 0) };

        let frame = encode_data_frame(42, &schema, &one_row_batch(&schema, 1, 10));
        assert!(wp.dispatch(ctx, SalMessageKind::DdlSync, 42, Some(frame)).is_none());
        assert_eq!(
            wp.exchange.deferred.len(),
            1,
            "DdlSync must stage its batch, not apply it"
        );
        assert_eq!(wp.exchange.deferred[0].target_id, 42);
        assert!(
            wp.exchange.deferred_ticks.is_empty(),
            "DdlSync must not touch the deferred-tick queue"
        );
    }

    // -- next_sal_message invariant tests -------------------------------------

    /// The SAL read gate, driven through `next_sal_message`:
    ///
    /// 1. Groups in the expected epoch are consumed in order.
    /// 2. A group from a *later* epoch is rejected and stays parked — repeatedly,
    ///    so the cursor did not advance past it.
    /// 3. `checkpoint_reset` rewinds to cursor 0 in the next epoch, which is
    ///    exactly where the master writes after its own reset.
    #[test]
    fn test_next_sal_message_epoch_gating() {
        use crate::runtime::sal::{sal_write_group, SalReader, FLAG_DDL_SYNC, FLAG_PUSH};

        const SAL_SIZE: usize = 1 << 20;
        let sal_region = crate::test_support::SharedRegion::new(SAL_SIZE);
        let sal_ptr = sal_region.ptr();

        // Single worker; each group puts a one-byte payload at slot 0.
        let payload = [0u8; 1];
        let payloads: [&[u8]; 1] = [&payload];
        let write = |cursor, target, lsn, flags, epoch| {
            unsafe { sal_write_group(sal_ptr, cursor, target, lsn, flags, epoch, SAL_SIZE as u64, &payloads) }
                .expect("group fits")
        };

        let c1 = write(0, 42, 100, FLAG_PUSH, 1);
        let c2 = write(c1, 43, 101, FLAG_DDL_SYNC, 1);
        // A group from the next epoch, ahead of the reader.
        write(c2, 44, 102, FLAG_PUSH, 2);

        let mut wp = make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() });
        wp.sal_reader = SalReader::new(sal_ptr as *const u8, 0, SAL_SIZE, -1, 1);

        assert_eq!(
            wp.next_sal_message().map(|(k, t, _)| (k, t)),
            Some((SalMessageKind::Push, 42))
        );
        assert_eq!(
            wp.next_sal_message().map(|(k, t, _)| (k, t)),
            Some((SalMessageKind::DdlSync, 43))
        );

        // The epoch-2 group parks: rejected now, and still rejected on a retry —
        // the cursor did not slip past it.
        assert!(wp.next_sal_message().is_none(), "an epoch-ahead group must park");
        assert!(wp.next_sal_message().is_none(), "and stay parked");

        // After the reset the reader is at cursor 0 in epoch 2, where the master
        // writes its first post-checkpoint group.
        write(0, 77, 103, FLAG_PUSH, 2);
        wp.sal_reader.checkpoint_reset();
        assert_eq!(
            wp.next_sal_message().map(|(k, t, _)| (k, t)),
            Some((SalMessageKind::Push, 77))
        );
    }

    // -- pending stream chunking tests -----------------------------------------------

    fn make_ring() -> (crate::test_support::SharedRegion, W2mWriter) {
        let size = w2m_ring::W2M_REGION_SIZE;
        let region = crate::test_support::SharedRegion::new(size);
        let ptr = region.ptr();
        unsafe {
            w2m_ring::init_region(ptr, size as u64);
        }
        let writer = W2mWriter::new(ptr, size as u64);
        (region, writer)
    }

    fn make_n_row_batch(schema: SchemaDescriptor, n: usize) -> Batch {
        let mut b = Batch::with_capacity(schema, n.max(1));
        for i in 0..n {
            b.extend_pk(i as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &(i as u64).to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn consume_one(ptr: *mut u8) -> Vec<u8> {
        let (_, frame) = walk_frames(ptr).into_iter().next().expect("expected one ring message");
        frame
    }

    /// Per-row wire stride for a test batch, matching `enqueue_stream`.
    fn row_stride(batch: &Batch) -> usize {
        range_size(batch, 1, None) - range_size(batch, 0, None)
    }

    /// Wire size of a `count`-row range of `batch`, with an optional schema block.
    fn range_size(batch: &Batch, count: usize, prebuilt: Option<&[u8]>) -> usize {
        ipc::WireMsg {
            data: ipc::WireData::Range {
                batch,
                start_row: 0,
                count,
            },
            prebuilt_schema_block: prebuilt,
            ..Default::default()
        }
        .size()
    }

    /// First (and only) PendingScan chunk — next_row == 0, so the prebuilt schema
    /// block must appear in the frame and decode_wire_ipc must succeed without a hint.
    #[test]
    fn test_pending_scan_first_chunk_includes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);
        let schema_block = Rc::new(ipc::build_schema_wire_block(&schema, 1));

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        let wire_row_stride = row_stride(&batch);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            request_id: 7,
            client_id: 42,
            target_id: 1,
            prebuilt_schema: Some(schema_block),
            server_version: 0,
            kind: PendingScanKind::WireSafe {
                next_row: 0,
                wire_row_stride,
            },
        });

        wp.emit_pending_scan_chunk();
        assert!(
            wp.pending_streams.is_empty(),
            "10 rows fit in one chunk; the train must pop off the queue"
        );

        let data = consume_one(ptr);
        let decoded = ipc::decode_wire_ipc(&data).expect("first chunk must decode without schema hint");
        assert!(decoded.schema.is_some(), "first chunk must carry schema block");
        let b = decoded.data_batch.expect("first chunk must carry data");
        assert_eq!(b.count, 10);
        for i in 0..10usize {
            assert_eq!(b.get_pk(i), i as u128);
        }
        assert_ne!(
            decoded.control.flags & FLAG_SCAN_LAST,
            0,
            "FLAG_SCAN_LAST must be set on the only chunk"
        );
        assert_ne!(
            decoded.control.flags & FLAG_CONTINUATION,
            0,
            "FLAG_CONTINUATION must always be set on worker scan frames"
        );
    }

    /// Continuation chunk — next_row > 0, prebuilt_schema == None. The frame carries
    /// no schema block; decode_wire_ipc fails but decode_wire_ipc_with_schema succeeds
    /// and returns only the remaining rows (rows [5, 10)).
    #[test]
    fn test_pending_scan_continuation_chunk_excludes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        let wire_row_stride = row_stride(&batch);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            request_id: 9,
            client_id: 0,
            target_id: 1,
            prebuilt_schema: None,
            server_version: 0,
            kind: PendingScanKind::WireSafe {
                next_row: 5,
                wire_row_stride,
            },
        });

        wp.emit_pending_scan_chunk();
        assert!(wp.pending_streams.is_empty(), "remaining 5 rows fit in one chunk");

        let data = consume_one(ptr);
        assert!(
            ipc::decode_wire_ipc(&data).is_err(),
            "continuation frame without schema must fail decode_wire_ipc"
        );
        let hint = ipc::SchemaWithVersion {
            descriptor: &schema,
            version: 0,
        };
        let decoded = ipc::decode_wire_ipc_with_schema(&data, hint)
            .expect("decode_wire_ipc_with_schema must succeed for continuation frame");
        let b = decoded.data_batch.expect("continuation chunk must carry data");
        assert_eq!(b.count, 5);
        for i in 0..5usize {
            assert_eq!(b.get_pk(i), (i + 5) as u128);
        }
        assert_ne!(
            decoded.control.flags & FLAG_SCAN_LAST,
            0,
            "FLAG_SCAN_LAST must be set on the last chunk"
        );
        assert_ne!(decoded.control.flags & FLAG_CONTINUATION, 0);
    }

    /// send_scan_response with schema=None (avoids catalog) emits a single ring
    /// message with FLAG_CONTINUATION | FLAG_SCAN_LAST for a small wire-safe batch,
    /// and leaves the stream queue empty.
    #[test]
    fn test_send_scan_response_single_frame() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 5);

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        let err = wp.send_scan_response(1, Rc::new(batch), ReplySchema::None, 3, 0, 0, false);
        assert!(err.is_ok(), "small wire-safe batch must not error");
        assert!(
            wp.pending_streams.is_empty(),
            "batch fits in one frame; send_scan_response must not enqueue a train"
        );

        let data = consume_one(ptr);
        let ctrl = ipc::peek_control_block_ipc(&data).expect("peek_control_block");
        assert_eq!(ctrl.status, STATUS_OK);
        assert_ne!(
            ctrl.flags & FLAG_SCAN_LAST,
            0,
            "single-frame response must set FLAG_SCAN_LAST"
        );
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);

        let hint = ipc::SchemaWithVersion {
            descriptor: &schema,
            version: 0,
        };
        let decoded = ipc::decode_wire_ipc_with_schema(&data, hint).expect("decode with schema hint");
        let b = decoded.data_batch.expect("data block");
        assert_eq!(b.count, 5);
        for i in 0..5usize {
            assert_eq!(b.get_pk(i), i as u128);
        }
    }

    /// FLAG_SCAN_FIFO_REPLY (force_fifo=true) routes even an immediate-emit-
    /// eligible wire-safe reply through `pending_streams`, so a multi-scan's
    /// relations reach the ring in request order. Without the flag the identical
    /// reply emits inline (test_send_scan_response_single_frame). Its lone chunk
    /// is byte-shaped exactly like the single-frame path.
    #[test]
    fn test_force_fifo_queues_wire_safe_single_frame() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 5);
        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        wp.send_scan_response(1, Rc::new(batch), ReplySchema::None, 3, 0, 0, true)
            .unwrap();
        assert_eq!(wp.pending_streams.len(), 1, "force_fifo must enqueue, not emit");
        assert!(
            matches!(
                wp.pending_streams.front().map(|p| &p.kind),
                Some(PendingScanKind::WireSafe { .. })
            ),
            "wire-safe reply queues as the WireSafe variant"
        );
        assert!(walk_frames(ptr).is_empty(), "nothing is emitted at enqueue time");

        wp.emit_pending_scan_chunk();
        assert!(wp.pending_streams.is_empty(), "a one-chunk train pops after one emit");
        let frames = walk_frames(ptr);
        assert_eq!(frames.len(), 1);
        let ctrl = ipc::peek_control_block_ipc(&frames[0].1).unwrap();
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
    }

    /// The non-wire-safe (STRING/TEXT) reply must FIFO too: under force_fifo it
    /// queues as `PendingScan::NonWireSafe` (not immediately emitted), and
    /// `emit_pending_scan_chunk` emits its one blob-capable frame with
    /// FLAG_CONTINUATION | FLAG_SCAN_LAST, then pops. This is the mainline case a
    /// TEXT dimension table hits.
    #[test]
    fn test_force_fifo_queues_non_wire_safe_single_frame() {
        use crate::schema::type_code;

        let dir = worker_temp_dir("force_fifo_text");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def("s", type_code::STRING)];
        let tid = engine.create_table("public.tfifo", &cols, &[0]).unwrap();
        let schema = engine.get_schema_desc(tid).unwrap();
        assert!(!schema_wire_safe(&schema), "STRING schema must be non-wire-safe");

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

        // One all-zero TEXT row (empty inline string), well under MAX_W2M_MSG.
        let batch = zero_batch(schema, 1);
        wp.send_scan_response(tid as u64, Rc::new(batch), ReplySchema::Table(&schema), 5, 0, 0, true)
            .unwrap();
        assert_eq!(wp.pending_streams.len(), 1);
        assert!(
            matches!(
                wp.pending_streams.front().map(|p| &p.kind),
                Some(PendingScanKind::NonWireSafe)
            ),
            "TEXT reply must queue as the NonWireSafe variant under force_fifo"
        );
        assert!(walk_frames(ptr).is_empty(), "nothing is emitted at enqueue time");

        wp.emit_pending_scan_chunk();
        assert!(wp.pending_streams.is_empty(), "the single blob frame pops the train");
        let frames = walk_frames(ptr);
        assert_eq!(frames.len(), 1);
        let ctrl = ipc::peek_control_block_ipc(&frames[0].1).unwrap();
        assert_eq!(ctrl.status, STATUS_OK);
        assert_ne!(ctrl.flags & FLAG_SCAN_LAST, 0);
        assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
        // Decodes via the blob-capable path; the first frame carries the schema block.
        let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("non-wire-safe frame decodes standalone");
        assert!(decoded.schema.is_some(), "the frame carries the schema block");
        assert_eq!(decoded.data_batch.map(|b| b.count).unwrap_or(0), 1);

        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// STRING-column schemas are not wire-safe: send_scan_response sends them as a
    /// single frame without chunking (and returns an error past `ipc::FRAME_CAP`).
    /// The full error path requires a CatalogEngine + an oversized batch; this test
    /// verifies the predicate that gates that branch.
    #[test]
    fn test_string_schema_not_wire_safe() {
        use crate::schema::{type_code, SchemaColumn};
        let sd = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        );
        assert!(
            !schema_wire_safe(&sd),
            "STRING-column schema must not be wire-safe (no chunking)"
        );
        assert!(
            schema_wire_safe(&test_schema()),
            "U64-only schema must be wire-safe (chunking enabled)"
        );
    }

    // -- stream_batch_response / pending_streams FIFO tests --------------------

    /// Read every published message off a test ring in publish order,
    /// returning `(ring_prefix_req_id, frame_bytes)`.
    fn walk_frames(ptr: *mut u8) -> Vec<(u32, Vec<u8>)> {
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut out = Vec::new();
        while let Some((data_ptr, sz, new_rc, req_id)) = unsafe { w2m_ring::try_consume(hdr, ptr as *const u8, rc) } {
            let data = unsafe { std::slice::from_raw_parts(data_ptr, sz as usize) };
            out.push((req_id, data.to_vec()));
            rc = new_rc;
        }
        out
    }

    /// A batch of `count` decodable all-zero rows — used to cross a wire-size
    /// limit without writing that many real bytes.
    fn zero_batch(schema: SchemaDescriptor, count: usize) -> Batch {
        Batch::zeroed(schema, count)
    }

    /// Two queued trains drain strictly FIFO: every frame of train A
    /// (multi-chunk, terminal FLAG_SCAN_LAST) precedes train B's, and B's
    /// first chunk carries B's own schema block.
    #[test]
    fn test_pending_streams_fifo_two_trains() {
        use crate::schema::{type_code, SchemaColumn};

        let schema_a = test_schema();
        // B's schema has 3 columns so its frames are distinguishable from A's.
        let schema_b = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0],
        );
        let batch_a = make_n_row_batch(schema_a, 10);
        let batch_b = make_n_row_batch(schema_b, 5);
        let block_a = Rc::new(ipc::build_schema_wire_block(&schema_a, 1));
        let block_b = Rc::new(ipc::build_schema_wire_block(&schema_b, 2));

        // Budget: first chunk (with A's schema block) carries ~4 rows, so
        // train A spans at least two frames.
        let sz_0 = range_size(&batch_a, 0, Some(block_a.as_slice()));
        let sz_1 = range_size(&batch_a, 1, Some(block_a.as_slice()));
        let budget = sz_0 + (sz_1 - sz_0) * 4;

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        let stride_a = row_stride(&batch_a);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch_a),
            request_id: 11,
            client_id: 0,
            target_id: 1,
            prebuilt_schema: Some(block_a),
            server_version: 0,
            kind: PendingScanKind::WireSafe {
                next_row: 0,
                wire_row_stride: stride_a,
            },
        });
        let stride_b = row_stride(&batch_b);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch_b),
            request_id: 22,
            client_id: 0,
            target_id: 2,
            prebuilt_schema: Some(block_b),
            server_version: 0,
            kind: PendingScanKind::WireSafe {
                next_row: 0,
                wire_row_stride: stride_b,
            },
        });

        // One chunk per pass, as drain_sal drives it.
        wp.reply_frame_budget = budget;
        let mut passes = 0;
        while !wp.pending_streams.is_empty() {
            wp.emit_pending_scan_chunk();
            passes += 1;
            assert!(passes < 50, "trains must drain within a bounded pass count");
        }

        let frames = walk_frames(ptr);
        let a_frames: Vec<_> = frames.iter().filter(|(req, _)| *req == 11).collect();
        let b_frames: Vec<_> = frames.iter().filter(|(req, _)| *req == 22).collect();
        assert!(a_frames.len() >= 2, "budget must split train A into multiple chunks");
        assert!(!b_frames.is_empty());
        let first_b = frames.iter().position(|(req, _)| *req == 22).unwrap();
        let last_a = frames.iter().rposition(|(req, _)| *req == 11).unwrap();
        assert!(last_a < first_b, "train A's frames must FULLY precede train B's");

        // Per train: every frame is a continuation, only the last is terminal,
        // the chunks cover every row exactly once, and the FIRST chunk carries
        // that train's own schema block (decodes standalone with its column
        // count; continuations decode only against the train's schema hint).
        for (req, schema, ncols, total_rows) in
            [(11u32, &schema_a, 2usize, 10usize), (22u32, &schema_b, 3usize, 5usize)]
        {
            let train: Vec<_> = frames.iter().filter(|(r, _)| *r == req).collect();
            let mut rows = 0usize;
            for (i, (_, bytes)) in train.iter().enumerate() {
                let ctrl = ipc::peek_control_block_ipc(bytes).expect("ctrl");
                assert_ne!(ctrl.flags & FLAG_CONTINUATION, 0);
                let is_last = i == train.len() - 1;
                assert_eq!(
                    ctrl.flags & FLAG_SCAN_LAST != 0,
                    is_last,
                    "FLAG_SCAN_LAST only on the train's terminal chunk"
                );
                if i == 0 {
                    let decoded = ipc::decode_wire_ipc(bytes).expect("first chunk must decode standalone");
                    let s = decoded.schema.expect("first chunk carries a schema block");
                    assert_eq!(s.num_columns(), ncols, "the block is this train's schema");
                    rows += decoded.data_batch.map(|b| b.count).unwrap_or(0);
                } else {
                    let hint = ipc::SchemaWithVersion {
                        descriptor: schema,
                        version: 0,
                    };
                    let decoded = ipc::decode_wire_ipc_with_schema(bytes, hint)
                        .expect("continuation decodes against the schema hint");
                    rows += decoded.data_batch.map(|b| b.count).unwrap_or(0);
                }
            }
            assert_eq!(rows, total_rows, "the train's chunks cover all rows exactly once");
        }
    }

    /// A fitting result through `stream_batch_response` must be byte-identical
    /// to `send_response` (same flags, `seek_pk` + `request_id` echo): unicast
    /// consumers forward these slots verbatim, so the single-frame wire shape
    /// must not change. Covers both the non-empty and the empty-result paths.
    #[test]
    fn test_stream_batch_response_single_frame_byte_identical() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 5);

        let (region_ref, writer_ref) = make_ring();
        let (region_new, writer_new) = make_ring();
        let ptr_ref = region_ref.ptr();
        let ptr_new = region_new.ptr();
        let mut wp_ref = make_test_worker(std::ptr::null_mut(), writer_ref);
        let mut wp_new = make_test_worker(std::ptr::null_mut(), writer_new);

        let req = 0xCAFE_u64;
        let client = 7u64;
        let pk = 0xDEAD_BEEF_u128;
        wp_ref
            .send_response(8, Some(&batch), ReplySchema::None, req, client, pk)
            .unwrap();
        wp_ref
            .send_response(8, None, ReplySchema::None, req + 1, client, 0)
            .unwrap();

        assert!(wp_new
            .stream_batch_response(8, Some(batch.clone()), ReplySchema::None, req, client, pk)
            .is_ok());
        assert!(wp_new
            .stream_batch_response(8, None, ReplySchema::None, req + 1, client, 0)
            .is_ok());
        assert!(wp_new.pending_streams.is_empty(), "fitting results never enqueue");

        let ref_frames = walk_frames(ptr_ref);
        let new_frames = walk_frames(ptr_new);
        assert_eq!(ref_frames.len(), 2);
        assert_eq!(
            ref_frames, new_frames,
            "single-frame stream_batch_response must be byte-identical to send_response"
        );
    }

    /// An oversized wire-safe result enqueues a train instead of emitting a
    /// frame past `ipc::FRAME_CAP`; nothing is emitted until drain_sal.
    #[test]
    fn test_stream_batch_response_oversized_enqueues_train() {
        let schema = test_schema(); // 32 B/row on the wire
        let rows = (ipc::FRAME_CAP / 32) + 4096;
        let batch = zero_batch(schema, rows);

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        let err = wp.stream_batch_response(3, Some(batch), ReplySchema::None, 5, 9, 0);
        assert!(err.is_ok(), "oversized wire-safe result must chunk, not error");
        assert_eq!(wp.pending_streams.len(), 1);
        let ps = wp.pending_streams.front().unwrap();
        let PendingScanKind::WireSafe { next_row, .. } = &ps.kind else {
            panic!("oversized wire-safe result must enqueue a WireSafe train");
        };
        assert_eq!(*next_row, 0);
        assert_eq!(ps.request_id, 5);
        assert_eq!(ps.client_id, 9);
        assert!(
            walk_frames(ptr).is_empty(),
            "the train's first chunk is emitted by drain_sal, not at enqueue time"
        );
    }

    fn worker_temp_dir(name: &str) -> String {
        crate::test_support::scratch_dir("worker", name)
    }

    /// An oversized non-wire-safe (STRING) result returns the clean error —
    /// the variable-width streaming chunker is an explicit non-goal.
    #[test]
    fn test_stream_batch_response_oversized_string_errors() {
        use crate::schema::type_code;

        let dir = worker_temp_dir("string_oversized");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def("s", type_code::STRING)];
        let tid = engine.create_table("public.tstr", &cols, &[0]).unwrap();
        let schema = engine.get_schema_desc(tid).unwrap();
        assert!(!schema_wire_safe(&schema));

        // 40 B/row (8 pk + 8 weight + 8 null + 16 string struct), empty blob.
        let rows = (ipc::FRAME_CAP / 40) + 4096;
        let batch = zero_batch(schema, rows);

        let (_region, writer) = make_ring();
        let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);
        let err = wp
            .stream_batch_response(tid as u64, Some(batch), ReplySchema::Table(&schema), 5, 0, 0)
            .expect_err("oversized STRING result must surface the clean error");
        assert!(err.contains("cannot be chunked"), "error names the limitation: {err}");
        assert!(wp.pending_streams.is_empty(), "non-wire-safe results never enqueue");

        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A projected (gather) reply schema must ride a ONE-OFF wire block: the
    /// table-keyed cache must neither serve it (the master would decode
    /// projected rows with the base table's stride) nor store it (a later
    /// table reply would be decoded with the projected stride).
    #[test]
    fn test_stream_batch_response_projected_schema_one_off_block() {
        use crate::schema::type_code;

        let dir = worker_temp_dir("projected_one_off");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ];
        let tid = engine.create_table("public.tproj", &cols, &[0]).unwrap();
        let table_schema = engine.get_schema_desc(tid).unwrap();
        let projected = crate::schema::project_schema(&table_schema, &[1]);
        assert_ne!(projected.num_columns(), table_schema.num_columns());

        let (region, writer) = make_ring();
        let ptr = region.ptr();
        let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

        // Fitting projected reply: one frame carrying the projected schema.
        let small = zero_batch(projected, 2);
        assert!(wp
            .stream_batch_response(tid as u64, Some(small), ReplySchema::OneOff(&projected), 5, 0, 0)
            .is_ok());
        let frames = walk_frames(ptr);
        assert_eq!(frames.len(), 1);
        let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("decode projected reply");
        assert_eq!(
            decoded.schema.expect("schema block present").num_columns(),
            projected.num_columns()
        );

        // Oversized projected reply: the queued train holds the one-off block.
        let rows = (ipc::FRAME_CAP / 32) + 4096;
        let big = zero_batch(projected, rows);
        assert!(wp
            .stream_batch_response(tid as u64, Some(big), ReplySchema::OneOff(&projected), 6, 0, 0)
            .is_ok());
        assert_eq!(wp.pending_streams.len(), 1);
        let expected_block = ipc::build_schema_wire_block(&projected, tid as u32);
        let ps = wp.pending_streams.front().unwrap();
        assert!(
            matches!(ps.kind, PendingScanKind::WireSafe { .. }),
            "oversized projected reply must enqueue a WireSafe train"
        );
        assert_eq!(
            ps.prebuilt_schema.as_deref().map(Vec::as_slice),
            Some(expected_block.as_slice()),
            "the train's schema block is the one-off projected block",
        );

        // Both paths left the table's cached wire block untouched.
        assert!(
            engine.get_cached_schema_wire_block(tid).is_none(),
            "a projected reply must never populate the table's schema-block cache"
        );

        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
