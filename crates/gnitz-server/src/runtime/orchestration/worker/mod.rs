//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::runtime::m2w::Wake;
use crate::runtime::sal::{SalMessage, SalMessageKind, SalReader};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{
    self as ipc, BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_STOP, BACKFILL_PAD_BIT, FLAG_SCAN_LAST,
};
use gnitz_engine::catalog::{CatalogEngine, IngestError, FIRST_USER_TABLE_ID};
use gnitz_engine::foundation::fault::Seam;
use gnitz_engine::query::{DagEngine, ExchangeCallback};
use gnitz_engine::schema::key::PkBuf;
use gnitz_engine::schema::SchemaDescriptor;
use gnitz_engine::storage::Batch;
use gnitz_engine::storage::BlobCacheGuard;
use gnitz_wire::{FLAG_CONTINUATION, FLAG_EXCHANGE, STATUS_OK};

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// Lookup target for HasPk requests.
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
    /// `CatalogEngine::validate_index_cols` at the dispatch arm.
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

/// A SAL message deferred out of a *blocking* evaluation poll (an exchange
/// wait), replayed at the next top-level drain in SAL arrival order.
///
/// Both variants decode EAGERLY at defer time: `Flush` runs inline in `InEval`
/// and resets the SAL, so no raw wire pointer may be stashed across the wait —
/// the discipline `DeferredDdl` already follows by storing a decoded `Batch`.
/// Eager is also why the replay dispatches on the entry rather than re-entering
/// `run_via_dispatch_inner`, which takes the raw `&'static [u8]` mapping and
/// decodes it itself.
///
/// **One queue for both**, drained in insertion order, which is SAL order: a tick
/// deferred before a read is replayed before it. `deferred: Vec<DeferredDdl>`
/// stays separate even though it reads alike, because it is drained at a
/// different point with a different meaning — `replay_deferred` runs at top
/// level *after* the outer tick's ACK, `dispatch_deferred` inside `evaluate_dag`
/// *before* it. Drain the merged queue at the earlier point and a deferred tick
/// re-enters the DAG, which is the thing tick-deferral exists to prevent; drain
/// it at the later point and a catalog mutation lands after an ACK that implied
/// it had not.
#[derive(Debug)]
enum Deferred {
    /// A `Tick`. Replay needs the view id, the round that produced it, and
    /// the original request id so the replayed ACK is routable.
    ///
    /// The round **travels with the message**: a latched "current round" would
    /// stamp the replay with a *later* round than the one that produced it.
    Tick { target_id: i64, round: u64, req_id: u64 },
    /// A delta read. Carries the whole request, `seek_pk` included — the one it
    /// would be easiest to leave out, and where the master put the interval's
    /// upper cut. A replayed read that lost it would cut at `T = 0`, return
    /// nothing, and still be answered with a terminal frame reporting the real
    /// `T`: the client would advance its cursor over rounds it never received,
    /// the exact silent loss the cut was added to prevent.
    DeltaRead {
        target_id: i64,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
        seek_pk_extra: Vec<u8>,
    },
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
    /// A maintenance `Tick` — or a delta read — encountered inside a *blocking*
    /// evaluation poll (an exchange wait) is stashed (decoded, see [`Deferred`])
    /// and replayed at the next top-level drain ([`WorkerProcess::replay_deferred`])
    /// in SAL arrival order, after the current tick's ACK is sent, so the master
    /// observes ACKs in SAL arrival order and a later tick cannot re-enter
    /// `view_id` while an outer exchange for the same view is still awaiting its
    /// relay.
    deferred_replay: Vec<Deferred>,
    /// ExchangeRelay messages whose `(view_id, source_id)` doesn't
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
/// that kicked off this DAG evaluation. Nothing routes on it: the master peels
/// a FLAG_EXCHANGE frame off by flag, and keys its rounds on
/// `target_id`/`seek_pk`. Carrying a live tick id is why that peel must happen
/// before the id's awaiter is completed.
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
    /// `release_cursor` (released in ring order, `w2m.rs`) could then never
    /// pass them, the ring fills, the worker blocks in `W2mWriter::send_msg`, and the
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
    /// `W2mWriter::send_msg` block on a full W2M ring — full because the queued
    /// train's master-side consumer paces a slow client TCP connection — while
    /// the `ExchangeRelay` this worker is waiting for sits unread in the SAL:
    /// the join would stall indefinitely on an unrelated slow client.
    /// Queued-but-unemitted is safe; the relay does not depend on any train
    /// draining.
    pending_streams: VecDeque<PendingScan>,
    /// Per-frame wire budget for chunked reply trains (`send_scan_response`,
    /// `stream_batch_response`, `emit_pending_scan_chunk`): [`ipc::FRAME_CAP`]
    /// in production, since every chunk reaches the client as one frame.
    /// `GNITZ_REPLY_FRAME_BUDGET` (read once at construction) shrinks it so e2e
    /// tests exercise multi-frame trains with small tables; a larger value is
    /// ignored. The master parks a full train per ring while draining another
    /// worker, but `InFlightState` grows to track it, so the train length an
    /// override produces is bounded only by the ring's byte capacity — there is
    /// no per-train frame-count ceiling.
    ///
    /// This budgets only the chunk split point; single-frame paths that cannot
    /// chunk (blob-bearing STRING replies) check `FRAME_CAP` directly.
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
/// client decodes the reply against its own copy, so no block is emitted, but
/// the descriptor is still consulted for `has_german_string`, which is what
/// keeps a STRING projection off the chunking path.
#[derive(Clone, Copy)]
enum ReplySchema<'a> {
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
                deferred_replay: Vec::new(),
                pending_relays: HashMap::new(),
                backfill_pad: None,
                backfill_signal: None,
            },
            pending_deltas,
            pending_streams: VecDeque::new(),
            reply_frame_budget: gnitz_engine::foundation::env::env_num("GNITZ_REPLY_FRAME_BUDGET", ipc::FRAME_CAP)
                .min(ipc::FRAME_CAP),
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    // ── Main event loop ────────────────────────────────────────────────

    pub fn run(&mut self, boot_error: Option<String>) -> i32 {
        if let Some(e) = boot_error {
            // Master's wait_all_workers turns this nonzero status into a boot
            // abort BEFORE the SAL is rewound — the replayed data's only durable
            // copy survives for the next boot.
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
                match self.sal_reader.wait(1000) {
                    Wake::Signalled => {}
                    Wake::Idle if self.master_is_gone() => self.shutdown(),
                    Wake::Idle | Wake::Failed => continue,
                }
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups. Shutdown `_exit`s inline.
    fn drain_sal(&mut self) {
        // Emit the next chunk of the FRONT pending train before draining new
        // SAL messages. One chunk per drain_sal pass; `send_msg` provides
        // backpressure. Single-frame replies for other requests still go out
        // immediately between chunks (distinct ring-prefix request ids; the
        // master reactor routes per id).
        if !self.pending_streams.is_empty() {
            self.emit_pending_scan_chunk();
        }
        while let Some(msg) = self.next_sal_message() {
            let wire = self.sal_reader.my_slot(&msg);
            // Only an exchange wait can match a relay; the top-level dispatcher
            // classifies ExchangeRelay as a protocol bug and returns nothing.
            let matched = self.dispatch(DispatchContext::TopLevel, &msg, wire);
            debug_assert!(matched.is_none(), "relay matched at top-level drain_sal");
            // Replay whatever an exchange wait deferred — a tick or a delta read
            // — now that the outer tick's ACK has been sent. Pushes are handled
            // inline in `do_exchange_wait` (safe because a user-table push only
            // appends to `pending_deltas`), so they are never deferred.
            if !self.exchange.deferred_replay.is_empty() {
                self.replay_deferred();
            }
        }
    }

    /// Replay whatever a blocking evaluation poll deferred, at top level and in
    /// SAL arrival order. Drained into a scratch vec first — a replayed tick may
    /// itself reach an exchange wait and defer more entries into a fresh
    /// `deferred_replay` — and looped until the queue stays empty.
    fn replay_deferred(&mut self) {
        while !self.exchange.deferred_replay.is_empty() {
            for entry in std::mem::take(&mut self.exchange.deferred_replay) {
                match entry {
                    Deferred::Tick {
                        target_id,
                        round,
                        req_id,
                    } => match self.handle_tick(target_id, round, req_id) {
                        Ok(()) => self.send_ack(target_id as u64, req_id),
                        Err(e) => self.send_error(&e, req_id),
                    },
                    // The same body the inline arm runs, so a deferred read
                    // answers with the frame train it would have answered with
                    // inline — a tick-shaped `send_ack` is not a reply it has.
                    Deferred::DeltaRead {
                        target_id,
                        request_id,
                        client_id,
                        seek_pk,
                        seek_pk_extra,
                    } => {
                        if let Err(f) = self.answer_scan_spec(target_id, request_id, client_id, seek_pk, &seek_pk_extra)
                        {
                            self.send_fault(&f, request_id);
                        }
                    }
                }
            }
        }
    }

    /// The next SAL group to dispatch. The single SAL-read choke point — both
    /// the top-level loop and the inside-exchange-wait loop funnel through here.
    ///
    /// The group header's `lsn` joins the tuple rather than being latched into the
    /// catalog the way `FlushEph`'s generation is: that latch is correct for
    /// a generation that governs the whole round, and wrong for a tick round one
    /// deferred message must carry to its replay.
    fn next_sal_message(&mut self) -> Option<SalMessage> {
        let msg = self.sal_reader.next()?;
        // The ephemeral flush round carries the checkpoint generation in the
        // group header's `lsn` field. Latch it before dispatch so
        // `manifest_header` stamps every view manifest this round publishes with
        // it — and so a later CREATE INDEX in this process gates its resume on
        // the same value.
        if msg.kind == SalMessageKind::FlushEph {
            self.cat().set_resume_generation(msg.lsn);
        }
        Some(msg)
    }

    /// The single source of truth for the inline-vs-defer matrix. Match
    /// exhaustiveness (`match (ctx, kind)`) means a new `SalMessageKind` variant
    /// cannot be added without explicitly deciding its behavior in both
    /// contexts; the walk-the-matrix tests pin the non-trivial cells.
    ///
    /// Nearly every kind runs inline in both contexts. Only five cells differ,
    /// and each is here because getting it wrong broke something:
    ///
    /// * **Tick defers inside an evaluation.** An inline tick eval would
    ///   re-enter `view_id` with a different source and produce
    ///   schema-mismatched relays. Defer + replay after the outer
    ///   tick's ACK so the master observes ACKs in SAL arrival order.
    ///   See `WorkerExchangeHandler::deferred_replay`.
    ///
    /// * **A delta ScanSpec defers inside an evaluation, and a plain one does
    ///   not.** A read answered while the worker is parked in `do_exchange_wait`
    ///   sees a half-ingested round — some of that round's views ingested, others
    ///   not, and a sibling tick group of the same round sitting deferred behind
    ///   it. A client that then advanced its cursor to that round would lose the
    ///   rest of it silently, and no row-set comparison would show it. It shares
    ///   the tick's replay FIFO so the replay order is SAL order. A plain read
    ///   holds no cursor across calls, so deferring it would only make an ad-hoc
    ///   point read wait out an exchange round-trip it has no stake in.
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
    fn dispatch(&mut self, ctx: DispatchContext, msg: &SalMessage, wire: Option<&'static [u8]>) -> Option<Batch> {
        // A slot exists but not ours — not for us. No slot for anyone — a
        // control-only group (the commit sentinel, a checkpoint round), which
        // every worker acts on.
        if wire.is_none() && msg.slots() > 0 {
            return None;
        }
        let (kind, target_id, group_lsn) = (msg.kind, msg.target_id as i64, msg.lsn);

        match (ctx, kind) {
            // ── Tick (maintenance): inline at top-level; defer inside an
            //    in-flight evaluation — an inline tick would re-enter the DAG
            //    with a different source and emit schema-mismatched relays.
            (DispatchContext::TopLevel, SalMessageKind::Tick) => {
                self.run_via_dispatch_inner(kind, target_id, group_lsn, wire)
            }
            (DispatchContext::InEval { .. }, SalMessageKind::Tick) => {
                // Only the request id is needed; peek the control block instead of
                // decoding the whole frame. The round comes off the group header,
                // so the replay stamps the round that produced this delta rather
                // than whatever the counter reaches by then.
                let req_id = wire
                    .and_then(|d| ipc::peek_frame_control(d).ok())
                    .map(|c| c.request_id)
                    .unwrap_or(0);
                self.exchange.deferred_replay.push(Deferred::Tick {
                    target_id,
                    round: group_lsn,
                    req_id,
                });
                None
            }

            // ── Delta ScanSpec: inline at top-level; defer inside an in-flight
            //    evaluation, where the answer would span a half-ingested round.
            (DispatchContext::TopLevel, SalMessageKind::ScanSpec { delta: true }) => {
                self.run_via_dispatch_inner(kind, target_id, group_lsn, wire)
            }
            (DispatchContext::InEval { .. }, SalMessageKind::ScanSpec { delta: true }) => {
                let Some(data) = wire else {
                    // Unicast-shaped, so the `wire.is_none()` guard above already
                    // filtered a slot that is not ours.
                    unreachable!("a delta read with no payload (filtered by dispatch's None guard)")
                };
                match ipc::decode_wire(data) {
                    Ok(mut decoded) => self.exchange.deferred_replay.push(Deferred::DeltaRead {
                        target_id,
                        request_id: decoded.control.request_id,
                        client_id: decoded.control.client_id,
                        seek_pk: decoded.control.seek_pk,
                        seek_pk_extra: std::mem::take(&mut decoded.control.seek_pk_extra),
                    }),
                    // Dropping it would leave the client waiting forever on a
                    // request the master has already leased ids for.
                    Err(e) => self.fatal_shutdown(&format!(
                        "failed to decode deferred delta read for tid={target_id}: {e}"
                    )),
                }
                None
            }

            // ── DdlSync (catalog mutation): apply at top-level; defer inside —
            //    an inline catalog mutation races in-flight DAG eval.
            (DispatchContext::TopLevel, SalMessageKind::DdlSync) => {
                self.run_via_dispatch_inner(kind, target_id, group_lsn, wire)
            }
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
            | (_, SalMessageKind::ScanSpec { delta: false })
            | (_, SalMessageKind::Scan) => self.run_via_dispatch_inner(kind, target_id, group_lsn, wire),
        }
    }

    /// Decode (with the context-appropriate schema cache policy) and dispatch
    /// through `dispatch_inner`. A failure is sent on the W2M ring with the
    /// inbound request_id so the master reactor can route it back to the original
    /// caller, carrying whatever status the fault names — this is the one place a
    /// worker's reply status is chosen, so a typed refusal needs no path of its
    /// own to reach the wire.
    fn run_via_dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        group_lsn: u64,
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
        if let Err(fault) = self.dispatch_inner(kind, target_id, group_lsn, decoded, request_id) {
            self.send_fault(&fault, request_id);
            if kind == SalMessageKind::DdlSync {
                // DDL application failure on trusted master→worker IPC means
                // memory corruption or an engine bug; continuing would leave
                // this worker with a permanently stale catalog.
                self.fatal_shutdown(&format!("DdlSync application failed for tid={target_id}: {fault}"));
            }
        }
        None
    }

    fn dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        group_lsn: u64,
        mut decoded: Option<ipc::DecodedWire>,
        request_id: u64,
    ) -> Result<(), gnitz_wire::WireFault> {
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
                self.sal_reader.rewind();
                self.handle_flush_all()?;
                self.send_ack(0, request_id);
                Ok(())
            }

            // Ephemeral-state flush round: persist every view's operator-trace
            // tables and output stores, stamped with the checkpoint generation this
            // round's header already latched into the catalog at the classify site.
            SalMessageKind::FlushEph => {
                self.sal_reader.rewind();
                self.cat().flush_ephemeral_round()?;
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
                    self.cat().validate_index_cols(target_id, cols, "has_pk")?;
                }
                self.handle_has_pk(target_id, batch, lookup, request_id, client_id, seek_pk)
            }

            SalMessageKind::Gather => {
                // The projected column index rides in `seek_col_idx`. The PK
                // batch arrives in `data_batch` (a worker with an empty
                // sublist still replies — the master joins one reply per
                // worker). PKs come pre-sorted from the master's global sort
                // (scatter preserves per-worker order), aiding the cursor.
                let ref_col = seek_col_idx as u8;
                // The batch PK region holds verbatim OPK bytes (the master packs
                // them via `extend_pk_bytes`), so lend them to the seek directly,
                // at every PK width. Round-tripping a narrow key back through
                // `get_pk` → `opk_key` would re-OPK-encode it (double sign-flip
                // for signed; scrambled compound bytes), probing a key that
                // matches no stored row.
                let (result, schema) = match batch.as_ref() {
                    Some(b) => {
                        let keys = (0..b.count).map(|i| b.get_pk_bytes(i));
                        self.cat().gather_family_bytes(target_id, keys, ref_col)?
                    }
                    // A worker with an empty sublist still replies — the master
                    // joins one reply per worker.
                    None => self.cat().gather_family_bytes(target_id, std::iter::empty(), ref_col)?,
                };
                // The projected reply schema is synthetic — never the
                // table's cached block.
                self.stream_batch_response(
                    target_id as u64,
                    Some(result),
                    ReplySchema::OneOff(&schema),
                    request_id,
                    client_id,
                    0,
                )
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    if batch.count > 0 {
                        self.handle_push(target_id, batch)?;
                    }
                }
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::Tick => {
                self.handle_tick(target_id, group_lsn, request_id)?;
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::SeekByIndex => {
                let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
                self.cat().validate_index_cols(target_id, &cols, "seek_by_index")?;
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

            SalMessageKind::ScanSpec { .. } => {
                self.answer_scan_spec(target_id, request_id, client_id, seek_pk, &seek_pk_extra)
            }

            SalMessageKind::UniquePreflight => {
                // CREATE UNIQUE INDEX global pre-flight: project this worker's
                // committed partition of `target_id` to OPK leading-key spans for
                // the column list in `seek_col_idx` (packed via pack_pk_cols),
                // sort them, and stream the sorted spans back for the master's
                // k-way merge. An error here surfaces as the terminal fault frame
                // the master's merge expects (send_error in run_via_dispatch_inner).
                let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
                self.cat().validate_index_cols(target_id, &cols, "unique pre-flight")?;
                self.handle_unique_preflight(target_id, cols.as_slice(), request_id)?;
                Ok(())
            }

            // `dispatch` consumes ExchangeRelay itself in both contexts and
            // never routes it here.
            SalMessageKind::ExchangeRelay => unreachable!("ExchangeRelay never reaches dispatch_inner"),
        }
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: i64, batch: Batch) -> Result<(), String> {
        // Master pre-partitions Push rows in `scatter::with_group`,
        // so every slot already contains only this worker's rows. A second
        // partition-hash filter here would be pure overhead.
        if batch.count == 0 {
            return Ok(());
        }
        let row_count = batch.count;
        if target_id < FIRST_USER_TABLE_ID {
            // Master never sends Push for system tables; system-table
            // changes arrive via DdlSync → ddl_sync. Reaching here
            // means a protocol invariant was violated.
            return Err(format!(
                "a Push group named system table_id={target_id}; a system family arrives as DdlSync"
            ));
        }
        // A storage fault here means committed data was not applied while the
        // client already holds a durability ACK, so this worker has diverged from
        // the durable SAL. Restart + SAL replay re-applies the batch — its zone
        // stays above the flushed-shard watermark — where a fault reply would
        // neither apply nor replay it and the next checkpoint would orphan it.
        let effective = match self.cat().ingest_returning_effective(target_id, batch) {
            Ok(b) => b,
            Err(IngestError::Rejected(msg)) => return Err(msg),
            Err(IngestError::Storage(e)) => gnitz_fatal_abort!(
                "worker: push apply failed (table_id={}): {} — committed data not \
                 applied, state diverged from durable SAL; aborting for restart+replay",
                target_id,
                e,
            ),
        };
        buffer_pending_delta(&mut self.pending_deltas, target_id, effective);
        gnitz_debug!("push tid={} rows={}", target_id, row_count);
        Ok(())
    }

    /// Drive one view-maintenance tick of `target_id`'s dependent closure.
    /// `round` is the tick round the master allocated for this group; every fed
    /// view's captured delta is stamped with it.
    fn handle_tick(&mut self, target_id: i64, round: u64, request_id: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.pending_deltas.remove(&target_id) {
            d
        } else {
            if !self.cat().has_id(target_id) {
                return Ok(());
            }
            let schema = self.cat().schema_or_err(target_id, "tick")?;
            Batch::empty_with_schema(&schema)
        };
        self.evaluate_dag(target_id, delta, round, request_id);
        Ok(())
    }

    /// Answer one `ReadSpec` read: split the control block's `seek_pk_extra` into
    /// the encoded spec and the client's reply-schema block, run the read, and
    /// stream the keeper back with no schema block of its own — the client
    /// authored that schema and decodes against its own copy.
    ///
    /// Shared by the inline dispatch arm and by the replay of a deferred delta
    /// read, so a deferred read answers with the frame train it would have
    /// answered with inline.
    ///
    /// `seek_pk`'s low half is the cut the master sampled when it wrote this
    /// read's group — the last tick round it had emitted — which bounds an
    /// incremental delta read above. Every other bound ignores it.
    ///
    /// A refusal that is not `STATUS_ERROR` — today only a delta cursor below this
    /// worker's retention floor — needs nothing of its own: the fault carries its
    /// status out through the one reply path every other failure here takes.
    fn answer_scan_spec(
        &mut self,
        target_id: i64,
        request_id: u64,
        client_id: u64,
        seek_pk: u128,
        seek_pk_extra: &[u8],
    ) -> Result<(), gnitz_wire::WireFault> {
        let (spec_bytes, reply_block) =
            gnitz_wire::unpack_scan_spec_extra(seek_pk_extra).map_err(|e| format!("scan_spec: {e}"))?;
        let spec = gnitz_wire::ReadSpec::decode(spec_bytes.0).map_err(|e| format!("scan_spec: {e}"))?;
        let reply_schema = gnitz_engine::schema::decode_schema_block(reply_block, true)
            .map_err(|e| format!("scan_spec: reply schema block: {e}"))?;
        let keeper = self
            .cat()
            .scan_spec_family(target_id, &spec, &reply_schema, seek_pk as u64)?;
        // No row of an incremental reply may exceed the cut: the interval is
        // closed by construction there, and a below-bound regression — a round
        // landing after the read group — is what this makes loud in the debug
        // binary the E2E suite runs.
        debug_assert!(
            !matches!(spec.bound, gnitz_wire::ReadBound::Delta { after_tick } if after_tick > 0)
                || (0..keeper.count).all(|r| {
                    let pk = keeper.get_pk_bytes(r);
                    pk.len() >= 8 && u64::from_be_bytes(pk[..8].try_into().unwrap()) <= seek_pk as u64
                }),
            "delta reply carries a row above the cut {}",
            seek_pk as u64,
        );
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
        if self.cat().view_is_invalid(view_id) {
            self.cat().reset_view_output_for_rebuild(view_id)?;
            self.cat().clear_invalid_view(view_id);
        }
        let chunk_rows = self.cat().ddl_scan_chunk_rows();
        // Needed to synthesize empty pad chunks. An unregistered source is a
        // fail-stop: DDL_SYNC applies in SAL order, so a worker that cannot see
        // the source has diverged from the catalog.
        let schema = self.cat().schema_or_err(source_tid, "backfill")?;
        let mut handle = self.cat().open_source_cursor(view_id, source_tid)?;
        let mut produced_any = false;

        loop {
            // `None` ⇒ partition exhausted: this round is an empty PAD. The
            // master ANDs the pad bit across workers and stamps the collective
            // stop/continue/checkpoint decision back onto each relay.
            let drained = handle.drain_chunk(chunk_rows);
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
        // Release the last chunk's pinned delta registers and trace cursors. This
        // loop drives only `view_id`, so that is the one regfile it can have pinned.
        self.cat().dag_mut().release_view_regfile_deltas(view_id);
        // `backfill_view_step` bypasses the closure driver's per-view flush, so
        // flush the view's output trace once after the final chunk. Only when it
        // produced rows (the first source of a join produces none — it just
        // fills its trace).
        if produced_any {
            // A spill fault leaves the view store unbounded, so the process
            // cannot continue. The master's watchdog turns the dead worker into a
            // cluster abort, and restart re-derives the view.
            if let Err(e) = self.cat().dag_mut().flush(view_id) {
                gnitz_fatal_abort!(
                    "worker: view store flush failed (view_id={}): {} — view state \
                     cannot be bounded; aborting for restart+re-derive",
                    view_id,
                    e,
                );
            }
        }
        Ok(())
    }

    /// View-scoped backfill of one chunk: run only `view_id`'s epoch over a
    /// chunk of `source_id` (through the exchange ctx, so its scatter/relay
    /// round runs across the worker barrier) and ingest the output. Returns
    /// whether the view produced rows. The worker analogue of `evaluate_dag`
    /// but for a single view rather than the source's whole closure.
    fn backfill_view_step(&mut self, view_id: i64, source_id: i64, delta: Batch, request_id: u64) -> bool {
        let dag = self.cat().dag_mut() as *mut DagEngine;
        let mut ctx = WorkerExchangeCtx {
            worker: self,
            tick_request_id: request_id,
        };
        let produced = unsafe { &mut *dag }.backfill_view_step_multi_worker(view_id, source_id, delta, &mut ctx);
        // Apply DDL_SYNC messages deferred during exchange waits (mirrors
        // `evaluate_dag`).
        self.dispatch_deferred();
        match produced {
            Ok(p) => p,
            Err(e) => gnitz_fatal_abort!(
                "worker: backfill ingest failed (view_id={}, source_id={}): {} — \
                 aborting for restart+re-derive",
                view_id,
                source_id,
                e,
            ),
        }
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
        // check. Deliberately NOT the Flush arm — no `handle_flush_all`, no
        // flush ACK; the master's consumption proof is the next round's
        // FLAG_EXCHANGE report, which a flush ACK would be misread as a
        // terminal ACK that retires the worker.
        if decision == BACKFILL_DECISION_CHECKPOINT {
            self.sal_reader.rewind();
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
        let schema = self.cat().schema_or_err(owner_id, "unique pre-flight")?;
        // The index circuit is not registered until this pre-flight succeeds, so
        // build its schema from the owner schema + column list — identical inputs
        // to the master's own build, so the reply frame layout agrees by
        // construction. `make_index_schema` also bounds-checks the columns (a
        // protocol-level mismatch rather than a user error) and yields the
        // promoted per-column types/sizes for the span.
        let idx_schema = gnitz_engine::schema::make_index_schema(col_indices, &schema)?;
        let spec = gnitz_engine::schema::IndexKeySpec::new(col_indices, &schema, &idx_schema);
        let frame_schema = crate::runtime::wire::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // The spill file is an anonymous inode on the owner table's own data
        // disk, so it never leaks and shares the table's filesystem.
        let stride = spec.key_size();
        let dir = self
            .cat()
            .table_directory(owner_id)
            .ok_or_else(|| format!("unique pre-flight: no directory for table {owner_id}"))?
            .to_string();
        let chunk_rows = self.cat().ddl_scan_chunk_rows();

        // Stream the partition chunk-wise, projecting each row to its span and
        // feeding it to the external sort. Peak RAM is the spill budget, not the
        // partition. `key_bytes` keeps the single column→span definition shared
        // with the filter warmup and the master merge.
        let mut sorter = gnitz_engine::storage::SpillSort::new(&dir, stride, unique_preflight_spill_bytes());
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
    ) -> Result<(), gnitz_wire::WireFault> {
        // Each arm resolves the probe and names the schema its reply carries;
        // the single exit below frames both the same way.
        let (result, schema, one_off) = match lookup {
            HasPkLookup::SecondaryIndex { cols, want_holder } => {
                // One resolution of `(target_id, cols)`: the circuit carries both
                // the index table and its schema.
                let ic = self
                    .cat()
                    .index_circuit_for_cols(target_id, cols.as_slice())
                    .ok_or_else(|| format!("No index on columns {:?} for table {}", cols.as_slice(), target_id))?;
                // The check target is the unique INDEX table, whose schema is
                // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                // `idx_key_size` and the PK-byte reads below come from it, so an
                // owner-table schema would compute the wrong prefix width.
                let schema = batch.as_ref().map_or(ic.index_schema, |b| b.schema);
                // Index layout: PK = (indexed-key field, src_pk_cols). Any
                // positive-weight match means the value is already in the index.
                // `open_cursor` keeps a compaction Io/InvalidShard failure from
                // silently turning a present key into "absent".
                let mut cursor = ic.table_mut().open_cursor();
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
                let schema = self.cat().schema_or_err(target_id, "has_pk")?;
                let store = self.cat().get_store_handle(target_id);
                // Route on verbatim OPK bytes for every PK width: feeding `get_pk`
                // (OPK-widened) to `has_pk(u128)` would re-OPK-encode it, a double
                // sign-flip that misses signed PKs.
                let result = filter_by_pk_bytes(batch.as_ref(), schema, |pkb, _| {
                    if store.is_some_and(|t| t.has_pk_bytes(pkb)) {
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
        self.cat().flush_base_round()
    }

    /// Run multi-worker DAG evaluation with the exchange context.
    /// `request_id` is the master's request id of the message that
    /// triggered this evaluation (Tick / Push / Backfill);
    /// echoed by `do_exchange_wait` so the master accumulator's wakers
    /// stay routable. `tick_round` is the round that group carried, which stamps
    /// every fed view's captured delta.
    fn evaluate_dag(&mut self, source_id: i64, delta: Batch, tick_round: u64, request_id: u64) {
        let dag = self.cat().dag_mut() as *mut DagEngine;
        let mut ctx = WorkerExchangeCtx {
            worker: self,
            tick_request_id: request_id,
        };
        let res = unsafe { &mut *dag }.evaluate_dag_multi_worker(source_id, delta, tick_round, &mut ctx);
        // Apply DDL_SYNC messages deferred during exchange waits.
        self.dispatch_deferred();
        // The whole tick path funnels through the call above, so this is the one
        // site that answers a storage fault during view maintenance. Restart
        // re-derives every view from its durable base tables.
        if let Err(e) = res {
            gnitz_fatal_abort!(
                "worker: view-maintenance tick failed (source_id={}, round={}): {} — \
                 aborting for restart+re-derive",
                source_id,
                tick_round,
                e,
            );
        }
    }

    /// Whether the master process has exited (killed, or `gnitz_fatal_abort`).
    /// The fallback to `PR_SET_PDEATHSIG`; `master_pid == 0` in unit tests, which
    /// have no master to outlive.
    fn master_is_gone(&self) -> bool {
        self.master_pid != 0 && unsafe { libc::getppid() } != self.master_pid
    }

    /// Publish and exit. The publish is not optional — a mid-backfill
    /// `checkpoint_reset` can have discarded the SAL entries that are these rows'
    /// only other durable copy — but there may be no master left to bump the
    /// generation ahead of it, since the watchdog's crashed-worker path
    /// broadcasts `Shutdown` with no barrier behind it. So the worker
    /// invalidates its own derived state instead, and unlinks before publishing
    /// so a crash between the two errs toward a rebuild.
    ///
    /// A graceful stop already ran a full sequence, so the base cut has not
    /// advanced and only the flush runs — which is what lets it still resume.
    fn shutdown(&mut self) -> ! {
        if self.cat().dag_mut().base_advanced_since_publish() {
            self.unlink_derived_manifests();
        }
        let _ = self.handle_flush_all();
        unsafe { libc::_exit(0) }
    }

    /// Unlink the manifest of every store the ephemeral round persists, so the
    /// next open peeks `None` and erases those shards instead of resuming them.
    fn unlink_derived_manifests(&mut self) {
        let (traces, outputs) = self.cat().dag_mut().collect_ephemeral_flush_tables();
        for t in traces.into_iter().chain(outputs) {
            t.unlink_manifest();
        }
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

/// Frame size for the unique pre-flight stream, overridable via
/// `GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME` so tests exercise multi-frame trains
/// with small tables; any value is safe now that `InFlightState` grows with the
/// parked depth (see [`UNIQUE_PREFLIGHT_KEYS_PER_FRAME`]).
fn unique_preflight_keys_per_frame() -> usize {
    gnitz_engine::foundation::env::env_num("GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME", UNIQUE_PREFLIGHT_KEYS_PER_FRAME)
}

/// Default in-RAM key-byte budget before the pre-flight sort spills a run (128 MiB).
const UNIQUE_PREFLIGHT_SPILL_BYTES: usize = 128 * 1024 * 1024;

/// Byte budget of accumulated key spans before the pre-flight's external sort
/// spills a sorted run to disk. Read from `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`
/// (a production memory lever, honoured in every build), default 128 MiB. Peak
/// worker RAM during the pre-flight is roughly this budget plus the sort index
/// and one reorder buffer — bounded regardless of partition size.
fn unique_preflight_spill_bytes() -> usize {
    gnitz_engine::foundation::env::env_num("GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES", UNIQUE_PREFLIGHT_SPILL_BYTES)
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;
