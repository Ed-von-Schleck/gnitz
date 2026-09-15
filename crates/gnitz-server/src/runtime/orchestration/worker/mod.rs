//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::query::{DagEngine, ExchangeCallback};
use crate::runtime::sal::{SalMessage, SalMessageKind, SalReader};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{self as ipc};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationRegistry;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_store::storage::StoreError;
use gnitz_wire::BackfillDecision;
use gnitz_wire::{WireFlags, WireStatus};

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
    SecondaryIndex { cols: gnitz_wire::PkColList },
}

/// One dispatched request. Fully owned — a parked request must not borrow the
/// SAL mapping, which an inline `Flush` resets. No `target_id`: the group header
/// and every slot's control block come off one `WireMsg` template, so `wire`
/// already carries it.
struct Request {
    kind: SalMessageKind,
    /// The header's `lsn`: the `Tick` arm's round, a `FlushEph`'s checkpoint
    /// generation. Travels with the message, so a replayed tick stamps the round
    /// that produced its delta rather than whatever a counter has reached.
    lsn: u64,
    /// The group's request id, which every reply answers on.
    request_id: u32,
    /// Whether each reply must reach the ring in request order.
    fifo: bool,
    wire: ipc::DecodedWire,
}

// ---------------------------------------------------------------------------
// The inline-vs-defer decision
// ---------------------------------------------------------------------------

/// Where a request of some kind runs when the worker is blocked inside an
/// evaluation. At top level every kind runs inline, so this is the whole matrix.
enum InEval {
    /// Run it where it arrives, in the middle of the wait.
    Inline,
    /// Replayed at top level after the outer epoch's ACK.
    DeferPostAck,
    /// Applied when the DAG returns, before the ACK that implies it.
    DeferPreAck,
}

/// The one classifier for the inline-vs-defer decision — a worker policy, hence
/// a free function here rather than a method on the protocol enum. Total, so a
/// new `SalMessageKind` cannot compile without a decision; the behavioural
/// matrix tests pin the non-trivial cells.
fn in_eval(kind: SalMessageKind) -> InEval {
    match kind {
        // An inline tick would re-enter the view with a different source and
        // emit schema-mismatched relays.
        SalMessageKind::Tick => InEval::DeferPostAck,
        // Answered mid-wait, a delta read spans a half-ingested round, and a
        // client that advanced its cursor over it would lose the rest silently.
        SalMessageKind::DeltaRead => InEval::DeferPostAck,
        // An inline catalog mutation races the in-flight evaluation.
        SalMessageKind::DdlSync => InEval::DeferPreAck,
        // Deferring deadlocks: `flush_round` holds a `SalExcl` across the ACK
        // wait, and the tick's relay needs one to write the relay this worker is
        // parked on — so a flush that waits for the relay waits forever.
        SalMessageKind::Flush | SalMessageKind::FlushEph => InEval::Inline,
        // Correct to defer (`commit_pushes` drops its lock before the ACK wait),
        // but it would park the ingest ACK behind an exchange round-trip.
        SalMessageKind::Push => InEval::Inline,
        // A read holding no cursor across calls has no stake in the exchange.
        SalMessageKind::Scan
        | SalMessageKind::ScanSpec
        | SalMessageKind::HasPk
        | SalMessageKind::UniquePreflight
        | SalMessageKind::Backfill
        | SalMessageKind::Shutdown => InEval::Inline,
        // Consumed above the split, and slotless: both die at `dispatch_inner`.
        SalMessageKind::ExchangeRelay | SalMessageKind::ZoneCommit => InEval::Inline,
    }
}

/// A relay delivered to the wait blocked on it: the relayed batch, and the
/// collective backfill decision the master stamped onto it.
struct RelayHit {
    batch: Batch,
    decision: BackfillDecision,
}

struct WorkerExchangeHandler {
    /// [`InEval::DeferPreAck`]: drained inside `evaluate_dag`, so a catalog
    /// mutation lands before the ACK that implies it.
    deferred: Vec<Request>,
    /// [`InEval::DeferPostAck`]: drained at top level, so a replayed tick cannot
    /// re-enter a view whose outer exchange is still awaiting its relay.
    ///
    /// Both queues drain in insertion order, which is SAL order.
    deferred_replay: Vec<Request>,
    /// ExchangeRelay messages whose `(view_id, source_id)` does not match the
    /// active wait, with the backfill decision (`flags.backfill`) that rode them.
    /// Keyed by the pair, not the view: a relay for one source of a join view
    /// must not satisfy a wait for another, which would drive the DAG with the
    /// wrong sharding columns. Nothing in production parks one — a worker has a
    /// single outstanding exchange report — so this only catches a mismatch if
    /// that stops holding.
    pending_relays: HashMap<(i64, i64), (Batch, BackfillDecision)>,
}

/// Bridges the DAG's `ExchangeCallback` requirement to `WorkerProcess` for a
/// maintenance tick. Holds a mutable reference to the worker so `do_exchange`
/// can re-enter the worker's handlers (`handle_push`, `handle_flush_all`)
/// inline when those messages arrive mid-wait.
///
/// A tick issues no pad bit and reads no backfill decision — it has no field to
/// record one in, which is what keeps the collective-termination protocol to
/// the backfill path.
struct TickExchangeCtx<'a> {
    worker: &'a mut WorkerProcess,
}

impl ExchangeCallback for TickExchangeCtx<'_> {
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch {
        let (batch, _) = self.worker.do_exchange_wait(view_id, batch, source_id, false);
        batch
    }
}

/// [`TickExchangeCtx`] for one chunk of a distributed backfill: it stamps the
/// chunk's pad bit onto every exchange it issues, and records the decision the
/// master stamps back.
struct BackfillExchangeCtx<'a> {
    worker: &'a mut WorkerProcess,
    /// Whether this worker's source partition is already drained, so this
    /// chunk's rounds are empty pads. The master ANDs it across workers.
    pad: bool,
    /// The chunk's collective verdict, `None` exactly when the chunk issued no
    /// exchange — a view with no barrier, which `handle_backfill` terminates on
    /// local drain exhaustion instead. Every round of a chunk yields the same
    /// verdict, so writing it per round is a restatement, not a race.
    verdict: Option<BackfillDecision>,
}

impl ExchangeCallback for BackfillExchangeCtx<'_> {
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch {
        let (batch, decision) = self.worker.do_exchange_wait(view_id, batch, source_id, self.pad);
        // The master writes the next round at cursor 0 of a new epoch. Not the
        // flush path: its ACK would read as this worker's terminal ACK.
        if decision == BackfillDecision::Checkpoint {
            self.worker.sal_reader.rewind();
        }
        self.verdict = Some(decision);
        batch
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
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
    /// group under a `SalExcl`, so all worker queues
    /// share one global request order; each master task drains workers in
    /// ascending index order; the earliest-ordered awaited train always has
    /// its frames at the front of some worker's queue with a live consumer.
    ///
    /// A train's chunks are emitted ONLY from `drain_sal` / `run` — never from
    /// `do_exchange_wait`'s inline dispatch loop. That loop can ENQUEUE trains
    /// (the Scan/seek/gather arms run inline inside a wait too); they must stay
    /// queued until the exchange completes. Emitting there would let
    /// `W2mWriter::send_msg` block on a full W2M ring — full because the queued
    /// train's master-side consumer paces a slow client TCP connection — while
    /// the `ExchangeRelay` this worker is waiting for sits unread in the SAL:
    /// the join would stall indefinitely on an unrelated slow client.
    /// Queued-but-unemitted is safe; the relay does not depend on any train
    /// draining.
    ///
    /// A reply that fits ONE frame is not a train and does go out from inside
    /// the wait, taking one ring slot the master drains immediately — where a
    /// train's later chunks would wait on a consumer this worker is blocking.
    pending_streams: VecDeque<PendingScan>,
    /// Per-frame wire budget for every reply train and for the pre-flight key
    /// train: [`ipc::FRAME_CAP`] in production, since every frame reaches the
    /// client verbatim. `GNITZ_REPLY_FRAME_BUDGET` (read once at construction)
    /// shrinks it so e2e tests exercise multi-frame trains with small tables; a
    /// larger value is ignored. A frame that cannot be built inside `FRAME_CAP`
    /// even at one row is the only reply-size failure left.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

pub(crate) use reply::send_unique_preflight_keys;
use reply::PendingScan;
/// The two halves of the pre-flight frame budget, reached only by
/// `runtime::suites::unique_preflight` — production goes through
/// `send_unique_preflight_keys`, which applies both itself.
#[cfg(test)]
pub(crate) use reply::{preflight_frame_overhead, preflight_keys_per_frame};

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
        existing.append_batch(&delta, 0, delta.len());
    } else {
        pending.insert(tid, delta);
    }
}

/// Where one reply goes and how: the relation it names, the request id the
/// master reactor routes it by, and whether it must reach the ring in request
/// order. `dispatch_inner` resolves them together, so no reply helper takes them
/// apart — and no arm can forget the ordering directive its group carried.
#[derive(Clone, Copy, Default)]
struct ReplyRoute {
    target_id: u64,
    request_id: u32,
    /// Queue the reply behind earlier trains even when it fits one frame.
    fifo: bool,
}

/// Which schema wire block a worker reply carries, declared by the dispatch arm
/// that knows where the descriptor came from and resolved by
/// `reply_schema_block`.
#[derive(Clone, Copy)]
enum ReplySchema<'a> {
    /// The target's own schema: the reply serves and populates the table's
    /// cached block, and negotiates its version against the client's.
    Table(&'a SchemaDescriptor),
    /// A schema the reader already holds: no block, the request's version back.
    ReaderHeld,
}

impl WorkerProcess {
    pub fn new(
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
            catalog,
            sal_reader,
            w2m_writer,
            exchange: WorkerExchangeHandler {
                deferred: Vec::new(),
                deferred_replay: Vec::new(),
                pending_relays: HashMap::new(),
            },
            pending_deltas,
            pending_streams: VecDeque::new(),
            reply_frame_budget: gnitz_foundation::env::env_num("GNITZ_REPLY_FRAME_BUDGET", ipc::FRAME_CAP)
                .min(ipc::FRAME_CAP),
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    // ── Main event loop ────────────────────────────────────────────────

    /// Never returns normally. A boot that failed does not reach here at all:
    /// the fork child reports it on the W2M ring and exits. The ready ACK answers
    /// `ready_request_id`, which the master leases before it collects.
    pub fn run(&mut self, ready_request_id: u32) -> i32 {
        self.send_ack(0, ready_request_id);

        loop {
            // Skip the SAL wait while a chunked reply train is in progress: the
            // queued state drives the next drain_sal to emit the next chunk
            // immediately.
            if self.pending_streams.is_empty() {
                self.w2m_writer.sal_park().park(|| self.sal_reader.is_empty());
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
        while let Some((msg, wire)) = self.next_sal_message() {
            self.dispatch_top_level(&msg, wire);
            // Replay whatever an exchange wait deferred — a tick or a delta read
            // — now that the outer tick's ACK has been sent.
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
            for req in std::mem::take(&mut self.exchange.deferred_replay) {
                self.handle_request(req);
            }
        }
    }

    /// The next SAL group to dispatch, with this worker's slot of it. The single
    /// SAL-read choke point — both the top-level loop and the inside-exchange-wait
    /// loop funnel through here.
    ///
    /// The group header's `lsn` stays on the message rather than being latched
    /// into the catalog the way `FlushEph`'s generation is: that latch is correct
    /// for a generation that governs the whole round, and wrong for a tick round
    /// one deferred message must carry to its replay.
    fn next_sal_message(&mut self) -> Option<(SalMessage, &'static [u8])> {
        let (msg, wire) = self.sal_reader.next()?;
        // The ephemeral flush round carries the checkpoint generation in the
        // group header's `lsn` field. Latch it before dispatch so
        // `manifest_header` stamps every view manifest this round publishes with
        // it — and so a later CREATE INDEX in this process gates its resume on
        // the same value.
        if msg.kind == SalMessageKind::FlushEph {
            self.cat().registry_mut().set_resume_generation(msg.lsn);
        }
        Some((msg, wire))
    }

    /// Decode one SAL group's slot into an owned [`Request`]. The single decode
    /// point: both dispatchers and every parked request come through here.
    fn decode_request(&mut self, msg: &SalMessage, wire: &'static [u8]) -> Request {
        // Fail-stop: a dropped group diverges this worker from the master.
        match ipc::decode_sal_slot(wire) {
            Ok(w) => Request {
                kind: msg.kind,
                lsn: msg.lsn,
                request_id: msg.request_id,
                fifo: msg.in_request_order,
                wire: w,
            },
            Err(e) => self.fatal_shutdown(&format!(
                "failed to decode {:?} for tid={}: {e}",
                msg.kind, msg.target_id
            )),
        }
    }

    /// Dispatch a group drained by the main run loop, with no DAG evaluation in
    /// flight: every kind runs where it arrives.
    fn dispatch_top_level(&mut self, msg: &SalMessage, wire: &'static [u8]) {
        // A relay is an exchange wait's completion signal; it can only arrive
        // while the worker is blocked in one.
        if msg.kind == SalMessageKind::ExchangeRelay {
            gnitz_warn!("unexpected ExchangeRelay at top-level dispatch tid={}", msg.target_id);
            return;
        }
        let req = self.decode_request(msg, wire);
        self.handle_request(req);
    }

    /// Dispatch a group drained while blocked in `do_exchange_wait` for the
    /// relay keyed `relay_wait = (view_id, source_id)`. Returns that relay when
    /// this group is it; everything else runs or parks per [`in_eval`].
    fn dispatch_in_eval(&mut self, relay_wait: (i64, i64), msg: &SalMessage, wire: &'static [u8]) -> Option<RelayHit> {
        let req = self.decode_request(msg, wire);
        if req.kind == SalMessageKind::ExchangeRelay {
            return self.take_or_park_relay(relay_wait, req);
        }
        match in_eval(req.kind) {
            InEval::Inline => self.handle_request(req),
            InEval::DeferPostAck => self.exchange.deferred_replay.push(req),
            InEval::DeferPreAck => self.exchange.deferred.push(req),
        }
        None
    }

    /// Deliver a relay to the wait blocked on exactly its `(view_id, source_id)`
    /// pair, or park it in `pending_relays` for a later one.
    fn take_or_park_relay(&mut self, relay_wait: (i64, i64), req: Request) -> Option<RelayHit> {
        let ipc::DecodedWire { control, schema, data_batch } = req.wire;
        let target_id = control.hdr.target_id as i64;
        let key = (target_id, control.hdr.arg0 as i64);
        let decision = control.hdr.flags.backfill;
        // Header-only relay: the master stamps a schema block onto every slot of
        // a relay group, so an empty batch built from the relayed schema is the
        // correct payload. A schema-less relay would leave no way to build it
        // without silently guessing the shape.
        let batch = match (data_batch, schema) {
            (Some(b), _) => b,
            (None, Some(s)) => Batch::empty_with_schema(&s),
            (None, None) => self.fatal_shutdown(&format!("ExchangeRelay for tid={target_id} carries no schema")),
        };
        if relay_wait == key {
            return Some(RelayHit { batch, decision });
        }
        self.exchange.pending_relays.insert(key, (batch, decision));
        None
    }

    /// Run one request, with a failure sent back on its own request id — the one
    /// place a worker's reply status is chosen, and the one fatal path a failed
    /// DDL takes.
    fn handle_request(&mut self, req: Request) {
        let (kind, request_id, target_id) = (req.kind, req.request_id, req.wire.control.hdr.target_id);
        if let Err(fault) = self.dispatch_inner(req) {
            self.send_fault(&fault, request_id);
            if kind == SalMessageKind::DdlSync {
                // DDL application failure on trusted master→worker IPC means
                // memory corruption or an engine bug; continuing would leave
                // this worker with a permanently stale catalog — silently wrong
                // results.
                self.fatal_shutdown(&format!("DdlSync application failed for tid={target_id}: {fault}"));
            }
        }
    }

    fn dispatch_inner(&mut self, req: Request) -> Result<(), gnitz_wire::WireFault> {
        let Request {
            kind,
            lsn,
            request_id,
            fifo,
            wire: mut decoded,
        } = req;
        let hdr = decoded.control.hdr;
        let target_id = hdr.target_id as i64;
        // The version the reader holds: the one the master negotiated for it.
        let client_version = hdr.flags.schema_version;
        let route = ReplyRoute {
            target_id: hdr.target_id,
            request_id,
            fifo,
        };
        let blob: Vec<u8> = std::mem::take(&mut decoded.control.blob);
        let batch = decoded.data_batch;

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
                    if !batch.is_empty() {
                        self.cat().ddl_sync(target_id, lsn, batch)?;
                        // A DROP retracts the table/view's catalog row, so its id is
                        // no longer live. If a push landed between its last tick and
                        // the drop, its pending_deltas entry would never tick again
                        // (the master's tick loop filters dropped ids), so GC any
                        // now-dead ids here — this is a dead entry's only reaper.
                        // Raw reborrow: `self.cat()` would borrow all of self and
                        // conflict with the `pending_deltas` field borrow.
                        let cat = unsafe { &*self.catalog };
                        self.pending_deltas.retain(|tid, _| cat.registry().has_id(*tid));
                        gnitz_debug!("ddl_sync tid={}", target_id);
                    }
                }
                Ok(())
            }

            SalMessageKind::Backfill => {
                // Stop-the-world (the DDL parks the reactor): no yield.
                self.handle_backfill(target_id, hdr.arg0 as i64)?;
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::HasPk => {
                let Some(batch) = batch else {
                    return Err("has_pk: a probe carries its keys".into());
                };
                let lookup = match gnitz_wire::probe_key_columns(hdr.arg1) {
                    None => HasPkLookup::PrimaryKey,
                    Some(packed) => HasPkLookup::SecondaryIndex {
                        cols: self.cat().registry().index_cols(target_id, packed, "has_pk")?,
                    },
                };
                self.handle_has_pk(route, batch, lookup, hdr.flags.probe_mode, hdr.arg0 as usize)
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    if !batch.is_empty() {
                        self.handle_push(target_id, batch)?;
                    }
                }
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::Tick => {
                self.handle_tick(target_id, lsn)?;
                self.send_ack(target_id as u64, request_id);
                Ok(())
            }

            SalMessageKind::Scan => {
                let (result, schema) = self.cat().scan(target_id)?;
                self.send_shared_scan_response(route, result, ReplySchema::Table(&schema), client_version);
                Ok(())
            }

            SalMessageKind::ScanSpec => self.answer_scan_spec(route, &blob, client_version),
            SalMessageKind::DeltaRead => self.answer_delta_read(route, hdr.arg1, hdr.arg0, &blob, client_version),

            SalMessageKind::UniquePreflight => {
                // CREATE UNIQUE INDEX global pre-flight: project this worker's
                // committed partition of `target_id` to OPK leading-key spans,
                // sort them, and stream the sorted spans back for the master's
                // k-way merge. An error here surfaces as the terminal fault frame
                // the master's merge expects (send_fault in handle_request).
                let cols = self
                    .cat()
                    .registry()
                    .index_cols(target_id, hdr.arg1, "unique pre-flight")?;
                self.handle_unique_preflight(target_id, cols.as_slice(), request_id)?;
                Ok(())
            }

            // `dispatch` consumes ExchangeRelay itself in both contexts and
            // never routes it here; the sentinel never reaches `dispatch` at all.
            SalMessageKind::ExchangeRelay | SalMessageKind::ZoneCommit => {
                unreachable!("{kind:?} never reaches dispatch_inner")
            }
        }
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: i64, batch: Batch) -> Result<(), String> {
        // The master's `with_commit_indices` sends a keyed table's worker exactly
        // the rows `route_rows_by_pk` assigns it, and a replicated table's worker
        // the whole batch, so the slot is what this worker ingests.
        if batch.is_empty() {
            return Ok(());
        }
        let row_count = batch.len();
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
        let effective = match self.cat().registry_mut().ingest_returning_effective(target_id, batch) {
            Ok(b) => b,
            // An ingest never produces `DeltaExpired`; the or-pattern is what
            // keeps the match total without a third arm.
            Err(StoreError::Rejected(msg) | StoreError::DeltaExpired(msg)) => return Err(msg),
            Err(e @ StoreError::Storage { .. }) => gnitz_fatal_abort!(
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
    fn handle_tick(&mut self, target_id: i64, round: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.pending_deltas.remove(&target_id) {
            d
        } else {
            if !self.cat().registry().has_id(target_id) {
                return Ok(());
            }
            let schema = self.cat().registry().relation_or_err(target_id)?.schema();
            Batch::empty_with_schema(&schema)
        };
        self.evaluate_dag(target_id, delta, round);
        Ok(())
    }

    /// Answer one `ReadSpec` read, streaming the keeper back without a schema block.
    fn answer_scan_spec(
        &mut self,
        route: ReplyRoute,
        blob: &[u8],
        client_version: u16,
    ) -> Result<(), gnitz_wire::WireFault> {
        let target_id = route.target_id as i64;
        let (spec, reply_block) = gnitz_wire::ReadSpec::decode(blob).map_err(|e| format!("scan_spec: {e}"))?;
        let reply_schema = gnitz_store::schema::decode_schema_block(reply_block)
            .map_err(|e| format!("scan_spec: reply schema block: {e}"))?;
        let keeper = self.cat().scan_spec(target_id, spec, &reply_schema)?;
        self.send_shared_scan_response(route, keeper, ReplySchema::ReaderHeld, client_version);
        Ok(())
    }

    /// Answer one DELTA_POLL view: its deltas in rounds `(after_tick, cut_tick]`.
    fn answer_delta_read(
        &mut self,
        route: ReplyRoute,
        after_tick: u64,
        cut_tick: u64,
        reply_block: &[u8],
        client_version: u16,
    ) -> Result<(), gnitz_wire::WireFault> {
        let target_id = route.target_id as i64;
        let reply_schema = gnitz_store::schema::decode_schema_block(reply_block)
            .map_err(|e| format!("delta_read: reply schema block: {e}"))?;
        let keeper = self.cat().delta_read(target_id, after_tick, cut_tick, &reply_schema)?;
        self.send_shared_scan_response(route, keeper, ReplySchema::ReaderHeld, client_version);
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
    /// exchange frame it issues (`do_exchange_wait`), the master ANDs them and
    /// stamps the verdict back onto each relay, and the worker records it into a
    /// single per-chunk slot read here.
    ///
    /// A view that runs NO exchange has no barrier: no relay arrives, the slot
    /// stays `None`, and the worker self-terminates on local drain exhaustion.
    ///
    /// **View-scoped.** Drives ONLY `view_id` (`backfill_chunk`), never the
    /// source's whole dependent closure: the source may already have populated
    /// dependents (live CREATE VIEW over a source with prior views; recovery
    /// step-4 rebuild next to resumed siblings) that a closure re-drive would
    /// double-count. A view backfill runs stop-the-world (the DDL parks the
    /// reactor), so it never yields to live traffic between chunks.
    fn handle_backfill(&mut self, source_tid: i64, view_id: i64) -> Result<(), String> {
        // Recovery step-4: the FIRST backfill command for an invalid view resets
        // its output store + operator scratch on THIS worker before any fill,
        // so the rebuild starts from an empty, well-formed store (the tick sweep
        // may have polluted its tentatively-loaded state). Gated on the
        // COW-inherited `invalid_views` set and self-clearing, so a multi-source
        // join resets once (on its first source) and the remaining sources fill
        // the just-reset store.
        if self.cat().view_is_invalid(view_id) {
            let (dag, registry) = self.cat().dag_and_registry_mut();
            dag.reset_view_for_rebuild(registry, view_id)?;
            self.cat().clear_invalid_view(view_id);
        }
        // Compiled before the first chunk: a failure here is an error reply, where the
        // same failure inside a chunk's epoch is a fatal abort mid-round.
        let (dag, registry) = self.cat().dag_and_registry_mut();
        dag.ensure_compiled(registry, view_id)?;
        let chunk_rows = self.cat().registry().scan_chunk_rows();
        // Needed to synthesize empty pad chunks. An unregistered source is a
        // fail-stop: DDL_SYNC applies in SAL order, so a worker that cannot see
        // the source has diverged from the catalog.
        let schema = self.cat().registry().relation_or_err(source_tid)?.schema();
        let mut handle = self.cat().open_source_cursor(view_id, source_tid)?;
        let mut produced_any = false;

        loop {
            // `None` ⇒ partition exhausted: this round is an empty PAD. The
            // master ANDs the pad bit across workers and stamps the collective
            // stop/continue/checkpoint decision back onto each relay.
            let drained = handle.drain_chunk(chunk_rows);
            let pad = drained.is_none();
            let chunk = drained.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            let (produced, signal) = self.backfill_chunk(view_id, source_tid, chunk, pad);
            produced_any |= produced;
            // Stop on the master's collective verdict, or — with no barrier,
            // hence no verdict — on local drain exhaustion.
            if signal == Some(BackfillDecision::Stop) || (signal.is_none() && pad) {
                break;
            }
        }

        // `produced_any` is false for a join's first source, which only fills its
        // trace. A spill fault leaves the view store unbounded, so the process
        // cannot continue; the watchdog turns this into a cluster abort.
        let (dag, registry) = self.cat().dag_and_registry_mut();
        if let Err(e) = dag.finish_backfill(registry, view_id, produced_any) {
            gnitz_fatal_abort!(
                "worker: {} — view state cannot be bounded; aborting for restart+re-derive",
                e,
            );
        }
        Ok(())
    }

    /// View-scoped backfill of one chunk: run only `view_id`'s epoch over a
    /// chunk of `source_id` (through the exchange ctx, so its scatter/relay
    /// round runs across the worker barrier) and ingest the output. `pad` says
    /// this worker's partition is already drained. Returns whether the view
    /// produced rows, plus [`BackfillExchangeCtx::verdict`]. The worker analogue
    /// of `evaluate_dag`, for a single view rather than the whole closure.
    fn backfill_chunk(
        &mut self,
        view_id: i64,
        source_id: i64,
        delta: Batch,
        pad: bool,
    ) -> (bool, Option<BackfillDecision>) {
        let (dag, reg) = self.cat().dag_and_registry_mut();
        let (dag, reg) = (dag as *mut DagEngine, reg as *mut RelationRegistry);
        let mut ctx = BackfillExchangeCtx { worker: self, pad, verdict: None };
        let produced = unsafe { &mut *dag }.backfill_chunk(unsafe { &mut *reg }, view_id, source_id, delta, &mut ctx);
        let verdict = ctx.verdict;
        // Apply DDL_SYNC messages deferred during exchange waits (mirrors
        // `evaluate_dag`).
        self.dispatch_deferred();
        match produced {
            Ok(p) => (p, verdict),
            Err(e) => gnitz_fatal_abort!(
                "worker: backfill ingest failed (view_id={}, source_id={}): {} — \
                 aborting for restart+re-derive",
                view_id,
                source_id,
                e,
            ),
        }
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
    /// returns `Err` (surfaced to the master as a clean pre-flight fault by
    /// `handle_request`), never a truncated train.
    ///
    /// MUST observe the same snapshot `backfill_index` will later project:
    /// the master sends this command inside the DDL critical section
    /// (committer barrier drained, catalog write lock held), before the
    /// IDX_TAB +1 broadcast, so no concurrent INSERT can interleave.
    fn handle_unique_preflight(&mut self, owner_id: i64, col_indices: &[u32], request_id: u32) -> Result<(), String> {
        if UNIQUE_PREFLIGHT_ERROR.armed() {
            return Err("injected unique pre-flight fault".to_string());
        }
        // One resolve for all three — the cursor owns its sources by `Rc`, so it
        // outlives the entry borrow and pins the snapshot the DDL section froze.
        let e = self.cat().registry().relation_or_err(owner_id)?;
        let (schema, dir, mut handle) = (e.schema(), e.directory().to_string(), e.cursor());
        // The index circuit is not registered until this pre-flight succeeds, so
        // the key spec is built from the owner schema + column list — the same
        // inputs the master builds from, so the reply frame layout agrees. It
        // also bounds-checks the columns: a protocol mismatch, not a user error.
        let spec = gnitz_store::schema::IndexKeySpec::new(col_indices, &schema)?;
        let idx_schema = spec
            .output_schema(&schema)
            .ok_or_else(|| "Index: composite key is not a valid primary key".to_string())?;
        let frame_schema = crate::runtime::wire::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        let stride = spec.key_size();
        let chunk_rows = self.cat().registry().scan_chunk_rows();

        // Stream the partition chunk-wise, projecting each row to its span and
        // feeding it to the external sort. Peak RAM is the spill budget, not the
        // partition. `key_bytes` keeps the single column→span definition shared
        // with the filter warmup and the master merge. The spill file is an
        // anonymous inode on the owner table's own data disk, so it never leaks
        // and shares the table's filesystem.
        let mut sorter = gnitz_store::storage::SpillSort::new(&dir, stride, unique_preflight_spill_bytes());
        let mut keybuf = PkBuf::zeroed(0);
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            let mb = chunk.as_mem_batch();
            for row in 0..chunk.len() {
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

        // `finish` runs the final spill + merge setup (the last fallible I/O);
        // the returned producer then lends the globally-sorted spans
        // infallibly, one at a time, into the frame train.
        let mut producer = sorter.finish()?;
        send_unique_preflight_keys(
            &self.w2m_writer,
            owner_id as u64,
            &frame_schema,
            request_id,
            self.reply_frame_budget,
            &mut producer,
        );
        Ok(())
    }

    /// Answer one HasPk probe over the keys that exist committed on this
    /// worker; `mode` decides what a match is answered with.
    fn handle_has_pk(
        &mut self,
        route: ReplyRoute,
        batch: Batch,
        lookup: HasPkLookup,
        mode: gnitz_wire::WireProbeMode,
        mode_param: usize,
    ) -> Result<(), gnitz_wire::WireFault> {
        let target_id = route.target_id as i64;
        let n = batch.len();
        if let gnitz_wire::WireProbeMode::Project = mode {
            // `arg1` is the PK sentinel here, so the column to project rides the
            // per-mode parameter word instead.
            if !matches!(lookup, HasPkLookup::PrimaryKey) {
                return Err("has_pk: a projecting probe reads the table's own PK store".into());
            }
            let ref_col = mode_param as u8;
            let mut keys = Vec::with_capacity(n * batch.schema().pk_stride());
            for i in 0..n {
                keys.extend_from_slice(batch.get_pk_bytes(i));
            }
            let result = self.cat().registry().gather_bytes(target_id, keys, ref_col)?;
            self.send_scan_response(route, result, ReplySchema::ReaderHeld, 0);
            return Ok(());
        }
        match lookup {
            HasPkLookup::SecondaryIndex { cols } => {
                let result = {
                    // One resolution of `(target_id, cols)`: the circuit carries
                    // the index table and the span width.
                    let ic = self
                        .cat()
                        .registry()
                        .relation(target_id)
                        .and_then(|r| r.index_on(cols.as_slice()))
                        .ok_or_else(|| format!("No index on columns {:?} for table {}", cols.as_slice(), target_id))?;
                    // The probe's schema is the INDEX table's,
                    // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                    let schema = *batch.schema();
                    // Index layout: PK = (indexed-key span, src_pk_cols). Any
                    // positive-weight match means the value is already in the
                    // index. `open_cursor` keeps a compaction Io/InvalidShard
                    // failure from silently turning a present key into "absent".
                    let mut cursor = ic.cursor();
                    // Prefix-match the WHOLE indexed-value span: OPK puts the
                    // distinguishing bytes last, so a source-width prefix would
                    // match only the zero high bytes. Width off the circuit's own
                    // key spec, so no width crosses the process boundary.
                    let idx_key_size = ic.key_spec().key_size();
                    // Grown on demand, not reserved at the probe count: the
                    // expected hit count on a fresh-key insert is zero, and
                    // `with_capacity` bypasses the batch arena above 2 MiB.
                    let mut result = Batch::empty_with_schema(&schema);
                    for i in 0..n {
                        let pkb = batch.get_pk_bytes(i);
                        let prefix = &pkb[..idx_key_size];
                        // `[span ‖ holder PK]` verbatim: `IndexKeySpec::write_entry`
                        // wrote the source PK at `idx_key_size`, so the caller
                        // splits it back out without decoding anything.
                        if let gnitz_wire::WireProbeMode::AllHolders = mode {
                            // Capped by the asking write's size; at least one,
                            // or an occupied span would answer "no holders".
                            cursor.for_each_positive_with_prefix_capped(prefix, mode_param.max(1), |c| {
                                result.push_key_row(c.current_pk_bytes(), 1);
                            });
                            continue;
                        }
                        if !cursor.seek_first_positive_with_prefix(prefix) {
                            continue;
                        }
                        let holder = matches!(mode, gnitz_wire::WireProbeMode::FirstHolder);
                        result.push_key_row(if holder { cursor.current_pk_bytes() } else { pkb }, 1);
                    }
                    result
                };
                self.send_scan_response(route, result, ReplySchema::ReaderHeld, 0);
                Ok(())
            }
            HasPkLookup::PrimaryKey => {
                let relation = self.cat().registry().relation(target_id);
                // Grown on demand — see the index arm above.
                let mut result = Batch::empty_with_schema(batch.schema());
                for i in 0..n {
                    let pkb = batch.get_pk_bytes(i);
                    // `false` where this process holds no store.
                    if relation.is_some_and(|r| r.has_pk(pkb)) {
                        result.push_key_row(pkb, 1);
                    }
                }
                self.send_scan_response(route, result, ReplySchema::ReaderHeld, 0);
                Ok(())
            }
        }
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

    /// Run multi-worker DAG evaluation with the exchange context. `tick_round` is
    /// the round the triggering group carried, which stamps every fed view's
    /// captured delta.
    fn evaluate_dag(&mut self, source_id: i64, delta: Batch, tick_round: u64) {
        let (dag, reg) = self.cat().dag_and_registry_mut();
        let (dag, reg) = (dag as *mut DagEngine, reg as *mut RelationRegistry);
        let mut ctx = TickExchangeCtx { worker: self };
        let res = unsafe { &mut *dag }.evaluate_dag(unsafe { &mut *reg }, source_id, delta, tick_round, &mut ctx);
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
        if self.cat().registry_mut().base_advanced_since_publish() {
            self.cat().unlink_derived_manifests();
        }
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

/// Keys per W2M frame for the unique pre-flight stream, before
/// `preflight_keys_per_frame` clamps it to what the reply frame budget leaves
/// after the frame's own overhead. A key costs
/// `idx_key_size + 16` on the wire (the OPK span, the weight and the null word),
/// so a full frame at this count is 24 MiB for a single 8-byte column and would
/// be ~96 MiB for an 80-byte composite — which is where the clamp binds.
///
/// A throughput/memory knob with no correctness floor: `InFlightState` grows
/// with however many frames a ring holds.
const UNIQUE_PREFLIGHT_KEYS_PER_FRAME: usize = 1 << 20;

/// [`UNIQUE_PREFLIGHT_KEYS_PER_FRAME`], overridable via
/// `GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME` so tests exercise multi-frame trains
/// with small tables. Read only through `preflight_keys_per_frame`.
fn unique_preflight_keys_per_frame() -> usize {
    gnitz_foundation::env::env_num("GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME", UNIQUE_PREFLIGHT_KEYS_PER_FRAME)
}

/// Default in-RAM key-byte budget before the pre-flight sort spills a run (128 MiB).
const UNIQUE_PREFLIGHT_SPILL_BYTES: usize = 128 * 1024 * 1024;

/// Byte budget of accumulated key spans before the pre-flight's external sort
/// spills a sorted run to disk. Read from `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`
/// (a production memory lever, honoured in every build), default 128 MiB. Peak
/// worker RAM during the pre-flight is roughly this budget plus the sort index
/// and one reorder buffer — bounded regardless of partition size.
fn unique_preflight_spill_bytes() -> usize {
    gnitz_foundation::env::env_num("GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES", UNIQUE_PREFLIGHT_SPILL_BYTES)
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;
