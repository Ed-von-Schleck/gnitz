//! Worker process event loop.
//!
//! Owns a subset of partitions for every user table. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::{HashMap, VecDeque};
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::query::ExchangeCallback;
use crate::runtime::sal::{
    schema_wire_safe, SalMessageKind, SalReader, BACKFILL_DECISION_CHECKPOINT, BACKFILL_DECISION_STOP,
    BACKFILL_PAD_BIT, FLAG_CHECKPOINT, FLAG_EXCHANGE,
};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::w2m_ring;
use crate::runtime::wire::{self as ipc, FLAG_CONTINUATION, FLAG_SCAN_LAST, STATUS_ERROR, STATUS_OK};
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;
use crate::storage::{BlobCacheGuard, FlushWork, PkBuf};

// ---------------------------------------------------------------------------
// PendingScan
// ---------------------------------------------------------------------------

/// State for one multi-chunk reply train (scan, or an oversized seek/gather
/// reply) in progress. The worker emits one frame per `drain_sal` pass;
/// `next_row == 0` means the first chunk still needs a schema block. Non-zero
/// `next_row` is a pure-data continuation.
struct PendingScan {
    batch: Rc<Batch>,
    next_row: usize,
    request_id: u64,
    client_id: u64,
    target_id: u64,
    prebuilt_schema: Option<Rc<Vec<u8>>>,
    server_version: u16,
}

// ---------------------------------------------------------------------------
// WorkerExchangeHandler
// ---------------------------------------------------------------------------

/// Lookup target for FLAG_HAS_PK requests.
enum HasPkLookup {
    /// Check the table's primary-key store.
    PrimaryKey,
    /// Check a unique secondary index on the carried column list (single- or
    /// multi-column; a composite index is located by its exact list).
    UniqueIndex(gnitz_wire::PkColList),
}

impl HasPkLookup {
    /// `seek_col_idx == 0` → PrimaryKey. Otherwise `seek_col_idx` carries
    /// `pack_pk_cols(col_indices)` whose packed flag (bit 63) is always set, so a
    /// real index check is never 0 and never collides with the PK sentinel.
    fn from_wire(seek_col_idx: u64) -> Self {
        if seek_col_idx == 0 {
            HasPkLookup::PrimaryKey
        } else {
            HasPkLookup::UniqueIndex(gnitz_wire::unpack_pk_cols(seek_col_idx))
        }
    }
}

/// A DDL_SYNC message received during an exchange wait, decoded eagerly.
struct DeferredDdl {
    target_id: i64,
    batch: Batch,
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
// DispatchContext + DispatchOutcome
//
// `dispatch` is the single source of truth for the inline-vs-defer matrix
// (see the doc above `WorkerProcess::dispatch`). The context tells the
// dispatcher whether the worker is at top-level draining the SAL or
// blocked inside `do_exchange_wait`; the same `(context, kind)` pair
// always maps to the same decision so behavior cannot drift between the
// two call sites.
// ---------------------------------------------------------------------------

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Copy)]
enum DispatchContext {
    /// The worker is draining the SAL from its main run loop.
    TopLevel,
    /// The worker is blocked in `do_exchange_wait`, awaiting an
    /// EXCHANGE_RELAY whose `(view_id, source_id)` matches `want_key`.
    /// `schema` is the outgoing batch's schema; used to fabricate an
    /// empty relay payload if the master sends a header-only relay.
    InsideExchangeWait {
        want_key: (i64, i64),
        schema: Option<SchemaDescriptor>,
    },
}

/// Result of a single `dispatch` call.
#[allow(clippy::large_enum_variant)]
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
            "stash_preloaded: view_id={view_id} already has a preloaded batch — \
             multi-source preloaded for the same view would silently overwrite",
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
    fn do_exchange(&mut self, view_id: i64, batch: &Batch, source_id: i64) -> Batch {
        self.worker
            .do_exchange_wait(view_id, batch, source_id, self.tick_request_id)
    }
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    worker_id: u32,
    #[allow(dead_code)]
    num_workers: usize,
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
    /// would sit parked in `scan_parked` holding un-released ring slots;
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
    /// `stream_batch_response`, `emit_pending_scan_chunk`): `MAX_W2M_MSG` in
    /// production. Debug builds may shrink it via `GNITZ_REPLY_FRAME_BUDGET`
    /// (read once at construction) so e2e tests exercise multi-frame trains
    /// with small tables. Any override must keep each worker's whole train
    /// under `W2M_MAX_IN_FLIGHT` (64) frames — the master parks a full train
    /// per ring while draining another worker — so reply sizes in such tests
    /// must stay below 64 × budget.
    ///
    /// This budgets only the chunk split point; single-frame paths that cannot
    /// chunk (non-wire-safe STRING replies) check the hard `MAX_W2M_MSG` ring
    /// limit instead.
    reply_frame_budget: usize,
    read_cursor: u64,
    expected_epoch: u32,
}

mod exchange;
mod fsync;
mod reply;

use fsync::uring_batch_fdatasync;

/// Debug-only test seam: parse env var `var` as a `usize`. Always `None` in
/// release builds, which never read the environment.
fn debug_env_usize(var: &str) -> Option<usize> {
    #[cfg(debug_assertions)]
    {
        std::env::var(var).ok().and_then(|v| v.parse().ok())
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = var;
        None
    }
}

/// Provenance of the schema attached to a worker reply, declared by the
/// dispatch arm that knows where the descriptor came from.
///
/// `Table` is the target table's own schema: the reply may serve (and
/// populate) the table's cached schema wire block. `OneOff` is a projected or
/// synthetic schema — Gather's projection, HasPk UniqueIndex's index schema —
/// whose block is built fresh per reply: serving the table's cached block for
/// those would make the master decode the frames with the table's row stride,
/// and caching them would poison the table's block.
#[derive(Clone, Copy)]
enum ReplySchema<'a> {
    None,
    Table(&'a SchemaDescriptor),
    OneOff(&'a SchemaDescriptor),
}

impl<'a> ReplySchema<'a> {
    /// The table's own schema, or `None` when the catalog has no such table.
    fn table(s: Option<&'a SchemaDescriptor>) -> Self {
        s.map_or(ReplySchema::None, ReplySchema::Table)
    }

    fn descriptor(self) -> Option<&'a SchemaDescriptor> {
        match self {
            ReplySchema::None => None,
            ReplySchema::Table(s) | ReplySchema::OneOff(s) => Some(s),
        }
    }
}

/// Filter a check-batch to the rows whose PK `exists` accepts, copying each
/// matched row into the result. Keys on verbatim OPK bytes, correct for every
/// PK width. `exists` receives the row's raw OPK PK bytes.
fn filter_by_pk_bytes(
    batch: &Option<Batch>,
    schema: SchemaDescriptor,
    n: usize,
    mut exists: impl FnMut(&[u8]) -> bool,
) -> Batch {
    let mut result = Batch::with_schema(schema, n);
    if let Some(ref b) = batch {
        let mut blob_cache = BlobCacheGuard::acquire(&schema, n);
        for i in 0..n {
            let pkb = b.get_pk_bytes(i);
            if exists(pkb) {
                result.append_row_from_source_bytes(pkb, 1, b, i, blob_cache.get_mut());
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
        // Isolate this worker's scratch operator-state tables from siblings'
        // (all forked workers share the view directory under one base_dir).
        crate::foundation::worker_ctx::set_worker_rank(worker_id, num_workers as u32);
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
                backfill_pad: None,
                backfill_signal: None,
            },
            pending_deltas: HashMap::new(),
            pending_streams: VecDeque::new(),
            reply_frame_budget: debug_env_usize("GNITZ_REPLY_FRAME_BUDGET")
                .filter(|&n| n > 0 && n <= w2m_ring::MAX_W2M_MSG as usize)
                .unwrap_or(w2m_ring::MAX_W2M_MSG as usize),
            read_cursor: 0,
            expected_epoch: 1,
        }
    }

    fn cat(&mut self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// Decode `seek_col_idx` — `pack_pk_cols(col_indices)`, whose packed flag
    /// (bit 63) is always set, so a real index seek is never 0 — and validate
    /// every column against the table's schema before touching the catalog.
    /// Shared by the SeekByIndex and SeekByIndexRange arms.
    fn validated_index_cols(
        &mut self,
        target_id: i64,
        seek_col_idx: u64,
        op: &str,
    ) -> Result<gnitz_wire::PkColList, String> {
        let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
        match self.cat().get_schema_desc(target_id).map(|s| s.num_columns()) {
            Some(nc) if cols.is_well_formed() && cols.as_slice().iter().all(|&c| (c as usize) < nc) => Ok(cols),
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
        self.send_ack(0, 0, 0);

        loop {
            // Skip the SAL wait while a chunked reply train is in progress: the
            // queued state drives the next drain_sal to emit the next chunk
            // immediately.
            if self.pending_streams.is_empty() {
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
            }

            if self.drain_sal() {
                return 0;
            }
        }
    }

    /// Process all pending SAL message groups. Returns true on shutdown.
    fn drain_sal(&mut self) -> bool {
        // Emit the next chunk of the FRONT pending train before draining new
        // SAL messages. One chunk per drain_sal pass; send_encoded provides
        // backpressure. Single-frame replies for other requests still go out
        // immediately between chunks (distinct ring-prefix request ids; the
        // master reactor routes per id).
        if !self.pending_streams.is_empty() {
            self.emit_pending_scan_chunk();
        }
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

    /// Emit one frame of the FRONT pending train; pops it off the queue when
    /// its terminal chunk is sent. Called at the top of every `drain_sal` pass
    /// while `pending_streams` is non-empty (see the field doc for why
    /// emission is FIFO and confined to `drain_sal` / `run`). Unit tests set
    /// a small `reply_frame_budget` to force multi-frame trains from small
    /// batches.
    fn emit_pending_scan_chunk(&mut self) {
        let budget = self.reply_frame_budget;
        let (batch, next_row, request_id, client_id, target_id, prebuilt_schema, server_version) = {
            let ps = match self.pending_streams.front() {
                Some(ps) => ps,
                None => return,
            };
            (
                Rc::clone(&ps.batch),
                ps.next_row,
                ps.request_id,
                ps.client_id,
                ps.target_id,
                ps.prebuilt_schema.clone(),
                ps.server_version,
            )
        };

        let is_first = next_row == 0;
        // prebuilt_opt drives the schema block: Some on the first chunk when the
        // client needs the schema, None on continuations and schema-suppressed frames.
        // encode_wire_into_range uses the prebuilt bytes directly; no schema arg needed.
        let prebuilt_opt: Option<&[u8]> = if is_first {
            prebuilt_schema.as_deref().map(Vec::as_slice)
        } else {
            None
        };

        let remaining = batch.count - next_row;
        // Compute rows per chunk via linear interpolation (wire-safe schemas have
        // constant per-row stride, so wire size is linear in count).
        let sz_0 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 0, prebuilt_opt);
        let sz_1 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch, 1, prebuilt_opt);
        let per_row = sz_1.saturating_sub(sz_0);
        let usable = budget.saturating_sub(sz_0);
        let max_rows = match usable.checked_div(per_row) {
            Some(rows) => rows.max(1).min(remaining),
            None => remaining.max(1), // per_row == 0: constant wire size, send all
        };
        let has_more = next_row + max_rows < batch.count;
        // FLAG_CONTINUATION is always set on worker scan frames so the client's
        // loop termination ("stop on no FLAG_CONTINUATION") still works.
        // FLAG_SCAN_LAST is the W2M-internal signal that this is the last chunk.
        // server_version is always embedded so the master decode path can verify.
        let flags: u64 = gnitz_wire::wire_flags_set_schema_version(
            FLAG_CONTINUATION | if !has_more { FLAG_SCAN_LAST } else { 0 },
            server_version,
        );
        // wire_size_range is linear in count for wire-safe schemas; avoid a third call.
        let sz = sz_0 + per_row * max_rows;
        self.w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_range(
                buf,
                0,
                target_id,
                client_id,
                flags,
                0,
                STATUS_OK, // request_id=0 in payload; ring prefix carries the req_id
                None,
                &batch,
                next_row,
                max_rows,
                prebuilt_opt,
            );
        });

        if has_more {
            self.pending_streams
                .front_mut()
                .expect("emit_pending_scan_chunk: front train vanished mid-emit")
                .next_row = next_row + max_rows;
        } else {
            self.pending_streams.pop_front();
        }
    }

    /// Drain one SAL group, advancing `read_cursor` only on a clean read.
    ///
    /// `expected_epoch` is re-read on each call: a FLAG_FLUSH dispatched
    /// inline bumps it, and a stale snapshot would cause every
    /// post-checkpoint message to fail the epoch check forever (spinning
    /// at an unchanged cursor). The cursor is advanced *only* on a
    /// successful epoch match — a post-epoch group remains parked at the
    /// cursor until the cursor is reset by checkpoint.
    fn next_sal_message(&mut self) -> Option<(SalMessageKind, i64, Option<&'static [u8]>)> {
        if self.read_cursor + 8 >= self.sal_reader.mmap_size() {
            return None;
        }
        let (msg, new_cursor) = self.sal_reader.try_read(self.read_cursor)?;
        if msg.epoch != self.expected_epoch {
            return None;
        }
        self.read_cursor = new_cursor;
        Some((msg.kind, msg.target_id as i64, msg.wire_data))
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
                Some((k, t, w)) => {
                    kind = k;
                    target_id = t;
                    wire = w;
                }
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
    /// | UniquePreflight   | inline                 | inline                                |
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
            (DispatchContext::TopLevel, SalMessageKind::Tick) => self.run_via_dispatch_inner(kind, target_id, wire),
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::Tick) => {
                let req_id = wire
                    .and_then(|d| ipc::decode_wire(d).ok())
                    .map(|d| d.control.request_id)
                    .unwrap_or(0);
                self.exchange.deferred_ticks.push((target_id, req_id));
                DispatchOutcome::Continue
            }

            // ── DdlSync: apply at top-level, defer inside ──────────────
            (DispatchContext::TopLevel, SalMessageKind::DdlSync) => self.run_via_dispatch_inner(kind, target_id, wire),
            (DispatchContext::InsideExchangeWait { .. }, SalMessageKind::DdlSync) => {
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
                            // Shut down rather than continue with a stale catalog.
                            gnitz_warn!(
                                "W{} FATAL: failed to decode deferred DDL for tid={}: {}",
                                self.worker_id,
                                target_id,
                                e,
                            );
                            self.shutdown(); // calls libc::_exit — does not return
                            return DispatchOutcome::Shutdown; // unreachable; satisfies type
                        }
                    }
                }
                DispatchOutcome::Continue
            }

            // ── ExchangeRelay: unreachable at top, match-or-queue inside
            (DispatchContext::TopLevel, SalMessageKind::ExchangeRelay) => {
                gnitz_warn!(
                    "W{} unexpected ExchangeRelay at top-level dispatch tid={}",
                    self.worker_id,
                    target_id,
                );
                DispatchOutcome::Continue
            }
            (DispatchContext::InsideExchangeWait { want_key, schema }, SalMessageKind::ExchangeRelay) => {
                // source_id is echoed back via seek_pk; the backfill round
                // decision rides in seek_col_idx. Decode both before taking the
                // batch out.
                let decoded = wire.and_then(|d| ipc::decode_wire(d).ok());
                let relay_source_id = decoded.as_ref().map(|d| d.control.seek_pk as i64).unwrap_or(0);
                let relay_decision = decoded.as_ref().map(|d| d.control.seek_col_idx).unwrap_or(0);
                let relay_batch = decoded.and_then(|d| d.data_batch).unwrap_or_else(|| {
                    let empty_schema = schema.unwrap_or_default();
                    Batch::with_schema(empty_schema, 0)
                });
                let relay_key = (target_id, relay_source_id);
                if relay_key == want_key {
                    // Consumed for the active wait: act on the decision (record
                    // the slot, apply any inline checkpoint) before returning.
                    self.consume_backfill_decision(relay_decision);
                    return DispatchOutcome::RelayMatched(relay_batch);
                }
                // Not the relay we're blocked on: park it (with its decision) for
                // a later wait. During backfill the cluster runs in lockstep and
                // this never fires, but carrying the decision keeps it correct if
                // it ever does.
                self.exchange
                    .pending_relays
                    .insert(relay_key, (relay_batch, relay_decision));
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
            | (_, SalMessageKind::Gather)
            | (_, SalMessageKind::UniquePreflight)
            | (_, SalMessageKind::Push)
            | (_, SalMessageKind::SeekByIndex)
            | (_, SalMessageKind::SeekByIndexRange)
            | (_, SalMessageKind::Seek)
            | (_, SalMessageKind::Scan) => self.run_via_dispatch_inner(kind, target_id, wire),
        }
    }

    /// Decode (with the context-appropriate schema cache policy) and
    /// dispatch through `dispatch_inner`, mapping its `DispatchResult`
    /// into a `DispatchOutcome`. Errors are sent on the W2M ring with
    /// the inbound request_id so the master reactor can route the
    /// failure back to the original caller.
    fn run_via_dispatch_inner(
        &mut self,
        kind: SalMessageKind,
        target_id: i64,
        wire: Option<&'static [u8]>,
    ) -> DispatchOutcome {
        let decoded = wire.and_then(|data| ipc::decode_wire(data).ok());
        let request_id = decoded.as_ref().map(|d| d.control.request_id).unwrap_or(0);
        match self.dispatch_inner(kind, target_id, decoded, request_id) {
            DispatchResult::Continue => DispatchOutcome::Continue,
            DispatchResult::Error(msg) => {
                self.send_error(&msg, request_id);
                if kind == SalMessageKind::DdlSync {
                    // DDL application failure on trusted master→worker IPC means
                    // memory corruption or an engine bug; continuing would leave
                    // this worker with a permanently stale catalog.
                    gnitz_warn!(
                        "W{} FATAL: DdlSync application failed for tid={}: {}. Shutting down.",
                        self.worker_id,
                        target_id,
                        msg,
                    );
                    self.shutdown(); // calls libc::_exit — does not return
                    return DispatchOutcome::Shutdown; // unreachable; satisfies type
                }
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
        let ctrl_wire_flags = decoded.as_ref().map(|d| d.control.flags).unwrap_or(0);
        let client_version = gnitz_wire::wire_flags_get_schema_version(ctrl_wire_flags);
        // Wide-PK seek key tail (bytes 16..stride); empty for narrow PKs. Must be
        // extracted before `decoded` is consumed by the `data_batch` take below.
        let seek_pk_extra: Vec<u8> = decoded
            .as_ref()
            .map(|d| d.control.seek_pk_extra.clone())
            .unwrap_or_default();

        // Extract batch (consumes decoded)
        let batch = decoded.and_then(|d| d.data_batch);

        match kind {
            SalMessageKind::Shutdown => {
                self.shutdown();
                DispatchResult::Shutdown
            }

            SalMessageKind::Flush => {
                self.advance_read_epoch();
                match self.handle_flush_all() {
                    Ok(()) => self.send_ack(0, FLAG_CHECKPOINT as u64, request_id),
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
                        gnitz_debug!("W{} ddl_sync tid={}", self.worker_id, target_id);
                    }
                }
                DispatchResult::Continue
            }

            SalMessageKind::Backfill => {
                // `target_id` is the source table; `seek_pk` carries the view to
                // drive (0 = boot's whole-closure mode, see `handle_backfill`).
                if let Err(msg) = self.handle_backfill(target_id, seek_pk as i64, request_id) {
                    return DispatchResult::Error(msg);
                }
                self.send_ack(target_id as u64, 0, request_id);
                DispatchResult::Continue
            }

            SalMessageKind::HasPk => {
                let lookup = HasPkLookup::from_wire(seek_col_idx);
                if let Err(msg) = self.handle_has_pk(target_id, batch, lookup, request_id, client_id, seek_pk) {
                    return DispatchResult::Error(msg);
                }
                DispatchResult::Continue
            }

            SalMessageKind::Gather => {
                // The projected column mask rides in `seek_col_idx`. The PK
                // batch arrives in `data_batch` (a worker with an empty
                // sublist still replies — the master joins one reply per
                // worker). PKs come pre-sorted from the master's global sort
                // (scatter preserves per-worker order), aiding the cursor.
                let project: Vec<u8> = crate::runtime::sal::unpack_gather_cols(seek_col_idx).collect();
                // The batch PK region holds verbatim OPK bytes (the master packs
                // them via `extend_pk_bytes`), so seek them directly with
                // `gather_family_bytes` for every PK width. Round-tripping a
                // narrow key back through `get_pk` → `opk_key` would re-OPK-encode
                // it (double sign-flip for signed; scrambled compound bytes),
                // probing a key that matches no stored row. `get_pk_bytes` works
                // for both narrow and wide PKs.
                let pks: Vec<PkBuf> = match &batch {
                    Some(b) => (0..b.count).map(|i| PkBuf::from_bytes(b.get_pk_bytes(i))).collect(),
                    None => Vec::new(),
                };
                let gathered = self.cat().gather_family_bytes(target_id, &pks, &project);
                match gathered {
                    Ok(result) => {
                        // The projected reply schema is synthetic — never the
                        // table's cached block.
                        let schema = result.schema;
                        if let Some(err) = self.stream_batch_response(
                            target_id as u64,
                            Some(result),
                            schema.as_ref().map_or(ReplySchema::None, ReplySchema::OneOff),
                            request_id,
                            client_id,
                            0,
                        ) {
                            return DispatchResult::Error(err);
                        }
                    }
                    Err(msg) => return DispatchResult::Error(msg),
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
                let cols = match self.validated_index_cols(target_id, seek_col_idx, "seek_by_index") {
                    Ok(cols) => cols,
                    Err(msg) => return DispatchResult::Error(msg),
                };
                // Reassemble the K native values: slot 0 in seek_pk, slots 1..K in
                // seek_pk_extra. A prefix seek supplies K < cols.len() values. Each
                // extra slot is exactly 16 bytes; reject a misaligned payload at
                // the trust boundary rather than silently dropping trailing bytes.
                if !seek_pk_extra.len().is_multiple_of(16) {
                    return DispatchResult::Error(
                        "seek_by_index: seek_pk_extra length is not a multiple of 16".to_string(),
                    );
                }
                let k = 1 + seek_pk_extra.len() / 16;
                if k > cols.as_slice().len() {
                    return DispatchResult::Error(format!(
                        "seek_by_index: {k} key values exceed index arity {}",
                        cols.as_slice().len()
                    ));
                }
                let mut natives = [0u128; gnitz_wire::PK_LIST_MAX_COLS];
                natives[0] = seek_pk;
                for i in 1..k {
                    natives[i] = u128::from_le_bytes(seek_pk_extra[(i - 1) * 16..i * 16].try_into().unwrap());
                }
                match self.cat().seek_by_index(target_id, cols.as_slice(), &natives[..k]) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        if let Some(err) = self.stream_batch_response(
                            target_id as u64,
                            result,
                            ReplySchema::table(schema.as_ref()),
                            request_id,
                            client_id,
                            seek_pk,
                        ) {
                            return DispatchResult::Error(err);
                        }
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::SeekByIndexRange => {
                let cols = match self.validated_index_cols(target_id, seek_col_idx, "seek_by_index_range") {
                    Ok(cols) => cols,
                    Err(msg) => return DispatchResult::Error(msg),
                };
                // Decode the range descriptor from seek_pk_extra. `decode`
                // validates the exact length and arity cap at the trust boundary
                // (mirroring the SeekByIndex `% 16` guard), so a malformed frame
                // is rejected rather than mis-decoded; the arity check against
                // the actual column list is the engine method's self-guard,
                // whose Err surfaces through the same error path below.
                let desc = match gnitz_wire::RangeDescriptor::decode(&seek_pk_extra) {
                    Ok(desc) => desc,
                    Err(e) => return DispatchResult::Error(format!("seek_by_index_range: {e}")),
                };
                match self.cat().seek_by_index_range(target_id, cols.as_slice(), &desc) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        if let Some(err) = self.stream_batch_response(
                            target_id as u64,
                            result,
                            ReplySchema::table(schema.as_ref()),
                            request_id,
                            client_id,
                            0,
                        ) {
                            return DispatchResult::Error(err);
                        }
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Seek => {
                // The full seek key arrives as the wire pair seek_pk (low ≤16
                // native bytes) + seek_pk_extra (the 16..stride suffix, empty for
                // narrow PKs). `seek_family` decodes it through `seek_opk_bytes`
                // at every width — user and system tables alike, no width fork.
                let result = self.cat().seek_family(target_id, seek_pk, &seek_pk_extra);
                match result {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        self.send_response(
                            target_id as u64,
                            result.as_ref(),
                            ReplySchema::table(schema.as_ref()),
                            request_id,
                            client_id,
                            seek_pk,
                        );
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::Scan => {
                match self.cat().scan_family(target_id) {
                    Ok(result) => {
                        let schema = self.cat().get_schema_desc(target_id);
                        if let Some(err) = self.send_scan_response(
                            target_id as u64,
                            result,
                            ReplySchema::table(schema.as_ref()),
                            request_id,
                            client_id,
                            client_version,
                        ) {
                            return DispatchResult::Error(err);
                        }
                    }
                    Err(msg) => return DispatchResult::Error(msg),
                }
                DispatchResult::Continue
            }

            SalMessageKind::UniquePreflight => {
                // CREATE UNIQUE INDEX global pre-flight: project this worker's
                // committed partition of `target_id` to OPK leading-key spans for
                // the column list in `seek_col_idx` (packed via pack_pk_cols),
                // sort them, and stream the sorted spans back for the master's
                // k-way merge. An error here surfaces as the terminal fault frame
                // the master's merge expects (send_error in run_via_dispatch_inner).
                let cols = gnitz_wire::unpack_pk_cols(seek_col_idx);
                if !cols.is_well_formed() {
                    return DispatchResult::Error(format!(
                        "unique pre-flight: invalid column list for table {target_id}"
                    ));
                }
                if let Err(msg) = self.handle_unique_preflight(target_id, cols.as_slice(), request_id) {
                    return DispatchResult::Error(msg);
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
                    self.worker_id,
                    kind,
                    target_id,
                );
                DispatchResult::Continue
            }
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
            return Err(format!(
                "W{}: FLAG_PUSH for system table_id={}; expected DDL_SYNC",
                self.worker_id, target_id
            ));
        } else {
            let effective = self.cat().ingest_returning_effective(target_id, batch)?;
            if let Some(existing) = self.pending_deltas.get_mut(&target_id) {
                existing.append_batch(&effective, 0, effective.count);
            } else {
                self.pending_deltas.insert(target_id, effective);
            }
        }
        gnitz_debug!("W{} push tid={} rows={}", self.worker_id, target_id, row_count);
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
            Batch::with_schema(schema, 0)
        };
        self.evaluate_dag(target_id, delta, request_id);
        Ok(())
    }

    /// Distributed CREATE-VIEW backfill, worker side. Streams this worker's
    /// committed `source_tid` partition through the incremental plan one chunk at
    /// a time (peak RAM ~O(chunk), not O(partition)), driving an exchange round
    /// per chunk per exchanging view across the cross-worker barrier.
    ///
    /// All workers must issue the SAME number of rounds, but partitions are
    /// unequal — so a worker that has drained its partition keeps issuing EMPTY
    /// (pad) rounds to stay in lockstep, until the master signals stop. The stop
    /// decision is collective: each worker stamps a per-chunk pad bit onto every
    /// FLAG_EXCHANGE it issues (`do_exchange_wait`), the master ANDs them and
    /// stamps the verdict back onto each relay, and the worker records it into a
    /// single per-chunk slot read here. A worker missing the source opens no
    /// cursor and pads every round — the barrier still needs its report.
    ///
    /// A source feeding NO exchange view has no barrier: no relay arrives, the
    /// slot stays `None`, and the worker self-terminates on local drain
    /// exhaustion.
    ///
    /// **Scope (`view_id`).** `view_id == 0` is boot's *closure* mode: drive
    /// `source_tid`'s whole dependent closure (`evaluate_dag`), correct because
    /// every view starts empty at boot. A nonzero `view_id` is the live
    /// CREATE-VIEW *view-scoped* mode: drive ONLY that new view
    /// (`backfill_view_step`). The source already has populated existing
    /// dependents that a closure re-drive would double-count — view-scoping is
    /// what keeps a live CREATE over a source with prior views correct.
    fn handle_backfill(&mut self, source_tid: i64, view_id: i64, request_id: u64) -> Result<(), String> {
        let chunk_rows = self.cat().ddl_scan_chunk_rows;
        let has = self.cat().has_id(source_tid);
        // Needed to synthesize empty pad chunks. A missing source still pads.
        let schema = self
            .cat()
            .get_schema_desc(source_tid)
            .ok_or_else(|| format!("backfill: no schema for source {source_tid}"))?;
        let mut handle = if has {
            self.cat().open_store_cursor(source_tid)
        } else {
            None
        };
        let mut produced_any = false;

        loop {
            // `None` ⇒ partition exhausted (or absent): this round is an empty
            // PAD. The master ANDs the pad bit across workers and stamps the
            // collective stop/continue/checkpoint decision back onto each relay.
            let drained = handle.as_mut().and_then(|h| h.cursor.drain_chunk(chunk_rows));
            let pad = drained.is_none();
            let chunk = drained.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            self.exchange.backfill_pad = Some(pad);
            if view_id == 0 {
                self.evaluate_dag(source_tid, chunk, request_id);
            } else {
                produced_any |= self.backfill_view_step(view_id, source_tid, chunk, request_id);
            }
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
        // Distributed analogue of backfill_view's post-loop release: free the last
        // chunk's pinned delta registers across the source's dependent closure
        // (and assert each backfilled view is ephemeral).
        self.cat().dag.clear_regfile_deltas_from_source(source_tid);
        // View-scoped: the closure driver (`drive_dag`) flushes each dirty view
        // itself, but `backfill_view_step` bypasses it, so flush the new view's
        // output trace once after the final chunk. Only when it produced rows
        // (the first source of a join produces none — it just fills its trace).
        if view_id != 0 && produced_any {
            self.cat().dag.flush(view_id);
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

    /// Reset the SAL read side — rewind the read cursor and advance the expected
    /// epoch — mirroring the master's `sal.checkpoint_reset` on the read side.
    /// Shared by the FLAG_FLUSH dispatch arm and the inline backfill checkpoint
    /// in `consume_backfill_decision`.
    fn advance_read_epoch(&mut self) {
        self.read_cursor = 0;
        self.expected_epoch += 1;
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
        // FLAG_CHECKPOINT ACK; the master's consumption proof is the next round's
        // FLAG_EXCHANGE report, which a checkpoint ACK would be misread as a
        // terminal ACK that retires the worker.
        if decision == BACKFILL_DECISION_CHECKPOINT {
            self.advance_read_epoch();
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
    /// sort the spans byte-lexicographically, and stream them back sorted. A
    /// consolidated row at weight ≥ 2 emits its span twice — it IS that many
    /// live instances of the key, and the duplicate must be visible to the merge
    /// as an adjacent pair. No local dedup and no within-partition duplicate
    /// check: the master's adjacent-equal merge subsumes both, and a
    /// `Vec<PkBuf>` (contiguous, sorted via `PkBuf: Ord`) beats a `BTreeSet` on
    /// both memory and sort cost. The span is the composite generalisation of the
    /// old single-`u128` key — a `UNIQUE (a, b)` span exceeds 16 bytes.
    ///
    /// MUST observe the same snapshot `backfill_index` will later project:
    /// the master sends this command inside the DDL critical section
    /// (committer barrier drained, catalog write lock held), before the
    /// IDX_TAB +1 broadcast, so no concurrent INSERT can interleave.
    fn handle_unique_preflight(&mut self, owner_id: i64, col_indices: &[u32], request_id: u64) -> Result<(), String> {
        // Crash-injection seam: fail the pre-flight on every worker so tests
        // can assert the master surfaces the fault, drains the fan-out, and
        // leaves the catalog and unique-filter state untouched.
        #[cfg(debug_assertions)]
        if std::env::var("GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR").is_ok() {
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
        let idx_schema = crate::catalog::make_index_schema(col_indices, &schema)?;
        let spec = crate::schema::IndexKeySpec::new(col_indices, &schema, &idx_schema);
        let frame_schema = crate::runtime::sal::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        // Stream the partition chunk-wise and project each chunk to spans —
        // peak memory beyond one chunk is the span Vec. `key_bytes` keeps the
        // single column→span definition shared with the filter warmup and the
        // master merge.
        let chunk_rows = self.cat().ddl_scan_chunk_rows;
        let mut keys: Vec<PkBuf> = Vec::new();
        let mut keybuf = PkBuf::empty(0);
        if let Some(mut handle) = self.cat().open_store_cursor(owner_id) {
            while let Some(chunk) = handle.cursor.drain_chunk(chunk_rows) {
                let mb = chunk.as_mem_batch();
                keys.reserve(chunk.count);
                for row in 0..chunk.count {
                    let w = chunk.get_weight(row);
                    if w <= 0 {
                        continue;
                    }
                    if !spec.key_bytes(&mb, row, &mut keybuf) {
                        continue;
                    }
                    keys.push(keybuf);
                    // Chunks are consolidated: weight ≥ 2 is the same row w
                    // times; one extra copy suffices to put an adjacent equal
                    // pair in the sorted stream for the master's merge.
                    if w > 1 {
                        keys.push(keybuf);
                    }
                }
            }
        }
        keys.sort_unstable();

        send_unique_preflight_keys(
            &self.w2m_writer,
            owner_id as u64,
            &keys,
            &frame_schema,
            request_id,
            unique_preflight_keys_per_frame(),
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
        let n = batch.as_ref().map(|b| b.count).unwrap_or(0);

        match lookup {
            HasPkLookup::UniqueIndex(cols) => {
                let index_handle = self.cat().get_index_store_handle(target_id, cols.as_slice());
                if index_handle.is_null() {
                    return Err(format!(
                        "No unique index on columns {:?} for table {}",
                        cols.as_slice(),
                        target_id
                    ));
                }
                // The check target is the unique INDEX table, whose schema is
                // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                // `idx_key_size` and the PK-byte interpretation below are taken
                // from this schema, so an owner-table fallback would compute the
                // wrong prefix width. The broadcast always carries the index
                // schema today, so the fallback is currently unreachable, but
                // `get_index_schema_by_cols` keeps it type-correct.
                let schema = batch
                    .as_ref()
                    .and_then(|b| b.schema)
                    .or_else(|| self.cat().get_index_schema_by_cols(target_id, cols.as_slice()))
                    .ok_or_else(|| format!("no index schema for tid={} cols={:?}", target_id, cols.as_slice()))?;
                // Index layout: PK = (indexed-key field, src_pk_cols). The
                // check-batch carries the promoted indexed value in the low
                // bytes of its PK. Any positive-weight match means the value
                // is already in the index. `open_cursor` avoids letting a
                // compaction Io/InvalidShard failure silently turn a present
                // key into "absent".
                let table = unsafe { &*index_handle };
                let mut cursor = table.open_cursor();
                // The check batch's PK is the OPK index composite
                // `(indexed-value…, src_pk_cols)`; the leading `idx_key_size`
                // bytes are the OPK-encoded indexed value(s). Prefix-match that
                // whole leading span — OPK puts the distinguishing bytes last, so
                // a source-width prefix would match only the zero high bytes.
                let idx_key_size = schema.leading_key_size(cols.as_slice().len());
                let result = filter_by_pk_bytes(&batch, schema, n, |pkb| {
                    cursor.cursor.seek_first_positive_with_prefix(&pkb[..idx_key_size])
                });
                // The index schema is not table `target_id`'s own — one-off block.
                self.send_response(
                    target_id as u64,
                    Some(&result),
                    ReplySchema::OneOff(&schema),
                    request_id,
                    client_id,
                    seek_pk,
                );
            }
            HasPkLookup::PrimaryKey => {
                let schema = self
                    .cat()
                    .get_schema_desc(target_id)
                    .ok_or_else(|| format!("no schema for tid={target_id}"))?;
                let mut ptable = self.cat().get_ptable_handle(target_id);
                // Route on verbatim OPK bytes for every PK width. The old narrow
                // arm fed `get_pk` (OPK-widened) to `has_pk(u128)`, which
                // re-OPK-encodes it — a double sign-flip that misses signed PKs.
                let result = filter_by_pk_bytes(&batch, schema, n, |pkb| {
                    ptable.as_mut().is_some_and(|pt| pt.has_pk_bytes(pkb))
                });
                self.send_response(
                    target_id as u64,
                    Some(&result),
                    ReplySchema::Table(&schema),
                    request_id,
                    client_id,
                    seek_pk,
                );
            }
        }
        Ok(())
    }

    fn handle_flush_all(&mut self) -> Result<(), String> {
        // pending_deltas is intentionally NOT cleared here. A checkpoint can
        // fire before buffered effective deltas are ticked into their views;
        // discarding them would leave views diverged from the base tables until a
        // restart rebuilds them. The checkpoint only persists base-table data —
        // views are re-derived — so the buffered deltas must survive to be ticked
        // by the next auto-tick or the scan barrier. Live entries drain on the
        // next tick (bounded by the 10k-row auto-tick); a dropped table's entry is
        // GC'd in the DdlSync arm (retain(has_id)).
        let ids = self.cat().iter_user_table_ids();
        // One dir fd per unique (dev, ino), so a shared directory is fsynced
        // once — deduped at insertion in `flush_chunk` (a duplicate's `OwnedFd`
        // closes immediately).
        let mut dir_fds: HashMap<(u64, u64), OwnedFd> = HashMap::new();

        let mut ring = io_uring::IoUring::new(256).map_err(|e| format!("io_uring::new failed: {e}"))?;

        const FD_CHUNK_THRESHOLD: usize = 256;
        let mut pending: Vec<(i64, Vec<(usize, FlushWork)>)> = Vec::new();
        let mut pending_fds = 0usize;

        for tid in ids {
            let works = self
                .cat()
                .flush_family_prepare(tid)
                .map_err(|e| format!("flush_prepare tid={tid}: {e}"))?;
            if works.is_empty() {
                continue;
            }
            let added_fds: usize = works
                .iter()
                .map(|(_, w)| w.shard_fd().is_some() as usize + w.manifest_fd().is_some() as usize)
                .sum();
            pending.push((tid, works));
            pending_fds += added_fds;
            if pending_fds >= FD_CHUNK_THRESHOLD {
                self.flush_chunk(&mut ring, &mut pending, &mut dir_fds)?;
                pending_fds = 0;
            }
        }
        self.flush_chunk(&mut ring, &mut pending, &mut dir_fds)?;

        // The dir fds are per-flush (opened in `flush_prepare`, returned by
        // `flush_commit`). `dir_fds` drops at end of scope — on the success
        // return and every `?` unwind alike — closing all of them, so the flush
        // leaks nothing even under a persistent disk error.
        for fd in dir_fds.values() {
            if let Err(e) = crate::foundation::posix_io::fsync_eintr(fd.as_raw_fd()) {
                return Err(format!("dir fsync failed: {e}"));
            }
        }
        Ok(())
    }

    fn flush_chunk(
        &mut self,
        ring: &mut io_uring::IoUring,
        pending: &mut Vec<(i64, Vec<(usize, FlushWork)>)>,
        dir_fds: &mut HashMap<(u64, u64), OwnedFd>,
    ) -> Result<(), String> {
        if pending.is_empty() {
            return Ok(());
        }

        let fds: Vec<libc::c_int> = pending
            .iter()
            .flat_map(|(_, ws)| ws.iter())
            .flat_map(|(_, w)| w.shard_fd().into_iter().chain(w.manifest_fd()))
            .collect();
        uring_batch_fdatasync(ring, &fds)?;

        for (_, ws) in pending.iter_mut() {
            for (_, w) in ws {
                w.close_fds();
            }
        }

        for (tid, works) in pending.drain(..) {
            let dirfds = self
                .cat()
                .flush_family_commit_batch(tid, works)
                .map_err(|e| format!("flush_commit tid={tid}: {e}"))?;
            for dirfd in dirfds {
                let mut stat: libc::stat = unsafe { std::mem::zeroed() };
                if unsafe { libc::fstat(dirfd.as_raw_fd(), &mut stat) } < 0 {
                    let err = std::io::Error::last_os_error();
                    return Err(format!("fstat: {err}"));
                }
                dir_fds.entry((stat.st_dev, stat.st_ino)).or_insert(dirfd);
            }
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

    fn shutdown(&mut self) {
        let _ = self.handle_flush_all();
        unsafe {
            libc::_exit(0);
        }
    }
}

enum DispatchResult {
    Continue,
    Shutdown,
    Error(String),
}

// ---------------------------------------------------------------------------
// Unique pre-flight key stream
// ---------------------------------------------------------------------------

/// Keys per W2M frame for the unique pre-flight stream. The per-key wire size is
/// `idx_key_size + 16` (the OPK leading-key span + 8 B weight + 8 B null word).
/// The span is variable-width now: a single ≤8-byte column promotes to an 8-byte
/// (U64/I64) span — `PREFLIGHT_MIN_KEY_BYTES` below — so the *narrowest* frame is
/// the binding case, where the most frames fit in a ring. A composite span can reach
/// `MAX_PK_BYTES` (80 B) → ~96 B/key, still far under `MAX_W2M_MSG` (256 MiB) at
/// this key count.
///
/// The reactor parks a still-streaming worker's frames per req_id while the
/// master's merge drains a different worker, and every parked frame holds one of
/// the worker's `W2M_MAX_IN_FLIGHT` `InFlightState` slots — so the *minimum*
/// frame size must keep `W2M_REGION_SIZE / frame_bytes` below that bound. The old
/// fixed `U128` reply column made every frame 32 B/key; the variable span can be
/// narrower, so the count is raised so the narrowest frame still clears the bound.
const UNIQUE_PREFLIGHT_KEYS_PER_FRAME: usize = 1 << 20;

const _: () = {
    // Smallest possible per-key wire size: an 8-byte promoted span (the narrowest
    // a unique index key can be — every ≤8-byte integer promotes to an 8-byte
    // U64/I64 key) + 8 B weight + 8 B null word — the binding case for the
    // in-flight-slot bound.
    const PREFLIGHT_MIN_KEY_BYTES: usize = 8 + 8 + 8;
    assert!(
        w2m_ring::W2M_REGION_SIZE / (PREFLIGHT_MIN_KEY_BYTES * UNIQUE_PREFLIGHT_KEYS_PER_FRAME + 8)
            < crate::runtime::w2m::W2M_MAX_IN_FLIGHT,
        "unique pre-flight frames must be large enough that a full W2M ring holds \
         fewer frames than the in-flight slots InFlightState can track",
    );
};

/// Frame size for the unique pre-flight stream. Debug builds may shrink it
/// via GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME so tests exercise multi-frame
/// trains with small tables; such tests must keep the per-worker frame count
/// under the `W2M_MAX_IN_FLIGHT` limit (see UNIQUE_PREFLIGHT_KEYS_PER_FRAME).
fn unique_preflight_keys_per_frame() -> usize {
    debug_env_usize("GNITZ_UNIQUE_PREFLIGHT_KEYS_PER_FRAME")
        .filter(|&n| n > 0)
        .unwrap_or(UNIQUE_PREFLIGHT_KEYS_PER_FRAME)
}

/// Stream `keys` (already sorted) to the master as a train of continuation
/// frames carrying the synthetic pre-flight frame schema (`frame_schema` =
/// `unique_preflight_wire_schema`, whose PK region is exactly the OPK
/// leading-key span; each `keys[i]` is that span verbatim). Every frame is
/// tagged `FLAG_CONTINUATION`; the terminal frame additionally
/// `FLAG_SCAN_LAST`. An empty key set emits one empty terminal frame so the
/// master's drain still sees the train end.
///
/// Deliberately NOT `send_scan_response`: that path attaches the owner
/// table's *cached* schema wire block, which would make the master decode
/// these frames with the table's row stride, and its `pending_streams`
/// chunking would require materialising all keys as one 32 B/row `Batch` (2×
/// the caller's `Vec`). The synthetic schema's wire block is built one-off (the
/// `ReplySchema::OneOff` pattern) and never written to the
/// table-keyed schema-block cache, so the table's cached block is never
/// poisoned. `send_encoded` blocks on a full ring until the master's merge
/// drains it — acceptable backpressure: the worker has nothing else to do
/// during the DDL window.
pub(crate) fn send_unique_preflight_keys(
    w2m_writer: &W2mWriter,
    target_id: u64,
    keys: &[PkBuf],
    frame_schema: &SchemaDescriptor,
    request_id: u64,
    keys_per_frame: usize,
) {
    debug_assert!(keys_per_frame > 0, "keys_per_frame must be positive");
    let schema_block = ipc::build_schema_wire_block(frame_schema, &[], target_id as u32);

    // Reusable chunk batch: filled, encoded, and cleared per frame.
    let mut chunk = Batch::with_schema(*frame_schema, keys.len().min(keys_per_frame));
    let mut start = 0usize;
    loop {
        let end = keys.len().min(start + keys_per_frame);
        chunk.clear();
        for k in &keys[start..end] {
            chunk.ensure_row_capacity();
            // The span is already OPK; write the raw bytes into the PK region
            // (len == pk_stride). The master reads them back verbatim via
            // `mb.get_pk_bytes(row)` → `PkBuf` — the wire is byte-transparent.
            chunk.extend_pk_bytes(k.pk_bytes());
            chunk.extend_weight(&1i64.to_le_bytes());
            chunk.extend_null_bmp(&0u64.to_le_bytes());
            chunk.count += 1;
        }
        let is_first = start == 0;
        let is_last = end == keys.len();
        // Schema block only on the first frame; continuations decode against
        // the master's saved schema hint (synthetic schema version is 0, so
        // no wire_flags_set_schema_version is needed).
        let prebuilt: Option<&[u8]> = if is_first { Some(&schema_block) } else { None };
        let schema_for_encode = if is_first { Some(frame_schema) } else { None };
        let flags = FLAG_CONTINUATION | if is_last { FLAG_SCAN_LAST } else { 0 };
        let sz = ipc::wire_size_range(STATUS_OK, &[], schema_for_encode, None, &chunk, chunk.count, prebuilt);
        w2m_writer.send_encoded(sz, request_id as u32, |buf| {
            ipc::encode_wire_into_range(
                buf,
                0,
                target_id,
                0,
                flags,
                0,
                STATUS_OK, // request_id 0 in payload; ring prefix carries the req_id
                schema_for_encode,
                &chunk,
                0,
                chunk.count,
                prebuilt,
            );
        });
        if is_last {
            break;
        }
        start = end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaDescriptor;

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

    fn make_handler() -> WorkerExchangeHandler {
        WorkerExchangeHandler {
            stash: HashMap::new(),
            deferred: Vec::<DeferredDdl>::new(),
            deferred_ticks: Vec::new(),
            pending_relays: HashMap::new(),
            backfill_pad: None,
            backfill_signal: None,
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
        h.pending_relays.insert((200, 0), (batch_b, 0));
        h.pending_relays.insert((300, 0), (batch_c, 0));

        // Nested wait for view 200 finds its relay without touching the SAL.
        let (b, _) = h.pending_relays.remove(&(200, 0)).expect("view 200 relay queued");
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
        h.pending_relays.insert((100, 10), (batch_a, 0));
        h.pending_relays.insert((100, 20), (batch_b, 0));

        // Retrieving one does NOT retrieve the other.
        let (a, _) = h.pending_relays.remove(&(100, 10)).expect("(100,10) queued");
        assert_eq!(a.count, 3);
        assert!(
            h.pending_relays.contains_key(&(100, 20)),
            "retrieving (100,10) must leave (100,20) in place"
        );
        let (b, _) = h.pending_relays.remove(&(100, 20)).expect("(100,20) queued");
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
        use crate::runtime::w2m_ring;
        use crate::runtime::wire as ipc;
        // Use the production-sized region — the mmap reservation is
        // lazy-populated, so the 1 GiB backing is cheap.
        let region_size = w2m_ring::W2M_REGION_SIZE;
        let region_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                region_size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            )
        };
        assert_ne!(region_ptr, libc::MAP_FAILED);
        let region_ptr = region_ptr as *mut u8;
        unsafe {
            w2m_ring::init_region(region_ptr, region_size as u64);
        }

        let w2m_writer = W2mWriter::new(region_ptr, region_size as u64);

        let mut wp = make_test_worker(std::ptr::null_mut(), w2m_writer);

        let req_ack: u64 = 42;
        let req_resp: u64 = 0xCAFE_BABE_DEAD_BEEF;
        let req_err: u64 = u64::MAX;
        wp.send_ack(7, 0, req_ack);
        // Pass ReplySchema::None: send_response consults the catalog only
        // when a schema is present, and this test uses a null catalog pointer.
        // The id round-trip is the assertion of interest.
        wp.send_response(8, None, ReplySchema::None, req_resp, 0, 0u128);
        wp.send_error("boom", req_err);

        // Decode the three messages back from the ring via try_consume.
        let hdr = unsafe { w2m_ring::W2mRingHeader::from_raw(region_ptr as *const u8) };
        let mut rc = w2m_ring::W2M_HEADER_SIZE as u64;
        let mut decoded_ids = Vec::new();
        for _ in 0..3 {
            let (data_ptr, sz, new_rc, _req_id) =
                unsafe { w2m_ring::try_consume(hdr, region_ptr as *const u8, rc).expect("expected a message") };
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
    fn from_wire_packed_single_col_is_unique_index() {
        // seek_col_idx carries pack_pk_cols(&[col]); the packed flag (bit 63) is
        // always set, so it is never 0 and decodes back to the column list.
        for col in [0u32, 1, 5, 63] {
            let packed = gnitz_wire::pack_pk_cols(&[col]);
            match HasPkLookup::from_wire(packed) {
                HasPkLookup::UniqueIndex(cols) => assert_eq!(cols.as_slice(), [col]),
                HasPkLookup::PrimaryKey => panic!("packed list must decode to UniqueIndex"),
            }
        }
    }

    #[test]
    fn from_wire_packed_is_never_primary_key() {
        // A packed single-column list always sets the flag bit, so it never
        // collides with the PK sentinel (0).
        assert!(gnitz_wire::pack_pk_cols(&[0]) != 0);
        assert!(matches!(
            HasPkLookup::from_wire(gnitz_wire::pack_pk_cols(&[0])),
            HasPkLookup::UniqueIndex(_)
        ));
    }

    // -- Walk-the-matrix dispatch tests ---------------------------------------

    /// The one test constructor for `WorkerProcess`. Fields a test does not
    /// exercise stay null/zeroed/default; pre-seeded state (`sal_reader`,
    /// `pending_streams`, `reply_frame_budget`) is assigned after construction.
    fn make_test_worker(catalog: *mut CatalogEngine, writer: W2mWriter) -> WorkerProcess {
        WorkerProcess {
            worker_id: 0,
            num_workers: 1,
            master_pid: 0,
            catalog,
            sal_reader: unsafe { std::mem::zeroed() },
            w2m_writer: writer,
            exchange: make_handler(),
            pending_deltas: HashMap::new(),
            pending_streams: VecDeque::new(),
            reply_frame_budget: w2m_ring::MAX_W2M_MSG as usize,
            read_cursor: 0,
            expected_epoch: 1,
        }
    }

    /// Build a worker that's safe for `dispatch` calls whose behavior
    /// does not enter the catalog (Tick/DdlSync/ExchangeRelay/PreloadedExchange
    /// inside an exchange wait, plus ExchangeRelay/PreloadedExchange at
    /// top-level which warn-or-stash without touching the catalog).
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
        let ctx = DispatchContext::InsideExchangeWait {
            want_key: (100, 5),
            schema: Some(test_schema()),
        };
        assert!(wp.exchange.deferred_ticks.is_empty());
        let outcome = wp.dispatch(ctx, SalMessageKind::Tick, 999, None);
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert_eq!(wp.exchange.deferred_ticks.len(), 1);
        assert_eq!(
            wp.exchange.deferred_ticks[0].0, 999,
            "Tick target_id must be carried into deferred_ticks"
        );
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
        let ctx = DispatchContext::InsideExchangeWait {
            want_key,
            schema: Some(schema),
        };

        // Mismatched view: parked.
        let empty: &'static [u8] = &[];
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 200, Some(empty));
        assert!(matches!(outcome, DispatchOutcome::Continue));
        assert!(
            wp.exchange.pending_relays.contains_key(&(200, 0)),
            "non-matching relay must be parked in pending_relays"
        );

        // Matching key: returned via RelayMatched.
        let outcome = wp.dispatch(ctx, SalMessageKind::ExchangeRelay, 100, Some(empty));
        assert!(
            matches!(outcome, DispatchOutcome::RelayMatched(_)),
            "key-matching relay must short-circuit with RelayMatched"
        );
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
        assert!(
            wp.exchange.pending_relays.is_empty(),
            "TopLevel must NOT park relays — they belong to do_exchange_wait"
        );
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
            if !must_defer.contains(&kind) {
                continue;
            }

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
                    assert_eq!(
                        after_ticks,
                        before_ticks + 1,
                        "Tick must defer to deferred_ticks inside exchange wait"
                    );
                    assert_eq!(after_ddl, before_ddl, "Tick must NOT touch the deferred (DDL) queue");
                }
                SalMessageKind::DdlSync => {
                    // DdlSync requires a decodable wire to actually defer
                    // (the deferred batch is taken from decoded.data_batch).
                    // With wire=None the dispatch arm is reached and runs
                    // its (trivially empty) effect: this test asserts that
                    // it returns Continue and doesn't panic, and that the
                    // deferred queue did not gain a synthetic entry
                    // (None wire ⇒ no data_batch ⇒ no push).
                    assert_eq!(
                        after_ddl, before_ddl,
                        "DdlSync with no wire data must not synthesize a deferred entry"
                    );
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
        use crate::runtime::sal::{sal_write_group, SalReader, FLAG_DDL_SYNC, FLAG_PUSH};

        const SAL_SIZE: usize = 1 << 20;
        let sal_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                SAL_SIZE,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            )
        };
        assert_ne!(sal_ptr, libc::MAP_FAILED);
        let sal_ptr = sal_ptr as *mut u8;
        unsafe {
            std::ptr::write_bytes(sal_ptr, 0, SAL_SIZE);
        }

        // Single worker; each group puts a one-byte payload at slot 0.
        let nw = 1u32;
        let payload = [0u8; 1];
        let ptrs: Vec<*const u8> = vec![payload.as_ptr()];
        let sizes: Vec<u32> = vec![1];

        // Group 1: epoch=1, FLAG_PUSH, target=42
        let r1 = unsafe {
            sal_write_group(
                sal_ptr,
                0,
                nw,
                42,
                100,
                FLAG_PUSH,
                1,
                SAL_SIZE as u64,
                ptrs.as_ptr(),
                sizes.as_ptr(),
            )
        };
        assert_eq!(r1.status, 0);
        // Group 2: epoch=1, FLAG_DDL_SYNC, target=43
        let r2 = unsafe {
            sal_write_group(
                sal_ptr,
                r1.new_cursor,
                nw,
                43,
                101,
                FLAG_DDL_SYNC,
                1,
                SAL_SIZE as u64,
                ptrs.as_ptr(),
                sizes.as_ptr(),
            )
        };
        assert_eq!(r2.status, 0);
        // Group 3: epoch=2 — must be skipped while expected_epoch==1.
        let r3 = unsafe {
            sal_write_group(
                sal_ptr,
                r2.new_cursor,
                nw,
                44,
                102,
                FLAG_PUSH,
                2,
                SAL_SIZE as u64,
                ptrs.as_ptr(),
                sizes.as_ptr(),
            )
        };
        assert_eq!(r3.status, 0);

        let mut wp = make_test_worker(std::ptr::null_mut(), unsafe { std::mem::zeroed() });
        wp.sal_reader = SalReader::new(sal_ptr as *const u8, 0, SAL_SIZE, -1);

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
        assert!(
            wp.next_sal_message().is_none(),
            "epoch-mismatched group must be rejected"
        );
        assert_eq!(
            wp.read_cursor, cursor_at_epoch_fence,
            "cursor must NOT advance on epoch mismatch (group remains parked)"
        );

        // Bumping expected_epoch makes the same group consumable on the
        // next call — this is the load-bearing invariant cited by the
        // doc comment in `next_sal_message`. (Cursor was unchanged, so
        // we re-read the same group.)
        wp.expected_epoch = 2;
        let (kind, target_id, _wire) = wp
            .next_sal_message()
            .expect("group 3 should be consumable after epoch bump");
        assert_eq!(kind, SalMessageKind::Push);
        assert_eq!(target_id, 44);
        assert!(wp.read_cursor > cursor_at_epoch_fence);

        // No more groups.
        assert!(wp.next_sal_message().is_none());

        unsafe {
            libc::munmap(sal_ptr as *mut libc::c_void, SAL_SIZE);
        }
    }

    // -- pending stream chunking tests -----------------------------------------------

    fn make_ring() -> (*mut u8, W2mWriter) {
        let size = w2m_ring::W2M_REGION_SIZE;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_SHARED,
                -1,
                0,
            ) as *mut u8
        };
        assert_ne!(ptr, libc::MAP_FAILED as *mut u8);
        unsafe {
            w2m_ring::init_region(ptr, size as u64);
        }
        (ptr, W2mWriter::new(ptr, size as u64))
    }

    fn make_n_row_batch(schema: SchemaDescriptor, n: usize) -> Batch {
        let mut b = Batch::with_schema(schema, n.max(1));
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

    /// First (and only) PendingScan chunk — next_row == 0, so the prebuilt schema
    /// block must appear in the frame and decode_wire_ipc must succeed without a hint.
    #[test]
    fn test_pending_scan_first_chunk_includes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);
        let schema_block = Rc::new(ipc::build_schema_wire_block(&schema, &[], 1));

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            next_row: 0,
            request_id: 7,
            client_id: 42,
            target_id: 1,
            prebuilt_schema: Some(schema_block),
            server_version: 0,
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

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    /// Continuation chunk — next_row > 0, prebuilt_schema == None. The frame carries
    /// no schema block; decode_wire_ipc fails but decode_wire_ipc_with_schema succeeds
    /// and returns only the remaining rows (rows [5, 10)).
    #[test]
    fn test_pending_scan_continuation_chunk_excludes_schema() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 10);

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch),
            next_row: 5,
            request_id: 9,
            client_id: 0,
            target_id: 1,
            prebuilt_schema: None,
            server_version: 0,
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

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    /// send_scan_response with schema=None (avoids catalog) emits a single ring
    /// message with FLAG_CONTINUATION | FLAG_SCAN_LAST for a small wire-safe batch,
    /// and leaves the stream queue empty.
    #[test]
    fn test_send_scan_response_single_frame() {
        let schema = test_schema();
        let batch = make_n_row_batch(schema, 5);

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);

        let err = wp.send_scan_response(1, Rc::new(batch), ReplySchema::None, 3, 0, 0);
        assert!(err.is_none(), "small wire-safe batch must not error");
        assert!(
            wp.pending_streams.is_empty(),
            "batch fits in one frame; send_scan_response must not enqueue a train"
        );

        let data = consume_one(ptr);
        let ctrl = ipc::peek_control_block(&data).expect("peek_control_block");
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

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    /// STRING-column schemas are not wire-safe: send_scan_response sends them as a
    /// single frame without chunking (and returns an error if the batch exceeds
    /// MAX_W2M_MSG). The full error path requires a CatalogEngine + a > 256 MiB
    /// batch; this test verifies the predicate that gates that branch.
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

    /// A zero-filled batch of `count` rows (with_schema zero-fills the data
    /// region, so setting `count` directly yields decodable all-zero rows).
    /// Used to make wire sizes cross MAX_W2M_MSG without writing 256 MiB.
    fn zero_batch(schema: SchemaDescriptor, count: usize) -> Batch {
        let mut b = Batch::with_schema(schema, count);
        b.count = count;
        b
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
        let block_a = Rc::new(ipc::build_schema_wire_block(&schema_a, &[], 1));
        let block_b = Rc::new(ipc::build_schema_wire_block(&schema_b, &[], 2));

        // Budget: first chunk (with A's schema block) carries ~4 rows, so
        // train A spans at least two frames.
        let sz_0 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch_a, 0, Some(block_a.as_slice()));
        let sz_1 = ipc::wire_size_range(STATUS_OK, &[], None, None, &batch_a, 1, Some(block_a.as_slice()));
        let budget = sz_0 + (sz_1 - sz_0) * 4;

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch_a),
            next_row: 0,
            request_id: 11,
            client_id: 0,
            target_id: 1,
            prebuilt_schema: Some(block_a),
            server_version: 0,
        });
        wp.pending_streams.push_back(PendingScan {
            batch: Rc::new(batch_b),
            next_row: 0,
            request_id: 22,
            client_id: 0,
            target_id: 2,
            prebuilt_schema: Some(block_b),
            server_version: 0,
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
                let ctrl = ipc::peek_control_block(bytes).expect("ctrl");
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

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
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

        let (ptr_ref, writer_ref) = make_ring();
        let (ptr_new, writer_new) = make_ring();
        let mut wp_ref = make_test_worker(std::ptr::null_mut(), writer_ref);
        let mut wp_new = make_test_worker(std::ptr::null_mut(), writer_new);

        let req = 0xCAFE_u64;
        let client = 7u64;
        let pk = 0xDEAD_BEEF_u128;
        wp_ref.send_response(8, Some(&batch), ReplySchema::None, req, client, pk);
        wp_ref.send_response(8, None, ReplySchema::None, req + 1, client, 0);

        assert!(wp_new
            .stream_batch_response(8, Some(batch.clone()), ReplySchema::None, req, client, pk)
            .is_none());
        assert!(wp_new
            .stream_batch_response(8, None, ReplySchema::None, req + 1, client, 0)
            .is_none());
        assert!(wp_new.pending_streams.is_empty(), "fitting results never enqueue");

        let ref_frames = walk_frames(ptr_ref);
        let new_frames = walk_frames(ptr_new);
        assert_eq!(ref_frames.len(), 2);
        assert_eq!(
            ref_frames, new_frames,
            "single-frame stream_batch_response must be byte-identical to send_response"
        );

        unsafe {
            libc::munmap(ptr_ref as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
            libc::munmap(ptr_new as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    /// An oversized wire-safe result (> MAX_W2M_MSG) enqueues a train instead
    /// of hitting the ring-size assert; nothing is emitted until drain_sal.
    #[test]
    fn test_stream_batch_response_oversized_enqueues_train() {
        let schema = test_schema(); // 32 B/row on the wire
        let rows = (w2m_ring::MAX_W2M_MSG as usize / 32) + 4096;
        let batch = zero_batch(schema, rows);

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(std::ptr::null_mut(), writer);
        let err = wp.stream_batch_response(3, Some(batch), ReplySchema::None, 5, 9, 0);
        assert!(err.is_none(), "oversized wire-safe result must chunk, not error");
        assert_eq!(wp.pending_streams.len(), 1);
        let ps = wp.pending_streams.front().unwrap();
        assert_eq!(ps.next_row, 0);
        assert_eq!(ps.request_id, 5);
        assert_eq!(ps.client_id, 9);
        assert!(
            walk_frames(ptr).is_empty(),
            "the train's first chunk is emitted by drain_sal, not at enqueue time"
        );

        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    fn worker_temp_dir(name: &str) -> String {
        crate::foundation::posix_io::raise_fd_limit_for_tests();
        let path = std::env::temp_dir()
            .join(format!("gnitz_worker_test_{name}"))
            .to_str()
            .unwrap()
            .to_owned();
        let _ = std::fs::remove_dir_all(&path);
        path
    }

    /// An oversized non-wire-safe (STRING) result returns the clean error —
    /// the variable-width streaming chunker is an explicit non-goal.
    #[test]
    fn test_stream_batch_response_oversized_string_errors() {
        use crate::catalog::ColumnDef;
        use crate::schema::type_code;

        let dir = worker_temp_dir("string_oversized");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![
            ColumnDef {
                name: "id".into(),
                type_code: type_code::U64,
                is_nullable: false,
                fk_table_id: 0,
                fk_col_idx: 0,
            },
            ColumnDef {
                name: "s".into(),
                type_code: type_code::STRING,
                is_nullable: false,
                fk_table_id: 0,
                fk_col_idx: 0,
            },
        ];
        let tid = engine.create_table("public.tstr", &cols, &[0], true).unwrap();
        let schema = engine.get_schema_desc(tid).unwrap();
        assert!(!schema_wire_safe(&schema));

        // 40 B/row (8 pk + 8 weight + 8 null + 16 string struct), empty blob.
        let rows = (w2m_ring::MAX_W2M_MSG as usize / 40) + 4096;
        let batch = zero_batch(schema, rows);

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);
        let err = wp
            .stream_batch_response(tid as u64, Some(batch), ReplySchema::Table(&schema), 5, 0, 0)
            .expect("oversized STRING result must surface the clean error");
        assert!(
            err.contains("STRING-column chunking"),
            "error names the limitation: {err}"
        );
        assert!(wp.pending_streams.is_empty(), "non-wire-safe results never enqueue");

        engine.close();
        let _ = std::fs::remove_dir_all(&dir);
        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }

    /// A projected (gather) reply schema must ride a ONE-OFF wire block: the
    /// table-keyed cache must neither serve it (the master would decode
    /// projected rows with the base table's stride) nor store it (a later
    /// table reply would be decoded with the projected stride).
    #[test]
    fn test_stream_batch_response_projected_schema_one_off_block() {
        use crate::catalog::ColumnDef;
        use crate::schema::type_code;

        let dir = worker_temp_dir("projected_one_off");
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let mk = |name: &str| ColumnDef {
            name: name.into(),
            type_code: type_code::U64,
            is_nullable: false,
            fk_table_id: 0,
            fk_col_idx: 0,
        };
        let cols = vec![mk("id"), mk("a"), mk("b")];
        let tid = engine.create_table("public.tproj", &cols, &[0], true).unwrap();
        let table_schema = engine.get_schema_desc(tid).unwrap();
        let projected = crate::catalog::project_schema(&table_schema, &[1]);
        assert_ne!(projected.num_columns(), table_schema.num_columns());

        let (ptr, writer) = make_ring();
        let mut wp = make_test_worker(&mut engine as *mut CatalogEngine, writer);

        // Fitting projected reply: one frame carrying the projected schema.
        let small = zero_batch(projected, 2);
        assert!(wp
            .stream_batch_response(tid as u64, Some(small), ReplySchema::OneOff(&projected), 5, 0, 0)
            .is_none());
        let frames = walk_frames(ptr);
        assert_eq!(frames.len(), 1);
        let decoded = ipc::decode_wire_ipc(&frames[0].1).expect("decode projected reply");
        assert_eq!(
            decoded.schema.expect("schema block present").num_columns(),
            projected.num_columns()
        );

        // Oversized projected reply: the queued train holds the one-off block.
        let rows = (w2m_ring::MAX_W2M_MSG as usize / 32) + 4096;
        let big = zero_batch(projected, rows);
        assert!(wp
            .stream_batch_response(tid as u64, Some(big), ReplySchema::OneOff(&projected), 6, 0, 0)
            .is_none());
        assert_eq!(wp.pending_streams.len(), 1);
        let expected_block = ipc::build_schema_wire_block(&projected, &[], tid as u32);
        let ps = wp.pending_streams.front().unwrap();
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
        unsafe {
            libc::munmap(ptr as *mut libc::c_void, w2m_ring::W2M_REGION_SIZE);
        }
    }
}
