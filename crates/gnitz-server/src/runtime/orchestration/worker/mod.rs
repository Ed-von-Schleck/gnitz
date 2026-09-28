//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region.

use std::collections::VecDeque;
use std::rc::Rc;

use crate::catalog::{CatalogEngine, SysFamily};
use crate::query::{DagEngine, Drive, DriveHost};
use crate::runtime::sal::{SalMessage, SalMessageKind, SalReader};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{self as ipc};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationRegistry;
use gnitz_store::schema::key::PkBuf;
use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::Batch;
use gnitz_store::storage::StoreError;
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
    /// Ends the wait.
    Relay,
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
        // Answered mid-wait, a read would see a half-run tick.
        SalMessageKind::Scan | SalMessageKind::ScanSpec | SalMessageKind::DeltaRead => InEval::DeferPostAck,
        // An inline catalog mutation races the in-flight evaluation.
        SalMessageKind::DdlSync => InEval::DeferPreAck,
        // Deferring deadlocks: `flush_round` holds a `SalExcl` across the ACK
        // wait, and the tick's relay needs one to write the relay this worker is
        // parked on — so a flush that waits for the relay waits forever.
        SalMessageKind::Flush | SalMessageKind::FlushEph => InEval::Inline,
        // Correct to defer (`commit_pushes` drops its lock before the ACK wait),
        // but it would park the ingest ACK behind an exchange round-trip.
        SalMessageKind::Push => InEval::Inline,
        // Probes of base-table keys, which a tick does not write.
        SalMessageKind::HasPk | SalMessageKind::UniquePreflight => InEval::Inline,
        SalMessageKind::Backfill | SalMessageKind::Shutdown => InEval::Inline,
        SalMessageKind::ExchangeRelay => InEval::Relay,
    }
}

/// A relay delivered to the wait blocked on it: the relayed batch, and whether
/// every worker's partition was drained.
struct RelayHit {
    batch: Batch,
    drained: bool,
}

struct WorkerExchangeHandler {
    /// [`InEval::DeferPreAck`]: drained inside `drive_dag`, so a catalog
    /// mutation lands before the ACK that implies it.
    deferred: Vec<Request>,
    /// [`InEval::DeferPostAck`]: drained at top level, so a replayed tick cannot
    /// re-enter a view whose outer exchange is still awaiting its relay.
    ///
    /// Both queues drain in insertion order, which is SAL order.
    deferred_replay: Vec<Request>,
}

/// The worker as a drive's [`DriveHost`].
struct DagExchangeCtx<'a> {
    worker: &'a mut WorkerProcess,
    /// This worker's source partition is already drained, so its rounds are empty
    /// pads. Only a backfill chunk sets it.
    pad: bool,
    /// `pad` until a relay comes back, then the relay's `drained`: every worker's.
    drained: bool,
}

impl DriveHost for DagExchangeCtx<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = self.worker.cat();
        (&mut cat.dag, &mut cat.registry)
    }

    fn exchange(&mut self, view_id: u64, batch: Batch, key: u64) -> Batch {
        let hit = self.worker.do_exchange_wait(view_id, batch, key, self.pad);
        self.drained = hit.drained;
        hit.batch
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
    /// Reply trains, one frame emitted per SAL drain, front first: the master
    /// reads one lease at a time and a ring frees only in order.
    pending_streams: VecDeque<PendingScan>,
    /// Per-frame wire budget of every W2M train. `GNITZ_REPLY_FRAME_BUDGET`
    /// lowers it, so tests reach multi-frame trains on small tables.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

pub(crate) use reply::send_unique_preflight_keys;
use reply::PendingScan;

/// `GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR`: fail the pre-flight on every worker so
/// tests can assert the master surfaces the fault, drains the fan-out, and
/// leaves the catalog and unique-filter state untouched.
static UNIQUE_PREFLIGHT_ERROR: Seam = Seam::new("GNITZ_INJECT_UNIQUE_PREFLIGHT_ERROR");

/// Where one reply goes and how, read off its request.
#[derive(Clone, Copy)]
struct ReplyRoute {
    target_id: u64,
    /// The id the master routes the reply by.
    request_id: u32,
    /// Queue the reply behind earlier trains even when it fits one frame.
    fifo: bool,
    /// The request's schema version, which every frame of the reply echoes.
    schema_version: u16,
}

impl WorkerProcess {
    pub fn new(catalog: *mut CatalogEngine, sal_reader: SalReader, w2m_writer: W2mWriter) -> Self {
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
            },
            pending_streams: VecDeque::new(),
            reply_frame_budget: gnitz_foundation::env::env_num(
                "GNITZ_REPLY_FRAME_BUDGET",
                gnitz_wire::MAX_FRAME_PAYLOAD,
            )
            .min(gnitz_wire::MAX_FRAME_PAYLOAD),
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
            // A queued train emits its next frame without waiting on the SAL.
            if self.pending_streams.is_empty() {
                self.w2m_writer.sal_park().park(|| self.sal_reader.is_empty());
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups. Shutdown `_exit`s inline.
    fn drain_sal(&mut self) {
        // One frame per pass, so requests keep being served between frames.
        self.emit_pending_scan_chunk();
        while let Some((msg, wire)) = self.sal_reader.next() {
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

    /// Decode one SAL group's slot into an owned [`Request`]. The single decode
    /// point: both dispatchers and every parked request come through here.
    fn decode_request(&mut self, msg: &SalMessage, wire: &'static [u8]) -> Request {
        // The kinds the master frames with their target's catalog record.
        let catalog_record = matches!(msg.kind, SalMessageKind::Push | SalMessageKind::DdlSync);
        let known = |tid, record: &[u8]| catalog_record.then(|| self.cat().known_decode(tid, record)).flatten();
        // Fail-stop: a dropped group diverges this worker from the master.
        match ipc::decode_sal_slot(wire, known) {
            Ok(w) => Request {
                kind: msg.kind,
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
        let req = self.decode_request(msg, wire);
        self.handle_request(req);
    }

    /// Dispatch a group drained while blocked in `do_exchange_wait` for the
    /// relay keyed `relay_wait = (view_id, source_id)`. Returns that relay when
    /// this group is it; everything else runs or is deferred per [`in_eval`].
    fn dispatch_in_eval(&mut self, relay_wait: (u64, u64), msg: &SalMessage, wire: &'static [u8]) -> Option<RelayHit> {
        let req = self.decode_request(msg, wire);
        match in_eval(req.kind) {
            InEval::Inline => self.handle_request(req),
            InEval::DeferPostAck => self.exchange.deferred_replay.push(req),
            InEval::DeferPreAck => self.exchange.deferred.push(req),
            InEval::Relay => return Some(self.take_relay(relay_wait, req)),
        }
        None
    }

    /// Deliver a relay to the wait blocked on exactly its `(view_id, source_id)`
    /// pair; a relay for any other pair means this worker diverged from the master.
    fn take_relay(&mut self, relay_wait: (u64, u64), req: Request) -> RelayHit {
        let ipc::DecodedWire { control, schema, data_batch, .. } = req.wire;
        let target_id = control.hdr.target_id;
        let key = (target_id, control.hdr.arg0);
        let drained = control.hdr.flags.drained;
        let batch = match (data_batch, schema) {
            (Some(b), _) => b,
            (None, Some(s)) => Batch::empty_with_schema(&s),
            (None, None) => self.fatal_shutdown(&format!("ExchangeRelay for tid={target_id} carries no schema")),
        };
        if key != relay_wait {
            self.fatal_shutdown(&format!(
                "ExchangeRelay for {key:?} while waiting on {relay_wait:?} — diverged from the master"
            ));
        }
        RelayHit { batch, drained }
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
        let Request { kind, request_id, fifo, wire: decoded } = req;
        let hdr = decoded.control.hdr;
        let target_id = hdr.target_id;
        let route = ReplyRoute {
            target_id: hdr.target_id,
            request_id,
            fifo,
            schema_version: hdr.flags.schema_version,
        };
        let blob = decoded.blob;
        let batch = decoded.data_batch;

        match kind {
            SalMessageKind::Shutdown => unsafe { libc::_exit(0) },

            SalMessageKind::Flush => {
                self.sal_reader.rewind();
                self.handle_flush_all()?;
                self.send_ack(0, request_id);
                Ok(())
            }

            SalMessageKind::FlushEph => {
                self.sal_reader.rewind();
                self.cat().flush_ephemeral_round(hdr.arg0)?;
                self.send_ack(0, request_id);
                Ok(())
            }

            SalMessageKind::DdlSync => {
                // `_sequences` is master state: no worker reads it.
                if SysFamily::from_id(target_id) == Some(SysFamily::Sequence) {
                    return Ok(());
                }
                if let Some(batch) = batch {
                    if !batch.is_empty() {
                        self.cat().ddl_sync(target_id, batch)?;
                        gnitz_debug!("ddl_sync tid={}", target_id);
                    }
                }
                Ok(())
            }

            SalMessageKind::Backfill => {
                // Stop-the-world (the DDL parks the reactor): no yield.
                self.handle_backfill(target_id, hdr.arg0)?;
                self.send_ack(target_id, request_id);
                Ok(())
            }

            SalMessageKind::HasPk => {
                let Some(batch) = batch else {
                    return Err("has_pk: a probe carries its keys".into());
                };
                let lookup = match gnitz_wire::probe_key_columns(hdr.arg1) {
                    None => HasPkLookup::PrimaryKey,
                    Some(packed) => HasPkLookup::SecondaryIndex {
                        cols: self.cat().registry.index_cols(target_id, packed, "has_pk")?,
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
                self.send_ack(target_id, request_id);
                Ok(())
            }

            SalMessageKind::Tick => {
                self.handle_tick(target_id, hdr.arg0)?;
                self.send_ack(target_id, request_id);
                Ok(())
            }

            SalMessageKind::Scan => {
                let result = self.cat().scan(target_id)?;
                self.send_reply(route, result);
                Ok(())
            }

            SalMessageKind::ScanSpec => self.answer_scan_spec(route, &blob, hdr.arg0),
            SalMessageKind::DeltaRead => self.answer_delta_read(route, hdr.arg1, hdr.arg0, &blob),

            SalMessageKind::UniquePreflight => {
                let cols = self
                    .cat()
                    .registry
                    .index_cols(target_id, hdr.arg1, "unique pre-flight")?;
                self.handle_unique_preflight(target_id, cols.as_slice(), request_id)?;
                Ok(())
            }

            SalMessageKind::ExchangeRelay => {
                self.fatal_shutdown(&format!("ExchangeRelay for tid={target_id} outside an exchange wait"))
            }
        }
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: u64, batch: Batch) -> Result<(), String> {
        let row_count = batch.len();
        if target_id < gnitz_wire::FIRST_USER_TABLE_ID {
            return Err(format!(
                "a Push group named system table_id={target_id}; a system family arrives as DdlSync"
            ));
        }
        // A view is the other non-ingestion-point, and the master already
        // rejects one client-facing.
        let kind = self.cat().registry.relation_or_err(target_id)?.kind();
        if !kind.is_ingestion_point() {
            return Err(format!(
                "a Push group named relation {target_id}, which is a {}; \
                 a push targets a base table or a stream",
                kind.noun()
            ));
        }
        // A storage fault leaves an ACKed push unapplied. Only a restart replays
        // it; a fault reply would let the next checkpoint discard it.
        match self.cat().ingest_unticked(target_id, batch) {
            Ok(()) => {}
            // An ingest never produces `DeltaExpired`; the or-pattern is what
            // keeps the match total without a third arm.
            Err(StoreError::Rejected(msg) | StoreError::DeltaExpired(msg)) => return Err(msg),
            Err(e @ StoreError::Storage { .. }) => gnitz_fatal_abort!(
                "worker: push apply failed (table_id={}): {} — committed data not \
                 applied, state diverged from durable SAL; aborting for restart+replay",
                target_id,
                e,
            ),
        }
        gnitz_debug!("push tid={} rows={}", target_id, row_count);
        Ok(())
    }

    /// Drive one view-maintenance tick of `target_id`'s dependent closure.
    /// `round` is the tick round the master allocated for this group; every fed
    /// view's captured delta is stamped with it.
    fn handle_tick(&mut self, target_id: u64, round: u64) -> Result<(), String> {
        let delta = if let Some(d) = self.cat().dag.take_unticked(target_id) {
            d
        } else {
            match self.cat().registry.relation(target_id) {
                Some(r) => Batch::empty_with_schema(&r.schema()),
                None => return Ok(()),
            }
        };
        self.drive_dag(Drive::Tick { source: target_id, round }, delta, false);
        Ok(())
    }

    /// Answer one `ReadSpec` read, streaming the keeper back.
    fn answer_scan_spec(
        &mut self,
        route: ReplyRoute,
        blob: &[u8],
        reply_layout: u64,
    ) -> Result<(), gnitz_wire::WireFault> {
        let target_id = route.target_id;
        let spec = gnitz_wire::ReadSpec::decode(blob)?;
        let keeper = self.cat().scan_spec(target_id, spec, reply_layout)?;
        self.send_reply(route, keeper);
        Ok(())
    }

    /// Answer one DELTA_POLL view: its deltas in rounds `(after_tick, cut_tick]`.
    fn answer_delta_read(
        &mut self,
        route: ReplyRoute,
        after_tick: u64,
        cut_tick: u64,
        blob: &[u8],
    ) -> Result<(), gnitz_wire::WireFault> {
        let target_id = route.target_id;
        let reply_layout = gnitz_wire::decode_all(blob, "delta read", |r| r.u64())?;
        let keeper = self.cat().delta_read(target_id, after_tick, cut_tick, reply_layout)?;
        self.send_reply(route, keeper);
        Ok(())
    }

    /// Distributed CREATE-VIEW backfill, worker side: drives this worker's
    /// committed slice of `source_tid` through the plan one chunk at a time. A
    /// worker whose slice is drained drives empty pad chunks, so every worker runs
    /// the same exchange rounds, until [`Self::drive_dag`] reports the backfill done.
    ///
    /// **View-scoped.** Drives ONLY `view_id` ([`Drive::Backfill`]), never the
    /// source's whole dependent closure: the source may already have populated
    /// dependents (live CREATE VIEW over a source with prior views; recovery
    /// step-4 rebuild next to resumed siblings) that a closure re-drive would
    /// double-count. A view backfill runs stop-the-world (the DDL parks the
    /// reactor), so it never yields to live traffic between chunks.
    fn handle_backfill(&mut self, source_tid: u64, view_id: u64) -> Result<(), String> {
        self.cat().dag.rebuild_started(view_id);
        // Compiled before the first chunk: a failure here is an error reply, where the
        // same failure inside a chunk's epoch is a fatal abort mid-round.
        let cat = self.cat();
        cat.dag.open_plan(&cat.registry, view_id)?;
        let chunk_rows = self.cat().registry.scan_chunk_rows();
        // Needed to synthesize empty pad chunks. An unregistered source is a
        // fail-stop: DDL_SYNC applies in SAL order, so a worker that cannot see
        // the source has diverged from the catalog.
        let schema = self.cat().registry.relation_or_err(source_tid)?.schema();
        let mut handle = self.cat().open_source_cursor(view_id, source_tid)?;

        loop {
            let drained = handle.drain_chunk(chunk_rows);
            let pad = drained.is_none();
            let chunk = drained.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            if self.drive_dag(Drive::Backfill { view: view_id, source: source_tid }, chunk, pad) {
                break;
            }
        }

        // A spill fault leaves the view store unbounded, so the process cannot
        // continue; the watchdog turns this into a cluster abort.
        let cat = self.cat();
        if let Err(e) = cat.dag.finish_backfill(&mut cat.registry, view_id) {
            gnitz_fatal_abort!(
                "worker: {} — view state cannot be bounded; aborting for restart+re-derive",
                e,
            );
        }
        Ok(())
    }

    /// CREATE UNIQUE INDEX pre-flight, worker side: stream the sorted index
    /// key spans of every non-NULL row of this worker's committed partition of
    /// `owner_id` to the master, whose merge finds the duplicates. A spill
    /// fault is an `Err` before the first frame.
    fn handle_unique_preflight(&mut self, owner_id: u64, col_indices: &[u32], request_id: u32) -> Result<(), String> {
        if UNIQUE_PREFLIGHT_ERROR.armed() {
            return Err("injected unique pre-flight fault".to_string());
        }
        // One resolve for all three — the cursor owns its sources by `Rc`, so it
        // outlives the entry borrow and pins the snapshot the DDL section froze.
        let e = self.cat().registry.relation_or_err(owner_id)?;
        let (schema, mut handle) = (e.schema(), e.cursor());
        let dir = gnitz_store::relation::relation_dir(self.cat().registry.base_dir(), owner_id);
        let (spec, idx_schema) = gnitz_store::schema::index_spec_and_schema(col_indices, &schema)?;
        let frame_schema = crate::runtime::wire::unique_preflight_wire_schema(&idx_schema, col_indices.len());

        let stride = spec.key_size();
        let chunk_rows = self.cat().registry.scan_chunk_rows();

        let mut sorter = gnitz_store::storage::SpillSort::new(&dir, stride, unique_preflight_spill_bytes());
        let mut keybuf = PkBuf::zeroed(0);
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            let mb = chunk.as_mem_batch();
            for row in 0..chunk.len() {
                debug_assert_eq!(chunk.get_weight(row), 1, "a base table's consolidated row");
                if spec.key_bytes(&mb, row, &mut keybuf) {
                    sorter.push(keybuf.pk_bytes())?;
                }
            }
        }

        let mut producer = sorter.finish()?;
        debug_assert!(self.pending_streams.is_empty(), "pre-flight train behind a scan train");
        send_unique_preflight_keys(
            &self.w2m_writer,
            owner_id,
            &frame_schema,
            request_id,
            self.reply_frame_budget,
            chunk_rows,
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
        let target_id = route.target_id;
        let n = batch.len();
        if let gnitz_wire::WireProbeMode::Project = mode {
            // `arg1` is the PK sentinel here, so the column to project rides the
            // per-mode parameter word instead.
            if !matches!(lookup, HasPkLookup::PrimaryKey) {
                return Err("has_pk: a projecting probe reads the table's own PK store".into());
            }
            let ref_col = mode_param as u8;
            let keys = gnitz_wire::PkKeys::from_sorted(batch.schema().pk_stride(), batch.pk_data().to_vec());
            let result = self.cat().registry.gather_bytes(target_id, keys, ref_col)?;
            self.send_reply(route, result);
            return Ok(());
        }
        match lookup {
            HasPkLookup::SecondaryIndex { cols } => {
                let result = {
                    // One resolution of `(target_id, cols)`: the circuit carries
                    // the index table and the span width.
                    let ic = self
                        .cat()
                        .registry
                        .relation(target_id)
                        .and_then(|r| r.index_on(cols.as_slice()))
                        .ok_or_else(|| format!("No index on columns {:?} for table {}", cols.as_slice(), target_id))?;
                    // The probe's schema is the INDEX table's,
                    // `(indexed_col, src_pk…)` — NOT the owner table's schema.
                    let schema = *batch.schema();
                    // Index layout: PK = (indexed-key span, src_pk_cols). Any
                    // positive-weight match means the value is already in the
                    // index. `open_cursor` keeps a compaction Io/Corrupt
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
                self.send_reply(route, result);
                Ok(())
            }
            HasPkLookup::PrimaryKey => {
                let relation = self.cat().registry.relation(target_id);
                // Grown on demand — see the index arm above.
                let mut result = Batch::empty_with_schema(batch.schema());
                for i in 0..n {
                    let pkb = batch.get_pk_bytes(i);
                    // `false` for an id this worker has not registered.
                    if relation.is_some_and(|r| r.has_pk(pkb)) {
                        result.push_key_row(pkb, 1);
                    }
                }
                self.send_reply(route, result);
                Ok(())
            }
        }
    }

    /// Base checkpoint round.
    fn handle_flush_all(&mut self) -> Result<(), String> {
        Ok(self.cat().registry.checkpoint_base()?)
    }

    /// Run one DAG drive with the exchange context, returning its
    /// [`DagExchangeCtx::drained`].
    fn drive_dag(&mut self, what: Drive, delta: Batch, pad: bool) -> bool {
        let mut ctx = DagExchangeCtx { worker: self, pad, drained: pad };
        let res = crate::query::drive(&mut ctx, what, delta);
        let drained = ctx.drained;
        // DDL_SYNC messages deferred during exchange waits land before the ACK.
        self.dispatch_deferred();
        // The one site that answers a storage fault during view maintenance:
        // restart re-derives every view from its durable base tables.
        if let Err(e) = res {
            gnitz_fatal_abort!(
                "worker: view maintenance failed ({:?}): {} — aborting for restart+re-derive",
                what,
                e,
            );
        }
        drained
    }

    /// Unrecoverable worker fault. The master's watchdog turns the dead worker
    /// into a cluster abort.
    fn fatal_shutdown(&mut self, msg: &str) -> ! {
        gnitz_warn!("FATAL: {}. Shutting down.", msg);
        unsafe { libc::_exit(0) }
    }
}

// ---------------------------------------------------------------------------
// Unique pre-flight key stream
// ---------------------------------------------------------------------------

const UNIQUE_PREFLIGHT_SPILL_BYTES: usize = 128 * 1024 * 1024;

/// The pre-flight sort's run size in bytes: `GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES`,
/// in every build.
fn unique_preflight_spill_bytes() -> usize {
    gnitz_foundation::env::env_num("GNITZ_UNIQUE_PREFLIGHT_SPILL_BYTES", UNIQUE_PREFLIGHT_SPILL_BYTES)
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;
