//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region, and trades exchange rounds with its peers on
//! the mesh.

use std::collections::VecDeque;
use std::rc::Rc;

use crate::catalog::CatalogEngine;
use crate::query::{DagEngine, Drive, DriveHost};
use crate::runtime::mesh::Mesh;
use crate::runtime::sal::{SalMessageKind, SalReader};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{self as ipc};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::{Relation, RelationRegistry};
use gnitz_wire::control::{peek_control_block, ControlHeader};
use gnitz_wire::{WireFlags, WireStatus};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// One dispatched request. Fully owned — a parked request must not borrow the
/// SAL mapping, which an inline `Flush` resets.
struct Request {
    kind: SalMessageKind,
    route: ReplyRoute,
    hdr: ControlHeader,
    blob: Vec<u8>,
    batch: Option<Batch>,
}

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess {
    catalog: *mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    /// Where this worker trades its exchange rounds with its peers.
    mesh: Mesh,
    /// Requests an exchange wait deferred, in SAL order: replayed at top level
    /// right after the request whose drive deferred them.
    deferred: Vec<Request>,
    /// Reply trains, one frame emitted per SAL drain, front first: the master
    /// reads one lease at a time and a ring frees only in order.
    pending_streams: VecDeque<PendingScan>,
    /// Per-frame wire budget of every W2M train. `GNITZ_REPLY_FRAME_BUDGET`
    /// lowers it, so tests reach multi-frame trains on small tables.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

use exchange::DagExchangeCtx;
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
}

impl WorkerProcess {
    pub fn new(catalog: *mut CatalogEngine, sal_reader: SalReader, w2m_writer: W2mWriter, mesh: Mesh) -> Self {
        // Worker rank/count (and role) are latched in the fork child before any
        // catalog work — see `server_main`, not here: boot-compiled plans
        // would otherwise carry rank 0 / num_workers 1.
        WorkerProcess {
            catalog,
            sal_reader,
            w2m_writer,
            mesh,
            deferred: Vec::new(),
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
        self.send_ack(ready_request_id);

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
        while let Some(req) = self.next_request() {
            self.handle_request(req);
            // Before the next group, so a deferred read sees the state it was
            // sent against.
            for req in std::mem::take(&mut self.deferred) {
                self.handle_request(req);
            }
            assert!(self.deferred.is_empty(), "a replayed request deferred another");
        }
    }

    /// The next SAL group this worker acts on, decoded into an owned
    /// [`Request`]: the single decode point of both drain loops.
    fn next_request(&mut self) -> Option<Request> {
        let (msg, wire) = self.sal_reader.next()?;
        // The kinds the master frames with their target's catalog record.
        let catalog_record = matches!(msg.kind, SalMessageKind::Push | SalMessageKind::DdlSync);
        let known = |tid, record: &[u8]| catalog_record.then(|| self.cat().known_decode(tid, record)).flatten();
        // Fail-stop: a dropped group diverges this worker from the master.
        let decoded =
            peek_control_block(wire).and_then(|control| Ok((ipc::decode_sal_rows(wire, &control, known)?, control)));
        match decoded {
            Ok((batch, control)) => Some(Request {
                kind: msg.kind,
                route: ReplyRoute {
                    target_id: control.hdr.target_id,
                    request_id: msg.request_id,
                    fifo: msg.in_request_order,
                },
                hdr: control.hdr,
                blob: wire[control.blob].to_vec(),
                batch,
            }),
            Err(e) => gnitz_fatal_abort!("failed to decode {:?} for tid={}: {e}", msg.kind, msg.target_id),
        }
    }

    /// Run one request, with a failure sent back on its own request id — the one
    /// place a worker's reply status is chosen.
    fn handle_request(&mut self, req: Request) {
        let request_id = req.route.request_id;
        if let Err(fault) = self.dispatch_inner(req) {
            self.send_fault(&fault, request_id);
        }
    }

    fn dispatch_inner(&mut self, req: Request) -> Result<(), gnitz_wire::WireFault> {
        let Request { kind, route, hdr, blob, batch } = req;
        let (target_id, request_id) = (route.target_id, route.request_id);

        match kind {
            SalMessageKind::Shutdown => unsafe { libc::_exit(0) },

            SalMessageKind::Flush => {
                self.cat().registry.checkpoint_base()?;
                self.send_ack(request_id);
                Ok(())
            }

            SalMessageKind::FlushEph => {
                self.cat().flush_ephemeral_round(hdr.arg0)?;
                self.send_ack(request_id);
                Ok(())
            }

            // Unaddressed, so a failure has no one to answer: DDL application
            // failure on trusted master→worker IPC means memory corruption or an
            // engine bug, and continuing would leave this worker with a
            // permanently stale catalog — silently wrong results.
            SalMessageKind::DdlSync => {
                if let Some(batch) = batch {
                    if let Err(e) = self.cat().ddl_sync(target_id, batch) {
                        gnitz_fatal_abort!("DdlSync application failed for tid={target_id}: {e}");
                    }
                    gnitz_debug!("ddl_sync tid={}", target_id);
                }
                Ok(())
            }

            SalMessageKind::Backfill => {
                // Stop-the-world (the DDL parks the reactor): no yield.
                self.handle_backfill(target_id, hdr.arg0)?;
                self.send_ack(request_id);
                Ok(())
            }

            SalMessageKind::HasPk => {
                let Some(batch) = batch else {
                    return Err("has_pk: a probe carries its keys".into());
                };
                let probe = gnitz_wire::Probe::from_wire(hdr.flags.probe_mode, hdr.arg0, hdr.arg1)
                    .map_err(|e| format!("has_pk: {e}"))?;
                let result = self.cat().registry.probe(target_id, probe, &batch)?;
                self.send_reply(route, result);
                Ok(())
            }

            SalMessageKind::Push => {
                if let Some(batch) = batch {
                    self.handle_push(target_id, batch);
                }
                self.send_ack(request_id);
                Ok(())
            }

            SalMessageKind::Tick => {
                let (tids, rest) = blob.as_chunks::<8>();
                if !rest.is_empty() {
                    return Err("tick: the blob is not whole tids".into());
                }
                for (i, tid) in tids.iter().enumerate() {
                    self.handle_tick(u64::from_le_bytes(*tid), hdr.arg0 + i as u64);
                }
                self.send_ack(request_id);
                Ok(())
            }

            SalMessageKind::ScanSpec => {
                let keeper = self
                    .cat()
                    .scan_spec(target_id, gnitz_wire::ReadSpec::decode(&blob)?, hdr.arg0)?;
                self.send_reply(route, keeper);
                Ok(())
            }

            // Its deltas in rounds `(arg1, arg0]`.
            SalMessageKind::DeltaRead => {
                let reply_layout = gnitz_wire::decode_all(&blob, "delta read", |r| r.u64())?;
                let keeper = self
                    .cat()
                    .registry
                    .delta_read(target_id, hdr.arg1, hdr.arg0, reply_layout)?;
                self.send_reply(route, keeper);
                Ok(())
            }

            SalMessageKind::UniquePreflight => {
                let cols = gnitz_wire::unpack_pk_cols(hdr.arg1).map_err(|e| {
                    format!(
                        "unique pre-flight on table {target_id}: {}",
                        e.for_role(gnitz_wire::PkListRole::ColumnList)
                    )
                })?;
                self.handle_unique_preflight(target_id, cols.as_slice(), request_id)?;
                Ok(())
            }
        }
    }

    // ── Request handlers ───────────────────────────────────────────────

    fn handle_push(&mut self, target_id: u64, batch: Batch) {
        let row_count = batch.len();
        // The master admitted this group against the catalog it committed under, and
        // the SAL holds it durably: any failure here leaves this worker diverged from
        // it. Only a restart replays it.
        let res = match self.cat().registry.relation_or_err(target_id).map(Relation::kind) {
            Ok(kind) if kind.is_ingestion_point() => self.cat().ingest_unticked(target_id, batch),
            Ok(kind) => Err(format!(
                "relation {target_id} is a {}, not an ingestion point",
                kind.noun()
            )),
            Err(e) => Err(e),
        };
        if let Err(e) = res {
            gnitz_fatal_abort!(
                "worker: push apply failed (table_id={}): {} — aborting for restart+replay",
                target_id,
                e,
            );
        }
        gnitz_debug!("push tid={} rows={}", target_id, row_count);
    }

    /// Drive one view-maintenance tick of `target_id`'s dependent closure.
    /// `round` is the tick round the master allocated for this tid; every fed
    /// view's captured delta is stamped with it.
    fn handle_tick(&mut self, target_id: u64, round: u64) {
        let Some(schema) = self.cat().registry.relation(target_id).map(Relation::schema) else {
            return;
        };
        let delta = match self.cat().registry.seal(target_id) {
            Ok(delta) => delta.unwrap_or_else(|| Batch::empty_with_schema(&schema)),
            Err(e) => gnitz_fatal_abort!(
                "worker: seal failed (table_id={}): {} — aborting for restart",
                target_id,
                e
            ),
        };
        self.drive_dag(Drive::Tick { source: target_id, round }, delta, false);
    }

    /// Distributed CREATE-VIEW backfill, worker side: drives this worker's
    /// committed slice of `source_tid` through the plan one chunk at a time. A
    /// worker whose slice is drained drives empty pad chunks, so every worker runs
    /// the same exchange rounds, until [`Self::drive_dag`] reports the backfill done.
    ///
    /// View-scoped: see [`Drive::Backfill`].
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
            let chunk = handle.drain_chunk(chunk_rows);
            let drained = chunk.is_none();
            let chunk = chunk.unwrap_or_else(|| Batch::empty_with_schema(&schema));
            if self.drive_dag(Drive::Backfill { view: view_id, source: source_tid }, chunk, drained) {
                break;
            }
        }

        // A spill fault leaves the view store unbounded, so the process cannot
        // continue.
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
        let spec = gnitz_zset::schema::KeySpec::new(col_indices, &schema)?;
        let frame_schema = spec.span_schema();

        let stride = spec.key_size();
        let chunk_rows = self.cat().registry.scan_chunk_rows();

        let mut sorter = gnitz_zset::repr::SpillSort::new(&dir, stride, unique_preflight_spill_bytes());
        while let Some(chunk) = handle.drain_chunk(chunk_rows) {
            sorter.push_spans(&chunk, &spec)?;
        }

        let mut producer = sorter.finish()?;
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

    /// Run one DAG drive with the exchange context, returning its
    /// [`DagExchangeCtx::all_drained`]. `drained`: this worker's partition is.
    fn drive_dag(&mut self, what: Drive, delta: Batch, drained: bool) -> bool {
        let mut ctx = DagExchangeCtx {
            worker: self,
            own_drained: drained,
            all_drained: drained,
        };
        let res = crate::query::drive(&mut ctx, what, delta);
        let all_drained = ctx.all_drained;
        // The one site that answers a storage fault during view maintenance:
        // restart re-derives every view from its durable base tables.
        if let Err(e) = res {
            gnitz_fatal_abort!(
                "worker: view maintenance failed ({:?}): {} — aborting for restart+re-derive",
                what,
                e,
            );
        }
        all_drained
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
