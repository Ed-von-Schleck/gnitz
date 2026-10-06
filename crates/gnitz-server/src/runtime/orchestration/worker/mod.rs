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
use crate::runtime::park::WorkerPark;
use crate::runtime::sal::{Apply, Inbound, Read, ReplyRoute, SalReader, SalRequest};
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::{self as ipc};
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::RelationRegistry;
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess<'c> {
    catalog: &'c mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    /// Where this worker sleeps for a SAL group or an exchange part.
    park: WorkerPark,
    /// Where this worker trades its exchange rounds with its peers.
    mesh: Mesh,
    /// The running drive's vote on "every source is drained": this worker's own
    /// claim until an exchange round closes, every worker's from then.
    drained: bool,
    /// Replies this worker owes, in the order they reach the ring. A cut's reader
    /// drains its leases in request order and a ring frees only a released prefix,
    /// so within a cut ring order must be request order. Replies of different cuts
    /// go to different leases and need no order between them.
    replies: VecDeque<Owed>,
    /// Per-frame wire budget of every W2M train. `GNITZ_REPLY_FRAME_BUDGET`
    /// lowers it, so tests reach multi-frame trains on small tables.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

use reply::{Owed, Rows};

/// `GNITZ_INJECT_KEY_SPANS_ERROR`: fail every `KeySpans` request.
static KEY_SPANS_ERROR: Seam = Seam::new("GNITZ_INJECT_KEY_SPANS_ERROR");

impl<'c> WorkerProcess<'c> {
    pub fn new(
        catalog: &'c mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
        park: WorkerPark,
        mesh: Mesh,
    ) -> Self {
        WorkerProcess {
            catalog,
            sal_reader,
            w2m_writer,
            park,
            mesh,
            drained: false,
            replies: VecDeque::new(),
            reply_frame_budget: gnitz_foundation::env::env_num(
                "GNITZ_REPLY_FRAME_BUDGET",
                gnitz_wire::MAX_FRAME_PAYLOAD,
            )
            .min(gnitz_wire::MAX_FRAME_PAYLOAD),
        }
    }

    // ── Main event loop ────────────────────────────────────────────────

    /// Serve the SAL until a `Shutdown` exits the process.
    pub fn run(&mut self) -> ! {
        loop {
            // An owed reply emits its next frame without waiting on the SAL.
            if self.replies.is_empty() {
                self.park.park(|| self.sal_reader.is_empty());
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups. Shutdown `_exit`s inline.
    fn drain_sal(&mut self) {
        self.emit_reply_frame();
        while let Some(req) = self.next_request() {
            self.handle_request(req);
            if !self.replies.is_empty() {
                // Before the next group: a parked read answers from the state its
                // drive left.
                self.answer_parked();
                // One frame per request served: neither waits for the other to
                // run dry.
                self.emit_reply_frame();
            }
        }
    }

    /// The next SAL group this worker acts on: the single decode point of both
    /// drain loops.
    fn next_request(&mut self) -> Option<Inbound> {
        let cat = &*self.catalog;
        self.sal_reader
            .next_request(|tid, record| cat.known_decode(tid, record))
    }

    /// Run one request — the one place its outcome is chosen: a read's failure
    /// is a fault frame to its asker, a state change's a fail-stop.
    fn handle_request(&mut self, req: Inbound) {
        match req.what {
            SalRequest::Shutdown => unsafe { libc::_exit(0) },
            SalRequest::Read(read) => {
                let answer = answer(self.catalog, &read, req.rows.as_deref());
                self.reply(req.route, answer);
            }
            SalRequest::Apply(apply) => self.apply_request(req.route, apply, req.rows),
        }
    }

    /// Apply a state change and ACK it. The SAL holds the change and every other
    /// worker applies it: a worker that cannot has diverged from the log, and only
    /// a restart brings it back.
    fn apply_request(&mut self, route: ReplyRoute, apply: Apply<'_>, rows: Option<Box<Batch>>) {
        if let Err(e) = self.apply(&apply, rows) {
            gnitz_fatal_abort!("worker: {apply:?} failed: {e} — aborting for restart");
        }
        if route.request_id != 0 {
            self.w2m_writer.send_ack(route.request_id);
        }
    }

    fn apply(&mut self, apply: &Apply<'_>, rows: Option<Box<Batch>>) -> Result<(), String> {
        match *apply {
            Apply::Flush => self.catalog.registry.checkpoint_base(),
            Apply::FlushEph { generation } => self.catalog.flush_ephemeral_round(generation),
            // A group without a data block carries an empty delta.
            Apply::DdlSync { family } => {
                let Some(rows) = rows else { return Ok(()) };
                self.catalog.ddl_sync(family, *rows)?;
                gnitz_debug!("ddl_sync tid={}", family);
                Ok(())
            }
            Apply::Push { tid } => {
                let Some(rows) = rows else { return Ok(()) };
                let row_count = rows.len();
                self.catalog.ingest_unticked(tid, *rows)?;
                gnitz_debug!("push tid={} rows={}", tid, row_count);
                Ok(())
            }
            Apply::Tick { first_round, ref tids } => {
                for (i, &source) in tids.iter().enumerate() {
                    let delta = self.catalog.registry.seal(source)?;
                    self.drive(Drive::Tick { source, round: first_round + i as u64 }, delta)?;
                }
                Ok(())
            }
            Apply::Backfill { source, view } => {
                let mut cursor = self.catalog.open_source_cursor(view, source)?;
                let chunk_rows = self.catalog.registry.scan_chunk_rows();
                // A drained worker keeps driving pad chunks, so every worker runs the
                // same exchange rounds until every source is drained.
                while !self.drive(Drive::Backfill { view, source }, cursor.drain_chunk(chunk_rows))? {}
                let cat = &mut *self.catalog;
                cat.dag.finish_backfill(&mut cat.registry, view, source)
            }
        }
    }
}

/// Answer `read`; `rows` are the keys a probe carries. A function of the catalog
/// alone: it cannot reach the mesh, so answering a read never enters an exchange
/// wait.
fn answer(cat: &mut CatalogEngine, read: &Read<'_>, rows: Option<&Batch>) -> Result<Rows, WireFault> {
    // Each arm builds its own `Ok`: wrapped around the whole match, every answer
    // would be copied at the width of the widest one.
    match *read {
        Read::HasPk { tid, probe } => {
            let keys = rows.ok_or("has_pk: a probe carries its keys")?;
            Ok(Rows::Own(cat.registry.probe(tid, probe, keys)?))
        }
        Read::ScanSpec { tid, reply_layout, ref spec } => Ok(Rows::Shared(cat.scan_spec(
            tid,
            gnitz_wire::ReadSpec::decode(spec)?,
            reply_layout,
        )?)),
        Read::Delta { view, after_tick, cut_round, ref read } => {
            let (spec, reply_layout) = Read::delta_parts(read);
            Ok(Rows::Shared(cat.registry.delta_read(
                view,
                after_tick,
                cut_round,
                gnitz_wire::ReadSpec::decode(spec)?,
                reply_layout,
            )?))
        }
        Read::KeySpans { tid, cols } => {
            if KEY_SPANS_ERROR.armed() {
                return Err("injected key-spans fault".into());
            }
            Ok(Rows::Spans(Box::new(cat.registry.key_spans(tid, cols.as_slice())?)))
        }
    }
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/worker.rs"]
mod bench;
