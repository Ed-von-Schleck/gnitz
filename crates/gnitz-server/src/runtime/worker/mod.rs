//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region, and trades exchange rounds with its peers on
//! the mesh.

use std::collections::VecDeque;
use std::rc::Rc;

use crate::catalog::CatalogEngine;
use crate::query::{DagEngine, DriveHost};
use crate::runtime::mesh::Mesh;
use crate::runtime::sal::{Apply, Inbound, Read, ReplyRoute, SalReader};
use crate::runtime::w2m::W2mWriter;
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::{IndexClaim, PinScope, RelationRegistry};
use gnitz_wire::TypeCode;
use gnitz_wire::{PkKeys, WireFault};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

// ---------------------------------------------------------------------------
// WorkerProcess
// ---------------------------------------------------------------------------

pub struct WorkerProcess<'c> {
    catalog: &'c mut CatalogEngine,
    sal_reader: SalReader,
    w2m_writer: W2mWriter,
    /// Where this worker trades its exchange rounds with its peers, and parks
    /// for a SAL group or an exchange part.
    mesh: Mesh,
    /// Replies this worker owes, in the order they reach the ring, which is
    /// request order within a [`Cut`](crate::runtime::sal::Cut).
    replies: VecDeque<Owed>,
    /// Per-frame wire budget of every W2M train. `GNITZ_REPLY_FRAME_BUDGET`
    /// lowers it, so tests reach multi-frame trains on small tables.
    reply_frame_budget: usize,
}

mod exchange;
mod reply;

use reply::{Owed, Rows};

/// `GNITZ_INJECT_KEY_SPANS_ERROR`: fail every `KeySpans` request, after its claim.
static KEY_SPANS_ERROR: Seam = Seam::new("GNITZ_INJECT_KEY_SPANS_ERROR");

impl<'c> WorkerProcess<'c> {
    pub fn new(catalog: &'c mut CatalogEngine, sal_reader: SalReader, w2m_writer: W2mWriter, mesh: Mesh) -> Self {
        WorkerProcess {
            catalog,
            sal_reader,
            w2m_writer,
            mesh,
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
                self.mesh.park().park(|| self.sal_reader.is_empty());
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups. Shutdown `_exit`s inline.
    fn drain_sal(&mut self) {
        self.emit_reply_frame();
        while let Some(req) = self.next_request() {
            self.dispatch(req, false);
            // One frame per request served: neither waits for the other to run dry.
            self.emit_reply_frame();
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
    /// is a fault frame to its asker, a state change's a fail-stop. `in_drive`:
    /// the request arrived inside an exchange wait of a `Tick` or `Backfill`.
    /// Total, so a new request cannot compile without a decision.
    pub(super) fn dispatch(&mut self, req: Inbound, in_drive: bool) {
        match req {
            Inbound::Shutdown => unsafe { libc::_exit(0) },
            // A drive writes views, so a read of one would see the tick half-run:
            // it parks.
            Inbound::Read { route, read }
                if in_drive
                    && self
                        .catalog
                        .registry
                        .relation(route.target_id)
                        .is_some_and(|r| r.kind().is_view()) =>
            {
                self.replies.push_back(Owed::Parked(route, read))
            }
            // A drive writes no other store, so every other read is answered here,
            // at its place in the SAL — a push behind it must not reach it.
            Inbound::Read { route, read } => {
                let answer = answer(self.catalog, &read);
                self.reply(route, answer);
            }
            Inbound::Apply { ack, apply, rows } => {
                let role = drive_role(&apply);
                if in_drive && role != DriveRole::BesideDrive {
                    gnitz_fatal_abort!("{apply:?} inside an exchange wait — diverged from the master");
                }
                if let Err(e) = self.apply(&apply, rows) {
                    gnitz_fatal_abort!("worker: {apply:?} failed: {e} — aborting for restart");
                }
                if let Some(id) = ack {
                    self.w2m_writer.send_ack(id);
                }
                // Before the next group: a parked read answers from the state its
                // drive left.
                if role == DriveRole::Drives {
                    self.answer_parked();
                }
            }
        }
    }

    /// Apply a state change. The SAL holds the change and every other worker
    /// applies it: a worker that cannot has diverged from the log, and only a
    /// restart brings it back.
    fn apply(&mut self, apply: &Apply<'_>, rows: Option<Box<Batch>>) -> Result<(), String> {
        match *apply {
            Apply::Flush => self.catalog.registry.checkpoint_base(),
            Apply::FlushEph { generation } => self.catalog.flush_ephemeral_round(generation),
            // A group without a data block carries an empty delta.
            Apply::DdlSync { family } => {
                let Some(rows) = rows else { return Ok(()) };
                self.catalog.ddl_sync(family, *rows)?;
                gnitz_debug!("ddl_sync tid={}", family.id());
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
                    crate::query::tick(self, source, first_round + i as u64)?;
                }
                Ok(())
            }
            Apply::Backfill { ref views } => {
                debug_assert!(views.is_sorted(), "a backfill fills its views in id order");
                views.iter().try_for_each(|&view| {
                    crate::query::backfill(self, view).map_err(|e| format!("backfill of view {view}: {e}"))
                })
            }
        }
    }
}

/// How a state change relates to a drive: the exchange waits of a `Tick` or
/// `Backfill`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DriveRole {
    /// Applied when it arrives, inside an exchange wait too.
    BesideDrive,
    /// Is a drive.
    Drives,
    /// Never arrives inside an exchange wait.
    Outside,
}

fn drive_role(apply: &Apply<'_>) -> DriveRole {
    match apply {
        // The master holds the SAL writer until every worker ACKs a flush;
        // deferred, a push's ACK would wait for the drive.
        Apply::Flush | Apply::Push { .. } => DriveRole::BesideDrive,
        Apply::Tick { .. } | Apply::Backfill { .. } => DriveRole::Drives,
        Apply::DdlSync { .. } | Apply::FlushEph { .. } => DriveRole::Outside,
    }
}

/// Answer `read`. A function of the catalog alone: it cannot reach the mesh, so
/// answering a read never enters an exchange wait.
fn answer(cat: &mut CatalogEngine, read: &Read<'_>) -> Result<Rows, WireFault> {
    // Each arm builds its own `Ok`: wrapped around the whole match, every answer
    // would be copied at the width of the widest one.
    match *read {
        Read::HasPk { tid, probe, ref keys } => {
            let keys = PkKeys::decode_probe(keys)?;
            Ok(Rows::Own(cat.registry.probe(tid, probe, keys)?))
        }
        Read::ScanSpec { tid, reply_layout, ref spec } => Ok(Rows::Shared(cat.scan_spec(
            tid,
            gnitz_wire::ReadSpec::decode(spec)?,
            reply_layout,
        )?)),
        Read::Delta { view, after_tick, reply_layout, ref spec } => Ok(Rows::Shared(cat.registry.delta_read(
            view,
            after_tick,
            gnitz_wire::ReadSpec::decode(spec)?,
            reply_layout,
        )?)),
        Read::KeySpans { tid, cols, claim } => {
            if claim {
                cat.registry.add_indexes(tid, &[(IndexClaim::Preflight, cols)])?;
            }
            if KEY_SPANS_ERROR.armed() {
                return Err("injected key-spans fault".into());
            }
            Ok(Rows::Spans(Box::new(cat.registry.key_spans(tid, cols.as_slice())?)))
        }
        Read::ReleasePreflight => {
            cat.registry.release_claim(IndexClaim::Preflight);
            Ok(no_rows())
        }
        // A worker holds no system family a snapshot reads: the master answers
        // those from its own pin.
        Read::Pin { snapshot } => {
            cat.registry.pin(snapshot, PinScope::BaseTables)?;
            Ok(no_rows())
        }
        Read::Unpin { snapshot } => {
            cat.registry.unpin(snapshot);
            Ok(no_rows())
        }
        Read::SnapshotPage { snapshot, tid, ref page } => {
            let page = gnitz_wire::snapshot::PageRequest::decode(page)?;
            Ok(Rows::Own(cat.registry.pinned_page(
                snapshot,
                tid,
                &page,
                cat.registry.slot(),
            )?))
        }
        Read::Backup { ref dir } => {
            let dir = std::str::from_utf8(dir).map_err(|_| "a backup directory that is not UTF-8".to_string())?;
            cat.registry.link_published_into(dir)?;
            Ok(no_rows())
        }
    }
}

/// The reply of a read that answers with no rows: one empty frame.
fn no_rows() -> Rows {
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    Rows::Own(Batch::empty_with_schema(&schema))
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/worker.rs"]
mod bench;
