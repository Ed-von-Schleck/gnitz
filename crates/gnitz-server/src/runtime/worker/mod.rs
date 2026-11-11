//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region, and trades exchange rounds with its peers on
//! the mesh.
//!
//! This file reads the SAL and runs what it asks: a read is answered off the
//! catalog, rows are ingested, a round is run and ACKed. `reply` owns the ring
//! and what the worker owes on it; `exchange` is the wait a drive's round spends
//! on its peers, and what of the SAL is served meanwhile.

mod exchange;
mod reply;

use crate::catalog::CatalogEngine;
use crate::runtime::mesh::Mesh;
use crate::runtime::sal::{Apply, Inbound, Ingest, Read, SalReader};
use crate::runtime::w2m::W2mWriter;
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::IndexClaim;
use gnitz_wire::TypeCode;
use gnitz_wire::{PkKeys, WireFault};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};
use reply::{Replies, Rows};

pub(super) struct WorkerProcess<'c> {
    catalog: &'c mut CatalogEngine,
    sal_reader: SalReader,
    /// Where this worker trades its exchange rounds with its peers, and parks
    /// for a SAL group or an exchange part.
    mesh: Mesh,
    replies: Replies,
    /// A source of the `Tick` group being run is still to be sealed. Each tick
    /// seals its source as it starts, and a seal takes every row pending: a push
    /// written behind the group must not be applied ahead of one.
    seals_ahead: bool,
}

/// `GNITZ_INJECT_KEY_SPANS_ERROR`: fail every `KeySpans` request, after its claim.
static KEY_SPANS_ERROR: Seam = Seam::new("GNITZ_INJECT_KEY_SPANS_ERROR");

impl<'c> WorkerProcess<'c> {
    pub(super) fn new(
        catalog: &'c mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
        mesh: Mesh,
    ) -> Self {
        WorkerProcess {
            catalog,
            sal_reader,
            mesh,
            replies: Replies::new(w2m_writer),
            seals_ahead: false,
        }
    }

    /// Serve the SAL until the process is killed or fail-stops: no request ends
    /// the loop.
    pub(super) fn run(&mut self) -> ! {
        loop {
            // An owed reply goes out without waiting on the SAL, unless the ring
            // refused the next frame to leave or the master holds every reply
            // owed; then the master's release or end of a hold wakes the park too.
            if self.replies.is_empty() {
                self.mesh.park().park(|| self.sal_reader.is_empty());
            } else {
                self.replies.wait_for_master(|| self.sal_reader.is_empty());
            }

            self.drain_sal();
        }
    }

    /// Process all pending SAL message groups.
    fn drain_sal(&mut self) {
        self.replies.emit_frame();
        while self.serve_next(false).is_some() {
            // One frame per request served: neither waits for the other to run dry.
            self.replies.emit_frame();
        }
    }

    /// Read the next SAL group this worker acts on and run it: the one place
    /// either drain loop decodes a group, and the one place a request's outcome
    /// is chosen — a read's failure is a fault frame to its asker, a state
    /// change's a fail-stop. `in_drive`: the group arrives inside an exchange
    /// wait of a `Tick` or `Backfill`.
    ///
    /// `None` when the SAL holds no group for this worker, else whether the one
    /// served was a read.
    fn serve_next(&mut self, in_drive: bool) -> Option<bool> {
        let cat = &mut *self.catalog;
        match self
            .sal_reader
            .next_request(|tid, record| cat.known_decode(tid, record))?
        {
            Inbound::Read { route, read } => {
                // A drive writes views, so a read of one would see the tick
                // half-run: it parks, kept, since the drive reads further
                // groups before it is answered. A drive writes no other
                // store, so every other read is answered here, at its place in
                // the SAL — a push behind it must not reach it.
                if in_drive
                    && cat
                        .registry
                        .relation(route.target_id)
                        .is_some_and(|r| r.kind().is_view())
                {
                    self.replies.park(route, &read);
                } else {
                    let answer = answer(cat, &read);
                    self.replies.reply(route, answer);
                }
                Some(true)
            }
            // The views and the DAG are a drive's own while it runs: of the
            // requests that touch them the master pipelines only reads, which
            // park above, and holds every other back until the drive's ACK, as
            // it does every round. So a drive that serves the SAL meets reads
            // and pushes alone.
            Inbound::Ingest { ingest, rows } => {
                if in_drive && matches!(ingest, Ingest::DdlSync { .. }) {
                    gnitz_fatal_abort!("{ingest:?} inside an exchange wait — diverged from the master");
                }
                // A group without a data block carries an empty delta.
                if let Some(rows) = rows {
                    if let Err(e) = self.ingest(ingest, rows) {
                        gnitz_fatal_abort!("worker: {ingest:?} failed: {e} — aborting for restart");
                    }
                }
                Some(false)
            }
            Inbound::Apply { apply } => {
                if in_drive {
                    gnitz_fatal_abort!("{apply:?} inside an exchange wait — diverged from the master");
                }
                if let Err(e) = self.apply(&apply) {
                    gnitz_fatal_abort!("worker: {apply:?} failed: {e} — aborting for restart");
                }
                // The ACK: every apply at or below the drain's position has run.
                self.replies.applied(self.sal_reader.position());
                // Before the next group: a parked read answers from the state its
                // drive left.
                let cat = &mut *self.catalog;
                self.replies.answer_parked(|read| answer(cat, read));
                Some(false)
            }
        }
    }

    /// Take a group's rows. The SAL holds them and every worker they reach
    /// takes them: a worker that cannot has diverged from the log, and only a
    /// restart brings it back.
    fn ingest(&mut self, ingest: Ingest, rows: Box<Batch>) -> Result<(), String> {
        match ingest {
            Ingest::DdlSync { family } => {
                self.catalog.ddl_sync(family, *rows)?;
                gnitz_debug!("ddl_sync tid={}", family.id());
            }
            Ingest::Push { tid, rule } => {
                let row_count = rows.len();
                self.catalog.ingest_unticked(tid, *rows, rule)?;
                gnitz_debug!("push tid={} rows={}", tid, row_count);
            }
        }
        Ok(())
    }

    /// Run a round every worker runs: one that cannot has diverged from the
    /// log, and only a restart brings it back.
    fn apply(&mut self, apply: &Apply<'_>) -> Result<(), String> {
        match *apply {
            Apply::Flush => self.catalog.registry.checkpoint_base(),
            Apply::FlushEph { generation } => self.catalog.flush_ephemeral_round(generation),
            Apply::Tick { first_round, ref tids } => {
                for (i, &source) in tids.iter().enumerate() {
                    self.seals_ahead = i + 1 < tids.len();
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

/// Answer `read`. A function of the catalog alone: it cannot reach the mesh, so
/// answering a read never enters an exchange wait.
fn answer(cat: &mut CatalogEngine, read: &Read<'_>) -> Result<Rows, WireFault> {
    // Each arm builds its own `Ok`: wrapped around the whole match, every answer
    // would be copied at the width of the widest one.
    match *read {
        Read::HasPk { tid, probe, keys } => {
            let keys = PkKeys::decode_probe(keys)?;
            Ok(Rows::from(cat.registry.probe(tid, probe, keys)?))
        }
        Read::ScanSpec { tid, reply_layout, spec } => Ok(Rows::from(cat.scan_spec(
            tid,
            gnitz_wire::ReadSpec::decode(spec)?,
            reply_layout,
        )?)),
        Read::Delta { view, after_tick, reply_layout, spec } => Ok(Rows::from(cat.registry.delta_read(
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
            cat.registry.pin(snapshot)?;
            Ok(no_rows())
        }
        Read::Unpin { snapshot } => {
            cat.registry.unpin(snapshot);
            Ok(no_rows())
        }
        Read::SnapshotPage { snapshot, tid, page } => {
            let page = gnitz_wire::snapshot::PageRequest::decode(page)?;
            Ok(Rows::from(cat.registry.pinned_page(snapshot, tid, &page)?))
        }
        Read::Backup { dir } => {
            cat.registry.link_published_into(dir)?;
            Ok(no_rows())
        }
    }
}

/// The reply of a read that answers with no rows: one empty frame.
fn no_rows() -> Rows {
    let schema = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    Rows::from(Batch::empty_with_schema(&schema))
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/worker.rs"]
mod bench;
