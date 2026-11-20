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
//! on its peers, while it serves the SAL and sends owed replies.

mod exchange;
mod reply;

use crate::catalog::CatalogEngine;
use crate::runtime::mesh::Mesh;
use crate::runtime::sal::{Apply, Inbound, Ingest, Read, SalReader};
use crate::runtime::w2m::W2mWriter;
use gnitz_foundation::fault::Switch;
use gnitz_foundation::gnitz_debug;
use gnitz_store::relation::{IndexClaim, Snapshot};
use gnitz_wire::{PkKeys, WireFault};
use gnitz_zset::repr::Batch;
use reply::{Replies, Rows};
use rustc_hash::FxHashMap;

pub(super) struct WorkerProcess<'c> {
    catalog: &'c mut CatalogEngine,
    sal_reader: SalReader,
    /// Where this worker trades its exchange rounds with its peers, and parks
    /// for a SAL group or an exchange part.
    mesh: Mesh,
    replies: Replies,
    /// The snapshots this worker holds pinned, by id.
    snapshots: FxHashMap<u64, Snapshot>,
    /// A source of the `Tick` group being run is still to be sealed. Each tick
    /// seals its source as it starts, and a seal takes every row pending: a push
    /// written behind the group must not be applied ahead of one.
    seals_ahead: bool,
}

/// `GNITZ_INJECT_KEY_SPANS_ERROR`: fail every `KeySpans` request, after its claim.
static KEY_SPANS_ERROR: Switch = Switch::new("GNITZ_INJECT_KEY_SPANS_ERROR");

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
            snapshots: FxHashMap::default(),
            seals_ahead: false,
        }
    }

    /// Serve the SAL until the process is killed or fail-stops: no request ends
    /// the loop.
    pub(super) fn run(&mut self) -> ! {
        loop {
            self.wait(|| !self.sal_reader.is_empty());
            self.drain_sal(false);
        }
    }

    /// Park this worker until `woken`, unless an owed reply can leave now.
    /// While every owed reply waits on the master, for room the ring refused or
    /// the end of a hold, the master's release or end of a hold wakes the park
    /// too.
    fn wait(&self, woken: impl Fn() -> bool) {
        if self.replies.is_empty() {
            self.mesh.park().park(|| !woken());
        } else {
            self.replies.wait_for_master(|| !woken());
        }
    }

    /// Serve the SAL's pending groups and send owed reply frames, neither
    /// waiting for the other to run dry. `in_drive`: inside an exchange wait.
    ///
    /// - At the top level: one frame ahead of the first group, and one after
    ///   each group served.
    /// - In an exchange wait whose round is still out: the same, so a train
    ///   moves while the SAL is empty. While a seal of the running `Tick` group
    ///   is still ahead no group is served, so every worker seals the log as
    ///   far as the group; the frame ahead still goes.
    /// - In an exchange wait whose round is complete: one group is served and
    ///   no frame is sent, then the drive goes on. That one group keeps the
    ///   reads on the SAL moving through a long drive, whose rounds may each be
    ///   complete when its wait begins (with one worker, always). The next
    ///   wait, or the top level, serves what the SAL still holds, in order.
    fn drain_sal(&mut self, in_drive: bool) {
        let serves = !self.seals_ahead;
        let open = |wp: &Self| !in_drive || !wp.mesh.complete();
        if open(self) {
            self.replies.emit_frame();
        }
        while serves && self.serve_next(in_drive) && open(self) {
            self.replies.emit_frame();
        }
    }

    /// Read the next SAL group this worker acts on and run it: the one place
    /// the drain decodes a group, and the one place a request's outcome
    /// is chosen — a read's failure is a fault frame to its asker, a state
    /// change's a fail-stop. `in_drive`: the group arrives inside an exchange
    /// wait of a `Tick` or `Backfill`.
    ///
    /// `false` when the SAL holds no group for this worker.
    fn serve_next(&mut self, in_drive: bool) -> bool {
        let cat = &mut *self.catalog;
        let Some(inbound) = self
            .sal_reader
            .next_request(|tid, record| cat.known_decode(tid, record))
        else {
            return false;
        };
        match inbound {
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
                    let answer = answer(cat, &mut self.snapshots, &read);
                    self.replies.reply(route, answer);
                }
                true
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
                if let Some(rows) = rows
                    && let Err(e) = self.ingest(ingest, rows)
                {
                    gnitz_fatal_abort!("worker: {ingest:?} failed: {e} — aborting for restart");
                }
                true
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
                self.replies
                    .answer_parked(|read| answer(self.catalog, &mut self.snapshots, read));
                true
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
                crate::query::backfill(self, views).map_err(|e| format!("backfill of views {views:?}: {e}"))
            }
        }
    }
}

/// Answer `read`. A function of the catalog and this worker's pins: it cannot
/// reach the mesh, so answering a read never enters an exchange wait.
fn answer(
    cat: &mut CatalogEngine,
    snapshots: &mut FxHashMap<u64, Snapshot>,
    read: &Read<'_>,
) -> Result<Rows, WireFault> {
    Ok(match *read {
        Read::HasPk { tid, probe, keys } => {
            let keys = PkKeys::decode_probe(keys)?;
            Rows::from(cat.registry.probe(tid, probe, keys)?)
        }
        Read::ScanSpec { tid, reply_layout, spec } => {
            Rows::from(cat.scan_spec(tid, gnitz_wire::ReadSpec::decode(spec)?, reply_layout)?)
        }
        Read::Delta { view, after_tick, reply_layout, spec } => {
            let spec = gnitz_wire::ReadSpec::decode(spec)?;
            Rows::from(cat.registry.delta_read(view, after_tick, spec, reply_layout)?)
        }
        Read::KeySpans { tid, cols, claim } => {
            if claim {
                cat.registry.add_indexes(tid, &[(IndexClaim::Preflight, cols)])?;
            }
            if KEY_SPANS_ERROR.armed() {
                return Err("injected key-spans fault".into());
            }
            Rows::Spans(Box::new(cat.registry.key_spans(tid, cols.as_slice())?))
        }
        Read::ReleasePreflight => {
            cat.registry.release_claim(IndexClaim::Preflight);
            Rows::Nothing
        }
        // A worker holds no system family a snapshot reads: the master answers
        // those from its own pin.
        // The master hands out each id once a boot.
        Read::Pin { snapshot } => {
            let prior = snapshots.insert(snapshot, cat.registry.pin());
            debug_assert!(prior.is_none(), "snapshot {snapshot} pinned twice");
            Rows::Nothing
        }
        Read::Unpin { snapshot } => {
            snapshots.remove(&snapshot);
            Rows::Nothing
        }
        Read::SnapshotPage { snapshot, tid, page } => {
            let page = gnitz_wire::snapshot::PageRequest::decode(page)?;
            let pin = snapshots
                .get(&snapshot)
                .ok_or_else(|| format!("snapshot {snapshot} is not pinned"))?;
            Rows::from(pin.page(tid, &page)?)
        }
        Read::Backup { dir } => {
            cat.registry.link_published_into(dir)?;
            Rows::Nothing
        }
    })
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/worker.rs"]
mod bench;
