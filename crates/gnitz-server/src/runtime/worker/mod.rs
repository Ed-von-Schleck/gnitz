//! Worker process event loop.
//!
//! Owns one store per user relation — this worker's slice of it. Receives requests from
//! the master via the SAL (shared append-only log), sends responses via a
//! per-worker W2M shared region, and trades exchange rounds with its peers on
//! the mesh.
//!
//! This file reads the SAL and runs what it asks: a read is answered off the
//! catalog, rows are ingested, a round is run and ACKed, and a drive's exchange
//! round is waited out while the SAL is served and owed replies sent. `reply`
//! owns the ring and what the worker owes on it.

mod reply;

use crate::catalog::CatalogEngine;
use crate::query::{DagEngine, DriveHost, Lane};
use crate::runtime::mesh::Mesh;
use crate::runtime::sal::{Apply, Inbound, Ingest, Read, SalReader};
use crate::runtime::w2m::W2mWriter;
use gnitz_foundation::fault::Switch;
use gnitz_foundation::gnitz_debug;
use gnitz_store::relation::{IndexClaim, RelationRegistry, Snapshot};
use gnitz_wire::{PkKeys, WireFault};
use gnitz_zset::repr::Batch;
use reply::{Replies, Rows};
use rustc_hash::FxHashMap;

/// How many `spin_loop` polls a worker on a core of its own spends waiting for an
/// exchange part before it parks. A peer's arrival in a small tick usually lands
/// within tens of microseconds, sooner than a futex sleep and wake turn around.
const SPINS_BEFORE_PARK: u32 = 1000;

pub(super) struct WorkerProcess<'c> {
    catalog: &'c mut CatalogEngine,
    sal_reader: SalReader,
    /// Where this worker trades its exchange rounds with its peers.
    mesh: Mesh,
    replies: Replies,
    /// The snapshots this worker holds pinned, by id.
    snapshots: FxHashMap<u64, Snapshot>,
    /// A source of the `Tick` group being run is still to be sealed. Each tick
    /// seals its source as it starts, and a seal takes every row pending: a push
    /// written behind the group must not be applied ahead of one.
    seals_ahead: bool,
    /// How many polls an exchange wait spends on its part before it parks: zero
    /// unless this worker has a core of its own, where a spin holds off no peer.
    spins: u32,
}

/// `GNITZ_INJECT_KEY_SPANS_ERROR`: fail every `KeySpans` request, after its claim.
static KEY_SPANS_ERROR: Switch = Switch::new("GNITZ_INJECT_KEY_SPANS_ERROR");

impl<'c> WorkerProcess<'c> {
    /// `pinned`: this worker runs on a core of its own, so an exchange wait
    /// spins before it parks.
    pub(super) fn new(
        catalog: &'c mut CatalogEngine,
        sal_reader: SalReader,
        w2m_writer: W2mWriter,
        mesh: Mesh,
        pinned: bool,
    ) -> Self {
        WorkerProcess {
            catalog,
            sal_reader,
            mesh,
            replies: Replies::new(w2m_writer),
            snapshots: FxHashMap::default(),
            seals_ahead: false,
            spins: if pinned { SPINS_BEFORE_PARK } else { 0 },
        }
    }

    /// Serve the SAL until the process is killed or fail-stops: no request ends
    /// the loop.
    pub(super) fn run(&mut self) -> ! {
        loop {
            self.turn();
        }
    }

    /// Wait for work, then do it: serve the SAL's pending groups and send owed
    /// reply frames, neither waiting for the other to run dry. Inside an
    /// exchange wait, the open round's completion is work too.
    ///
    /// The wait parks this worker until there is work, unless an owed reply
    /// can leave now; an exchange wait polls first, for the spin budget.
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
    fn turn(&mut self) {
        let serves = !self.seals_ahead;
        let spins = if self.mesh.in_round() { self.spins } else { 0 };
        let part_ready = |wp: &Self| wp.mesh.in_round() && wp.mesh.complete();
        let woken = || serves && !self.sal_reader.is_empty() || part_ready(self);
        if self.replies.waits_on_master()
            && !woken()
            && !(0..spins).any(|_| {
                std::hint::spin_loop();
                woken()
            })
        {
            self.replies.wait(|| !woken());
        }
        if !part_ready(self) {
            self.replies.emit_frame();
        }
        while serves && self.serve_next() && !part_ready(self) {
            self.replies.emit_frame();
        }
    }

    /// Read the next SAL group this worker acts on and run it: the one place
    /// the drain decodes a group, and the one place a request's outcome
    /// is chosen — a read's failure is a fault frame to its asker; a state
    /// change every worker makes that fails has diverged this worker from the
    /// log, and fail-stops it for a restart. With a round open, the group
    /// arrives inside an exchange wait of a `Tick` or `Backfill`.
    ///
    /// `false` when the SAL holds no group for this worker.
    fn serve_next(&mut self) -> bool {
        let in_drive = self.mesh.in_round();
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
                let Some(rows) = rows else { return true };
                let n = rows.len();
                let taken = match ingest {
                    Ingest::DdlSync { family } => cat.ddl_sync(family, *rows),
                    Ingest::Push { tid, rule } => cat.ingest_unticked(tid, *rows, rule),
                };
                if let Err(e) = taken {
                    gnitz_fatal_abort!("worker: {ingest:?} failed: {e} — aborting for restart");
                }
                gnitz_debug!("{ingest:?} rows={n}");
            }
            Inbound::Apply { apply } => {
                if in_drive {
                    gnitz_fatal_abort!("{apply:?} inside an exchange wait — diverged from the master");
                }
                let ran = match apply {
                    Apply::Flush => self.catalog.registry.checkpoint_base(),
                    Apply::FlushEph { generation } => self.catalog.flush_ephemeral_round(generation),
                    Apply::Tick { first_round, ref tids } => (0..tids.len()).try_for_each(|i| {
                        self.seals_ahead = i + 1 < tids.len();
                        crate::query::tick(self, tids[i], first_round + i as u64)
                    }),
                    Apply::Backfill { ref views } => crate::query::backfill(self, views),
                };
                if let Err(e) = ran {
                    gnitz_fatal_abort!("worker: {apply:?} failed: {e} — aborting for restart");
                }
                self.replies.applied(self.sal_reader.position(), |read| {
                    answer(self.catalog, &mut self.snapshots, read)
                });
            }
        }
        true
    }
}

impl DriveHost for WorkerProcess<'_> {
    fn parts(&mut self) -> (&mut DagEngine, &mut RelationRegistry) {
        let cat = &mut *self.catalog;
        (&mut cat.dag, &mut cat.registry)
    }

    /// Publish `lanes` as this worker's rounds — one, or as few as hold them —
    /// and wait out every part of each, serving the SAL and sending owed reply
    /// frames meanwhile.
    fn exchange<'b>(
        &mut self,
        mut lanes: impl ExactSizeIterator<Item = Lane<'b>>,
        drained: bool,
        gathered: &mut Vec<Batch>,
    ) -> bool {
        let (max, mut all) = (self.mesh.max_lanes(), true);
        while lanes.len() > 0 {
            let mut round = self.mesh.open(lanes.by_ref().take(max), drained);
            all &= loop {
                self.turn();
                if let Some(drained) = self.mesh.step(&mut round, gathered) {
                    break drained;
                }
            };
        }
        all
    }
}

/// Answer `read`. A function of the catalog and this worker's pins: it cannot
/// reach the mesh, so answering a read never enters an exchange wait.
fn answer(
    cat: &mut CatalogEngine,
    snapshots: &mut FxHashMap<u64, Snapshot>,
    read: &Read<'_>,
) -> Result<Rows, WireFault> {
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
            Ok(Rows::Nothing)
        }
        // A worker holds no system family a snapshot reads: the master answers
        // those from its own pin.
        // The master hands out each id once a boot.
        Read::Pin { snapshot } => {
            let prior = snapshots.insert(snapshot, cat.registry.pin());
            debug_assert!(prior.is_none(), "snapshot {snapshot} pinned twice");
            Ok(Rows::Nothing)
        }
        Read::Unpin { snapshot } => {
            snapshots.remove(&snapshot);
            Ok(Rows::Nothing)
        }
        Read::SnapshotPage { snapshot, tid, page } => {
            let page = gnitz_wire::snapshot::PageRequest::decode(page)?;
            let pin = snapshots
                .get(&snapshot)
                .ok_or_else(|| format!("snapshot {snapshot} is not pinned"))?;
            Ok(Rows::from(pin.page(tid, &page)?))
        }
        Read::Backup { dir } => {
            cat.registry.link_published_into(dir)?;
            Ok(Rows::Nothing)
        }
    }
}

#[cfg(test)]
#[path = "tests/worker.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/worker.rs"]
mod bench;
