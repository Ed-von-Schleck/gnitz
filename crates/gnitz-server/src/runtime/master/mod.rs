//! Master-side dispatcher: fans out push/scan operations to worker processes
//! via the shared append-only log (SAL) and collects responses via per-worker
//! W2M regions.

pub(crate) mod route;

mod dispatch;
mod fk_presence;
mod preflight;
mod unique_filter;
mod unique_preflight;

/// The inert dispatcher the `master` submodules' unit tests construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
mod fixtures;

use std::cell::{Cell, RefCell};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::catalog::CatalogEngine;
use crate::runtime::park::WorkerSet;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::{GroupData, Read, SalExcl, SalGroup, SalWriter};
use gnitz_store::read::Probe;
use gnitz_wire::{PkColList, WireFault};
use gnitz_zset::repr::Batch;

pub(crate) use dispatch::WORKER_WATCH;
pub(crate) use preflight::TxnFamily;
pub(crate) use unique_filter::UniqueFilter;

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    worker_pids: Vec<i32>,
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Reactor,
    /// Dereferenced by [`MasterDispatcher::cat`] and [`MasterDispatcher::cat_mut`] alone.
    catalog: *mut CatalogEngine,
    /// The filter of each unique index that has one, by (table_id, column
    /// list). Every entry holds all of its index's spans.
    unique_filters: RefCell<FxHashMap<(u64, PkColList), UniqueFilter>>,

    /// The presence cache of each table an FK probe found a key of, by table id.
    fk_presence: RefCell<FxHashMap<u64, fk_presence::FkPresence>>,
    /// Keys one presence cache holds before it is dropped whole.
    /// `GNITZ_FK_PRESENCE_KEYS` lowers it, so tests reach it on small tables.
    fk_presence_cap: usize,

    /// The tick ledger: what the next tick owes, what the last one covered, and
    /// the rounds it allocated.
    ledger: TickLedger,

    /// Bytes of rows one write's cascade may delete, at each row's fixed width:
    /// what a client's own delete of them could carry in one frame.
    /// `GNITZ_CASCADE_BYTES` lowers it, so tests reach it on small tables.
    cascade_bytes: usize,
}

/// The tick ledger of a [`MasterDispatcher`]. `pending`, `round` and
/// `last_delta_round` grow under the SAL hold their tick or commit groups are
/// written under; a dropped relation's entries are removed under the catalog
/// write guard.
struct TickLedger {
    /// Tables with a pending delta, each with the row count feeding the
    /// coalesce threshold.
    pending: RefCell<FxHashMap<u64, usize>>,
    /// The pending rows of one table that fire an auto-tick.
    /// `GNITZ_TICK_COALESCE_ROWS` lowers it, so tests reach it on small tables.
    coalesce_rows: usize,
    /// The SAL watermark the last emitted tick snapshotted: every commit at or
    /// below it is reflected in every view, as a read written to the SAL from
    /// now on sees it. 0 until [`MasterDispatcher::seed_tick_lsn`] seeds it.
    last_lsn: Cell<u64>,
    /// The last tick round allocated, one per tid a tick group carries. Starts at 1, which is
    /// never emitted, so `after_tick = 0` names no round.
    round: Cell<u64>,
    /// Feed-enabled view id → the last round that reached it, absent reading as 1.
    /// May name a round that left the view no rows; never misses one that did.
    last_delta_round: RefCell<FxHashMap<u64, u64>>,
}

/// An integrity-violation refusal: a write, or an index build, that a PK,
/// unique-index or FK constraint forbids.
fn integrity_violation(text: String) -> WireFault {
    WireFault {
        status: gnitz_wire::WireStatus::IntegrityViolation,
        text,
    }
}

/// Scan-shaped requests written under one SAL hold: one cut, whose replies each
/// worker sends in request order.
pub(crate) struct ScanCut<'d> {
    disp: &'d MasterDispatcher,
    excl: SalExcl<'d>,
    scans: Vec<TrainLease>,
}

impl<'d> ScanCut<'d> {
    /// `read` written to the workers its target and bound reach, each sent its
    /// own key set when the bound splits one, under a reply id leased for them.
    pub(crate) fn read(&mut self, read: Read<'_>) -> Result<(), WireFault> {
        let route = route::route_read(&read, &self.disp.cat().registry, self.disp.num_workers());
        let group = SalGroup::new(read);
        let data = route.per_worker.as_deref().map_or(group.data, GroupData::Blobs);
        let lease = self
            .disp
            .reactor
            .lease_train(route.set, group.request.encode().0.variant_name());
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        self.excl.write(&SalGroup { data, targets: lease.targets(), ..group })?;
        self.scans.push(lease);
        Ok(())
    }
}

impl MasterDispatcher {
    /// Bring every cache derived from `tid`'s committed rows up to a `batch` of
    /// it that is now durable.
    pub(crate) fn committed(&self, tid: u64, batch: &Batch) {
        self.unique_filter_ingest_batch(tid, batch);
        self.fk_presence_ingest_batch(tid, batch);
    }

    /// Drop everything held for a dropped relation. Ids are never reused, so
    /// nothing else would ever reclaim it.
    pub(crate) fn forget_relation(&self, id: u64) {
        self.ledger.pending.borrow_mut().remove(&id);
        self.ledger.last_delta_round.borrow_mut().remove(&id);
        self.unique_filter_invalidate_table(id);
        self.fk_presence_invalidate_table(id);
    }

    /// The scan-shaped requests `build` writes under one SAL hold, one lease
    /// each; each worker replies in request order.
    pub(crate) async fn scan_cut(
        &self,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<(), WireFault>,
    ) -> Result<Vec<TrainLease>, WireFault> {
        let mut cut = ScanCut {
            disp: self,
            excl: self.sal.lock().await,
            scans: Vec::new(),
        };
        build(&mut cut)?;
        Ok(cut.scans)
    }

    /// One cut of delta reads, each given as `(view, after_tick, spec,
    /// reply_layout)`: a read of `view`'s deltas after `after_tick`. Each lease
    /// is paired with the round its read reaches, the view's last when the read
    /// was written. No reads take no SAL hold.
    pub(crate) async fn delta_cut<'s>(
        &self,
        reads: impl IntoIterator<Item = (u64, u64, &'s [u8], u64)>,
    ) -> Result<Vec<(TrainLease, u64)>, WireFault> {
        let mut reads = reads.into_iter().peekable();
        if reads.peek().is_none() {
            return Ok(Vec::new());
        }
        let mut reached = Vec::with_capacity(reads.size_hint().0);
        let leases = self
            .scan_cut(|cut| {
                for (view, after_tick, spec, reply_layout) in reads {
                    cut.read(Read::Delta {
                        view,
                        after_tick,
                        reply_layout,
                        spec: spec.into(),
                    })?;
                    reached.push(self.last_delta_round(view));
                }
                Ok(())
            })
            .await?;
        Ok(leases.into_iter().zip(reached).collect())
    }

    /// One scan-shaped read under its own SAL hold.
    pub(crate) async fn scan(&self, read: Read<'_>) -> Result<TrainLease, WireFault> {
        let mut leases = self.scan_cut(|cut| cut.read(read)).await?;
        Ok(leases.pop().expect("one read, one lease"))
    }

    /// `read`, answered with no rows by every worker it reaches, under its own
    /// SAL hold; the first worker fault.
    pub(crate) async fn command(&self, read: Read<'_>) -> Result<(), WireFault> {
        self.scan(read).await?.finish().await
    }
}
