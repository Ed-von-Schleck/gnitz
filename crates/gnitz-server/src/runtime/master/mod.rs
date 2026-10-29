//! Master-side dispatcher: the master's handle on its workers. It fans out
//! push/scan operations to worker processes via the shared append-only log
//! (SAL) and collects responses via per-worker W2M regions. The child `route`
//! decides which workers a read or push reaches; `dispatch` holds its writes,
//! the tick rounds they allocate and worker reaping.

pub(crate) mod route;

mod dispatch;

/// The inert dispatcher the unit tests of the `master` submodules and of the
/// executor's write validation construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

use std::cell::{Cell, RefCell};

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::park::WorkerSet;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::{GroupData, Read, SalExcl, SalGroup, SalWriter};
use gnitz_wire::WireFault;
use gnitz_zset::repr::Batch;

pub(crate) use dispatch::WORKER_WATCH;

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
    /// The tick rounds the dispatcher's tick groups allocated.
    rounds: TickRounds,
}

/// The tick rounds of a [`MasterDispatcher`]. `round` and `last_delta_round`
/// grow under the SAL hold their tick groups are written under; a dropped
/// relation's entries are removed under the catalog write guard.
struct TickRounds {
    /// The last tick round allocated, one per tid a tick group carries. Starts at 1, which is
    /// never emitted, so `after_tick = 0` names no round.
    round: Cell<u64>,
    /// Feed-enabled view id → the last round that reached it, absent reading as 1.
    /// May name a round that left the view no rows; never misses one that did.
    last_delta_round: RefCell<FxHashMap<u64, u64>>,
}

impl MasterDispatcher {
    pub(crate) fn new(worker_pids: Vec<i32>, catalog: *mut CatalogEngine, sal: SalWriter, reactor: Reactor) -> Self {
        debug_assert_eq!(
            sal.num_workers(),
            worker_pids.len(),
            "the SAL is written at the worker count"
        );
        MasterDispatcher {
            worker_pids,
            sal,
            reactor,
            catalog,
            rounds: TickRounds {
                round: Cell::new(1),
                last_delta_round: RefCell::new(FxHashMap::default()),
            },
        }
    }

    /// The catalog, which outlives the dispatcher.
    pub(crate) fn cat(&self) -> &CatalogEngine {
        unsafe { &*self.catalog }
    }

    /// The catalog, exclusively: no reference from this or [`Self::cat`] may be
    /// alive across a call.
    #[allow(clippy::mut_from_ref)]
    pub(crate) fn cat_mut(&self) -> &mut CatalogEngine {
        unsafe { &mut *self.catalog }
    }

    /// The SAL writer: its queries, and the [`SalExcl`] every write goes through.
    pub(crate) fn sal(&self) -> &SalWriter {
        &self.sal
    }

    /// The master's event loop.
    pub(crate) fn reactor(&self) -> &Reactor {
        &self.reactor
    }

    pub(crate) fn num_workers(&self) -> usize {
        self.sal.num_workers()
    }

    /// Drop a dropped relation's last delta round. Ids are never reused, so
    /// nothing else would ever reclaim it.
    pub(crate) fn forget_relation(&self, id: u64) {
        self.rounds.last_delta_round.borrow_mut().remove(&id);
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
        debug_assert!(lease.targets().set.len() > 0, "every read owes at least one reply");
        self.excl.write(&SalGroup { data, targets: lease.targets(), ..group })?;
        self.scans.push(lease);
        Ok(())
    }
}
