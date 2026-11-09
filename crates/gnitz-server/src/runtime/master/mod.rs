//! Master-side dispatcher: the master's handle on its workers. It writes every
//! master→worker request to the shared append-only log (SAL) as a group under a
//! reply lease from the reactor, which routes the workers' W2M replies to that
//! lease. The child `route` decides which workers a read or push reaches;
//! `dispatch` holds its writes, the tick rounds they allocate and worker reaping.

pub(crate) mod route;

mod dispatch;

/// The inert dispatcher the unit tests of the `master` submodules and of the
/// executor's write validation construct.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

use std::cell::{Cell, Ref, RefCell, RefMut};
use std::num::NonZero;

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::{GroupData, GroupTargets, Read, SalExcl, SalGroup, SalWriter};
use gnitz_wire::WireFault;

pub(crate) use dispatch::worker_death;

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub struct MasterDispatcher {
    /// One per worker, or none for a dispatcher with no processes behind it.
    worker_pids: Vec<NonZero<libc::pid_t>>,
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Reactor,
    catalog: RefCell<CatalogEngine>,
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
    pub(crate) fn new(
        worker_pids: Vec<NonZero<libc::pid_t>>,
        catalog: CatalogEngine,
        sal: SalWriter,
        reactor: Reactor,
    ) -> Self {
        assert!(
            worker_pids.is_empty() || worker_pids.len() == sal.num_workers(),
            "the SAL is written at the worker count"
        );
        MasterDispatcher {
            worker_pids,
            sal,
            reactor,
            catalog: RefCell::new(catalog),
            rounds: TickRounds {
                round: Cell::new(1),
                last_delta_round: RefCell::new(FxHashMap::default()),
            },
        }
    }

    /// The catalog.
    pub(crate) fn cat(&self) -> Ref<'_, CatalogEngine> {
        self.catalog.borrow()
    }

    /// The catalog, exclusively.
    pub(crate) fn cat_mut(&self) -> RefMut<'_, CatalogEngine> {
        self.catalog.borrow_mut()
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

    /// Drop a dropped relation's last delta round.
    pub(crate) fn forget_relation(&self, id: u64) {
        self.rounds.last_delta_round.borrow_mut().remove(&id);
    }

    /// The scan-shaped requests `build` writes as one
    /// [`Cut`](crate::runtime::sal::Cut); `build` keeps the lease each
    /// [`ScanCut::read`] hands it.
    pub(crate) async fn scan_cut<R>(
        &self,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<R, WireFault>,
    ) -> Result<R, WireFault> {
        let mut cut = ScanCut {
            disp: self,
            excl: self.sal.lock().await,
            written: false,
        };
        build(&mut cut)
    }

    /// One scan-shaped read under its own SAL hold.
    pub(crate) async fn scan(&self, read: Read<'_>) -> Result<TrainLease, WireFault> {
        self.scan_cut(|cut| cut.read(read)).await
    }

    /// `read`, answered with no rows by every worker it reaches, under its own
    /// SAL hold; the first worker fault.
    pub(crate) async fn command(&self, read: Read<'_>) -> Result<(), WireFault> {
        self.scan(read).await?.finish().await
    }
}

/// Scan-shaped requests written under one SAL hold: one
/// [`Cut`](crate::runtime::sal::Cut).
pub(crate) struct ScanCut<'d> {
    disp: &'d MasterDispatcher,
    excl: SalExcl<'d>,
    /// Whether the cut's first group has been written; the rest join it.
    written: bool,
}

impl<'d> ScanCut<'d> {
    /// The SAL hold the cut's groups are written under.
    pub(crate) fn sal(&self) -> &SalExcl<'d> {
        &self.excl
    }

    /// `read` written to the workers its target and bound reach, each sent its
    /// own key set when the bound splits one, under a reply id leased for them;
    /// the lease its replies are read through.
    pub(crate) fn read(&mut self, read: Read<'_>) -> Result<TrainLease, WireFault> {
        let route = route::route_read(&read, &self.disp.cat().registry, self.disp.num_workers());
        let group = SalGroup::new(read);
        let data = route.per_worker.as_deref().map_or(group.data, GroupData::Blobs);
        let lease = self
            .disp
            .reactor
            .lease_train(route.set, group.request.encode().0.variant_name());
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        let targets = GroupTargets {
            set: lease.workers(),
            reply: Some(lease.id()),
        };
        let group = SalGroup { data, targets, ..group };
        if self.written {
            self.excl.join(&group)?;
        } else {
            self.excl.write(&group)?;
            self.written = true;
        }
        Ok(lease)
    }
}
