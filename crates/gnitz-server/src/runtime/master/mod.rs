//! Master-side dispatcher: the master's catalog, SAL writer and reactor as one
//! value. Every group a worker answers is written here, under the lease the
//! reactor routes its W2M replies to: a read on a train lease, an apply on an
//! ACK lease. A push group and a catalog zone, which no worker answers, are laid
//! out here in a hold or scope of the caller's. The child `route` decides which
//! workers a read or push reaches; `dispatch` holds the writes and the tick
//! rounds they allocate; `workers` holds the worker processes.

pub(crate) mod route;

mod dispatch;
mod workers;

/// A dispatcher with no processes behind it, for the tests and benchmarks that
/// need one.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

use std::cell::{Cell, Ref, RefCell, RefMut};

use rustc_hash::FxHashMap;

use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::{Read, SalExcl, SalWriter};
use gnitz_wire::WireFault;

pub(crate) use workers::Workers;

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

pub(crate) struct MasterDispatcher {
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Reactor,
    catalog: RefCell<CatalogEngine>,
    /// The last tick round allocated, one per tid a tick group carries. Starts
    /// at 1, which is never emitted, so `after_tick = 0` names no round. Grows
    /// under the SAL hold its tick groups are written under.
    round: Cell<u64>,
    /// Ticked relation id → the last round that ticked it.
    last_tick: RefCell<FxHashMap<u64, u64>>,
}

impl MasterDispatcher {
    pub(crate) fn new(catalog: CatalogEngine, sal: SalWriter, reactor: Reactor) -> Self {
        MasterDispatcher {
            sal,
            reactor,
            catalog: RefCell::new(catalog),
            round: Cell::new(1),
            last_tick: RefCell::default(),
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

    /// The scan-shaped requests `build` writes as one
    /// [`Cut`](crate::runtime::sal::Cut); `build` keeps the lease each
    /// [`ScanCut::read`] hands it.
    pub(crate) async fn scan_cut<R>(
        &self,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<R, WireFault>,
    ) -> Result<R, WireFault> {
        let mut cut = ScanCut { disp: self, excl: self.sal.lock().await };
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
        let route = route::route_read(&read, &self.disp.cat().registry, self.disp.sal.num_workers());
        let lease = self.disp.reactor.lease_train(route.set, read.name());
        debug_assert!(lease.workers().len() > 0, "every read owes at least one reply");
        self.excl
            .read(read, lease.workers(), route.per_worker.as_deref(), lease.id())?;
        Ok(lease)
    }
}
