//! Master-side dispatcher: the master's catalog, SAL writer and reactor as one
//! value. Every group a worker answers is written here, with what the reactor
//! reads the answer through: a read under the train lease its W2M replies are
//! routed to, an apply — under the apply lock — with a wait on the position
//! the workers apply the log through. A catalog zone, which no worker answers,
//! is laid out here in a hold of the caller's. The child `route` decides which
//! workers a read or push reaches; `dispatch` holds the writes and the tick
//! ledger; `workers` holds the worker processes.

pub(crate) mod route;

mod dispatch;
mod workers;

/// A dispatcher with no processes behind it, for the tests and benchmarks that
/// need one.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

use std::cell::{Ref, RefCell, RefMut};

use crate::catalog::CatalogEngine;
use crate::runtime::reactor::{Reactor, TrainLease};
use crate::runtime::sal::{Read, SalExcl, SalWriter};
use crate::runtime::sync::{AsyncRwLock, WriteFuture};
use gnitz_store::relation::RelationKind;
use gnitz_wire::WireFault;

pub(crate) use workers::Workers;

// ---------------------------------------------------------------------------
// MasterDispatcher
// ---------------------------------------------------------------------------

/// The tag of the apply lock: a guard of it is held by the one task that may
/// write an apply, from the write through its ACKs.
pub(crate) enum ApplyLock {}

pub(crate) struct MasterDispatcher {
    sal: SalWriter,
    /// The master's one event loop, which reads every worker's replies.
    reactor: Reactor,
    catalog: RefCell<CatalogEngine>,
    ticks: dispatch::TickLedger,
    /// Taken by whoever writes an apply — a tick, a checkpoint's rounds, a
    /// backfill — and by a DDL body, whose zone every worker ingests. A worker
    /// inside the exchange wait of a `Tick` or `Backfill` fail-stops on either,
    /// so a holder awaits an apply's ACKs before it writes another apply or a
    /// zone, or releases the lock.
    applies: AsyncRwLock<ApplyLock>,
}

impl MasterDispatcher {
    pub(crate) fn new(catalog: CatalogEngine, sal: SalWriter, reactor: Reactor) -> Self {
        MasterDispatcher {
            ticks: dispatch::TickLedger::new(sal.watermark()),
            sal,
            reactor,
            catalog: RefCell::new(catalog),
            applies: AsyncRwLock::default(),
        }
    }

    /// The apply lock, exclusively, once no other task holds it.
    pub(crate) fn apply_lock(&self) -> WriteFuture<ApplyLock> {
        self.applies.write()
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
    pub(crate) fn reactor(&self) -> Reactor {
        self.reactor
    }

    /// The scan-shaped requests `build` writes under one SAL hold: the one
    /// lease their replies are read through, a train per read in the order
    /// written.
    pub(crate) async fn scan_cut(
        &self,
        build: impl FnOnce(&mut ScanCut<'_>) -> Result<(), WireFault>,
    ) -> Result<TrainLease, WireFault> {
        let mut cut = ScanCut {
            disp: self,
            excl: self.sal.lock().await,
            lease: self.reactor.lease_train(),
        };
        build(&mut cut)?;
        Ok(cut.lease)
    }

    /// One scan-shaped read under its own SAL hold.
    pub(crate) async fn scan(&self, read: Read<'_>) -> Result<TrainLease, WireFault> {
        self.scan_cut(|cut| cut.read(read)).await
    }

    /// `read`, answered with no rows by every worker it reaches, under its own
    /// SAL hold; the first worker fault.
    pub(crate) async fn command(&self, read: Read<'_>) -> Result<(), WireFault> {
        self.scan(read).await?.next_train().await.finish().await
    }
}

/// Scan-shaped requests written under one SAL hold, all answered on one lease:
/// every worker runs them at one point of the log, and answers them in the
/// order written.
pub(crate) struct ScanCut<'d> {
    disp: &'d MasterDispatcher,
    excl: SalExcl<'d>,
    /// The lease the cut's reads are answered on.
    lease: TrainLease,
}

impl<'d> ScanCut<'d> {
    /// `read` written to the workers its target and bound reach, each sent its
    /// own key set when the bound splits one, under the cut's reply id. A
    /// refused read is not one of the lease's.
    pub(crate) fn read(&mut self, read: Read<'_>) -> Result<(), WireFault> {
        let route = route::route_read(&read, &self.disp.cat().registry, self.disp.sal.num_workers());
        let what = read.name();
        self.excl
            .read(read, route.set, route.per_worker.as_deref(), self.lease.id())?;
        self.lease.add(route.set, what);
        Ok(())
    }

    /// The LSN at or below which every commit is reflected in a worker read of
    /// a `kind` relation written under this cut: every zone at or below the
    /// SAL watermark precedes the read's group in the log, and a view reflects
    /// its last tick.
    pub(crate) fn watermark(&self, kind: RelationKind) -> u64 {
        match kind {
            RelationKind::BaseTable => self.disp.sal.watermark(),
            RelationKind::View(_) => self.disp.last_lsn(),
            RelationKind::SystemCatalog | RelationKind::Stream => {
                unreachable!("a system family is read off the master, and a stream is never read")
            }
        }
    }
}
