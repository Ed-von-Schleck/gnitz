//! W2M ingest: every ring drained into the routes of [`AckLease`] and
//! [`TrainLease`].

use std::ops::Range;

use gnitz_wire::WireFault;
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::*;
use crate::runtime::park::WorkerSet;
use crate::runtime::sal::GroupTargets;
use crate::runtime::sync::WakeQueue;
use crate::runtime::w2m::{W2mSlot, BOOT_READY_REQUEST_ID, NO_REPLY_REQUEST_ID};

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// Every live lease's request id and what its workers have answered on it.
pub(super) struct Routes {
    map: RefCell<FxHashMap<u32, Route>>,
    /// The next request id a lease starts from.
    next_id: Cell<u32>,
}

impl Routes {
    pub(super) fn new() -> Self {
        Routes {
            map: RefCell::new(FxHashMap::default()),
            next_id: Cell::new(BOOT_READY_REQUEST_ID + 1),
        }
    }
}

/// What the workers have answered on one [`AckLease`] id.
struct AckRoute {
    /// The workers yet to answer.
    left: WorkerSet,
    /// The `acks` awaiter parked on this id.
    waker: Option<Waker>,
}

/// The undecoded frames of one [`TrainLease`]; each pins its ring space until
/// dropped.
struct TrainRoute {
    /// The workers the scan was written to.
    set: WorkerSet,
    /// One queue per worker of `set`, ascending.
    queues: Box<[WakeQueue<W2mSlot>]>,
}

impl TrainRoute {
    /// Worker `w`'s frame queue, `None` when the scan was not written to `w`.
    fn queue(&mut self, w: usize) -> Option<&mut WakeQueue<W2mSlot>> {
        self.set.position(w).map(|i| &mut self.queues[i])
    }
}

/// What a live lease's request id routes to.
enum Route {
    Acks(AckRoute),
    Train(TrainRoute),
}

impl Reactor {
    /// Route every unread slot of every worker's ring.
    pub(super) fn drain_all_w2m(&self) {
        for w in 0..self.w2m.num_workers() {
            while let Some(slot) = self.w2m.try_read_slot(w) {
                self.route(slot);
            }
        }
    }

    /// Hand a slot to the lease its ring id names, on behalf of the worker whose
    /// ring it was read from.
    fn route(&self, slot: W2mSlot) {
        let w = slot.worker as usize;
        match self.routes.map.borrow_mut().get_mut(&slot.request_id) {
            Some(Route::Train(t)) => match t.queue(w) {
                Some(q) => q.push(slot),
                // Dropped here, releasing its ring space.
                None => debug_assert!(false, "worker {w} answered a read it was not sent"),
            },
            Some(Route::Acks(r)) => {
                debug_assert!(
                    r.left.contains(w),
                    "worker {w} answered a request it was not sent, or answered it twice"
                );
                r.left = r.left.without(w);
                if r.left == WorkerSet::EMPTY {
                    if let Some(waker) = r.waker.take() {
                        waker.wake();
                    }
                }
            }
            // No lease takes it: it drops here undecoded, releasing its ring space.
            None => {}
        }
    }

    /// A request id no live lease holds, never [`NO_REPLY_REQUEST_ID`].
    fn alloc_request_id(&self) -> u32 {
        let routes = self.routes.map.borrow();
        let mut id = self.routes.next_id.get();
        while id == NO_REPLY_REQUEST_ID || routes.contains_key(&id) {
            id = id.wrapping_add(1);
        }
        self.routes.next_id.set(id.wrapping_add(1));
        id
    }

    /// `id`, routed to what `route` builds over `set` clipped to the launched
    /// workers.
    fn lease(&self, id: u32, set: WorkerSet, route: impl FnOnce(WorkerSet) -> Route) -> Lease {
        let workers = set.within(self.w2m.num_workers());
        self.routes.map.borrow_mut().insert(id, route(workers));
        Lease {
            reactor: self.clone(),
            targets: GroupTargets { set: workers, request_id: id },
        }
    }

    /// `id`, answered by one ACK from every worker in `set`.
    fn lease_acks_on(&self, id: u32, set: WorkerSet) -> AckLease {
        AckLease(self.lease(id, set, |left| Route::Acks(AckRoute { left, waker: None })))
    }

    /// One request id, answered by one ACK from every worker in `set`. See
    /// [`AckLease`].
    pub(crate) fn lease_acks(&self, set: WorkerSet) -> AckLease {
        self.lease_acks_on(self.alloc_request_id(), set)
    }

    /// The lease every worker's ready ACK answers.
    pub(crate) fn lease_ready(&self) -> AckLease {
        self.lease_acks_on(BOOT_READY_REQUEST_ID, WorkerSet::ALL)
    }

    /// One request id, answered by a train of frames from every worker in `set`.
    /// `what` names the request in every fault the lease reports. See
    /// [`TrainLease`].
    pub(crate) fn lease_train(&self, set: WorkerSet, what: &'static str) -> TrainLease {
        let lease = self.lease(self.alloc_request_id(), set, |set| {
            let queues = (0..set.len()).map(|_| WakeQueue::default()).collect();
            Route::Train(TrainRoute { set, queues })
        });
        let left = Cell::new(lease.targets.set);
        TrainLease { lease, left, what }
    }
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

/// A live route. Dropping it removes the route, and with it whatever the route
/// still holds.
struct Lease {
    reactor: Reactor,
    /// The route's request id and the set it was built over.
    targets: GroupTargets,
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.reactor.routes.map.borrow_mut().remove(&self.targets.request_id);
    }
}

// ---------------------------------------------------------------------------
// AckLease
// ---------------------------------------------------------------------------

/// One request id, answered by one ACK from every worker of its set, routed
/// while it lives, so an ACK beating its awaiter is kept.
pub(crate) struct AckLease(Lease);

impl AckLease {
    /// The targets of the group this lease's workers answer.
    pub(crate) fn targets(&self) -> GroupTargets {
        self.0.targets
    }

    /// Resolves once every worker of the set has ACKed. A worker that cannot
    /// apply what it is to ACK exits instead, which the master's death watch sees.
    pub(crate) async fn acks(&self) {
        std::future::poll_fn(|cx| {
            let mut routes = self.0.reactor.routes.map.borrow_mut();
            let Some(Route::Acks(r)) = routes.get_mut(&self.0.targets.request_id) else {
                unreachable!("an ACK lease's id routes to its ACKs")
            };
            if r.left == WorkerSet::EMPTY {
                return Poll::Ready(());
            }
            r.waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

// ---------------------------------------------------------------------------
// TrainLease
// ---------------------------------------------------------------------------

/// One request id, answered by a train of frames from each worker of its set,
/// routed while it lives. Dropping it releases every frame the route holds.
pub(crate) struct TrainLease {
    lease: Lease,
    /// The workers whose terminal frame has not been read.
    left: Cell<WorkerSet>,
    /// What the lease was written for, named in every fault it reports.
    what: &'static str,
}

/// One frame of a train that carries rows. `slot` pins its ring bytes.
pub(crate) struct TrainFrame {
    pub(crate) slot: W2mSlot,
    data: Range<usize>,
}

impl TrainFrame {
    /// The frame's rows under `schema`, aborting on failure: the ring is trusted.
    pub(crate) fn rows<'a>(&'a self, schema: &'a SchemaDescriptor) -> MemBatch<'a> {
        match MemBatch::of_wal_block(&self.slot.bytes()[self.data.clone()], schema) {
            Ok(block) => block,
            Err(e) => gnitz_fatal_abort!(
                "w2m: worker={} train frame does not decode under the reader's schema: {e}",
                self.slot.worker
            ),
        }
    }
}

impl TrainLease {
    /// The targets of the group this lease's workers answer.
    pub(crate) fn targets(&self) -> GroupTargets {
        self.lease.targets
    }

    /// Worker `w`'s next frame, row-less ones included, with the range of its
    /// rows when it carries any; `None` once its train has ended. The terminal
    /// frame is the one carrying `scan_last`, and is handed out like the rest.
    /// `Err` on a fault frame.
    pub(crate) async fn frame_of(&self, w: usize) -> Result<Option<(W2mSlot, Option<Range<usize>>)>, WireFault> {
        if !self.left.get().contains(w) {
            return Ok(None);
        }
        let slot = self.next_slot(w).await;
        let ctrl = slot.control();
        if let Some(f) = ctrl.fault(slot.bytes()) {
            return Err(f.in_context(format_args!("worker {w}: {}", self.what)));
        }
        debug_assert!(
            ctrl.schema.is_none(),
            "worker {w}: a reply frame carries a schema block"
        );
        if ctrl.hdr.flags.scan_last {
            self.left.set(self.left.get().without(w));
        }
        let data = ctrl.data;
        Ok(Some((slot, data)))
    }

    /// Worker `w`'s next frame that carries rows, `None` once its train has ended;
    /// `Err` on a fault frame.
    pub(crate) async fn next_of(&self, w: usize) -> Result<Option<TrainFrame>, WireFault> {
        while let Some((slot, data)) = self.frame_of(w).await? {
            if let Some(data) = data {
                return Ok(Some(TrainFrame { slot, data }));
            }
        }
        Ok(None)
    }

    /// The next frame that carries rows, workers in ascending order.
    pub(crate) async fn next(&self) -> Result<Option<TrainFrame>, WireFault> {
        while let Some(w) = self.left.get().iter().next() {
            if let Some(f) = self.next_of(w).await? {
                return Ok(Some(f));
            }
        }
        Ok(None)
    }

    /// Wait out every worker's train, for a request answered with no rows; the
    /// first fault.
    pub(crate) async fn finish(&self) -> Result<(), WireFault> {
        while self.next().await?.is_some() {}
        Ok(())
    }

    /// Worker `w`'s next routed slot.
    async fn next_slot(&self, w: usize) -> W2mSlot {
        std::future::poll_fn(|cx| {
            let mut routes = self.lease.reactor.routes.map.borrow_mut();
            let Some(Route::Train(route)) = routes.get_mut(&self.lease.targets.request_id) else {
                unreachable!("a train lease's id routes to its train")
            };
            route
                .queue(w)
                .expect("the train was written to `w`")
                .poll(cx)
                .map(|v| v.expect("a train queue never closes"))
        })
        .await
    }
}

#[cfg(test)]
#[path = "tests/lease.rs"]
mod tests;
