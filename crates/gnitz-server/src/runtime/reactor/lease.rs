//! W2M ingest: every ring drained into the routes of [`AckLease`] and
//! [`TrainLease`].

use std::collections::VecDeque;
use std::ops::Range;

use gnitz_wire::WireFault;
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;

use super::*;
use crate::runtime::park::WorkerSet;
use crate::runtime::w2m::{RequestId, W2mSlot};

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// Every live lease's request id and what its workers have answered on it.
pub(super) struct Routes {
    map: RefCell<FxHashMap<RequestId, Route>>,
    /// The next request id a lease starts from.
    next_id: Cell<RequestId>,
}

impl Routes {
    pub(super) fn new() -> Self {
        Routes {
            map: RefCell::new(FxHashMap::default()),
            next_id: Cell::new(RequestId::BOOT_READY.next()),
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
/// dropped, unless it was copied out of the ring when routed.
struct TrainRoute {
    /// The workers the scan was written to.
    set: WorkerSet,
    /// One queue per worker of `set`, ascending.
    queues: Box<[VecDeque<W2mSlot>]>,
    /// The lease's one reader while it waits: the position of the queue it
    /// waits on, and its waker. Each wait replaces the last, so a dropped wait
    /// stops counting once the reader waits again.
    reader: Option<(usize, Waker)>,
}

/// What a live lease's request id routes to.
enum Route {
    Acks(AckRoute),
    Train(TrainRoute),
}

impl Reactor {
    /// Route every unread ACK and slot of every worker's rings.
    pub(super) fn drain_all_w2m(&self) {
        for w in 0..self.w2m.num_workers() {
            self.w2m.drain_acks(w, |id| self.route_ack(w, id));
            while let Some(slot) = self.w2m.try_read_slot(w) {
                self.route(slot);
            }
        }
    }

    /// Count worker `w`'s ACK of `id` towards the lease it names.
    fn route_ack(&self, w: usize, id: RequestId) {
        match self.routes.map.borrow_mut().get_mut(&id) {
            Some(Route::Acks(r)) => {
                debug_assert!(
                    r.left.contains(w),
                    "worker {w} ACKed a request it was not sent, or ACKed it twice"
                );
                r.left = r.left.without(w);
                if r.left == WorkerSet::EMPTY {
                    if let Some(waker) = r.waker.take() {
                        waker.wake();
                    }
                }
            }
            Some(Route::Train(_)) => debug_assert!(false, "worker {w} ACKed a read"),
            // An abandoned request's late ACK.
            None => {}
        }
    }

    /// Hand a slot to the lease its ring id names, on behalf of the worker whose
    /// ring it was read from.
    ///
    /// A ring frees only a released prefix, so a slot held in place holds up
    /// every slot behind it. A worker sends a lease's frames, and the leases of
    /// one cut, in SAL order, and a reader takes the leases it reads in that
    /// order, so a slot it holds is one the earliest reader takes in turn. The
    /// one slot out of that order is a passing reply, which the worker sends ahead
    /// of an earlier lease's owed frames: it is copied out here unless its reader
    /// waits on its worker, which takes it now. So a reader waits on a ring only
    /// behind slots of leases written before its own.
    fn route(&self, slot: W2mSlot) {
        let w = slot.worker as usize;
        match self.routes.map.borrow_mut().get_mut(&slot.request_id) {
            Some(Route::Train(t)) => match t.set.position(w) {
                Some(at) => {
                    let reader = t.reader.take_if(|(r, _)| *r == at);
                    t.queues[at].push_back(match slot.passed() && reader.is_none() {
                        true => slot.detach(),
                        false => slot,
                    });
                    if let Some((_, waker)) = reader {
                        waker.wake();
                    }
                }
                // Dropped here, releasing its ring space.
                None => debug_assert!(false, "worker {w} answered a read it was not sent"),
            },
            Some(Route::Acks(_)) => debug_assert!(false, "worker {w} answered an ACKed request with a frame"),
            // No lease takes it: it drops here undecoded, releasing its ring space.
            None => {}
        }
    }

    /// A request id no live lease holds.
    fn alloc_request_id(&self) -> RequestId {
        let routes = self.routes.map.borrow();
        let mut id = self.routes.next_id.get();
        while routes.contains_key(&id) {
            id = id.next();
        }
        self.routes.next_id.set(id.next());
        id
    }

    /// `id`, routed to what `route` builds over `set` clipped to the launched
    /// workers.
    fn lease(&self, id: RequestId, set: WorkerSet, route: impl FnOnce(WorkerSet) -> Route) -> Lease {
        let workers = set.within(self.w2m.num_workers());
        self.routes.map.borrow_mut().insert(id, route(workers));
        Lease { reactor: self.clone(), id, set: workers }
    }

    /// `id`, answered by one ACK from every worker in `set`.
    fn lease_acks_on(&self, id: RequestId, set: WorkerSet) -> AckLease {
        AckLease(self.lease(id, set, |left| Route::Acks(AckRoute { left, waker: None })))
    }

    /// One request id, answered by one ACK from every worker in `set`. See
    /// [`AckLease`].
    pub(crate) fn lease_acks(&self, set: WorkerSet) -> AckLease {
        self.lease_acks_on(self.alloc_request_id(), set)
    }

    /// The lease every worker's ready ACK answers; [`Reactor::new`] takes it.
    pub(super) fn ready_lease(&self) -> ReadyLease {
        ReadyLease(self.lease_acks_on(RequestId::BOOT_READY, WorkerSet::ALL))
    }

    /// One request id, answered by a train of frames from every worker in `set`.
    /// `what` names the request in every fault the lease reports. See
    /// [`TrainLease`].
    pub(crate) fn lease_train(&self, set: WorkerSet, what: &'static str) -> TrainLease {
        let lease = self.lease(self.alloc_request_id(), set, |set| {
            let queues = (0..set.len()).map(|_| VecDeque::new()).collect();
            Route::Train(TrainRoute { set, queues, reader: None })
        });
        let left = Cell::new(lease.set);
        TrainLease { lease, left, what }
    }
}

// ---------------------------------------------------------------------------
// Lease
// ---------------------------------------------------------------------------

/// The workers a group is addressed to and the request id they answer on.
#[derive(Clone, Copy)]
pub(crate) struct GroupTargets {
    pub(crate) set: WorkerSet,
    pub(crate) reply: Option<RequestId>,
}

impl GroupTargets {
    /// Every worker, nothing answers.
    pub(crate) const SILENT: Self = GroupTargets { set: WorkerSet::ALL, reply: None };
}

/// A live route. Dropping it removes the route, and with it whatever the route
/// still holds.
struct Lease {
    reactor: Reactor,
    id: RequestId,
    /// The set the route was built over.
    set: WorkerSet,
}

impl Lease {
    fn targets(&self) -> GroupTargets {
        GroupTargets { set: self.set, reply: Some(self.id) }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let route = self.reactor.routes.map.borrow_mut().remove(&self.id);
        // Matched so an ACK route, which every round drops, drops inline rather
        // than through the route's drop glue.
        match route {
            Some(Route::Acks(acks)) => drop(acks),
            other => drop(other),
        }
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
        self.0.targets()
    }

    /// Resolves once every worker of the set has ACKed. A worker that cannot
    /// apply what it is to ACK exits instead, which the master's death watch sees.
    pub(crate) async fn acks(&self) {
        std::future::poll_fn(|cx| {
            let mut routes = self.0.reactor.routes.map.borrow_mut();
            let Some(Route::Acks(r)) = routes.get_mut(&self.0.id) else {
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

/// The lease every worker's ready ACK answers, which [`Reactor::new`] hands out.
pub(crate) struct ReadyLease(AckLease);

impl ReadyLease {
    /// Resolves once every worker has ACKed its boot recovery.
    pub(crate) async fn all_acked(self) -> ReadyAcked {
        self.0.acks().await;
        ReadyAcked(())
    }
}

/// Every worker has ACKed its boot recovery, which flushes the rows it replayed
/// from the SAL tail to its stores. Until then the tail is those rows' only
/// durable copy, so only this token lets a writer rewind the SAL at boot.
pub(crate) struct ReadyAcked(());

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

/// One frame of a train that carries rows.
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
        self.lease.targets()
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
        Ok(Some((slot, ctrl.data)))
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
            let Some(Route::Train(route)) = routes.get_mut(&self.lease.id) else {
                unreachable!("a train lease's id routes to its train")
            };
            let at = route.set.position(w).expect("the train was written to `w`");
            match route.queues[at].pop_front() {
                Some(slot) => Poll::Ready(slot),
                None => {
                    route.reader = Some((at, cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
        .await
    }
}

#[cfg(test)]
#[path = "tests/lease.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/lease.rs"]
mod bench;
