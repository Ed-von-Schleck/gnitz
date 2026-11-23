//! W2M ingest: every ring drained into the applied level an [`AckWait`] parks
//! on and the routes of [`TrainLease`].

use std::collections::VecDeque;
use std::ops::Range;

use gnitz_wire::WireFault;
use gnitz_zset::repr::MemBatch;
use gnitz_zset::schema::SchemaDescriptor;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use super::*;
use crate::runtime::park::WorkerSet;
use crate::runtime::w2m::{RequestId, W2mSlot};

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// Every live train lease's request id and what its workers have answered on it.
pub(super) struct Routes {
    trains: FxHashMap<RequestId, TrainRoute>,
    /// The next request id a lease starts from.
    next_id: RequestId,
    /// Emptied queue boxes of dropped train leases, taken by the next ones.
    spare: Vec<Box<[VecDeque<W2mSlot>]>>,
}

/// The most queue boxes [`Routes`] keeps for reuse.
const SPARE_QUEUES: usize = 64;

/// The largest capacity a queue keeps when its box goes back for reuse; a
/// queue grown past it under a slow reader is freed.
const SPARE_QUEUE_CAP: usize = 16;

impl Routes {
    pub(super) fn new() -> Self {
        Routes {
            trains: FxHashMap::default(),
            next_id: RequestId::MIN,
            spare: Vec::new(),
        }
    }

    /// A request id no live lease holds.
    fn fresh_id(&mut self) -> RequestId {
        let mut id = self.next_id;
        while self.trains.contains_key(&id) {
            id = id.next();
        }
        self.next_id = id.next();
        id
    }
}

/// The undecoded frames of one [`TrainLease`]; each pins its ring space until
/// dropped, unless it was copied out of the ring.
struct TrainRoute {
    /// The workers holding this lease's trains: their queued frames were copied
    /// out when their ring was given back, and each sends it nothing more until
    /// its reader has taken that worker's copies and waits on it for the next
    /// frame, or the lease drops.
    held: WorkerSet,
    /// One queue per launched worker, indexed by worker id.
    queues: Box<[VecDeque<W2mSlot>]>,
    /// The lease's one reader while it waits: the worker it waits on, and its
    /// waker. Each wait replaces the last, so a dropped wait stops counting
    /// once the reader waits again.
    reader: Option<(usize, Waker)>,
}

impl Reactor {
    /// Route every unread slot of every worker's ring, raise the applied level
    /// when a worker has moved its position, and give back each ring whose
    /// worker named a shortage of room.
    pub(super) fn drain_all_w2m(&self) {
        if let Some(at) = self.w2m.lowest_applied_if_moved() {
            self.applied.raise(at);
        }
        for w in 0..self.w2m.num_workers() {
            // Answered ahead of the reads: a worker names a shortage behind its
            // last publish, so the reads route every slot the give-back must reach.
            let short = self.w2m.answer_shortage(w);
            while let Some(slot) = self.w2m.try_read_slot(w) {
                self.route(slot);
            }
            if short {
                self.give_back(w);
            }
        }
    }

    /// Give worker `w`'s ring back (see `w2m`'s module header): copy every
    /// frame of it a lease holds queued out of the ring, holding each such
    /// lease's trains on `w`.
    #[cold]
    fn give_back(&self, w: usize) {
        for (&id, t) in self.routes.borrow_mut().trains.iter_mut() {
            let queue = &mut t.queues[w];
            if !queue.iter().any(W2mSlot::in_ring) {
                continue;
            }
            queue.iter_mut().for_each(W2mSlot::detach);
            // A worker holding as many leases as it can sends this one on, and
            // its frames are copied at the next shortage they cause.
            if !t.held.contains(w) && self.w2m.hold(w, id) {
                t.held = t.held.with(w);
            }
        }
    }

    /// Hand a slot to the lease its ring id names, on behalf of the worker whose
    /// ring it was read from.
    ///
    /// A ring frees only a released prefix, so a slot queued in place holds up
    /// every slot behind it until its reader takes it. A passing reply is one
    /// its worker sent ahead of the frames it owes another lease, whose reader
    /// may be far from taking it: it is copied out here unless that reader waits on
    /// its worker, which takes it now. Any other slot queued when its ring runs
    /// short of room is copied out by [`Self::give_back`].
    fn route(&self, mut slot: W2mSlot) {
        let w = slot.worker as usize;
        let mut routes = self.routes.borrow_mut();
        // No lease takes it: it drops here undecoded, releasing its ring space.
        let Some(t) = routes.trains.get_mut(&slot.request_id) else {
            return;
        };
        let reader = t.reader.take_if(|(r, _)| *r == w);
        if slot.passed() && reader.is_none() {
            slot.detach();
        }
        t.queues[w].push_back(slot);
        if let Some((_, waker)) = reader {
            waker.wake();
        }
    }

    /// A wait for every launched worker to apply the log through `at`: the
    /// position a group every worker runs ends at. See [`AckWait`].
    pub(crate) fn applied_through(&self, at: u64) -> AckWait {
        AckWait { reactor: self.clone(), at }
    }

    /// One request id, routed from no worker until a read is written under it.
    /// See [`TrainLease`].
    pub(crate) fn lease_train(&self) -> TrainLease {
        let nw = self.w2m.num_workers();
        let id = {
            let mut routes = self.routes.borrow_mut();
            let queues = routes
                .spare
                .pop()
                .unwrap_or_else(|| (0..nw).map(|_| VecDeque::new()).collect());
            let id = routes.fresh_id();
            routes.trains.insert(
                id,
                TrainRoute {
                    held: WorkerSet::EMPTY,
                    queues,
                    reader: None,
                },
            );
            id
        };
        TrainLease {
            reactor: self.clone(),
            id,
            reads: SmallVec::new(),
            taken: 0,
            left: Cell::new(WorkerSet::EMPTY),
        }
    }
}

// ---------------------------------------------------------------------------
// AckWait
// ---------------------------------------------------------------------------

/// A SAL position every launched worker is to apply the log through. The level
/// it waits on keeps what the workers published before anything awaited it.
pub(crate) struct AckWait {
    reactor: Reactor,
    at: u64,
}

/// Resolves once every launched worker has applied the log through the
/// position. A worker that cannot apply what lies below it exits instead, which
/// the master's death watch sees.
impl std::future::Future for AckWait {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        self.reactor.applied.poll_reached(self.at, cx)
    }
}

// ---------------------------------------------------------------------------
// TrainLease
// ---------------------------------------------------------------------------

/// One request id and the reads written under it, routed while it lives. Each
/// worker a read was written to answers it with one train of frames, ended by
/// its terminal: the frame carrying `scan_last`, or a fault frame. A worker
/// sends its trains in the order their reads were written, so the reads are
/// taken in that order, one [`Train`] at a time. Dropping the lease releases
/// every frame the route holds.
pub(crate) struct TrainLease {
    reactor: Reactor,
    id: RequestId,
    /// Each read written under the id, in the order written: what it was
    /// written for, named in every fault its train reports, and the workers it
    /// was written to.
    reads: SmallVec<[(&'static str, WorkerSet); 1]>,
    /// The trains handed out: read `taken - 1` is the one being taken, and
    /// every earlier one has ended or was given up.
    taken: usize,
    /// The workers of the read being taken whose terminal frame has not been
    /// read.
    left: Cell<WorkerSet>,
}

/// One read of a [`TrainLease`]: the train each of its workers answers it
/// with. A copy reads the same train.
#[derive(Clone, Copy)]
pub(crate) struct Train<'a>(&'a TrainLease);

impl Drop for TrainLease {
    fn drop(&mut self) {
        // Held across the drops below: a slot or a hold reaches no route.
        let mut routes = self.reactor.routes.borrow_mut();
        let mut train = routes
            .trains
            .remove(&self.id)
            .expect("a lease's id is routed while it lives");
        for w in train.held.iter() {
            self.reactor.w2m.unhold(w, self.id);
        }
        let written = self.reads.iter().fold(WorkerSet::EMPTY, |u, &(_, s)| u.union(s));
        // Every slot is dropped before the box is kept: one left queued would
        // hold up its ring's released prefix and reach the next lease to take
        // the box.
        for (w, q) in train.queues.iter_mut().enumerate() {
            debug_assert!(
                written.contains(w) || q.is_empty(),
                "worker {w} answered a read it was not sent"
            );
            q.clear();
            if q.capacity() > SPARE_QUEUE_CAP {
                *q = VecDeque::new();
            }
        }
        if routes.spare.len() < SPARE_QUEUES {
            routes.spare.push(train.queues);
        }
    }
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
    /// The request id its workers reply on.
    pub(crate) fn id(&self) -> RequestId {
        self.id
    }

    /// Count a read written to `set` under the id, named `what` in every fault
    /// its train reports.
    pub(crate) fn add(&mut self, set: WorkerSet, what: &'static str) {
        let set = set.within(self.reactor.w2m.num_workers());
        debug_assert!(set.len() > 0, "every read owes at least one reply");
        self.reads.push((what, set));
    }

    /// The next read's train, the reads taken in the order written: whatever
    /// the read before has left unread is read off first, each worker's train
    /// to its terminal.
    pub(crate) async fn next_train(&mut self) -> Train<'_> {
        while let Some(w) = self.left.get().iter().next() {
            // A fault frame ends `w`'s train as its terminal does.
            let _ = Train(self).frame_of(w).await;
        }
        let &(_, set) = self
            .reads
            .get(self.taken)
            .expect("a lease hands out one train per read written");
        self.taken += 1;
        self.left.set(set);
        Train(self)
    }

    /// The read being taken: what it was written for, and its workers.
    fn taking(&self) -> (&'static str, WorkerSet) {
        self.reads[self.taken - 1]
    }

    /// End worker `w`'s train of the read being taken on its fault frame: `f`,
    /// naming the worker and the read. Out of line: inlined, the registers it
    /// needs are saved on the path of every frame.
    #[cold]
    #[inline(never)]
    fn faulted(&self, w: usize, f: WireFault) -> WireFault {
        self.left.update(|v| v.without(w));
        f.in_context(format_args!("worker {w}: {}", self.taking().0))
    }

    /// Worker `w`'s next routed slot.
    async fn next_slot(&self, w: usize) -> W2mSlot {
        std::future::poll_fn(|cx| {
            let mut routes = self.reactor.routes.borrow_mut();
            let route = routes
                .trains
                .get_mut(&self.id)
                .expect("a lease's id is routed while it lives");
            match route.queues[w].pop_front() {
                Some(slot) => Poll::Ready(slot),
                None => {
                    // Every copied frame is taken: `w` sends the lease's trains on.
                    if route.held.contains(w) {
                        route.held = route.held.without(w);
                        self.reactor.w2m.unhold(w, self.id);
                    }
                    route.reader = Some((w, cx.waker().clone()));
                    Poll::Pending
                }
            }
        })
        .await
    }
}

impl Train<'_> {
    /// The workers that answer the read: the set it was written to, clipped to
    /// the launched workers.
    pub(crate) fn workers(self) -> WorkerSet {
        self.0.taking().1
    }

    /// Worker `w`'s next frame, row-less ones included, with the range of its
    /// rows when it carries any; `None` once its train has ended. The terminal
    /// frame is the one carrying `scan_last`, and is handed out like the rest.
    /// `Err` on a fault frame, which ends `w`'s train too.
    pub(crate) async fn frame_of(self, w: usize) -> Result<Option<(W2mSlot, Option<Range<usize>>)>, WireFault> {
        let lease = self.0;
        if !lease.left.get().contains(w) {
            return Ok(None);
        }
        let slot = lease.next_slot(w).await;
        let ctrl = slot.control();
        if let Some(f) = ctrl.fault(slot.bytes()) {
            return Err(lease.faulted(w, f));
        }
        if ctrl.hdr.flags.scan_last {
            lease.left.update(|v| v.without(w));
        }
        Ok(Some((slot, ctrl.data)))
    }

    /// Worker `w`'s next frame that carries rows, `None` once its train has ended;
    /// `Err` on a fault frame.
    pub(crate) async fn next_of(self, w: usize) -> Result<Option<TrainFrame>, WireFault> {
        while let Some((slot, data)) = self.frame_of(w).await? {
            if let Some(data) = data {
                return Ok(Some(TrainFrame { slot, data }));
            }
        }
        Ok(None)
    }

    /// The next frame that carries rows, workers in ascending order.
    pub(crate) async fn next(self) -> Result<Option<TrainFrame>, WireFault> {
        while let Some(w) = self.0.left.get().iter().next() {
            if let Some(f) = self.next_of(w).await? {
                return Ok(Some(f));
            }
        }
        Ok(None)
    }

    /// Wait out every worker's train, for a request answered with no rows; the
    /// first fault.
    pub(crate) async fn finish(self) -> Result<(), WireFault> {
        while self.next().await?.is_some() {}
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/lease.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/lease.rs"]
mod bench;
