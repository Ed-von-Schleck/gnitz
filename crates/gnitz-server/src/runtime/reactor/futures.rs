//! Reactor futures: the W2M routes of [`AckLease`] and [`TrainLease`], and the
//! futures woken by a routed frame or a passed deadline.

use std::ops::Range;

use gnitz_store::schema::SchemaDescriptor;
use gnitz_store::storage::WalBlock;
use gnitz_wire::WireFault;

use super::*;
use crate::runtime::sal::{SalMessageKind, WorkerSet};

/// Worker `w`'s fault `f`, its text naming the worker and `op`.
fn worker_fault(w: usize, op: &str, f: WireFault) -> WireFault {
    WireFault {
        text: format!("worker {w}: {op}: {}", f.text),
        ..f
    }
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/// What the workers have answered on one [`AckLease`] id.
pub(super) struct AckRoute {
    answered: WorkerSet,
    /// The fault of the lowest-numbered worker that failed.
    fault: Option<(usize, WireFault)>,
    /// The `acks` awaiter parked on this id.
    waker: Option<Waker>,
}

impl AckRoute {
    /// Record worker `w`'s answer.
    fn record(&mut self, w: usize, fault: Option<WireFault>) {
        debug_assert!(!self.answered.contains(w), "worker {w} answered one request id twice");
        self.answered = self.answered.with(w);
        if let Some(f) = fault {
            if self.fault.as_ref().is_none_or(|&(fw, _)| w < fw) {
                self.fault = Some((w, f));
            }
        }
    }
}

/// The undecoded frames of one [`TrainLease`]; each pins its ring space until
/// dropped.
pub(super) struct TrainRoute {
    /// The workers the scan was written to.
    set: WorkerSet,
    /// One queue per worker of `set`, ascending.
    queues: Box<[WakeQueue<W2mSlot>]>,
}

impl TrainRoute {
    /// Worker `w`'s frame queue, `None` when the scan was not written to `w`.
    fn queue(&mut self, w: usize) -> Option<&mut WakeQueue<W2mSlot>> {
        self.set.contains(w).then(|| &mut self.queues[self.set.rank(w)])
    }
}

impl ReactorShared {
    /// Hand worker `w`'s slot to the lease its ring id names. A slot no lease
    /// takes drops here undecoded, releasing its ring space.
    pub(super) fn route(&self, w: usize, slot: W2mSlot) {
        let id = slot.internal_req_id;
        if let Some(t) = self.trains.borrow_mut().get_mut(&id) {
            if let Some(q) = t.queue(w) {
                q.push(slot);
            }
            return;
        }
        let mut acks = self.acks.borrow_mut();
        let Some(r) = acks.get_mut(&id) else { return };
        r.record(w, slot.control().fault(slot.bytes()));
        if r.answered.covers(self.w2m.num_workers()) {
            if let Some(waker) = r.waker.take() {
                waker.wake();
            }
        }
    }
}

impl Reactor {
    /// A request id no live lease holds, never 0 (`GroupTargets::Unaddressed`'s).
    fn alloc_request_id(&self) -> u32 {
        let acks = self.inner.acks.borrow();
        let trains = self.inner.trains.borrow();
        let mut id = self.inner.next_request_id.get();
        while id == 0 || acks.contains_key(&id) || trains.contains_key(&id) {
            id = id.wrapping_add(1);
        }
        self.inner.next_request_id.set(id.wrapping_add(1));
        id
    }

    /// One request id, answered by one ACK from every worker, awaited under the
    /// label `ctx`. See [`AckLease`].
    pub(crate) fn lease_acks(&self, ctx: &'static str) -> AckLease {
        let id = self.alloc_request_id();
        self.inner.acks.borrow_mut().insert(
            id,
            AckRoute {
                answered: WorkerSet::EMPTY,
                fault: None,
                waker: None,
            },
        );
        AckLease { inner: Rc::clone(&self.inner), id, ctx }
    }

    /// One request id, answered by a train of frames from every worker in `set`,
    /// written for `kind`. See [`TrainLease`].
    pub(crate) fn lease_train(&self, set: WorkerSet, kind: SalMessageKind) -> TrainLease {
        let set = set.within(self.inner.w2m.num_workers());
        let id = self.alloc_request_id();
        let queues = (0..set.len()).map(|_| WakeQueue::default()).collect();
        self.inner.trains.borrow_mut().insert(id, TrainRoute { set, queues });
        TrainLease {
            inner: Rc::clone(&self.inner),
            id,
            workers: set,
            left: Cell::new(set),
            kind,
        }
    }
}

// ---------------------------------------------------------------------------
// AckLease
// ---------------------------------------------------------------------------

/// One request id, answered by one ACK from every worker, routed while it lives,
/// so an ACK beating its awaiter is kept.
pub(crate) struct AckLease {
    inner: Rc<ReactorShared>,
    id: u32,
    /// What the ACKs answer, named in every fault the lease reports.
    ctx: &'static str,
}

impl AckLease {
    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    /// What the ACKs answer.
    pub(crate) fn ctx(&self) -> &'static str {
        self.ctx
    }

    /// Once every worker has ACKed, the first failed ACK as `Err`.
    pub(crate) async fn acks(&self) -> Result<(), WireFault> {
        let nw = self.inner.w2m.num_workers();
        std::future::poll_fn(|cx| {
            let mut acks = self.inner.acks.borrow_mut();
            let r = acks.get_mut(&self.id).expect("a leased id is routed");
            if !r.answered.covers(nw) {
                r.waker = Some(cx.waker().clone());
                return Poll::Pending;
            }
            drop(acks);
            Poll::Ready(self.first_error().map_or(Ok(()), Err))
        })
        .await
    }

    /// The failed ACK of the lowest-numbered worker among those that answered.
    pub(crate) fn first_error(&self) -> Option<WireFault> {
        let (w, f) = self.inner.acks.borrow()[&self.id].fault.clone()?;
        Some(worker_fault(w, self.ctx, f))
    }
}

impl Drop for AckLease {
    fn drop(&mut self) {
        self.inner.acks.borrow_mut().remove(&self.id);
    }
}

// ---------------------------------------------------------------------------
// TrainLease
// ---------------------------------------------------------------------------

/// One request id, answered by a train of frames from each worker of its set,
/// routed while it lives. Dropping it releases every frame the route holds.
pub(crate) struct TrainLease {
    inner: Rc<ReactorShared>,
    id: u32,
    /// The set its route was built over.
    workers: WorkerSet,
    /// The workers whose terminal frame has not been read.
    left: Cell<WorkerSet>,
    /// What the lease was written for, named in every fault it reports.
    kind: SalMessageKind,
}

/// One frame of a train that carries rows. `slot` pins its ring bytes.
pub(crate) struct TrainFrame {
    pub(crate) slot: W2mSlot,
    data: Range<usize>,
}

impl TrainFrame {
    /// The frame's rows under `schema`, aborting on failure: the ring is trusted.
    pub(crate) fn rows(&self, schema: &SchemaDescriptor) -> WalBlock<'_> {
        match WalBlock::parse(&self.slot.bytes()[self.data.clone()], schema) {
            Ok(block) => block,
            Err(e) => gnitz_fatal_abort!(
                "w2m: worker={} train frame does not decode under the reader's schema: {e}",
                self.slot.worker
            ),
        }
    }
}

impl TrainLease {
    /// The request id.
    pub(crate) fn id(&self) -> u32 {
        self.id
    }

    /// The workers answering, each launched.
    pub(crate) fn workers(&self) -> WorkerSet {
        self.workers
    }

    /// Worker `w`'s next frame that carries rows, `None` once its train has ended;
    /// `Err` on a fault frame.
    pub(crate) async fn next_of(&self, w: usize) -> Result<Option<TrainFrame>, WireFault> {
        while self.left.get().contains(w) {
            let slot = self.next_slot(w).await;
            let ctrl = slot.control();
            if let Some(f) = ctrl.fault(slot.bytes()) {
                return Err(worker_fault(w, &format!("{:?}", self.kind), f));
            }
            debug_assert!(
                ctrl.schema.is_none(),
                "worker {w}: a reply frame carries a schema block"
            );
            if ctrl.hdr.flags.scan_last {
                self.left.set(self.left.get().without(w));
            }
            if let Some(data) = ctrl.data {
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

    /// Worker `w`'s next routed slot.
    async fn next_slot(&self, w: usize) -> W2mSlot {
        std::future::poll_fn(|cx| {
            let mut trains = self.inner.trains.borrow_mut();
            let route = trains.get_mut(&self.id).expect("a leased id is routed");
            route
                .queue(w)
                .expect("the train was written to `w`")
                .poll(cx)
                .map(|v| v.expect("a train queue never closes"))
        })
        .await
    }
}

impl Drop for TrainLease {
    fn drop(&mut self) {
        self.inner.trains.borrow_mut().remove(&self.id);
    }
}

// ---------------------------------------------------------------------------
// TimerFuture
// ---------------------------------------------------------------------------

/// A deadline. Its entry in the deadline map lives until the future drops.
pub(super) struct TimerFuture {
    deadline: Instant,
    id: u64,
    inner: Rc<ReactorShared>,
}

impl TimerFuture {
    pub(super) fn new(deadline: Instant, inner: Rc<ReactorShared>) -> Self {
        TimerFuture { deadline, id: inner.alloc_op_id(), inner }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if Instant::now() >= self.deadline {
            return Poll::Ready(());
        }
        self.inner
            .deadlines
            .borrow_mut()
            .insert((self.deadline, self.id), cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        self.inner.deadlines.borrow_mut().remove(&(self.deadline, self.id));
    }
}

#[cfg(test)]
#[path = "tests/futures.rs"]
mod tests;
