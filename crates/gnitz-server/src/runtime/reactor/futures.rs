//! Reactor futures: W2M routes and their [`Lease`], and the futures woken by a
//! CQE, a routed frame or a passed deadline.

use std::ops::Range;

use gnitz_wire::control::DecodedControl;
use gnitz_wire::{WireFault, WireStatus};

use super::*;
use crate::runtime::sal::WorkerSet;

/// Worker `w`'s reply as a fault, keeping its status, or `None` when it succeeded.
pub(crate) fn worker_error(w: usize, op: &str, ctrl: &DecodedControl) -> Option<WireFault> {
    (ctrl.hdr.status != WireStatus::Ok).then(|| {
        let msg = String::from_utf8_lossy(&ctrl.blob);
        WireFault {
            status: ctrl.hdr.status,
            text: format!("worker {w}: {op}: {msg}"),
        }
    })
}

// ---------------------------------------------------------------------------
// Routes + Lease
// ---------------------------------------------------------------------------

/// A route's key: `(request id, worker)`.
pub(super) type RouteKey = (u32, u32);

/// Where one worker's frames on a leased request id go.
pub(super) enum Route {
    /// An ACK: one control-only frame, read on arrival (so its ring slot is freed
    /// at once) and kept until the lease drops.
    Ack {
        ack: Option<DecodedControl>,
        waker: Option<Waker>,
    },
    /// A scan's frames, undecoded; each pins its ring space until dropped.
    Train(WakeQueue<W2mSlot>),
}

/// The train queue routed at `key`.
fn train(routes: &mut FxHashMap<RouteKey, Route>, key: RouteKey) -> &mut WakeQueue<W2mSlot> {
    match routes.get_mut(&key).expect("a leased id is routed") {
        Route::Train(q) => q,
        Route::Ack { .. } => unreachable!("a frame awaited on an ACK lease"),
    }
}

/// `len` consecutive request ids, each answered by every worker in `set`, routed
/// while it lives, so a frame beating its awaiter is kept. Dropping it releases
/// every frame its routes hold or later get.
pub(crate) struct Lease {
    inner: Rc<ReactorShared>,
    base: u32,
    len: u32,
    set: WorkerSet,
}

impl Lease {
    pub(super) fn new(inner: Rc<ReactorShared>, base: u32, len: u32, set: WorkerSet) -> Self {
        Lease { inner, base, len, set }
    }

    /// Request id `i`.
    pub(crate) fn id(&self, i: usize) -> u32 {
        debug_assert!(i < self.len as usize, "id {i} of a {}-id lease", self.len);
        self.base + i as u32
    }

    /// The workers answering each id, ascending.
    pub(crate) fn workers(&self) -> impl Iterator<Item = usize> {
        self.set.within(self.inner.w2m.num_workers()).iter()
    }

    /// The routes of the first `n` ids, id then worker.
    fn keys(&self, n: usize) -> impl Iterator<Item = RouteKey> + '_ {
        (self.base..self.base + n as u32).flat_map(move |id| self.workers().map(move |w| (id, w as u32)))
    }

    /// Once every worker has ACKed each of the lease's ids, the first failed ACK
    /// in id-then-worker order as `Err`, its text naming `ctx`.
    pub(crate) async fn acks(&self, ctx: &str) -> Result<(), WireFault> {
        // Every key before the peeked one is answered; only its route holds a waker.
        let mut pending = self.keys(self.len as usize).peekable();
        std::future::poll_fn(|cx| {
            let mut routes = self.inner.routes.borrow_mut();
            while let Some(key) = pending.peek() {
                match routes.get_mut(key).expect("a leased id is routed") {
                    Route::Ack { ack: Some(_), .. } => {
                        pending.next();
                    }
                    Route::Ack { ack: None, waker } => {
                        park_waker(waker, cx.waker());
                        return Poll::Pending;
                    }
                    Route::Train(_) => unreachable!("ACKs awaited on a train lease"),
                }
            }
            drop(routes);
            Poll::Ready(self.first_error(ctx).map_or(Ok(()), Err))
        })
        .await
    }

    /// The first failed ACK among the lease's ids that have arrived, in
    /// id-then-worker order, its text naming `ctx`.
    pub(crate) fn first_error(&self, ctx: &str) -> Option<WireFault> {
        let routes = self.inner.routes.borrow();
        // By reference: the verdict stays in the route until the lease drops.
        self.keys(self.len as usize).find_map(|key| match routes.get(&key) {
            Some(Route::Ack { ack: Some(ctrl), .. }) => worker_error(key.1 as usize, ctx, ctrl),
            _ => None,
        })
    }

    /// Release every id past the first `n`.
    pub(crate) fn truncate(&mut self, n: usize) {
        debug_assert!(n <= self.len as usize);
        self.release(self.base + n as u32..self.base + self.len);
        self.len = n as u32;
    }

    /// Remove `ids`' routes, then wake the `trains_idle` awaiter.
    fn release(&self, ids: Range<u32>) {
        {
            let mut routes = self.inner.routes.borrow_mut();
            for id in ids {
                for w in self.workers() {
                    routes.remove(&(id, w as u32));
                }
            }
        }
        // Every lease, not only a train: the waiter re-checks the routes itself.
        if let Some(w) = self.inner.trains_idle.take() {
            w.wake();
        }
    }

    /// Worker `w`'s next frame on the lease's first id.
    pub(crate) async fn next_frame(&self, w: usize) -> W2mSlot {
        let key = (self.base, w as u32);
        std::future::poll_fn(|cx| train(&mut self.inner.routes.borrow_mut(), key).poll(cx)).await
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.release(self.base..self.base + self.len);
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
        match self.inner.deadlines.borrow_mut().entry((self.deadline, self.id)) {
            Entry::Occupied(mut e) => e.get_mut().clone_from(cx.waker()),
            Entry::Vacant(e) => {
                e.insert(cx.waker().clone());
            }
        }
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        self.inner.deadlines.borrow_mut().remove(&(self.deadline, self.id));
    }
}

// ---------------------------------------------------------------------------
// OpFuture
// ---------------------------------------------------------------------------

/// One submitted op awaiting its CQE: the result and what the op carried.
/// Dropped early, it abandons the slot, which keeps the carry until the CQE.
pub(super) struct OpFuture {
    pub(super) id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for OpFuture {
    type Output = (i32, Option<SendBody>);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(i32, Option<SendBody>)> {
        self.inner.ops.poll(self.id, cx)
    }
}

impl Drop for OpFuture {
    fn drop(&mut self) {
        self.inner.ops.abandon(self.id);
    }
}

#[cfg(test)]
#[path = "tests/futures.rs"]
mod tests;
