//! Reactor futures: W2M routes and their [`Lease`], and the futures woken by a
//! CQE, a routed frame or a passed deadline.

use std::ops::Range;

use gnitz_wire::control::DecodedControl;

use super::*;

// ---------------------------------------------------------------------------
// Routes + Lease
// ---------------------------------------------------------------------------

/// Where a leased request id's frames go.
pub(super) enum Route {
    /// An ACK: one control-only frame, read on arrival (so its ring slot is freed
    /// at once) and kept as `(worker, control)` until the lease drops.
    Ack {
        ack: Option<(usize, DecodedControl)>,
        waker: Option<Waker>,
    },
    /// A scan's frames, undecoded; each pins its ring space until dropped.
    Train(WakeQueue<W2mSlot>),
}

/// The train queue routed at `id`.
fn train(routes: &mut FxHashMap<u32, Route>, id: u32) -> &mut WakeQueue<W2mSlot> {
    match routes.get_mut(&id).expect("a leased id is routed") {
        Route::Train(q) => q,
        Route::Ack { .. } => unreachable!("a frame awaited on an ACK lease"),
    }
}

/// `len` consecutive request ids, routed while it lives, so a frame beating its
/// awaiter is kept. Dropping it releases every frame its routes hold or later get.
pub(crate) struct Lease {
    inner: Rc<ReactorShared>,
    base: u32,
    len: u32,
}

impl Lease {
    pub(super) fn new(inner: Rc<ReactorShared>, base: u32, len: u32) -> Self {
        Lease { inner, base, len }
    }

    /// Request id `i`.
    pub(crate) fn id(&self, i: usize) -> u64 {
        debug_assert!(i < self.len as usize, "id {i} of a {}-id lease", self.len);
        (self.base + i as u32) as u64
    }

    /// The first id; worker `w` of a broadcast answers on `base + w`.
    pub(crate) fn base(&self) -> u64 {
        self.base as u64
    }

    fn ids(&self) -> Range<u32> {
        self.base..self.base + self.len
    }

    /// Once each of the first `n` ids has its ACK, `check` each in id order with
    /// the worker that sent it, returning the first `Some` as `Err`.
    pub(crate) async fn acks<E>(
        &self,
        n: usize,
        mut check: impl FnMut(usize, &DecodedControl) -> Option<E>,
    ) -> Result<(), E> {
        debug_assert!(n <= self.len as usize);
        let end = self.base + n as u32;
        // Every id below `next` is answered; only `next`'s route holds a waker.
        let mut next = self.base;
        std::future::poll_fn(|cx| {
            let mut routes = self.inner.routes.borrow_mut();
            while next < end {
                match routes.get_mut(&next).expect("a leased id is routed") {
                    Route::Ack { ack: Some(_), .. } => next += 1,
                    Route::Ack { ack: None, waker } => {
                        park_waker(waker, cx.waker());
                        return Poll::Pending;
                    }
                    Route::Train(_) => unreachable!("ACKs awaited on a train lease"),
                }
            }
            drop(routes);
            Poll::Ready(self.first_error(n, &mut check).map_or(Ok(()), Err))
        })
        .await
    }

    /// The first failed ACK among the first `n` ids that have arrived, in id order.
    pub(crate) fn first_error<E>(
        &self,
        n: usize,
        mut check: impl FnMut(usize, &DecodedControl) -> Option<E>,
    ) -> Option<E> {
        let routes = self.inner.routes.borrow();
        // By reference: the verdict stays in the route until the lease drops.
        (self.base..self.base + n as u32).find_map(|id| match routes.get(&id) {
            Some(Route::Ack { ack: Some((w, ctrl)), .. }) => check(*w, ctrl),
            _ => None,
        })
    }

    /// The first frame of every id, in id order.
    pub(crate) async fn first_frames(&self) -> Vec<W2mSlot> {
        std::future::poll_fn(|cx| {
            let mut routes = self.inner.routes.borrow_mut();
            // Park on every empty queue before popping any: a frame popped before
            // another queue is found empty would be lost with this poll.
            let mut pending = false;
            for id in self.ids() {
                let q = train(&mut routes, id);
                if q.is_empty() {
                    q.park(cx);
                    pending = true;
                }
            }
            if pending {
                return Poll::Pending;
            }
            Poll::Ready(
                self.ids()
                    .map(|id| train(&mut routes, id).pop().expect("every queue checked non-empty"))
                    .collect(),
            )
        })
        .await
    }

    /// The next frame on id `i`.
    pub(crate) async fn next_frame(&self, i: usize) -> W2mSlot {
        let id = self.id(i) as u32;
        std::future::poll_fn(|cx| train(&mut self.inner.routes.borrow_mut(), id).poll(cx)).await
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        {
            let mut routes = self.inner.routes.borrow_mut();
            for id in self.ids() {
                routes.remove(&id);
            }
        }
        // Every lease, not only a train: the waiter re-checks the routes itself.
        if let Some(w) = self.inner.trains_idle.take() {
            w.wake();
        }
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
