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

    fn ids(&self) -> Range<u32> {
        self.base..self.base + self.len
    }

    /// Once every id in `range` has its ACK, `check` each in id order with the
    /// worker that sent it, returning the first `Some` as `Err`.
    pub(crate) async fn acks<E>(
        &self,
        range: Range<usize>,
        mut check: impl FnMut(usize, &DecodedControl) -> Option<E>,
    ) -> Result<(), E> {
        debug_assert!(range.end <= self.len as usize);
        let ids = self.base + range.start as u32..self.base + range.end as u32;
        std::future::poll_fn(|cx| {
            let mut routes = self.inner.routes.borrow_mut();
            let mut pending = false;
            for id in ids.clone() {
                match routes.get_mut(&id).expect("a leased id is routed") {
                    Route::Ack { ack: Some(_), .. } => {}
                    Route::Ack { ack: None, waker } => {
                        match waker {
                            Some(w) => w.clone_from(cx.waker()),
                            None => *waker = Some(cx.waker().clone()),
                        }
                        pending = true;
                    }
                    Route::Train(_) => unreachable!("ACKs awaited on a train lease"),
                }
            }
            if pending {
                return Poll::Pending;
            }
            // By reference: the verdict stays in the route until the lease drops.
            for id in ids.clone() {
                let Some(Route::Ack { ack: Some((w, ctrl)), .. }) = routes.get(&id) else {
                    unreachable!("every ACK checked present above");
                };
                if let Some(e) = check(*w, ctrl) {
                    return Poll::Ready(Err(e));
                }
            }
            Poll::Ready(Ok(()))
        })
        .await
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
        std::future::poll_fn(|cx| {
            train(&mut self.inner.routes.borrow_mut(), id)
                .poll(cx)
                .map(|s| s.expect("a route is never closed"))
        })
        .await
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut routes = self.inner.routes.borrow_mut();
        for id in self.ids() {
            routes.remove(&id);
        }
    }
}

// ---------------------------------------------------------------------------
// TimerFuture
// ---------------------------------------------------------------------------

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
            self.inner.cancel_wake(self.deadline, self.id);
            return Poll::Ready(());
        }
        self.inner.wake_at(self.deadline, self.id, cx.waker());
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        self.inner.cancel_wake(self.deadline, self.id);
    }
}

// ---------------------------------------------------------------------------
// FsyncFuture / SendFuture
// ---------------------------------------------------------------------------

pub struct FsyncFuture {
    pub(super) id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for FsyncFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        self.inner.fsyncs.poll(self.id, cx).map(|(rc, ())| rc)
    }
}

impl Drop for FsyncFuture {
    fn drop(&mut self) {
        self.inner.fsyncs.abandon(self.id);
    }
}

/// One `OP_SEND` on `fd`. Past `deadline` it evicts the client (`shutdown`), which
/// errors the send out; the result is then clamped negative.
pub(super) struct SendFuture {
    pub(super) send_id: u64,
    pub(super) fd: i32,
    pub(super) deadline: Instant,
    pub(super) evicted: bool,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for SendFuture {
    type Output = (i32, SendBody);
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(i32, SendBody)> {
        if let Poll::Ready((rc, body)) = self.inner.sends.poll(self.send_id, cx) {
            self.inner.cancel_wake(self.deadline, self.send_id);
            return Poll::Ready((if self.evicted { rc.min(-1) } else { rc }, body));
        }
        if !self.evicted {
            if Instant::now() >= self.deadline {
                gnitz_warn!(
                    "client fd={} made no send progress for {:?}; evicting",
                    self.fd,
                    self.inner.limits.client_send_timeout
                );
                shutdown(self.fd);
                self.evicted = true;
            } else {
                self.inner.wake_at(self.deadline, self.send_id, cx.waker());
            }
        }
        Poll::Pending
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        self.inner.cancel_wake(self.deadline, self.send_id);
        self.inner.sends.abandon(self.send_id);
    }
}

// (oneshot, chan, AsyncMutex, AsyncRwLock, select2 live in sync.rs)

#[cfg(test)]
#[path = "tests/futures.rs"]
mod tests;
