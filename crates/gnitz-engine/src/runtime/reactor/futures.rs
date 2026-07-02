//! Reactor IO futures: Timer / Reply / ScanSlot (+ ScanLease) / Fsync /
//! Accept / Recv / Send — each driven by a CQE waking its registered waker.

use super::*;

// ---------------------------------------------------------------------------
// TimerFuture / ReplyFuture
// ---------------------------------------------------------------------------

pub(super) struct TimerFuture {
    deadline: Instant,
    /// Set to Some(id) on first poll, when the io_uring Timeout SQE is
    /// submitted. None means the future was never polled (no SQE in
    /// flight).
    timer_id: Option<u64>,
    /// Shared with the timer_wakers entry; flipped to true on Drop so
    /// dispatch_cqe silently discards the CQE without waking the task.
    cancelled: Rc<Cell<bool>>,
    inner: Rc<ReactorShared>,
}

impl TimerFuture {
    pub(super) fn new(deadline: Instant, inner: Rc<ReactorShared>) -> Self {
        TimerFuture {
            deadline,
            timer_id: None,
            cancelled: Rc::new(Cell::new(false)),
            inner,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let now = Instant::now();
        if now >= self.deadline {
            return Poll::Ready(());
        }
        if let Some(id) = self.timer_id {
            // Re-poll: update the waker in case it changed.
            if let Some(entry) = self.inner.timer_wakers.borrow_mut().get_mut(&id) {
                entry.0 = cx.waker().clone();
            }
        } else {
            // First poll: submit the io_uring Timeout SQE and register the
            // waker. The SQE is flushed to the kernel by tick's
            // submit_and_wait_timeout call at the end of the same tick.
            let id = self.inner.next_timer_id.get();
            self.inner.next_timer_id.set(id.wrapping_add(1));
            let ns = self.deadline.duration_since(now).as_nanos() as u64;
            self.inner.ring.borrow_mut().prep_timeout(ns, udata(KIND_TIMEOUT, id));
            self.inner
                .timer_wakers
                .borrow_mut()
                .insert(id, (cx.waker().clone(), Rc::clone(&self.cancelled)));
            self.timer_id = Some(id);
        }
        Poll::Pending
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        if self.timer_id.is_some() {
            // Signal dispatch_cqe to discard the CQE without waking.
            // No SQE cancellation is needed: the CQE will arrive and be
            // silently dropped by the KIND_TIMEOUT handler.
            self.cancelled.set(true);
        }
    }
}

pub struct ReplyFuture {
    pub(super) req_id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for ReplyFuture {
    type Output = DecodedWire;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<DecodedWire> {
        if let Some(decoded) = self.inner.parked_replies.borrow_mut().remove(&self.req_id) {
            return Poll::Ready(decoded);
        }
        self.inner
            .reply_wakers
            .borrow_mut()
            .insert(self.req_id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for ReplyFuture {
    fn drop(&mut self) {
        // select2 contract: a dropped awaiter must leave no registered state.
        // Without this, a reply arriving after the drop finds a stale waker and
        // is parked forever in parked_replies. On normal completion route_reply
        // has already removed the waker and poll removed the parked reply, so
        // Drop finds nothing — two absent-key removes, a few ns on a map already
        // cache-hot from the same poll. (route_reply parks a reply only when a
        // waker is registered, so withdrawing the waker is sufficient here — no
        // tombstone, unlike fsync/send.)
        self.inner.reply_wakers.borrow_mut().remove(&self.req_id);
        self.inner.parked_replies.borrow_mut().remove(&self.req_id);
    }
}

pub(super) struct ScanSlotFuture {
    pub(super) req_id: u32,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for ScanSlotFuture {
    type Output = W2mSlot;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<W2mSlot> {
        {
            let mut parked = self.inner.scan_parked.borrow_mut();
            if let Some(q) = parked.get_mut(&self.req_id) {
                if let Some(slot) = q.pop_front() {
                    if q.is_empty() {
                        parked.remove(&self.req_id);
                    }
                    return Poll::Ready(slot);
                }
            }
        }
        self.inner
            .scan_wakers
            .borrow_mut()
            .insert(self.req_id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for ScanSlotFuture {
    fn drop(&mut self) {
        // Only the waker: the ScanLease owns scan_parked for this req_id.
        // Removing the parked queue here would discard the still-queued
        // continuation frames a *resolved* future legitimately leaves behind
        // (between continuation frames there is no live ScanSlotFuture to run a
        // Drop, so the lease is the only scope that reliably spans the whole
        // operation).
        self.inner.scan_wakers.borrow_mut().remove(&self.req_id);
    }
}

/// RAII guard owning the active-scan + parked-queue lifecycle of a whole scan
/// operation. Registers its req_ids in `active_scans` on construction; on drop
/// deregisters them and purges any waker / queued frames they left (dropping a
/// queued `W2mSlot` advances `consume_cursor`, freeing ring space). Ids are
/// stored inline (at most `MAX_WORKERS` per fan-out), mirroring the
/// `[u64; MAX_WORKERS]` `req_ids` arrays in `dispatch_scan_fanout` — no
/// per-scan heap allocation.
pub(crate) struct ScanLease {
    inner: Rc<ReactorShared>,
    ids: [u32; MAX_WORKERS],
    len: u8, // MAX_WORKERS = 64 < 256
}

impl ScanLease {
    pub(super) fn new(inner: Rc<ReactorShared>, ids: &[u32]) -> Self {
        debug_assert!(ids.len() <= MAX_WORKERS);
        inner.active_scans.borrow_mut().extend(ids.iter().copied());
        let mut buf = [0u32; MAX_WORKERS];
        buf[..ids.len()].copy_from_slice(ids);
        ScanLease {
            inner,
            ids: buf,
            len: ids.len() as u8,
        }
    }
}

impl Drop for ScanLease {
    fn drop(&mut self) {
        let mut active = self.inner.active_scans.borrow_mut();
        let mut wakers = self.inner.scan_wakers.borrow_mut();
        let mut parked = self.inner.scan_parked.borrow_mut();
        for &id in &self.ids[..self.len as usize] {
            active.remove(&id);
            wakers.remove(&id);
            parked.remove(&id); // drops the whole VecDeque → every W2mSlot
                                // drop advances consume_cursor
        }
    }
}

// ---------------------------------------------------------------------------
// FsyncFuture / AcceptFuture / RecvFuture / SendFuture
// ---------------------------------------------------------------------------

pub struct FsyncFuture {
    pub(super) id: u64,
    /// Set once `poll` resolves `Ready`. Lets `Drop` skip the cancellation
    /// tombstone on the success path: `poll` already removed the parked result,
    /// so the absent-result check in `Drop` cannot distinguish "resolved" from
    /// "CQE still pending". Without this flag every successful fsync would
    /// tombstone an id whose CQE was already consumed — an unbounded
    /// `cancelled_fsyncs` leak, one entry per commit. `SendFuture` reuses its
    /// `_alive` keep-alive for the same purpose; `FsyncFuture` has no such field.
    pub(super) completed: bool,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for FsyncFuture {
    type Output = i32;
    // `mut self` so the ready arm can set `completed`; FsyncFuture is Unpin
    // (plain fields, no PhantomPinned/pin_project), so DerefMut through the Pin
    // is sound — the same shape SendFuture::poll already uses.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        let parked = self.inner.parked_fsync_results.borrow_mut().remove(&self.id);
        if let Some(rc) = parked {
            self.completed = true;
            return Poll::Ready(rc);
        }
        self.inner.fsync_wakers.borrow_mut().insert(self.id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for FsyncFuture {
    fn drop(&mut self) {
        // Resolved: poll already delivered the result and the CQE was consumed —
        // no waker left (the handler removed it when it woke), nothing to reclaim.
        if self.completed {
            return;
        }
        self.inner.fsync_wakers.borrow_mut().remove(&self.id);
        // CQE arrived after the last poll but before this drop: reclaim it.
        if self.inner.parked_fsync_results.borrow_mut().remove(&self.id).is_some() {
            return;
        }
        // CQE still pending: tombstone so the late KIND_FSYNC handler drops its
        // result instead of leaking it (the handler parks unconditionally).
        self.inner.cancelled_fsyncs.borrow_mut().insert(self.id);
    }
}

pub struct AcceptFuture {
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for AcceptFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        if let Some(fd) = self.inner.accept_queue.borrow_mut().pop_front() {
            return Poll::Ready(fd);
        }
        *self.inner.accept_waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for AcceptFuture {
    fn drop(&mut self) {
        // Waker hygiene only: accept_waker is a single Option<Waker> rewritten
        // by the next poll and take()n by the KIND_ACCEPT handler, and there is
        // at most one live AcceptFuture (the eternal accept_loop). Purely
        // defensive today — the accept loop is never cancelled — but completes
        // the park-state enumeration for a future timeout/select2 over accept().
        self.inner.accept_waker.borrow_mut().take();
    }
}

pub struct RecvFuture {
    pub(super) fd: i32,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for RecvFuture {
    type Output = Option<io::RecvBuf>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let taken = {
            let mut map = self.inner.pending_recv.borrow_mut();
            map.get_mut(&self.fd).and_then(|q| q.pop_front())
        };
        if let Some(buf) = taken {
            return Poll::Ready(Some(buf));
        }
        if self.inner.recv_closed.borrow().get(&self.fd).copied().unwrap_or(false) {
            return Poll::Ready(None);
        }
        self.inner.recv_waiters.borrow_mut().insert(self.fd, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for RecvFuture {
    fn drop(&mut self) {
        // Waker hygiene only: recv_waiters holds at most one waker per fd (the
        // single read loop for this connection), and no per-awaiter result is
        // parked behind its back. Runs in normal operation — a recv loop ends on
        // every client disconnect — so withdrawing the waker closes the family.
        self.inner.recv_waiters.borrow_mut().remove(&self.fd);
    }
}

pub(super) enum SendAlive {
    Pooled(Rc<crate::storage::batch_pool::PooledSendBuf>),
    Slot(Rc<W2mSlot>),
    /// One-shot UDP SENDMSG op: keeps the msghdr/iovec/sockaddr/payload the
    /// kernel reads alive until the CQE. `Rc`, not `Box`, because the op is
    /// self-referential (msghdr points at sibling fields) and moving an `Rc`
    /// handle never retags or relocates the pointee.
    UdpOp(Rc<udp::UdpSendOp>),
    /// The backing memory lives `'static` (e.g. the precomputed HELLO
    /// ACK), so no liveness tracking is required — the kernel pointer
    /// remains valid for the lifetime of the process.
    Static,
}

impl Clone for SendAlive {
    fn clone(&self) -> Self {
        match self {
            SendAlive::Pooled(rc) => SendAlive::Pooled(Rc::clone(rc)),
            SendAlive::Slot(rc) => SendAlive::Slot(Rc::clone(rc)),
            SendAlive::UdpOp(rc) => SendAlive::UdpOp(Rc::clone(rc)),
            SendAlive::Static => SendAlive::Static,
        }
    }
}

pub struct SendFuture {
    pub(super) send_id: u64,
    pub(super) _alive: Option<SendAlive>,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for SendFuture {
    type Output = i32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        let parked = self.inner.parked_send_results.borrow_mut().remove(&self.send_id);
        if let Some(rc) = parked {
            self._alive.take();
            return Poll::Ready(rc);
        }
        let send_id = self.send_id;
        self.inner.send_wakers.borrow_mut().insert(send_id, cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        // CQE already parked a result (and the handler already freed the
        // buffer): reclaim the orphaned result, nothing else to do.
        if self
            .inner
            .parked_send_results
            .borrow_mut()
            .remove(&self.send_id)
            .is_some()
        {
            self._alive.take();
            return;
        }
        // CQE still pending: keep the buffer alive for the kernel, withdraw the
        // waker, and tombstone so the late KIND_SEND handler drops its result.
        // (`_alive` is None on the resolved path — poll take()s it — so this arm
        // never fires after a successful send, keeping cancelled_sends bounded.)
        if let Some(alive) = self._alive.take() {
            self.inner
                .send_buffers_in_flight
                .borrow_mut()
                .insert(self.send_id, alive);
            self.inner.send_wakers.borrow_mut().remove(&self.send_id);
            self.inner.cancelled_sends.borrow_mut().insert(self.send_id);
        }
    }
}

// (oneshot, mpsc, AsyncMutex, AsyncRwLock, join2, join_all, select2 live in sync.rs)
