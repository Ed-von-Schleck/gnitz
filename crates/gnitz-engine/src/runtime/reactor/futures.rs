//! Reactor IO futures: Timer / Reply / ScanSlot (+ ScanLease) / Fsync /
//! Accept / Recv / Send — each driven by a CQE waking its registered waker.
//!
//! Everything that waits on a single CQE-delivered result goes through
//! [`super::park::ParkMap`], so `poll` and `Drop` are one line each; only the
//! op-specific submit and teardown live here.

use std::any::Any;

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
    inner: Rc<ReactorShared>,
}

impl TimerFuture {
    pub(super) fn new(deadline: Instant, inner: Rc<ReactorShared>) -> Self {
        TimerFuture {
            deadline,
            timer_id: None,
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
        let id = match self.timer_id {
            Some(id) => id,
            None => {
                // First poll: submit the io_uring Timeout SQE. It is flushed to
                // the kernel by tick's submit at the end of the same tick.
                let id = self.inner.alloc_op_id();
                let ns = self.deadline.duration_since(now).as_nanos() as u64;
                self.inner.ring.borrow_mut().prep_timeout(ns, udata(KIND_TIMEOUT, id));
                self.inner.timers.open(id, None);
                self.timer_id = Some(id);
                id
            }
        };
        self.inner.timers.poll(id, cx).map(|_| ())
    }
}

impl Drop for TimerFuture {
    fn drop(&mut self) {
        let Some(id) = self.timer_id else { return };
        // Reclaim the kernel timer promptly instead of letting it run to its
        // deadline: deadline guards (e.g. the per-frame send-slot eviction
        // timer) drop their timer on every happy-path completion, and without
        // the cancel each one would leave an armed Timeout and its Timespec box
        // behind for the full window. The cancel's own CQE lands on the no-op
        // KIND_CANCEL_SINK; the Timeout's own (-ECANCELED) lands on
        // KIND_TIMEOUT, which finds an abandoned slot and only releases the
        // Timespec.
        if self.inner.timers.abandon(id) {
            self.inner
                .ring
                .borrow_mut()
                .prep_async_cancel(udata(KIND_TIMEOUT, id), udata(KIND_CANCEL_SINK, 0));
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
        self.inner.replies.poll(self.req_id, cx).map(|(v, _)| v)
    }
}

impl Drop for ReplyFuture {
    fn drop(&mut self) {
        // select2 contract: a dropped awaiter leaves no registered state, so a
        // reply arriving afterwards is discarded rather than parked forever.
        // Closed, not abandoned: a worker that dies mid-request never sends the
        // reply that would retire an abandoned slot.
        self.inner.replies.close(self.req_id);
    }
}

/// One scan request id's routing state: the frames a worker has streamed ahead
/// and the awaiter parked on them. Its presence in `ReactorShared::scans` is
/// what marks the scan live — a frame for an unlisted id belongs to an
/// abandoned scan and is dropped at the ring boundary.
#[derive(Default)]
pub(super) struct ScanRoute {
    /// A *queue*, not a single slot: a worker streams continuation frames ahead
    /// while the master drains a different worker serially, so a single value
    /// would drop all but the last. Each queued `W2mSlot` holds its ring slot
    /// until dropped, so the worker blocks in `send_encoded` once the ring
    /// fills — depth is bounded by ring capacity.
    pub(super) queue: VecDeque<W2mSlot>,
    pub(super) waker: Option<Waker>,
}

pub(super) struct ScanSlotFuture {
    pub(super) req_id: u32,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for ScanSlotFuture {
    type Output = W2mSlot;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<W2mSlot> {
        let mut scans = self.inner.scans.borrow_mut();
        let Some(route) = scans.get_mut(&self.req_id) else {
            // The lease is gone; no further frame will ever be routed here.
            return Poll::Pending;
        };
        match route.queue.pop_front() {
            Some(slot) => Poll::Ready(slot),
            None => {
                route.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}

impl Drop for ScanSlotFuture {
    fn drop(&mut self) {
        // Only the waker: the ScanLease owns the queue for this req_id. Clearing
        // it here would discard the continuation frames a *resolved* future
        // legitimately leaves behind (between frames there is no live
        // ScanSlotFuture, so the lease is the only scope spanning the operation).
        if let Some(route) = self.inner.scans.borrow_mut().get_mut(&self.req_id) {
            route.waker = None;
        }
    }
}

/// RAII guard owning the routing state of a whole scan operation. Registers its
/// req_ids on construction; on drop deregisters them, which drops any frames
/// they queued (dropping a `W2mSlot` advances `consume_cursor`, freeing ring
/// space) so `route_scan_slot` discards the scan's later frames. Ids are stored
/// inline (at most `MAX_WORKERS` per fan-out) — no per-scan heap allocation.
pub(crate) struct ScanLease {
    inner: Rc<ReactorShared>,
    ids: [u32; MAX_WORKERS],
    len: u8, // MAX_WORKERS = 64 < 256
}

impl ScanLease {
    pub(super) fn new(inner: Rc<ReactorShared>, ids: &[u32]) -> Self {
        debug_assert!(ids.len() <= MAX_WORKERS);
        {
            let mut scans = inner.scans.borrow_mut();
            for &id in ids {
                scans.insert(id, ScanRoute::default());
            }
        }
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
        let mut scans = self.inner.scans.borrow_mut();
        for &id in &self.ids[..self.len as usize] {
            scans.remove(&id);
        }
    }
}

// ---------------------------------------------------------------------------
// FsyncFuture / AcceptFuture / RecvFuture / SendFuture
// ---------------------------------------------------------------------------

pub struct FsyncFuture {
    pub(super) id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for FsyncFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        self.inner.fsyncs.poll(self.id, cx).map(|(rc, _)| rc)
    }
}

impl Drop for FsyncFuture {
    fn drop(&mut self) {
        self.inner.fsyncs.abandon(self.id);
    }
}

pub struct AcceptFuture {
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for AcceptFuture {
    type Output = (i32, i32);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<(i32, i32)> {
        if let Some(pair) = self.inner.accept_queue.borrow_mut().pop_front() {
            return Poll::Ready(pair);
        }
        *self.inner.accept_waker.borrow_mut() = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for AcceptFuture {
    fn drop(&mut self) {
        // accept_waker is a single Option rewritten by the next poll and taken
        // by the KIND_ACCEPT handler, and there is at most one live
        // AcceptFuture (the eternal accept loop) — withdrawing it just keeps
        // the family's park state complete.
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
        let mut conns = self.inner.conns.borrow_mut();
        // No connection means the peer is gone and its state has been reaped —
        // the same verdict as an explicit close.
        let Some(conn) = conns.get_mut(&self.fd) else {
            return Poll::Ready(None);
        };
        if let Some(buf) = conn.pending.pop_front() {
            // Ownership of the charged `RecvBuf` passes to the caller; its
            // `Drop` refunds the global inbound-byte counter once the caller is
            // done, so the buffer stays accounted for its full residency.
            return Poll::Ready(Some(buf));
        }
        if conn.recv_closed {
            return Poll::Ready(None);
        }
        conn.recv_waiter = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for RecvFuture {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&self.fd) {
            conn.recv_waiter = None;
        }
    }
}

/// Keeps a send's kernel-visible buffer alive until its CQE. Type-erased: the
/// three payloads (a pooled buffer, a W2M ring slot, TLS ciphertext) are only
/// ever held, never inspected, and `Rc` keeps the per-chunk clone in
/// `send_buf_inner` O(1) — an owned buffer would deep-copy once per partial
/// write.
pub(super) type SendAlive = Rc<dyn Any>;

/// What a send op carries until its CQE: its target fd, whose in-flight count
/// the completion handler decrements, and the buffer keep-alive, which exists
/// only to be dropped once the kernel is done with the pointer.
pub(super) type SendCarry = (i32, SendAlive);

pub struct SendFuture {
    pub(super) send_id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for SendFuture {
    type Output = i32;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<i32> {
        self.inner.sends.poll(self.send_id, cx).map(|(rc, _)| rc)
    }
}

impl Drop for SendFuture {
    fn drop(&mut self) {
        // Abandoning keeps the buffer alive for the kernel (it is carried by
        // the slot, not by this future) and tells the late CQE to discard its
        // result.
        self.inner.sends.abandon(self.send_id);
    }
}

/// One-shot raw recv (`Reactor::recv_raw`): the kernel writes into the caller's
/// buffer, which the park slot carries so a late write always lands in live
/// memory, and which comes back with the result so the caller reuses one
/// allocation forever.
pub struct RawRecvFuture {
    pub(super) id: u64,
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for RawRecvFuture {
    type Output = (Vec<u8>, i32);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner
            .raw_recvs
            .poll(self.id, cx)
            .map(|(rc, buf)| (buf.expect("raw recv slot lost its buffer"), rc))
    }
}

impl Drop for RawRecvFuture {
    fn drop(&mut self) {
        // Still in flight: ask the kernel to cancel promptly rather than
        // leaving an idle connection's recv parked until socket death. The
        // buffer stays alive in the slot until the -ECANCELED CQE retires it.
        if self.inner.raw_recvs.abandon(self.id) {
            self.inner
                .ring
                .borrow_mut()
                .prep_async_cancel(udata(KIND_RAW_RECV, self.id), udata(KIND_CANCEL_SINK, 0));
        }
    }
}

// (oneshot, mpsc, AsyncMutex, AsyncRwLock, join_all, select2 live in sync.rs)
