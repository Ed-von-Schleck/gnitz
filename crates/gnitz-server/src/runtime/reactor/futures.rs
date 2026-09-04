//! Reactor IO futures: Timer / Reply / ScanSlot (+ ScanLease) / Fsync /
//! Accept / Recv / Send / Exchange — each driven by a CQE or a routed frame
//! waking its registered waker.
//!
//! Everything that waits on a single CQE-delivered result goes through
//! [`super::park::ParkMap`], and everything that waits on the next of many
//! through [`super::wake_queue::WakeQueue`], so `poll` and `Drop` are one line
//! each; only the op-specific submit and teardown live here.

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
        TimerFuture { deadline, timer_id: None, inner }
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
                // First poll: submit the io_uring Timeout SQE, flushed to the
                // kernel by tick's own submit at the end of this tick.
                let id = self.inner.alloc_op_id();
                let ns = self.deadline.duration_since(now).as_nanos() as u64;
                let mut ts = self
                    .inner
                    .spec_pool
                    .borrow_mut()
                    .pop()
                    .unwrap_or_else(|| Box::new(Timespec::new()));
                *ts = Timespec::new()
                    .sec(ns / 1_000_000_000)
                    .nsec((ns % 1_000_000_000) as u32);
                // SAFETY: `ts` is parked as the slot's carry on the next line
                // and stays there until the CQE retires the slot — including
                // across an abandon, whose `-ECANCELED` is what retires it.
                unsafe { self.inner.ring().prep_timeout(&ts, udata(KIND_TIMEOUT, id)) };
                self.inner.timers.open(id, Some(ts));
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
        // KIND_TIMEOUT, which finds an abandoned slot and only recycles the
        // Timespec.
        if self.inner.timers.abandon(id) {
            self.inner
                .ring()
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

/// One scan request id's frames and the awaiter parked on them. Presence in
/// `ReactorShared::scans` is what marks the scan live; deregistering is this
/// stream's only ending, so the queue is never `close`d.
///
/// A queue, not a slot: a worker streams ahead while the master drains another
/// serially. Depth is bounded by ring capacity — each held `W2mSlot` withholds
/// its ring space.
pub(super) type ScanRoute = WakeQueue<W2mSlot>;

pub struct ScanSlotFuture {
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
        route.poll(cx).map(|slot| slot.expect("a scan route is never closed"))
    }
}

impl Drop for ScanSlotFuture {
    fn drop(&mut self) {
        // Only the waker: the ScanLease owns the queue for this req_id. Clearing
        // it here would discard the continuation frames a *resolved* future
        // legitimately leaves behind (between frames there is no live
        // ScanSlotFuture, so the lease is the only scope spanning the operation).
        if let Some(route) = self.inner.scans.borrow_mut().get_mut(&self.req_id) {
            route.clear_waiter();
        }
    }
}

/// RAII guard owning the routing state of a whole scan operation. Registers its
/// req_ids on construction; on drop deregisters them, which drops any frames
/// they queued (dropping a `W2mSlot` advances `release_cursor`, freeing ring
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
        ScanLease { inner, ids: buf, len: ids.len() as u8 }
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
        self.inner
            .accepts
            .borrow_mut()
            .poll(cx)
            .map(|pair| pair.expect("the accept queue is never closed"))
    }
}

impl Drop for AcceptFuture {
    fn drop(&mut self) {
        self.inner.accepts.borrow_mut().clear_waiter();
    }
}

/// The next `FLAG_EXCHANGE` frame, with the worker that published it. Never
/// resolves to `None`: the queue lives as long as the reactor.
pub(super) struct ExchangeFuture {
    pub(super) inner: Rc<ReactorShared>,
}

impl Future for ExchangeFuture {
    type Output = (usize, DecodedWire);
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner
            .exchanges
            .borrow_mut()
            .poll(cx)
            .map(|frame| frame.expect("the exchange queue is never closed"))
    }
}

impl Drop for ExchangeFuture {
    fn drop(&mut self) {
        self.inner.exchanges.borrow_mut().clear_waiter();
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
        conn.q.poll_recv(cx)
    }
}

impl Drop for RecvFuture {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&self.fd) {
            conn.q.clear_waiter();
        }
    }
}

/// A registered connection's fd, obtainable only by holding it open: while a
/// `PeerToken` lives `reap_closing_conns` will not retire the fd, so the number
/// [`Self::fd`] hands out cannot have been recycled to another client. A
/// skipped reap keeps its `closing_fds` entry, so the next one retires it.
pub(crate) struct PeerToken {
    inner: Rc<ReactorShared>,
    fd: i32,
}

impl PeerToken {
    pub(crate) fn new(reactor: &Reactor, fd: i32) -> Self {
        if let Some(conn) = reactor.inner.conns.borrow_mut().get_mut(&fd) {
            conn.peer_held = true;
        }
        PeerToken { inner: Rc::clone(&reactor.inner), fd }
    }

    pub(crate) fn fd(&self) -> i32 {
        self.fd
    }
}

impl Drop for PeerToken {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&self.fd) {
            conn.peer_held = false;
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
                .ring()
                .prep_async_cancel(udata(KIND_RAW_RECV, self.id), udata(KIND_CANCEL_SINK, 0));
        }
    }
}

// (oneshot, chan, AsyncMutex, AsyncRwLock, join_into, select2 live in sync.rs)

#[cfg(test)]
#[path = "tests/futures.rs"]
mod tests;
