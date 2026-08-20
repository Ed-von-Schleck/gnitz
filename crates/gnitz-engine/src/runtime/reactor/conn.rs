//! Reactor client-connection framing: accept / register / recv / the
//! `send_*` family / `close_fd`, plus `handle_recv_cqe` and conn reaping.

use super::futures::{AcceptFuture, RawRecvFuture, RecvFuture, SendAlive, SendFuture};
use super::*;

/// Cached [`client_send_timeout`] in milliseconds; `0` = not yet read.
static CLIENT_SEND_TIMEOUT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Per-frame wall-clock deadline for client egress, read once from
/// `GNITZ_CLIENT_SEND_TIMEOUT_MS` (default 30 s). The deadline is per-frame, so
/// a client making steady progress across a large train is never penalised —
/// only one that makes zero progress for the full window (a stalled or
/// maliciously zero-window peer) is evicted. Generous by default so ordinary
/// transient congestion never sheds a healthy client; e2e tests shrink it to
/// bound the freeze window they assert on.
pub(crate) fn client_send_timeout() -> std::time::Duration {
    use std::sync::atomic::Ordering::Relaxed;
    let mut ms = CLIENT_SEND_TIMEOUT_MS.load(Relaxed);
    if ms == 0 {
        ms = crate::foundation::env::env_num("GNITZ_CLIENT_SEND_TIMEOUT_MS", 30_000);
        CLIENT_SEND_TIMEOUT_MS.store(ms, Relaxed);
    }
    std::time::Duration::from_millis(ms)
}

/// Shorten the deadline for a test that has to wait one out. Overrides whatever
/// an earlier send already cached, so it does not depend on test order; every
/// caller sets the same value, so concurrent tests agree.
#[cfg(test)]
pub(crate) fn force_client_send_timeout(d: std::time::Duration) {
    CLIENT_SEND_TIMEOUT_MS.store(d.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Run one client-bound send under [`client_send_timeout`]. Every egress path
/// on both transports goes through here, so none can reintroduce an unbounded
/// park by picking a different primitive.
///
/// On expiry the client is treated as dead: `shutdown(SHUT_RDWR)` forces the
/// in-flight `OP_SEND` to error out promptly, then the SAME future is awaited
/// to completion, so whatever its SQE still references — a pinned `W2mSlot`, a
/// pooled buffer, the TLS `send_mutex` — outlives its CQE. Returns the send rc:
/// `>= 0` sent, `< 0` the client disconnected or was evicted (clamped negative
/// in case the completion raced the deadline).
///
/// `select2` gets `fut.as_mut()` (a `Pin<&mut>` — itself a `Future`), so the
/// timer winning drops only that borrow, never the send future: it stays owned
/// here for the mandatory post-`shutdown` await.
pub(crate) async fn guard_client_egress<F: Future<Output = i32>>(
    reactor: &Reactor,
    fd: i32,
    what: &str,
    fut: F,
) -> i32 {
    let mut fut = std::pin::pin!(fut);
    let deadline = Instant::now() + client_send_timeout();
    match select2(fut.as_mut(), reactor.timer(deadline)).await {
        Either::A(rc) => rc,
        Either::B(()) => {
            gnitz_warn!(
                "client fd={} stalled {} past {:?}; evicting",
                fd,
                what,
                client_send_timeout(),
            );
            crate::foundation::posix_io::shutdown(fd);
            fut.await.min(-1)
        }
    }
}

/// Arm (or re-arm) `listener`'s multishot accept. The listener fd rides the
/// udata id, so its completions route back to it without reactor state.
fn arm_accept(ring: &mut IoUringRing, listener: i32) {
    ring.prep_accept(listener, udata(KIND_ACCEPT, listener as u32 as u64));
}

/// Arm a recv into `[ptr, ptr+len)` on `fd`. The fd rides the udata id, so the
/// completion routes back to this connection. Not flushed eagerly: the SQE
/// ships with the runloop's own submit at the end of the same tick — one fewer
/// io_uring_enter per inbound chunk, and the awaiting task cannot run before
/// then anyway.
fn arm_recv(ring: &mut IoUringRing, conn: &mut io::Conn, fd: i32, ptr: *mut u8, len: u32) {
    ring.prep_recv(fd, ptr, len, udata(KIND_RECV, fd as u32 as u64));
    conn.recv_armed = true;
}

impl Reactor {
    /// Attach a listen socket fd and arm its multishot-accept SQE. Callable
    /// once per listener (AF_UNIX + optional TLS); the listener fd rides the
    /// SQE's udata `id` field so each accepted connection resolves as
    /// `(conn_fd, listener_fd)`.
    pub fn attach_listener(&self, listener_fd: i32) {
        let mut ring = self.inner.ring.borrow_mut();
        arm_accept(&mut ring, listener_fd);
        if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
            gnitz_fatal_abort!(
                "reactor: accept SQE flush failed (errno={}) — no connections can be accepted",
                e,
            );
        }
    }

    /// Route an accept completion: queue the accepted fd for the accept loop
    /// and, when the multishot SQE has been cancelled, re-arm the listener.
    pub(super) fn handle_accept_cqe(&self, listener: i32, res: i32, flags: u32) {
        if res >= 0 {
            self.inner.accept_queue.borrow_mut().push_back((res, listener));
            if let Some(w) = self.inner.accept_waker.borrow_mut().take() {
                w.wake();
            }
        }
        if flags & CQE_F_MORE != 0 {
            return;
        }
        if res != -libc::EMFILE && res != -libc::ENFILE {
            arm_accept(&mut self.inner.ring.borrow_mut(), listener);
            return;
        }
        // Out of fds: back off ~50 ms so reap_closing_conns can free some
        // first. Exhaustion is global, so both listeners' accepts can cancel in
        // the same window — the pending set records every one and a single
        // backoff task, spawned on the 0→1 transition, re-arms them all.
        let first = {
            let mut pending = self.inner.accept_rearm_pending.borrow_mut();
            pending.insert(listener) && pending.len() == 1
        };
        if first {
            let inner = Rc::clone(&self.inner);
            self.spawn(async move {
                let deadline = Instant::now() + std::time::Duration::from_millis(50);
                TimerFuture::new(deadline, Rc::clone(&inner)).await;
                let pending: Vec<i32> = inner.accept_rearm_pending.borrow_mut().drain().collect();
                let mut ring = inner.ring.borrow_mut();
                for lfd in pending {
                    arm_accept(&mut ring, lfd);
                }
            });
        }
    }

    /// Future resolving to the next newly-accepted `(conn_fd, listener_fd)`
    /// pair. Called by the accept-loop task.
    pub fn accept(&self) -> AcceptFuture {
        AcceptFuture {
            inner: Rc::clone(&self.inner),
        }
    }

    /// One-shot raw recv into the caller's buffer, resolving with
    /// `(buffer, byte_count)` (≤ 0 = EOF/error). The buffer round-trips so
    /// the caller (the TLS read pump) reuses one allocation forever. The
    /// socket bytes are undeframed — under TLS they are ciphertext, so the
    /// fd path's `RecvState` machinery cannot run here; the pump deframes
    /// the decrypted plaintext itself.
    pub fn recv_raw(&self, fd: i32, mut buf: Vec<u8>) -> RawRecvFuture {
        let id = self.inner.alloc_op_id();
        // No eager flush: like the fd path's steady-state recv re-arm, the
        // queued SQE ships with the runloop's own submit when the caller
        // parks — same tick, one fewer io_uring_enter per inbound chunk.
        self.inner
            .ring
            .borrow_mut()
            .prep_recv(fd, buf.as_mut_ptr(), buf.len() as u32, udata(KIND_RAW_RECV, id));
        self.inner.raw_recvs.open(id, Some(buf));
        RawRecvFuture {
            id,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Send the whole `cipher` buffer, looping partial `OP_SEND` completions
    /// like the fd send path. `Rc`-wrapped (not owned): `send_buf_inner`
    /// clones the keep-alive per partial-write chunk, so an owned `Vec`
    /// would deep-copy the ciphertext once per chunk.
    pub async fn send_raw(&self, fd: i32, cipher: Rc<Vec<u8>>) -> i32 {
        let (ptr, len) = (cipher.as_ptr(), cipher.len());
        self.send_buf_inner(fd, ptr, len, cipher).await
    }

    /// Charge and allocate one inbound frame payload buffer against the
    /// global inbound-memory cap — the single accounting point shared by the
    /// fd recv path (`handle_recv_cqe`) and the TLS pump. Charges
    /// `frame_weight(plen)` at header-parse time, *before* any payload byte
    /// arrives, so a declared-but-dribbled frame can never accumulate
    /// uncounted bytes; `None` = cap breach or malloc failure, refused
    /// before allocation. Intentionally silent: the caller logs (it knows
    /// the fd).
    pub(crate) fn alloc_inbound_buf(&self, plen: usize) -> Option<io::RecvBuf> {
        let w = io::frame_weight(plen);
        if self.inner.total_inbound_bytes.get() + w > self.inner.global_cap.get() {
            return None; // refuse before malloc — no overshoot
        }
        // SAFETY: plen > 0 (zero-length frames are the close sentinel,
        // rejected before this call); null is checked below.
        let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
        if pbuf.is_null() {
            return None;
        }
        // RecvBuf::new charges frame_weight(plen); its Drop refunds.
        Some(io::RecvBuf::new(pbuf, plen, Rc::clone(&self.inner.total_inbound_bytes)))
    }

    /// Held bytes under the global inbound cap (test observability).
    #[cfg(test)]
    pub(crate) fn total_inbound_bytes(&self) -> usize {
        self.inner.total_inbound_bytes.get()
    }

    /// Elevate `fd`'s per-connection payload ceiling. Called after the
    /// HELLO handshake validates a connection. Must run synchronously
    /// before any `.await` inside `connection_loop`: the reactor re-arms
    /// `recv` immediately after the HELLO `MessageDone`, so a pipelined
    /// frame's length prefix can arrive while the handshake task is
    /// still parked. If `max_payload_len` is left at the pre-handshake
    /// 8-byte value, `handle_recv_cqe` would disconnect the second frame.
    pub fn set_max_payload_len(&self, fd: i32, limit: usize) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
            conn.max_payload_len = limit;
        }
    }

    /// Initial arm of a recv SQE on a new connection fd.
    pub fn register_conn(&self, fd: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        // The kernel reuses fd numbers after close, but only `reap_closing_conns`
        // closes an fd and it removes the `Conn` in the same step. A live entry
        // here means the number was reused while SQEs may still point into the
        // old connection's buffers.
        if conns.contains_key(&fd) {
            gnitz_fatal_abort!(
                "reactor: register_conn: fd={} is already registered — it was reused \
                 before reap_closing_conns retired the previous connection",
                fd,
            );
        }
        let conn = conns.entry(fd).or_insert_with(|| Box::new(io::Conn::new()));
        let hdr_ptr = conn.recv_state.hdr_buf_ptr();
        arm_recv(
            &mut self.inner.ring.borrow_mut(),
            conn,
            fd,
            hdr_ptr,
            gnitz_wire::FRAME_LEN_PREFIX_BYTES as u32,
        );
    }

    /// Future resolving to the next complete message on `fd` as an owned
    /// [`io::RecvBuf`] (freed on drop), or `None` when the peer has
    /// disconnected.  Each call drains at most one message.
    pub fn recv(&self, fd: i32) -> RecvFuture {
        RecvFuture {
            fd,
            inner: Rc::clone(&self.inner),
        }
    }

    /// Send a whole owned buffer, returning total bytes sent (>= 0) or negative
    /// errno. The send loop handles `rc < len`: OP_SEND on a stream socket
    /// returns short when the kernel's socket buffer fills up.
    ///
    /// Carries the egress deadline like [`Self::send_slot`]. The master-authored
    /// frames on this path pin no W2M ring slot, but a scan's terminal frame is
    /// sent while `handle_scan` still holds the catalog read guard, so a stalled
    /// client would block every DDL — and every reader queued behind a waiting
    /// writer — for as long as it stalls.
    pub async fn send_buffer(&self, fd: i32, buf: crate::storage::batch_pool::PooledSendBuf) -> i32 {
        let len = buf.0.len();
        let ptr = buf.0.as_ptr();
        guard_client_egress(self, fd, "egress", self.send_buf_inner(fd, ptr, len, Rc::new(buf))).await
    }

    /// Send the frame bytes of a W2M ring slot directly, without copying,
    /// under the per-frame client-egress deadline.
    ///
    /// The slot is kept alive (consume_cursor stays fixed) until the io_uring
    /// OP_SEND CQE fires, at which point the kernel has consumed the data and
    /// the slot is dropped, advancing the cursor. That pin is why every slot
    /// send carries the deadline: a client that stops draining its socket
    /// pins the slot; with enough stalled frames the worker's W2M ring fills
    /// and the single-threaded worker blocks synchronously in `send_encoded`'s
    /// futex, starving every other client's SAL progress — a cluster-wide
    /// freeze. On expiry [`guard_client_egress`] releases the slot only AFTER
    /// the send's CQE, never while an SQE still references its buffer.
    pub async fn send_slot(&self, fd: i32, slot: W2mSlot) -> i32 {
        let frame = slot.frame_bytes();
        let (ptr, len) = (frame.as_ptr(), frame.len());
        guard_client_egress(
            self,
            fd,
            "ring-slot egress",
            self.send_buf_inner(fd, ptr, len, Rc::new(slot)),
        )
        .await
    }

    /// Common send loop. `alive` keeps the backing memory valid until the CQE fires.
    async fn send_buf_inner(&self, fd: i32, ptr: *const u8, len: usize, alive: SendAlive) -> i32 {
        let mut sent: usize = 0;
        let mut final_rc: i32 = 0;
        while sent < len {
            let send_id = self.inner.alloc_op_id();
            if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
                conn.send_inflight += 1;
            }
            let cur_ptr = unsafe { ptr.add(sent) };
            let remaining = (len - sent) as u32;
            // No eager flush: the SQE ships with the runloop's own submit at the
            // end of this tick, and this task parks on the CQE until then either
            // way — one fewer io_uring_enter per chunk.
            self.inner
                .ring
                .borrow_mut()
                .prep_send(fd, cur_ptr, remaining, udata(KIND_SEND, send_id));
            self.inner.sends.open(send_id, Some((fd, Rc::clone(&alive))));
            let rc = SendFuture {
                send_id,
                inner: Rc::clone(&self.inner),
            }
            .await;
            if rc < 0 {
                final_rc = rc;
                break;
            }
            if rc == 0 {
                break;
            } // connection closed / EOF
            sent += rc as usize;
        }
        if final_rc < 0 {
            final_rc
        } else {
            sent as i32
        }
    }

    /// Request the reactor close `fd` once all outstanding SQEs
    /// complete.  Marks the connection as closing; `reap_closing` in
    /// the tick loop frees the fd when its recv/send slots go quiet.
    pub fn close_fd(&self, fd: i32) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
            conn.closing = true;
            self.inner.closing_fds.borrow_mut().insert(fd);
        }
    }

    /// Mark `conn`/`fd` closing and wake any parked recv waiter (so its
    /// `recv().await` resolves to `None`). Leaving `recv_armed` false means
    /// `reap_closing_conns` fires as soon as `send_inflight` reaches 0,
    /// dropping the whole delivery backlog.
    fn begin_recv_close(&self, conn: &mut io::Conn, fd: i32) {
        conn.closing = true;
        conn.recv_closed = true;
        self.inner.closing_fds.borrow_mut().insert(fd);
        if let Some(w) = conn.recv_waiter.take() {
            w.wake();
        }
    }

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        let conn = match conns.get_mut(&fd) {
            Some(c) => c,
            None => return,
        };
        conn.recv_armed = false;

        if res <= 0 || conn.closing {
            self.begin_recv_close(conn, fd);
            return;
        }

        match conn.recv_state.advance(res as usize) {
            io::RecvAdvance::NeedMore => {
                let (buf, len) = conn.recv_state.remaining();
                arm_recv(&mut self.inner.ring.borrow_mut(), conn, fd, buf, len);
            }
            io::RecvAdvance::HeaderDone => {
                let plen = conn.recv_state.payload_len();
                if plen > conn.max_payload_len {
                    self.begin_recv_close(conn, fd);
                    return;
                }
                // `alloc_inbound_buf` charges `frame_weight(plen)` (refunded
                // by the RecvBuf's `Drop`), refusing before malloc on a cap
                // breach.
                let Some(rbuf) = self.alloc_inbound_buf(plen) else {
                    gnitz_warn!(
                        "reactor: inbound cap would be exceeded, closing fd={} (held={} B + {} B, cap={} B)",
                        fd,
                        self.inner.total_inbound_bytes.get(),
                        io::frame_weight(plen),
                        self.inner.global_cap.get(),
                    );
                    self.begin_recv_close(conn, fd);
                    return;
                };
                let pbuf = rbuf.ptr;
                conn.recv_state.start_payload(rbuf);
                arm_recv(&mut self.inner.ring.borrow_mut(), conn, fd, pbuf, plen as u32);
            }
            io::RecvAdvance::MessageDone => {
                // The charged `RecvBuf` moves from the recv state machine into
                // the delivery queue; its accounting rides along untouched.
                let rbuf = conn.recv_state.take_message();
                conn.pending.push_back(rbuf);
                // Arm the next header recv immediately so the kernel can keep
                // draining the client's send buffer. Per-session FIFO is
                // preserved by the queue order — the handler still consumes
                // messages in arrival order.
                let hdr = conn.recv_state.hdr_buf_ptr();
                arm_recv(
                    &mut self.inner.ring.borrow_mut(),
                    conn,
                    fd,
                    hdr,
                    gnitz_wire::FRAME_LEN_PREFIX_BYTES as u32,
                );
                if let Some(w) = conn.recv_waiter.take() {
                    w.wake();
                }
            }
            io::RecvAdvance::Disconnect => {
                self.begin_recv_close(conn, fd);
            }
        }
    }

    /// Reap connections that are closing and have no outstanding SQEs.
    /// Called once per tick. Iterates only `closing_fds` (O(closing)),
    /// not all connections.
    pub(super) fn reap_closing_conns(&self) {
        if self.inner.closing_fds.borrow().is_empty() {
            return;
        }
        // Copy the fd set into a local before the loop: the body re-borrows
        // `closing_fds` and `conns`, which would panic the RefCell if we
        // iterated the set borrow live. The alloc only happens on a
        // non-empty reap (rare — the `is_empty` gate above).
        let closing: Vec<i32> = self.inner.closing_fds.borrow().iter().copied().collect();
        for &fd in &closing {
            // Dropping the `Conn` frees every undrained `RecvBuf` — the
            // in-flight payload and the delivery queue alike — and each one's
            // `Drop` refunds its charge, so a reaped connection never leaks the
            // accounting upward.
            let retired = {
                let mut conns = self.inner.conns.borrow_mut();
                match conns.get(&fd) {
                    Some(conn) if conn.has_outstanding() => false,
                    _ => {
                        conns.remove(&fd);
                        true
                    }
                }
            };
            if !retired {
                continue;
            }
            self.inner.closing_fds.borrow_mut().remove(&fd);
            unsafe {
                libc::close(fd);
            }
        }
    }
}
