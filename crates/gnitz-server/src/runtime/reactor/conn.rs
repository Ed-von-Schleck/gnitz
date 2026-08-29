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
        ms = gnitz_engine::foundation::env::env_num("GNITZ_CLIENT_SEND_TIMEOUT_MS", 30_000);
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
            shutdown(fd);
            fut.await.min(-1)
        }
    }
}

/// Abort both directions of a connected socket, so a pending io_uring `OP_SEND`
/// on `fd` errors out (`ECONNRESET`/`EPIPE`) and its CQE fires — which `close`
/// alone does not do while data is queued. Never closes the fd. A peer already
/// gone (`ENOTCONN`) is the goal state, so every failure but `EINTR` is dropped.
pub(crate) fn shutdown(fd: i32) {
    let _ = gnitz_engine::foundation::posix_io::retry_eintr(|| unsafe { libc::shutdown(fd, libc::SHUT_RDWR) });
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

/// Cancel [`arm_recv`]'s SQE if one is outstanding — kept adjacent because the
/// two must address the same udata. The op's `-ECANCELED` lands in
/// `handle_recv_cqe` as `res < 0`. Cancels the *operation*, not the socket, so
/// it works for the reactor's non-socket fds too.
fn cancel_recv(ring: &mut IoUringRing, conn: &io::Conn, fd: i32) {
    if conn.recv_armed {
        ring.prep_async_cancel(udata(KIND_RECV, fd as u32 as u64), udata(KIND_CANCEL_SINK, 0));
    }
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
    /// socket bytes are undeframed — under TLS they are ciphertext, so no
    /// deframing can run here; the pump feeds its own `RecvQueue` with the
    /// decrypted plaintext instead.
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

    /// Elevate `fd`'s per-connection payload ceiling. Called after the
    /// HELLO handshake validates a connection. Must run synchronously
    /// before any `.await` inside `connection_loop`: the reactor re-arms
    /// `recv` immediately after the HELLO `MessageDone`, so a pipelined
    /// frame's length prefix can arrive while the handshake task is
    /// still parked. If `max_payload_len` is left at the pre-handshake
    /// 8-byte value, `handle_recv_cqe` would disconnect the second frame.
    pub fn set_max_payload_len(&self, fd: i32, limit: usize) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
            conn.q.set_max_payload_len(limit);
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
        let conn = conns
            .entry(fd)
            .or_insert_with(|| Box::new(io::Conn::new(Rc::clone(&self.inner.inbound))));
        let (ptr, len) = conn.q.remaining(); // a fresh queue's window is the 4-byte header
        arm_recv(&mut self.inner.ring.borrow_mut(), conn, fd, ptr, len);
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
    pub async fn send_buffer(&self, fd: i32, buf: gnitz_engine::storage::batch_pool::PooledSendBuf) -> i32 {
        let len = buf.0.len();
        let ptr = buf.0.as_ptr();
        guard_client_egress(self, fd, "egress", self.send_buf_inner(fd, ptr, len, Rc::new(buf))).await
    }

    /// Send the frame bytes of a W2M ring slot directly, without copying,
    /// under the per-frame client-egress deadline.
    ///
    /// The slot is kept alive (release_cursor stays fixed) until the io_uring
    /// OP_SEND CQE fires, at which point the kernel has consumed the data and
    /// the slot is dropped, advancing the cursor. That pin is why every slot
    /// send carries the deadline: a client that stops draining its socket
    /// pins the slot; with enough stalled frames the worker's W2M ring fills
    /// and the single-threaded worker blocks synchronously in `W2mWriter::send_msg`'s
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

    /// Request the reactor close `fd` once nothing can still address it: no
    /// outstanding SQE and no live `PeerToken`. `reap_closing_conns` does the
    /// retiring on a later tick.
    ///
    /// The recv is cancelled rather than waited out — nothing else would ever
    /// complete it, so a client that goes silent after being rejected would
    /// otherwise pin its fd, its `Conn` and any charged `RecvBuf` for as long
    /// as it stays connected.
    pub fn close_fd(&self, fd: i32) {
        if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
            conn.closing = true;
            self.inner.closing_fds.borrow_mut().insert(fd);
            cancel_recv(&mut self.inner.ring.borrow_mut(), conn, fd);
        }
    }

    /// Mark `conn`/`fd` closing and finish its recv side, waking any parked
    /// waiter (so its `recv().await` resolves to `None`). Leaving `recv_armed`
    /// false means `reap_closing_conns` fires as soon as `send_inflight`
    /// reaches 0 and the `Peer` is gone, dropping the whole delivery backlog.
    fn begin_recv_close(&self, conn: &mut io::Conn, fd: i32) {
        conn.closing = true;
        conn.q.close();
        self.inner.closing_fds.borrow_mut().insert(fd);
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

        match conn.q.deliver(res as usize, fd) {
            Ok((ptr, len)) => arm_recv(&mut self.inner.ring.borrow_mut(), conn, fd, ptr, len),
            Err(()) => self.begin_recv_close(conn, fd),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_aborts_connected_socket() {
        // A send after the abort must fail rather than queue — the property
        // `guard_client_egress` leans on. MSG_NOSIGNAL keeps the failing send
        // from raising SIGPIPE and killing the test process.
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(rc, 0, "socketpair failed");
        let (a, b) = (fds[0], fds[1]);
        shutdown(a);
        let buf = [0u8; 4];
        let n = unsafe { libc::send(a, buf.as_ptr() as *const libc::c_void, buf.len(), libc::MSG_NOSIGNAL) };
        assert!(n < 0, "send after SHUT_RDWR must fail, got {n}");
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_shutdown_tolerates_enotconn() {
        // An unconnected socket → shutdown fails with ENOTCONN; the wrapper
        // swallows it, since evicting an already-gone peer is not an error.
        let fd = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        assert!(fd >= 0, "socket() failed");
        shutdown(fd);
        unsafe {
            libc::close(fd);
        }
    }
}
