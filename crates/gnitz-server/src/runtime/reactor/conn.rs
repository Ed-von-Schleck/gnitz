//! Reactor client-connection framing: accept / register / recv / send /
//! `close_fd`, plus `handle_recv_cqe` and conn reaping.
//!
//! Nothing here calls `flush_sqes`: an SQE ships with the runloop's own submit
//! at the end of the same tick, and the task awaiting it cannot run before
//! then. Only `fsync` (in `mod.rs`) flushes, to overlap with that tick's CPU
//! work.

use super::futures::{AcceptFuture, RawRecvFuture, RecvFuture, SendAlive, SendFuture};
use super::*;

/// What [`Reactor::send_owned`] sends: an owned payload's byte range, plus the
/// name it goes by in an eviction log line. The three implementors — a pooled
/// buffer, a W2M ring slot, TLS ciphertext — differ only in those two answers,
/// which is why there is one send method rather than one per payload.
pub(crate) trait SendPayload {
    fn bytes(&self) -> &[u8];
    fn what(&self) -> &'static str;
}

impl SendPayload for gnitz_store::storage::batch_pool::PooledSendBuf {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
    fn what(&self) -> &'static str {
        "egress"
    }
}

impl SendPayload for W2mSlot {
    fn bytes(&self) -> &[u8] {
        self.frame_bytes()
    }
    fn what(&self) -> &'static str {
        "ring-slot egress"
    }
}

impl SendPayload for Vec<u8> {
    fn bytes(&self) -> &[u8] {
        self
    }
    fn what(&self) -> &'static str {
        "control-byte egress"
    }
}

/// Per-frame wall-clock deadline for client egress, read from
/// `GNITZ_CLIENT_SEND_TIMEOUT_MS` (default 30 s) once per reactor. The deadline
/// is per-frame, so a client making steady progress across a large train is
/// never penalised — only one that makes zero progress for the full window (a
/// stalled or maliciously zero-window peer) is evicted. Generous by default so
/// ordinary transient congestion never sheds a healthy client; e2e tests shrink
/// it to bound the freeze window they assert on.
pub(super) fn resolve_client_send_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(gnitz_store::foundation::env::env_num(
        "GNITZ_CLIENT_SEND_TIMEOUT_MS",
        30_000,
    ))
}

/// Run one client-bound send under [`client_send_timeout`]. `Peer::send` applies
/// this once, around both transports, so no egress park anywhere is unbounded.
///
/// Why the deadline exists: a client that stops draining its socket pins
/// whatever the send holds — for a `W2mSlot`, its ring space — and enough
/// stalled frames fill a worker's W2M ring and futex-block that
/// single-threaded worker, freezing the cluster.
///
/// On expiry the client is treated as dead: `shutdown(SHUT_RDWR)` makes the
/// in-flight `OP_SEND` error out, then the SAME future is awaited to
/// completion, so whatever its SQE references outlives its CQE. Returns the
/// send rc, clamped negative if the completion raced the deadline.
pub(crate) async fn guard_egress_deadline<F: Future<Output = i32>>(
    reactor: &Reactor,
    fd: i32,
    what: &str,
    fut: F,
) -> i32 {
    let mut fut = std::pin::pin!(fut);
    let timeout = reactor.inner.limits.client_send_timeout;
    match select2(fut.as_mut(), reactor.timer(Instant::now() + timeout)).await {
        Either::A(rc) => rc,
        Either::B(()) => {
            gnitz_warn!("client fd={} stalled {} past {:?}; evicting", fd, what, timeout);
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
    let _ = gnitz_store::foundation::posix_io::retry_eintr(|| unsafe { libc::shutdown(fd, libc::SHUT_RDWR) });
}

/// Arm (or re-arm) `listener`'s multishot accept. The listener fd rides the
/// udata id, so its completions route back to it without reactor state.
fn arm_accept(ring: &mut IoUringRing, listener: i32) {
    ring.prep_accept(listener, udata(KIND_ACCEPT, listener as u32 as u64));
}

/// Arm a recv into `[ptr, ptr+len)` on `fd`. The fd rides the udata id, so the
/// completion routes back to this connection. Not flushed (see the module
/// header).
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
        let mut ring = self.inner.ring();
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
            self.inner.accepts.borrow_mut().push((res, listener));
        }
        if flags & CQE_F_MORE != 0 {
            return;
        }
        if res != -libc::EMFILE && res != -libc::ENFILE {
            arm_accept(&mut self.inner.ring(), listener);
            return;
        }
        // Out of fds: back off before re-arming so `reap_closing_conns` gets a
        // window. One task per cancelled listener — exhaustion is global, so
        // both can cancel at once and each is owed a full backoff.
        let inner = Rc::clone(&self.inner);
        self.spawn(async move {
            let deadline = Instant::now() + inner.limits.accept_rearm_backoff;
            TimerFuture::new(deadline, Rc::clone(&inner)).await;
            arm_accept(&mut inner.ring(), listener);
        });
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
        self.inner
            .ring()
            .prep_recv(fd, buf.as_mut_ptr(), buf.len() as u32, udata(KIND_RAW_RECV, id));
        self.inner.raw_recvs.open(id, Some(buf));
        RawRecvFuture {
            id,
            inner: Rc::clone(&self.inner),
        }
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
        arm_recv(&mut self.inner.ring(), conn, fd, ptr, len);
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

    /// Send `payload`'s whole byte range, returning total bytes sent (>= 0) or
    /// negative errno. The send loop handles `rc < len`: OP_SEND on a stream
    /// socket returns short when the kernel's socket buffer fills up.
    ///
    /// The payload is held by `Rc` until the CQE, so it stays valid for the
    /// kernel however the awaiter ends — and the caller keeps its own handle,
    /// which is how TLS reclaims its ciphertext scratch afterwards.
    ///
    /// Carries no deadline of its own: every caller is inside one applied by
    /// `Peer::send` or, for the TLS flusher's control bytes, by the flusher.
    pub(crate) async fn send_owned<T: SendPayload + 'static>(&self, fd: i32, payload: Rc<T>) -> i32 {
        let (ptr, len) = {
            let b = payload.bytes();
            (b.as_ptr(), b.len())
        };
        self.send_buf_inner(fd, ptr, len, payload).await
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
            self.inner
                .ring()
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
            self.inner.closing_fds.borrow_mut().insert(fd);
            cancel_recv(&mut self.inner.ring(), conn, fd);
        }
    }

    /// Mark `conn`/`fd` closing and finish its recv side, waking any parked
    /// waiter (so its `recv().await` resolves to `None`). Leaving `recv_armed`
    /// false means `reap_closing_conns` fires as soon as `send_inflight`
    /// reaches 0 and the `Peer` is gone, dropping the whole delivery backlog.
    fn begin_recv_close(&self, conn: &mut io::Conn, fd: i32) {
        conn.q.close();
        self.inner.closing_fds.borrow_mut().insert(fd);
    }

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        // Read the closing verdict before taking `conns`: a different `RefCell`,
        // and binding it first leaves the borrow visibly ended before
        // `begin_recv_close` takes `closing_fds` mutably.
        let closing = self.inner.closing_fds.borrow().contains(&fd);
        let mut conns = self.inner.conns.borrow_mut();
        let conn = match conns.get_mut(&fd) {
            Some(c) => c,
            None => return,
        };
        conn.recv_armed = false;

        if res <= 0 || closing {
            self.begin_recv_close(conn, fd);
            return;
        }

        match conn.q.deliver(res as usize, fd) {
            Ok((ptr, len)) => arm_recv(&mut self.inner.ring(), conn, fd, ptr, len),
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
        // iterated the set borrow live. An fd stays here until it is reapable,
        // so this runs on every tick of that window, not once per close.
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
#[path = "tests/conn.rs"]
mod tests;
