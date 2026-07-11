//! Reactor client-connection framing: accept / register / recv / the
//! `send_*` family / `close_fd`, plus `handle_recv_cqe` and conn reaping.

use super::futures::{AcceptFuture, RawRecvFuture, RecvFuture, SendAlive, SendFuture};
use super::*;

/// Per-frame wall-clock deadline for client egress, read once from
/// `GNITZ_CLIENT_SEND_TIMEOUT_MS` (default 30 s). Applied to zero-copy
/// ring-slot sends on the fd path and to EVERY client-bound TLS send (the
/// TLS `send_mutex` is held across sends, so an unbounded send would wedge
/// the whole connection). The deadline is per-frame, so a client making
/// steady progress across a large train is never penalised — only one that
/// makes zero progress for the full window (a stalled or maliciously
/// zero-window peer) is evicted. Generous by default so ordinary transient
/// congestion never sheds a healthy client; e2e tests shrink it to bound
/// the freeze window they assert on.
pub(crate) fn client_send_timeout() -> std::time::Duration {
    static TIMEOUT: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        let ms = std::env::var("GNITZ_CLIENT_SEND_TIMEOUT_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(30_000);
        std::time::Duration::from_millis(ms)
    })
}

impl Reactor {
    /// Allocate a per-send id distinct from reply / fsync id space. Also
    /// allocates `recv_raw` op ids (shared counter, disjoint park maps —
    /// no collisions either way).
    pub(super) fn alloc_send_id(&self) -> u64 {
        let id = self.inner.next_send_id.get();
        let next = match id.checked_add(1) {
            Some(n) if n != u64::MAX => n,
            _ => 1,
        };
        self.inner.next_send_id.set(next);
        id
    }

    /// Attach a listen socket fd and arm its multishot-accept SQE. Callable
    /// once per listener (AF_UNIX + optional TLS); the listener fd rides the
    /// SQE's udata `id` field so each accepted connection resolves as
    /// `(conn_fd, listener_fd)`.
    pub fn attach_listener(&self, listener_fd: i32) {
        let mut ring = self.inner.ring.borrow_mut();
        ring.prep_accept(listener_fd, udata(KIND_ACCEPT, listener_fd as u32 as u64));
        if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
            crate::gnitz_fatal_abort!(
                "reactor: accept SQE flush failed (errno={}) — no connections can be accepted",
                e,
            );
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
        let id = self.alloc_send_id();
        // No eager flush: like the fd path's steady-state recv re-arm, the
        // queued SQE ships with the runloop's own submit when the caller
        // parks — same tick, one fewer io_uring_enter per inbound chunk.
        self.inner
            .ring
            .borrow_mut()
            .prep_recv(fd, buf.as_mut_ptr(), buf.len() as u32, udata(KIND_RAW_RECV, id));
        RawRecvFuture {
            id,
            buf: Some(buf),
            inner: Rc::clone(&self.inner),
        }
    }

    /// Send the whole `cipher` buffer, looping partial `OP_SEND` completions
    /// like the fd send path. `Rc`-wrapped (not owned): `send_buf_inner`
    /// clones the keep-alive per partial-write chunk, so an owned `Vec`
    /// would deep-copy the ciphertext once per chunk.
    pub async fn send_raw(&self, fd: i32, cipher: Rc<Vec<u8>>) -> i32 {
        let (ptr, len) = (cipher.as_ptr(), cipher.len());
        self.send_buf_inner(fd, ptr, len, SendAlive::RcVec(cipher)).await
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
        // Clear any stale close-sentinel from a prior incarnation of
        // this fd number (kernel may reuse fds after close).
        self.inner.recv_closed.borrow_mut().remove(&fd);
        // Stray pending_recv entries at this point mean the previous
        // incarnation of this fd closed without `reap_closing_conns`
        // freeing its payload buffers — a bug. Assert in debug; in
        // release, free the buffers so we don't leak.
        let stale = self.inner.pending_recv.borrow_mut().remove(&fd);
        if let Some(stale) = stale {
            if !stale.is_empty() {
                crate::gnitz_fatal_abort!(
                    "reactor: register_conn: {} stale pending_recv entries for fd={} — \
                     reap_closing_conns did not run before fd was reused; \
                     freeing would be a use-after-free if any SQEs are still in flight",
                    stale.len(),
                    fd,
                );
            }
        }
        let mut conns = self.inner.conns.borrow_mut();
        conns.insert(fd, Box::new(io::Conn::new()));
        let conn = conns.get_mut(&fd).unwrap();
        let hdr_ptr = conn.recv_state.hdr_buf_ptr();
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_recv(fd, hdr_ptr, 4, udata(KIND_RECV, fd as u32 as u64));
            ring.flush_sqes("recv");
        }
        conn.recv_armed = true;
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

    /// Returns total bytes sent (>= 0) or negative errno. The loop is
    /// load-bearing: OP_SEND on a stream socket may return rc < len
    /// when the kernel's socket buffer fills up.
    pub async fn send_buffer(&self, fd: i32, buf: crate::storage::batch_pool::PooledSendBuf) -> i32 {
        let len = buf.0.len();
        let ptr = buf.0.as_ptr();
        self.send_buf_inner(fd, ptr, len, SendAlive::Pooled(Rc::new(buf))).await
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
    /// freeze. All ring-slot egress (scan trains, seek replies) shares this
    /// one guarded primitive, so no forwarding path can reintroduce the
    /// freeze by picking an unguarded variant.
    ///
    /// If the send makes no progress within `client_send_timeout()`, the
    /// client is treated as dead: `shutdown(SHUT_RDWR)` forces the in-flight
    /// `OP_SEND` to error out promptly, then the SAME send future is awaited
    /// to completion so the held `W2mSlot` is released (advancing
    /// consume_cursor) only AFTER its CQE — never dropped while an SQE still
    /// references its buffer. Returns the send rc: `>= 0` sent, `< 0` the
    /// client disconnected or was evicted (the post-`shutdown` rc, clamped
    /// negative in case the completion raced the deadline).
    ///
    /// Passing `send_fut.as_mut()` (a `Pin<&mut>` — itself a `Future`) to
    /// `select2` means the timer winning drops only that borrow, never the send
    /// future, so it stays owned here for the mandatory post-`shutdown` await.
    pub async fn send_slot(&self, fd: i32, slot: W2mSlot) -> i32 {
        let frame = slot.frame_bytes();
        let (ptr, len) = (frame.as_ptr(), frame.len());
        let mut send_fut = std::pin::pin!(self.send_buf_inner(fd, ptr, len, SendAlive::Slot(Rc::new(slot))));
        let deadline = Instant::now() + client_send_timeout();
        match select2(send_fut.as_mut(), self.timer(deadline)).await {
            Either::A(rc) => rc,
            Either::B(()) => {
                crate::gnitz_warn!(
                    "reactor: client fd={} stalled ring-slot egress past {:?}; evicting to free the W2M ring",
                    fd,
                    client_send_timeout(),
                );
                crate::foundation::posix_io::shutdown(fd);
                send_fut.await.min(-1)
            }
        }
    }

    /// Send a `'static` byte buffer (e.g. the precomputed HELLO ACK): no
    /// per-connection allocation or liveness tracking is required — the
    /// kernel pointer remains valid for the lifetime of the process.
    pub async fn send_static(&self, fd: i32, bytes: &'static [u8]) -> i32 {
        self.send_buf_inner(fd, bytes.as_ptr(), bytes.len(), SendAlive::Static)
            .await
    }

    /// Common send loop. `alive` keeps the backing memory valid until the CQE fires.
    async fn send_buf_inner(&self, fd: i32, ptr: *const u8, len: usize, alive: SendAlive) -> i32 {
        let mut sent: usize = 0;
        let mut final_rc: i32 = 0;
        while sent < len {
            let send_id = self.alloc_send_id();
            if let Some(conn) = self.inner.conns.borrow_mut().get_mut(&fd) {
                conn.send_inflight += 1;
            }
            self.inner.send_fd_for_id.borrow_mut().insert(send_id, fd);
            let cur_ptr = unsafe { ptr.add(sent) };
            let remaining = (len - sent) as u32;
            {
                let mut ring = self.inner.ring.borrow_mut();
                ring.prep_send(fd, cur_ptr, remaining, udata(KIND_SEND, send_id));
                ring.flush_sqes("send");
            }
            let rc = SendFuture {
                send_id,
                _alive: Some(alive.clone()),
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
    /// `recv().await` resolves to `None`). Borrows only the sibling
    /// `RefCell`s — never `conns` — so it composes with a caller that holds a
    /// live `conns` borrow and the `&mut Conn` it hands in. Omits the trailing
    /// `return`, so each caller keeps its own control flow: leaving
    /// `recv_armed` false means `reap_closing_conns` fires as soon as
    /// `send_inflight` reaches 0, dropping the whole `pending_recv` backlog.
    fn begin_recv_close(&self, conn: &mut io::Conn, fd: i32) {
        conn.closing = true;
        self.inner.closing_fds.borrow_mut().insert(fd);
        self.inner.recv_closed.borrow_mut().insert(fd);
        if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
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
                self.inner
                    .ring
                    .borrow_mut()
                    .prep_recv(fd, buf, len, udata(KIND_RECV, fd as u32 as u64));
                conn.recv_armed = true;
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
                    crate::gnitz_warn!(
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
                self.inner
                    .ring
                    .borrow_mut()
                    .prep_recv(fd, pbuf, plen as u32, udata(KIND_RECV, fd as u32 as u64));
                conn.recv_armed = true;
            }
            io::RecvAdvance::MessageDone => {
                // The charged `RecvBuf` moves from the recv state machine into
                // the delivery queue; its accounting rides along untouched.
                let rbuf = conn.recv_state.take_message();
                self.inner
                    .pending_recv
                    .borrow_mut()
                    .entry(fd)
                    .or_default()
                    .push_back(rbuf);
                // Arm next header recv immediately so the kernel can keep
                // draining the client's send buffer. Per-session FIFO is
                // preserved by the `VecDeque` order — the handler still
                // consumes messages in arrival order.
                let hdr = conn.recv_state.hdr_buf_ptr();
                self.inner
                    .ring
                    .borrow_mut()
                    .prep_recv(fd, hdr, 4, udata(KIND_RECV, fd as u32 as u64));
                conn.recv_armed = true;
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
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
            let ready = {
                let conns = self.inner.conns.borrow();
                match conns.get(&fd) {
                    Some(conn) => !conn.has_outstanding(),
                    None => true,
                }
            };
            if !ready {
                continue;
            }
            self.inner.closing_fds.borrow_mut().remove(&fd);
            // Dropping the `Conn` (with its in-flight `recv_state` payload) and
            // the `pending_recv` queue frees every undrained `RecvBuf`; each
            // one's `Drop` refunds its charge to the global counter, so a reaped
            // connection's buffers never leak the accounting upward.
            self.inner.conns.borrow_mut().remove(&fd);
            self.inner.recv_waiters.borrow_mut().remove(&fd);
            self.inner.pending_recv.borrow_mut().remove(&fd);
            unsafe {
                libc::close(fd);
            }
        }
    }
}
