//! Reactor client-connection framing: accept / register / recv / the
//! `send_*` family / `close_fd`, plus `handle_recv_cqe` and conn reaping.

use super::*;
use super::futures::{AcceptFuture, RecvFuture, SendAlive, SendFuture};

impl Reactor {
    /// Allocate a per-send id distinct from reply / fsync id space.
    fn alloc_send_id(&self) -> u64 {
        let id = self.inner.next_send_id.get();
        let next = match id.checked_add(1) {
            Some(n) if n != u64::MAX => n,
            _ => 1,
        };
        self.inner.next_send_id.set(next);
        id
    }

    /// Attach the listen socket fd and arm a multishot-accept SQE.
    pub fn attach_server_fd(&self, server_fd: i32) {
        self.inner.server_fd.set(server_fd);
        {
            let mut ring = self.inner.ring.borrow_mut();
            ring.prep_accept(server_fd, udata(KIND_ACCEPT, 0));
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_fatal_abort!(
                    "reactor: accept SQE flush failed (errno={}) — no connections can be accepted",
                    e,
                );
            }
        }
    }

    /// Future resolving to the next newly-accepted fd. Called by the
    /// accept-loop task.
    pub fn accept(&self) -> AcceptFuture {
        AcceptFuture { inner: Rc::clone(&self.inner) }
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
                    stale.len(), fd,
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
            if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                crate::gnitz_error!(
                    "reactor: recv SQE flush failed for fd={} (errno={}); \
                     SQE queued — will submit on next tick",
                    fd, e,
                );
            }
        }
        conn.recv_armed = true;
    }

    /// Future resolving to the next complete message on `fd`, or `None`
    /// when the peer has disconnected.  Each call drains at most one
    /// message; the caller is responsible for `libc::free`-ing the
    /// returned ptr.
    pub fn recv(&self, fd: i32) -> RecvFuture {
        RecvFuture { fd, inner: Rc::clone(&self.inner) }
    }

    /// Returns total bytes sent (>= 0) or negative errno. The loop is
    /// load-bearing: OP_SEND on a stream socket may return rc < len
    /// when the kernel's socket buffer fills up.
    pub async fn send_buffer(&self, fd: i32, buf: crate::storage::batch_pool::PooledSendBuf) -> i32 {
        let len = buf.0.len();
        let ptr = buf.0.as_ptr();
        self.send_buf_inner(fd, ptr, len, SendAlive::Pooled(Rc::new(buf))).await
    }

    /// Send the frame bytes of a W2M ring slot directly, without copying.
    ///
    /// The slot is kept alive (consume_cursor stays fixed) until the io_uring
    /// OP_SEND CQE fires, at which point the kernel has consumed the data and
    /// the slot is dropped, advancing the cursor.
    pub async fn send_slot(&self, fd: i32, slot: W2mSlot) -> i32 {
        let frame = slot.frame_bytes();
        let (ptr, len) = (frame.as_ptr(), frame.len());
        self.send_buf_inner(fd, ptr, len, SendAlive::Slot(Rc::new(slot))).await
    }

    /// Send the precomputed OK HELLO ACK frame. The bytes are a `'static`
    /// const, so no per-connection allocation or liveness tracking is
    /// required.
    pub async fn send_hello_ack(&self, fd: i32) -> i32 {
        const OK_ACK: [u8; gnitz_wire::HELLO_ACK_FRAME_SIZE] = gnitz_wire::encode_hello_ack(
            gnitz_wire::HELLO_STATUS_OK,
            gnitz_wire::MAX_FRAME_PAYLOAD_SERVER as u32,
        );
        self.send_buf_inner(fd, OK_ACK.as_ptr(), OK_ACK.len(), SendAlive::Static).await
    }

    /// Send a one-shot response buffer, closing `fd` on transport failure.
    /// This is the terminal action shared by every request handler: once the
    /// reply is on the wire there is nothing left to do on the connection, so a
    /// negative send rc (peer gone / write error) simply schedules the close.
    pub async fn send_buffer_or_close(&self, fd: i32, buf: crate::storage::batch_pool::PooledSendBuf) {
        if self.send_buffer(fd, buf).await < 0 { self.close_fd(fd); }
    }

    /// `send_slot` counterpart of [`Self::send_buffer_or_close`]: forward a
    /// worker ring slot as the final reply, closing `fd` on transport failure.
    pub async fn send_slot_or_close(&self, fd: i32, slot: W2mSlot) {
        if self.send_slot(fd, slot).await < 0 { self.close_fd(fd); }
    }

    /// Common send loop. `alive` keeps the backing memory valid until the CQE fires.
    async fn send_buf_inner(
        &self, fd: i32,
        ptr: *const u8, len: usize,
        alive: SendAlive,
    ) -> i32 {
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
                if let Err(e) = ring.submit_and_wait_timeout(0, 0) {
                    crate::gnitz_error!(
                        "reactor: send SQE flush failed for send_id={} (errno={}); \
                         SQE queued — will submit on next tick",
                        send_id, e,
                    );
                }
            }
            let rc = SendFuture {
                send_id,
                _alive: Some(alive.clone()),
                inner: Rc::clone(&self.inner),
            }.await;
            if rc < 0 { final_rc = rc; break; }
            if rc == 0 { break; }  // connection closed / EOF
            sent += rc as usize;
        }
        if final_rc < 0 { final_rc } else { sent as i32 }
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

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        let conn = match conns.get_mut(&fd) {
            Some(c) => c,
            None => return,
        };
        conn.recv_armed = false;

        if res <= 0 || conn.closing {
            conn.closing = true;
            self.inner.closing_fds.borrow_mut().insert(fd);
            self.inner.recv_closed.borrow_mut().insert(fd, true);
            if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                w.wake();
            }
            return;
        }

        match conn.recv_state.advance(res as usize) {
            io::RecvAdvance::NeedMore => {
                let (buf, len) = conn.recv_state.remaining();
                self.inner.ring.borrow_mut().prep_recv(
                    fd, buf, len, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
            }
            io::RecvAdvance::HeaderDone => {
                let plen = conn.recv_state.payload_len();
                if plen > conn.max_payload_len {
                    conn.closing = true;
                    self.inner.closing_fds.borrow_mut().insert(fd);
                    self.inner.recv_closed.borrow_mut().insert(fd, true);
                    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                        w.wake();
                    }
                    return;
                }
                let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
                if pbuf.is_null() {
                    conn.closing = true;
                    self.inner.closing_fds.borrow_mut().insert(fd);
                    self.inner.recv_closed.borrow_mut().insert(fd, true);
                    if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                        w.wake();
                    }
                    return;
                }
                conn.recv_state.start_payload(pbuf, plen);
                self.inner.ring.borrow_mut().prep_recv(
                    fd, pbuf, plen as u32, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
            }
            io::RecvAdvance::MessageDone => {
                let (ptr, len) = conn.recv_state.take_message();
                self.inner.pending_recv.borrow_mut()
                    .entry(fd).or_default().push_back((ptr, len));
                // Arm next header recv immediately so the kernel can keep
                // draining the client's send buffer. Per-session FIFO is
                // preserved by the `VecDeque` order — the handler still
                // consumes messages in arrival order.
                let hdr = conn.recv_state.hdr_buf_ptr();
                self.inner.ring.borrow_mut().prep_recv(
                    fd, hdr, 4, udata(KIND_RECV, fd as u32 as u64),
                );
                conn.recv_armed = true;
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                    w.wake();
                }
            }
            io::RecvAdvance::Disconnect => {
                conn.closing = true;
                self.inner.closing_fds.borrow_mut().insert(fd);
                self.inner.recv_closed.borrow_mut().insert(fd, true);
                if let Some(w) = self.inner.recv_waiters.borrow_mut().remove(&fd) {
                    w.wake();
                }
            }
        }
    }

    /// Reap connections that are closing and have no outstanding SQEs.
    /// Called once per tick. Iterates only `closing_fds` (O(closing)),
    /// not all connections.
    pub(super) fn reap_closing_conns(&self) {
        if self.inner.closing_fds.borrow().is_empty() { return; }
        // Take ownership of the scratch (leaves a zero-cap Vec sentinel so
        // the borrow of `closing_scratch` ends immediately) and refill it.
        // The body re-borrows `closing_fds` and `conns`, which would deadlock
        // if we held the scratch borrow live.
        let mut closing = std::mem::take(&mut *self.inner.closing_scratch.borrow_mut());
        closing.clear();
        closing.extend(self.inner.closing_fds.borrow().iter().copied());
        for &fd in &closing {
            let ready = {
                let conns = self.inner.conns.borrow();
                match conns.get(&fd) {
                    Some(conn) => !conn.has_outstanding(),
                    None => true,
                }
            };
            if !ready { continue; }
            self.inner.closing_fds.borrow_mut().remove(&fd);
            self.inner.conns.borrow_mut().remove(&fd);
            self.inner.recv_waiters.borrow_mut().remove(&fd);
            if let Some(queue) = self.inner.pending_recv.borrow_mut().remove(&fd) {
                for (ptr, _) in queue {
                    if !ptr.is_null() {
                        unsafe { libc::free(ptr as *mut libc::c_void); }
                    }
                }
            }
            unsafe { libc::close(fd); }
        }
        // Return the scratch Vec to the cell so its capacity is retained.
        *self.inner.closing_scratch.borrow_mut() = closing;
    }

}
