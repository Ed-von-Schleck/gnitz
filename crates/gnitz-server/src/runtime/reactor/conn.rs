//! Reactor client connections. Nothing here flushes: an SQE ships with the tick's
//! own submit, before the task awaiting it can run.

use std::os::fd::OwnedFd;

use gnitz_store::storage::batch_pool::PooledSendBuf;

use super::io::{ClientConn, RecvFilter};
use super::*;

/// A connection with a recv armed on it — the reactor's whole per-connection
/// state. The entry exists exactly while the recv SQE is outstanding.
pub(super) struct Armed {
    conn: Rc<ClientConn>,
    filter: Option<Box<dyn RecvFilter>>,
}

/// An owned client-bound payload. It rides the send's park slot while the kernel
/// may read it and comes back with the result, so no payload is boxed per send.
pub(crate) enum SendBody {
    Pooled(PooledSendBuf),
    Slot(W2mSlot),
    Cipher(Vec<u8>),
}

impl SendBody {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            SendBody::Pooled(b) => &b.0,
            SendBody::Slot(s) => s.frame_bytes(),
            SendBody::Cipher(v) => v,
        }
    }
}

impl From<PooledSendBuf> for SendBody {
    fn from(b: PooledSendBuf) -> Self {
        SendBody::Pooled(b)
    }
}

impl From<W2mSlot> for SendBody {
    fn from(s: W2mSlot) -> Self {
        SendBody::Slot(s)
    }
}

/// Client-egress deadline (`GNITZ_CLIENT_SEND_TIMEOUT_MS`): one kernel send making no
/// progress for this long evicts the client.
pub(super) fn resolve_client_send_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_CLIENT_SEND_TIMEOUT_MS", 30_000))
}

/// `shutdown(SHUT_RDWR)`: errors out a pending `OP_SEND` on `fd`, which `close` does
/// not. Never closes the fd; a peer already gone is not an error.
pub(crate) fn shutdown(fd: i32) {
    let _ = gnitz_foundation::posix_io::retry_eintr(|| unsafe { libc::shutdown(fd, libc::SHUT_RDWR) });
}

/// Arm (or re-arm) `listener`'s multishot accept. The listener fd rides the
/// udata id, so its completions route back to it without reactor state.
fn arm_accept(ring: &mut IoUringRing, listener: i32) {
    ring.prep_accept(listener, udata(KIND_ACCEPT, listener as u32 as u64));
}

/// Arm a recv into `[ptr, ptr+len)` on `fd`. The fd rides the udata id, so the
/// completion routes back to this connection. Not flushed (see the module
/// header).
fn arm_recv(ring: &mut IoUringRing, fd: i32, ptr: *mut u8, len: u32) {
    ring.prep_recv(fd, ptr, len, udata(KIND_RECV, fd as u32 as u64));
}

impl Reactor {
    /// Attach a listen socket fd and arm its multishot-accept SQE. Callable
    /// once per listener (AF_UNIX + optional TLS); the listener fd rides the
    /// SQE's udata `id` field so each accepted connection resolves as
    /// `(conn_fd, listener_fd)`.
    pub fn attach_listener(&self, listener_fd: i32) {
        let mut ring = self.inner.ring.borrow_mut();
        arm_accept(&mut ring, listener_fd);
        if let Err(e) = ring.submit() {
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
            arm_accept(&mut self.inner.ring.borrow_mut(), listener);
            return;
        }
        // Out of fds: back off before re-arming so closing connections get a
        // window to free fds. One task per cancelled listener — exhaustion is
        // global, so both can cancel at once and each is owed a full backoff.
        let inner = Rc::clone(&self.inner);
        self.spawn(async move {
            let deadline = Instant::now() + inner.limits.accept_rearm_backoff;
            TimerFuture::new(deadline, Rc::clone(&inner)).await;
            arm_accept(&mut inner.ring.borrow_mut(), listener);
        });
    }

    /// The next newly-accepted `(conn_fd, listener_fd)` pair. Called by the
    /// accept-loop task.
    pub async fn accept(&self) -> (i32, i32) {
        std::future::poll_fn(|cx| self.inner.accepts.borrow_mut().poll(cx)).await
    }

    /// A connection over `fd`, charging the reactor's inbound budget. Nothing is
    /// armed on it yet.
    pub(crate) fn client_conn(&self, fd: OwnedFd) -> Rc<ClientConn> {
        Rc::new(ClientConn::new(fd, Rc::clone(&self.inner.inbound)))
    }

    /// Arm `conn`'s first recv, into `filter.window()` or the queue's own window, and
    /// hold the connection until the recv side ends.
    pub(crate) fn register_conn(&self, conn: &Rc<ClientConn>, mut filter: Option<Box<dyn RecvFilter>>) {
        let fd = conn.fd();
        let (ptr, len) = match &mut filter {
            Some(f) => f.window(),
            None => conn.q.borrow_mut().remaining(),
        };
        arm_recv(&mut self.inner.ring.borrow_mut(), fd, ptr, len);
        // The entry holds the connection and so its fd open, so no live entry
        // can share this fd number.
        let prev = self
            .inner
            .conns
            .borrow_mut()
            .insert(fd, Armed { conn: Rc::clone(conn), filter });
        debug_assert!(prev.is_none(), "fd={fd} registered while an entry still holds it");
    }

    /// End `conn`'s recv side now: close its queue and, if a recv is armed, shut
    /// the socket down — nothing else would complete that recv for a client that
    /// goes silent after being refused.
    pub(crate) fn close_conn(&self, conn: &ClientConn) {
        conn.q.borrow_mut().close();
        // A recv that already ended retired its entry.
        if self.inner.conns.borrow().contains_key(&conn.fd()) {
            shutdown(conn.fd());
        }
    }

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        let Some(armed) = conns.get_mut(&fd) else { return };
        let mut q = armed.conn.q.borrow_mut();
        let next = if res <= 0 || q.recv_closed() {
            Err(())
        } else {
            match &mut armed.filter {
                None => q.deliver(res as usize, fd),
                Some(f) => f.ingest(res as usize, &mut q, fd).map(|()| f.window()),
            }
        };
        drop(q);
        match next {
            Ok((ptr, len)) => arm_recv(&mut self.inner.ring.borrow_mut(), fd, ptr, len),
            Err(()) => {
                let mut armed = conns.remove(&fd).expect("entry present");
                drop(conns);
                armed.conn.q.borrow_mut().close();
                if let Some(f) = &mut armed.filter {
                    f.recv_closed(fd);
                }
            } // `armed` drops: the reactor's hold on the connection ends
        }
    }

    /// Send `body`'s whole byte range on `conn`, returning the bytes sent (>= 0) or a
    /// negative errno, and `body` back. Loops on short sends; each kernel send has
    /// its own `Limits::client_send_timeout` deadline, past which the client is
    /// evicted.
    pub(crate) async fn send_owned(&self, conn: &ClientConn, mut body: SendBody) -> (i32, SendBody) {
        let (ptr, len) = {
            let b = body.bytes();
            (b.as_ptr(), b.len())
        };
        let mut sent = 0usize;
        while sent < len {
            // `body`'s bytes are heap- or mapping-backed, so moving it into the op
            // leaves `ptr` valid; the op holds it until this send's CQE.
            let op = self.submit_op(
                |ring, u| ring.prep_send(conn.fd(), unsafe { ptr.add(sent) }, (len - sent) as u32, u),
                Some(body),
            );
            let mut op = std::pin::pin!(op);
            let deadline = Instant::now() + self.inner.limits.client_send_timeout;
            let (rc, back) = match select2(op.as_mut(), self.timer(deadline)).await {
                Either::A(done) => done,
                // Evicted: `shutdown` errors the send out; a result that raced it counts as failed.
                Either::B(()) => {
                    gnitz_warn!(
                        "client fd={} made no send progress for {:?}; evicting",
                        conn.fd(),
                        self.inner.limits.client_send_timeout
                    );
                    shutdown(conn.fd());
                    let (rc, back) = op.await;
                    (rc.min(-1), back)
                }
            };
            body = back.expect("a send carries its body");
            if rc <= 0 {
                return (if rc < 0 { rc } else { sent as i32 }, body);
            }
            sent += rc as usize;
        }
        (sent as i32, body)
    }
}

#[cfg(test)]
#[path = "tests/conn.rs"]
mod tests;
