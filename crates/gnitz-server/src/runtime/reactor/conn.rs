//! Reactor client connections. Nothing here flushes: an SQE ships with the tick's
//! own submit, before the task awaiting it can run.

use std::os::fd::{FromRawFd, OwnedFd};

use gnitz_store::storage::batch_pool::PooledSendBuf;

use super::io::{ClientConn, RecvEnd, RecvFilter};
use super::*;

/// A connection with a recv armed on it — the reactor's whole per-connection
/// state. The entry exists exactly while the recv SQE is outstanding.
pub(super) struct Armed {
    conn: Rc<ClientConn>,
    filter: Box<dyn RecvFilter>,
}

/// One attached listener: the fd its accepts name, and where they are delivered.
pub(super) struct Listener {
    fd: i32,
    accepted: chan::Sender<OwnedFd>,
}

/// An owned client-bound payload. It rides the send's park slot while the kernel
/// may read it; the send loop takes it back between short sends.
pub(crate) enum SendBody {
    Pooled(PooledSendBuf),
    Slot(W2mSlot),
}

impl SendBody {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            SendBody::Pooled(b) => &b.0,
            SendBody::Slot(s) => s.frame_bytes(),
        }
    }
}

/// This client's egress side is finished: a send failed, or the client was
/// evicted for making no progress.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PeerGone;

/// Client-egress deadline (`GNITZ_CLIENT_SEND_TIMEOUT_MS`): one kernel send making no
/// progress for this long evicts the client.
pub(super) fn resolve_client_send_timeout() -> std::time::Duration {
    std::time::Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_CLIENT_SEND_TIMEOUT_MS", 30_000))
}

/// Arm (or re-arm) the multishot accept of the listener at index `id`, which rides
/// the udata so its completions route back through `listeners`.
fn arm_accept(ring: &mut IoUringRing, fd: i32, id: usize) {
    ring.prep_accept(fd, udata(KIND_ACCEPT, id as u64));
}

/// Arm a recv into `[ptr, ptr+len)` on `fd`. The fd rides the udata id, so the
/// completion routes back to this connection. Not flushed (see the module
/// header).
fn arm_recv(ring: &mut IoUringRing, fd: i32, ptr: *mut u8, len: u32) {
    ring.prep_recv(fd, ptr, len, udata(KIND_RECV, fd as u32 as u64));
}

impl Reactor {
    /// Attach a listen socket fd and arm its multishot accept; every connection it
    /// accepts arrives on the returned channel.
    pub fn attach_listener(&self, fd: i32) -> chan::Receiver<OwnedFd> {
        let (accepted, rx) = chan::unbounded::<OwnedFd>();
        let id = {
            let mut listeners = self.inner.listeners.borrow_mut();
            listeners.push(Listener { fd, accepted });
            listeners.len() - 1
        };
        arm_accept(&mut self.inner.ring.borrow_mut(), fd, id);
        rx
    }

    /// Route an accept completion: hand the accepted fd to its listener's accept
    /// loop and, when the multishot SQE has been cancelled, re-arm the listener.
    pub(super) fn handle_accept_cqe(&self, id: usize, res: i32, flags: u32) {
        let fd = {
            let listeners = self.inner.listeners.borrow();
            let listener = &listeners[id];
            if res >= 0 {
                // SAFETY: a fresh fd from the kernel, owned by nothing else.
                listener.accepted.send(unsafe { OwnedFd::from_raw_fd(res) });
            }
            listener.fd
        };
        if flags & CQE_F_MORE != 0 {
            return;
        }
        if res != -libc::EMFILE && res != -libc::ENFILE {
            arm_accept(&mut self.inner.ring.borrow_mut(), fd, id);
            return;
        }
        // Out of fds: back off before re-arming so closing connections get a
        // window to free fds. One task per cancelled listener — exhaustion is
        // global, so both can cancel at once and each is owed a full backoff.
        let inner = Rc::clone(&self.inner);
        self.spawn(async move {
            let deadline = Instant::now() + inner.limits.accept_rearm_backoff;
            TimerFuture::new(deadline, Rc::clone(&inner)).await;
            arm_accept(&mut inner.ring.borrow_mut(), fd, id);
        });
    }

    /// A connection over `fd`, charging the reactor's inbound budget. Nothing is
    /// armed on it yet.
    pub(crate) fn client_conn(&self, fd: OwnedFd) -> Rc<ClientConn> {
        Rc::new(ClientConn::new(fd, Rc::clone(&self.inner.inbound)))
    }

    /// Arm `conn`'s first recv into `filter`'s window, and hold the connection
    /// until the recv side ends.
    pub(crate) fn register_conn(&self, conn: &Rc<ClientConn>, mut filter: Box<dyn RecvFilter>) {
        let fd = conn.fd();
        let (ptr, len) = filter.window(&mut conn.q.borrow_mut());
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

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.inner.conns.borrow_mut();
        let Some(armed) = conns.get_mut(&fd) else { return };
        let mut q = armed.conn.q.borrow_mut();
        let next = if q.recv_closed() {
            Err(RecvEnd::Closed)
        } else if res < 0 {
            Err(RecvEnd::Socket)
        } else if res == 0 {
            Err(RecvEnd::PeerClosed)
        } else {
            armed
                .filter
                .ingest(res as usize, &mut q)
                .map(|()| armed.filter.window(&mut q))
        };
        drop(q);
        match next {
            Ok((ptr, len)) => arm_recv(&mut self.inner.ring.borrow_mut(), fd, ptr, len),
            Err(end) => {
                let armed = conns.remove(&fd).expect("entry present");
                drop(conns);
                if end.is_clean() {
                    gnitz_debug!("reactor: fd={fd} recv side ended: {end}");
                    armed.conn.q.borrow_mut().close();
                } else {
                    gnitz_warn!(
                        "reactor: fd={fd} recv side ended: {end} (res={res}, inbound held={} of {} B)",
                        self.inner.inbound.held(),
                        self.inner.inbound.cap(),
                    );
                    armed.conn.abort();
                    armed.conn.shutdown();
                }
            } // `armed` drops: the reactor's hold on the connection ends
        }
    }

    /// Send `body`'s whole byte range on `conn`. Loops on short sends; each kernel
    /// send has its own `Limits::client_send_timeout` deadline, past which the
    /// client is evicted.
    pub(crate) async fn send_owned(&self, conn: &ClientConn, mut body: SendBody) -> Result<(), PeerGone> {
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
            let rc = match select2(op.as_mut(), self.timer(deadline)).await {
                Either::A((rc, back)) => {
                    body = back.expect("a send carries its body");
                    rc
                }
                // Evicted; a completion that raced the timer still counts as failed.
                Either::B(()) => {
                    gnitz_warn!(
                        "client fd={} made no send progress for {:?}; evicting",
                        conn.fd(),
                        self.inner.limits.client_send_timeout
                    );
                    // The abandoned send completes only once the socket errors, and
                    // its park slot holds the body until it does.
                    conn.shutdown();
                    return Err(PeerGone);
                }
            };
            // A zero is the kernel accepting nothing, which is not progress either.
            if rc <= 0 {
                return Err(PeerGone);
            }
            sent += rc as usize;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/conn.rs"]
mod tests;
