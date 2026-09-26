//! Reactor client connections. Nothing here flushes: an SQE ships with the tick's
//! own submit, before the task awaiting it can run.

use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

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
    fd: OwnedFd,
    accepted: chan::Sender<OwnedFd>,
}

/// A send in flight: the connection whose fd its SQE names, and the bytes the
/// kernel reads. The op holds it until the CQE.
pub(super) struct Outbound {
    _conn: Rc<ClientConn>,
    body: SendBody,
}

/// An owned client-bound payload whose bytes stay in place when it moves.
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
fn arm_accept(inner: &ReactorShared, id: usize) {
    let fd = inner.listeners.borrow()[id].fd.as_raw_fd();
    let sqe = opcode::AcceptMulti::new(types::Fd(fd)).build();
    // SAFETY: `AcceptMulti` names no memory, and the `OwnedFd` lives in
    // `listeners`, which drops after `ring`.
    unsafe { inner.ring.borrow_mut().push(sqe, udata(KIND_ACCEPT, id as u64)) };
}

/// Arm a recv on `fd` into the window of its `conns` entry. The fd rides the
/// udata id, so the completion routes back to that entry. Not flushed (see the
/// module header).
fn arm_recv(ring: &mut IoUringRing, conns: &mut FxHashMap<i32, Armed>, fd: i32) {
    let armed = conns.get_mut(&fd).expect("a registered connection");
    let (ptr, len) = armed.filter.window(&mut armed.conn.q.borrow_mut());
    let sqe = opcode::Recv::new(types::Fd(fd), ptr, len).build();
    // SAFETY: only this recv's CQE removes or refills the entry, which holds the
    // connection (so `fd`) and the filter owning the window; `ring` drops first.
    unsafe { ring.push(sqe, udata(KIND_RECV, fd as u32 as u64)) };
}

impl Reactor {
    /// Attach a listen socket fd and arm its multishot accept; every connection it
    /// accepts arrives on the returned channel.
    pub fn attach_listener(&self, fd: OwnedFd) -> chan::Receiver<OwnedFd> {
        let (accepted, rx) = chan::unbounded::<OwnedFd>();
        let id = {
            let mut listeners = self.inner.listeners.borrow_mut();
            listeners.push(Listener { fd, accepted });
            listeners.len() - 1
        };
        arm_accept(&self.inner, id);
        rx
    }

    /// Route an accept completion: hand the accepted fd to its listener's accept
    /// loop and, when the multishot SQE has been cancelled, re-arm the listener.
    pub(super) fn handle_accept_cqe(&self, id: usize, res: i32, flags: u32) {
        if res >= 0 {
            // SAFETY: a fresh fd from the kernel, owned by nothing else.
            self.inner.listeners.borrow()[id]
                .accepted
                .send(unsafe { OwnedFd::from_raw_fd(res) });
        }
        if io_uring::cqueue::more(flags) {
            return;
        }
        if res != -libc::EMFILE && res != -libc::ENFILE {
            arm_accept(&self.inner, id);
            return;
        }
        // Out of fds: back off before re-arming so closing connections get a
        // window to free fds. One task per cancelled listener — exhaustion is
        // global, so both can cancel at once and each is owed a full backoff.
        let backoff = self.timer(Instant::now() + self.inner.limits.accept_rearm_backoff);
        let inner = Rc::clone(&self.inner);
        self.spawn(async move {
            backoff.await;
            arm_accept(&inner, id);
        });
    }

    /// A connection over `fd`, charging the reactor's inbound budget. Nothing is
    /// armed on it yet.
    pub(crate) fn client_conn(&self, fd: OwnedFd) -> Rc<ClientConn> {
        Rc::new(ClientConn::new(fd, Rc::clone(&self.inner.inbound)))
    }

    /// Arm `conn`'s first recv into `filter`'s window, and hold the connection
    /// until the recv side ends.
    pub(crate) fn register_conn(&self, conn: &Rc<ClientConn>, filter: Box<dyn RecvFilter>) {
        let fd = conn.fd();
        let mut conns = self.inner.conns.borrow_mut();
        // The entry holds the connection and so its fd open, so no live entry
        // can share this fd number.
        let prev = conns.insert(fd, Armed { conn: Rc::clone(conn), filter });
        debug_assert!(prev.is_none(), "fd={fd} registered while an entry still holds it");
        arm_recv(&mut self.inner.ring.borrow_mut(), &mut conns, fd);
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
            armed.filter.ingest(res as usize, &mut q)
        };
        drop(q);
        match next {
            Ok(()) => arm_recv(&mut self.inner.ring.borrow_mut(), &mut conns, fd),
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
    pub(crate) async fn send_owned(&self, conn: &Rc<ClientConn>, body: SendBody) -> Result<(), PeerGone> {
        let mut out = Outbound { _conn: Rc::clone(conn), body };
        let len = out.body.bytes().len();
        let mut sent = 0usize;
        while sent < len {
            let rest = &out.body.bytes()[sent..];
            let sqe = opcode::Send::new(types::Fd(conn.fd()), rest.as_ptr(), rest.len() as u32).build();
            let (u, op) = self.install_op(Some(out));
            // SAFETY: the op holds `out` until the CQE, and moving a `SendBody`
            // leaves its bytes in place.
            unsafe { self.inner.ring.borrow_mut().push(sqe, u) };
            let deadline = Instant::now() + self.inner.limits.client_send_timeout;
            let rc = match select2(op, self.timer(deadline)).await {
                Either::A((rc, back)) => {
                    out = back.expect("a send carries its connection and body");
                    rc
                }
                // Evicted; a completion that raced the timer still counts as failed.
                Either::B(()) => {
                    gnitz_warn!(
                        "client fd={} made no send progress for {:?}; evicting",
                        conn.fd(),
                        self.inner.limits.client_send_timeout
                    );
                    // The abandoned send completes only once the socket errors; its
                    // `ops` entry holds the connection and body until then.
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
