//! Reactor client connections. Nothing here flushes: an SQE ships with the loop's
//! own submit, before the task awaiting it can run.

use std::collections::hash_map::Entry;
use std::os::fd::{FromRawFd, OwnedFd};

use gnitz_zset::repr::PooledBuf;

use super::io::{ClientConn, RecvEnd, RecvFilter};
use super::*;

/// A connection with a recv armed on it — the reactor's whole per-connection
/// state. The entry exists exactly while the recv SQE is outstanding.
pub(super) struct Armed {
    conn: Rc<ClientConn>,
    filter: Box<dyn RecvFilter>,
}

/// A send in flight: the connection whose fd its SQE names, and the bytes the
/// kernel reads. Each kernel send's op holds it until that send's CQE.
pub(super) struct Outbound {
    _conn: Rc<ClientConn>,
    body: SendBody,
}

/// An owned client-bound payload whose bytes stay in place when it moves.
pub(crate) enum SendBody {
    Pooled(PooledBuf),
    Slot(W2mSlot),
    /// Bytes several connections are sent.
    Shared(Rc<Vec<u8>>),
}

impl SendBody {
    pub(crate) fn bytes(&self) -> &[u8] {
        match self {
            SendBody::Pooled(b) => &b.0,
            SendBody::Slot(s) => s.frame_bytes(),
            SendBody::Shared(b) => b,
        }
    }
}

/// This client's connection is finished.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct PeerGone;

impl Armed {
    /// Arm (or re-arm) the recv into the filter's window. The fd rides the udata
    /// id, so the completion routes back to this entry. Not flushed (see the
    /// module header).
    fn arm(&mut self, ring: &mut IoUringRing) {
        let fd = self.conn.fd();
        let mut q = self.conn.q.borrow_mut();
        let w = self.filter.window(&mut q);
        let sqe = opcode::Recv::new(types::Fd(fd), w.as_mut_ptr().cast(), w.len() as u32).build();
        // SAFETY: the window is the filter's own buffer or, for `Plain`, the payload
        // in progress in `conn.q`'s deframer. Only the filter's `window`/`ingest` and
        // `RecvQueue::feed`, which only an ingest calls, touch the deframer; the task
        // side's borrows of `q` reach only the frame queue and `max_frame`. Only this
        // recv's CQE removes or refills the entry, which holds the connection (so `fd`
        // and `q`) and the filter; `ring` drops first.
        unsafe { ring.push(sqe, udata(KIND_RECV, fd as u32 as u64)) };
    }
}

impl Reactor {
    /// The next connection accepted on `listener`. A failed accept is retried after
    /// `Limits::accept_rearm_backoff`, so an error that persists — fd exhaustion
    /// above all, which closing connections relieve — is not retried in a hot loop.
    ///
    /// Not cancel-safe: a future dropped mid-accept leaves an op whose later
    /// result is a raw fd nothing owns.
    pub(crate) async fn accept(&self, listener: BorrowedFd<'static>) -> OwnedFd {
        loop {
            let sqe = opcode::Accept::new(
                types::Fd(listener.as_raw_fd()),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
            .build();
            // SAFETY: the SQE names no memory, and `listener` is open for the
            // process's life.
            let res = unsafe { self.push_op(sqe, None) }.await;
            if res >= 0 {
                // SAFETY: a fresh fd from the kernel, owned by nothing else.
                return unsafe { OwnedFd::from_raw_fd(res) };
            }
            self.sleep(self.limits.accept_rearm_backoff).await;
        }
    }

    /// A connection over `fd`, taking one slot under the connection cap and
    /// charging frames to the inbound budget; `None`, closing `fd`, at the cap.
    /// Nothing is armed on it yet.
    pub(crate) fn client_conn(&self, fd: OwnedFd) -> Option<Rc<ClientConn>> {
        let Some(slot) = self.conn_slots.charge(1) else {
            gnitz_warn!(
                "connection cap {} reached; closing fd={}",
                self.conn_slots.cap(),
                fd.as_raw_fd()
            );
            return None;
        };
        Some(Rc::new(ClientConn::new(fd, slot, Rc::clone(&self.inbound))))
    }

    /// Arm `conn`'s first recv into `filter`'s window, and hold the connection
    /// until the recv side ends.
    pub(crate) fn register_conn(&self, conn: &Rc<ClientConn>, filter: Box<dyn RecvFilter>) {
        let fd = conn.fd();
        let mut conns = self.conns.borrow_mut();
        let Entry::Vacant(slot) = conns.entry(fd) else {
            // An entry holds its connection, so its fd is open and cannot be
            // reissued.
            unreachable!("fd={fd} registered while an entry still holds it")
        };
        slot.insert(Armed { conn: Rc::clone(conn), filter })
            .arm(&mut self.ring.borrow_mut());
    }

    pub(super) fn handle_recv_cqe(&self, fd: i32, res: i32) {
        let mut conns = self.conns.borrow_mut();
        let Entry::Occupied(mut entry) = conns.entry(fd) else {
            unreachable!("recv CQE for fd={fd} with no armed entry")
        };
        let armed = entry.get_mut();
        if armed.conn.is_gone() {
            // Retired while this recv was in flight: its bytes are discarded.
            entry.remove();
            return;
        }
        let end = match res {
            ..0 => RecvEnd::Socket,
            0 => RecvEnd::PeerClosed,
            n => {
                let ingested = armed.filter.ingest(n as usize, &mut armed.conn.q.borrow_mut());
                match ingested {
                    Ok(()) => return armed.arm(&mut self.ring.borrow_mut()),
                    Err(end) => end,
                }
            }
        };
        let armed = entry.remove();
        drop(conns);
        if let RecvEnd::PeerClosed = end {
            gnitz_debug!("reactor: fd={fd} recv side ended: {end}");
            return armed.conn.end_recv();
        }
        gnitz_warn!(
            "reactor: fd={fd} recv side ended: {end} (res={res}, inbound held={} of {} B)",
            self.inbound.held(),
            self.inbound.cap(),
        );
        match end {
            // The transport may owe the client a reply to the violation.
            RecvEnd::Protocol => armed.conn.retire(),
            // Owed nothing, and possibly not reading.
            _ => armed.conn.fail(),
        }
    } // `armed` drops: the reactor's hold on the connection ends

    /// Send `body`'s whole byte range on `conn`. Loops on short sends; each kernel
    /// send has its own `Limits::client_send_timeout` deadline, past which the
    /// client is evicted. A failed or evicted send finishes the connection
    /// ([`ClientConn::fail`]). An empty body sends nothing.
    pub(crate) async fn send_owned(&self, conn: &Rc<ClientConn>, body: SendBody) -> Result<(), PeerGone> {
        if body.bytes().is_empty() {
            return Ok(());
        }
        let out = Rc::new(Outbound { _conn: Rc::clone(conn), body });
        let bytes = out.body.bytes();
        let mut sent = 0usize;
        while sent < bytes.len() {
            let rest = &bytes[sent..];
            let sqe = opcode::Send::new(types::Fd(conn.fd()), rest.as_ptr(), rest.len() as u32).build();
            // SAFETY: the op holds `out` until the CQE.
            let op = unsafe { self.push_op(sqe, Some(Rc::clone(&out))) };
            let rc = match select2(op, self.sleep(self.limits.client_send_timeout)).await {
                Either::A(rc) => rc,
                // Evicted; a completion that raced the timer still counts as failed.
                // The abandoned send completes only once the socket errors; its
                // `ops` entry holds the connection and body until then.
                Either::B(()) => {
                    gnitz_warn!(
                        "client fd={} made no send progress for {:?}; evicting",
                        conn.fd(),
                        self.limits.client_send_timeout
                    );
                    0
                }
            };
            // A zero is the kernel accepting nothing, which is not progress either.
            if rc <= 0 {
                conn.fail();
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
