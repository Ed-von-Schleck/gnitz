//! Transport-neutral client-connection handle.
//!
//! The dispatch layer (connection loop, request handlers, scan-train
//! forwarders) talks to a `Peer`, not a raw fd, so the two connection
//! transports — AF_UNIX stream fds serviced by the reactor's framing, and
//! TLS-over-TCP sessions whose framing runs on decrypted plaintext in
//! `runtime::tls` — slot in without touching a handler. The handle is an
//! enum, not a trait: connection transports are a closed set, enum dispatch
//! keeps the async methods plain (no boxed futures, no dyn), and the
//! orchestration layer sits above both the reactor and the TLS engine, so
//! the layering stays intact (the reactor keeps its fd-based API and learns
//! nothing about peers).
//!
//! Every client-bound byte leaves through [`Peer::send_raw`], which is where the
//! egress deadline is applied — once per send, around both transport arms, so
//! neither transport carries a copy of the policy. Replies reach it either
//! directly or through the cork accumulator; nothing else writes to a client.

use std::cell::RefCell;
use std::rc::Rc;

use crate::runtime::reactor::{guard_egress_deadline, PeerToken, Reactor, RecvBuf, SendPayload};
use crate::runtime::tls::TlsShared;
use crate::runtime::w2m::W2mSlot;
use crate::runtime::wire::COALESCE_MAX_BYTES;
use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};

/// Transport-neutral handle to one client connection. Owned by the
/// connection task; handlers borrow it to send replies.
pub struct Peer {
    inner: PeerInner,
    /// Replies written but not yet sent, concatenated so a run of pipelined
    /// requests leaves as one send. `None` when nothing is pending.
    egress: RefCell<Option<PooledSendBuf>>,
}

enum PeerInner {
    /// AF_UNIX stream connection serviced by the reactor's fd machinery. The
    /// fd is reached through the `PeerToken`, which is what keeps the reactor
    /// from closing (and the kernel from recycling) it under this handle.
    Unix { conn: PeerToken, reactor: Rc<Reactor> },
    /// TLS 1.3 over TCP; framing and record I/O live in `runtime::tls`.
    Tls(Rc<TlsShared>),
}

impl Peer {
    pub fn unix(fd: i32, reactor: Rc<Reactor>) -> Peer {
        let conn = PeerToken::new(&reactor, fd);
        Peer {
            inner: PeerInner::Unix { conn, reactor },
            egress: RefCell::new(None),
        }
    }

    pub fn tls(conn: Rc<TlsShared>) -> Peer {
        Peer {
            inner: PeerInner::Tls(conn),
            egress: RefCell::new(None),
        }
    }

    /// Next complete inbound frame payload (owned, freed on drop on every
    /// exit path including task cancellation), or `None` on disconnect.
    pub async fn recv(&self) -> Option<RecvBuf> {
        match &self.inner {
            PeerInner::Unix { conn, reactor } => reactor.recv(conn.fd()).await,
            PeerInner::Tls(conn) => conn.recv().await,
        }
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, never that the peer is gone — only [`Self::recv`] says that.
    pub fn try_recv(&self) -> Option<RecvBuf> {
        match &self.inner {
            PeerInner::Unix { conn, reactor } => reactor.try_recv(conn.fd()),
            PeerInner::Tls(conn) => conn.try_recv(),
        }
    }

    /// The reactor driving this connection, and the fd it is on.
    fn transport(&self) -> (&Reactor, i32) {
        match &self.inner {
            PeerInner::Unix { conn, reactor } => (reactor, conn.fd()),
            PeerInner::Tls(conn) => (conn.reactor(), conn.fd()),
        }
    }

    /// Append a reply written by `write`, to leave with whatever is corked
    /// beside it. Returns the bytes now pending. Synchronous, so a caller
    /// holding a W2M ring slot can copy out of it and release it before any
    /// await.
    pub fn cork_with(&self, write: impl FnOnce(&mut Vec<u8>)) -> usize {
        let mut e = self.egress.borrow_mut();
        let acc = e.get_or_insert_with(|| PooledSendBuf(acquire_buf()));
        write(&mut acc.0);
        acc.0.len()
    }

    /// Append `frame`'s bytes. See [`Self::cork_with`].
    pub fn cork(&self, frame: &[u8]) -> usize {
        self.cork_with(|acc| acc.extend_from_slice(frame))
    }

    /// Bytes currently corked (test observability).
    #[cfg(test)]
    pub fn corked_len(&self) -> usize {
        self.egress.borrow().as_ref().map_or(0, |b| b.0.len())
    }

    /// Ship what is corked once it reaches [`COALESCE_MAX_BYTES`], bounding the
    /// accumulator across a long pipelined run.
    pub async fn flush_if_full(&self) -> i32 {
        let full = self
            .egress
            .borrow()
            .as_ref()
            .is_some_and(|b| b.0.len() >= COALESCE_MAX_BYTES);
        if full {
            self.flush_egress().await
        } else {
            0
        }
    }

    /// Write everything corked, as one send. No-op when nothing is pending.
    pub async fn flush_egress(&self) -> i32 {
        // Taken, not borrowed across the await: the flushed task re-enters `Peer`.
        let Some(buf) = self.egress.borrow_mut().take() else {
            return 0;
        };
        self.send_raw(Rc::new(buf)).await
    }

    /// Send one owned payload, behind whatever is corked — so nothing can
    /// overtake a reply already written. `< 0` (disconnect or eviction) means
    /// the client is gone.
    async fn send<T: SendPayload + 'static>(&self, payload: Rc<T>) -> i32 {
        let rc = self.flush_egress().await;
        if rc < 0 {
            return rc;
        }
        self.send_raw(payload).await
    }

    /// The transport send itself, under the egress deadline — applied here once,
    /// so neither transport arm carries a copy of the policy.
    async fn send_raw<T: SendPayload + 'static>(&self, payload: Rc<T>) -> i32 {
        let (reactor, fd) = self.transport();
        let what = payload.what();
        guard_egress_deadline(reactor, fd, what, async {
            match &self.inner {
                PeerInner::Unix { .. } => reactor.send_owned(fd, payload).await,
                PeerInner::Tls(conn) => conn.send_bytes(payload.bytes()).await,
            }
        })
        .await
    }

    /// Send an owned buffer to the client.
    pub async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        self.send(Rc::new(buf)).await
    }

    /// Forward a worker W2M ring slot to the client. Holding the slot until the
    /// send completes is what preserves the worker's W2M backpressure, and is
    /// why the deadline matters most on this path — see [`Reactor::send_owned`].
    pub async fn send_slot(&self, slot: W2mSlot) -> i32 {
        self.send(Rc::new(slot)).await
    }

    /// Send the OK HELLO ACK frame, seeding the client's OCC basis with
    /// `published_lsn` (the durability watermark at connect). The ACK's contents
    /// (status, advertised server frame limit) are protocol policy decided once
    /// here, for every transport. `published_lsn` is a runtime value, so the frame
    /// cannot be a compile-time `const` shipped by a zero-copy `'static` send:
    /// it is copied into a pooled send buffer and dispatched through the shared
    /// `send_buffer` path (per-connection, so the extra copy is off any hot path).
    pub async fn send_hello_ack(&self, published_lsn: u64) -> i32 {
        let ack = gnitz_wire::encode_hello_ack(crate::runtime::wire::FRAME_CAP as u32, published_lsn);
        let mut buf = gnitz_store::storage::batch_pool::acquire_buf();
        buf.extend_from_slice(&ack);
        self.send_buffer(PooledSendBuf(buf)).await
    }

    /// Terminal reply send: close the connection on transport failure. Once
    /// the reply is on the wire there is nothing left to do on the
    /// connection, so a negative send rc (peer gone / write error) simply
    /// schedules the close.
    pub async fn send_or_close<T: SendPayload + 'static>(&self, payload: T) {
        if self.send(Rc::new(payload)).await < 0 {
            self.close();
        }
    }

    /// Elevate the per-connection inbound frame ceiling after HELLO.
    /// Must run synchronously before any `.await` in the handshake task
    /// (see `Reactor::set_max_payload_len`).
    pub fn set_max_payload_len(&self, limit: usize) {
        match &self.inner {
            PeerInner::Unix { conn, reactor } => reactor.set_max_payload_len(conn.fd(), limit),
            PeerInner::Tls(conn) => conn.set_max_payload_len(limit),
        }
    }

    pub fn close(&self) {
        match &self.inner {
            PeerInner::Unix { conn, reactor } => reactor.close_fd(conn.fd()),
            PeerInner::Tls(conn) => conn.close(),
        }
    }
}
