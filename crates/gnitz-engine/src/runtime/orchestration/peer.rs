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

use std::rc::Rc;

use crate::runtime::reactor::{Reactor, RecvBuf};
use crate::runtime::tls::TlsShared;
use crate::runtime::w2m::W2mSlot;
use crate::storage::batch_pool::PooledSendBuf;

/// Transport-neutral handle to one client connection. Owned by the
/// connection task; handlers borrow it to send replies.
pub struct Peer {
    inner: PeerInner,
}

enum PeerInner {
    /// AF_UNIX stream connection serviced by the reactor's fd machinery.
    Unix { fd: i32, reactor: Rc<Reactor> },
    /// TLS 1.3 over TCP; framing and record I/O live in `runtime::tls`.
    Tls(Rc<TlsShared>),
}

impl Peer {
    pub fn unix(fd: i32, reactor: Rc<Reactor>) -> Peer {
        Peer {
            inner: PeerInner::Unix { fd, reactor },
        }
    }

    pub fn tls(conn: Rc<TlsShared>) -> Peer {
        Peer {
            inner: PeerInner::Tls(conn),
        }
    }

    /// Next complete inbound frame payload (owned, freed on drop on every
    /// exit path including task cancellation), or `None` on disconnect.
    pub async fn recv(&self) -> Option<RecvBuf> {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.recv(*fd).await,
            PeerInner::Tls(conn) => conn.recv().await,
        }
    }

    pub async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_buffer(*fd, buf).await,
            PeerInner::Tls(conn) => conn.send_buffer(buf).await,
        }
    }

    /// Forward a worker W2M ring slot to the client under the per-frame
    /// egress deadline: a client that stalls the send is shut down so the
    /// held ring slot frees (see [`Reactor::send_slot`]; the TLS side's
    /// deadline lives in `TlsShared::send_guarded`). Returns the send rc
    /// (`< 0` — disconnect or eviction — means the client is gone).
    pub async fn send_slot(&self, slot: W2mSlot) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_slot(*fd, slot).await,
            PeerInner::Tls(conn) => conn.send_slot(slot).await,
        }
    }

    /// Send the OK HELLO ACK frame, seeding the client's OCC basis with
    /// `published_lsn` (the durability watermark at connect). The ACK's contents
    /// (status, advertised server frame limit) are protocol policy decided once
    /// here, for every transport. `published_lsn` is a runtime value, so the frame
    /// is no longer a compile-time `const` shipped by a zero-copy `'static` send:
    /// it is copied into a pooled send buffer and dispatched through the shared
    /// `send_buffer` path (per-connection, so the extra copy is off any hot path).
    pub async fn send_hello_ack(&self, published_lsn: u64) -> i32 {
        let ack = gnitz_wire::encode_hello_ack(
            gnitz_wire::HELLO_STATUS_OK,
            gnitz_wire::MAX_FRAME_PAYLOAD_SERVER as u32,
            published_lsn,
        );
        let mut buf = crate::storage::batch_pool::acquire_buf();
        buf.extend_from_slice(&ack);
        self.send_buffer(PooledSendBuf(buf)).await
    }

    /// Terminal reply send: close the connection on transport failure. Once
    /// the reply is on the wire there is nothing left to do on the
    /// connection, so a negative send rc (peer gone / write error) simply
    /// schedules the close.
    pub async fn send_buffer_or_close(&self, buf: PooledSendBuf) {
        if self.send_buffer(buf).await < 0 {
            self.close();
        }
    }

    /// `send_slot` counterpart of [`Self::send_buffer_or_close`]: forward a
    /// worker ring slot as the final reply, closing on transport failure.
    pub async fn send_slot_or_close(&self, slot: W2mSlot) {
        if self.send_slot(slot).await < 0 {
            self.close();
        }
    }

    /// Elevate the per-connection inbound frame ceiling after HELLO.
    /// Must run synchronously before any `.await` in the handshake task
    /// (see `Reactor::set_max_payload_len`).
    pub fn set_max_payload_len(&self, limit: usize) {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.set_max_payload_len(*fd, limit),
            PeerInner::Tls(conn) => conn.set_max_payload_len(limit),
        }
    }

    pub fn close(&self) {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.close_fd(*fd),
            PeerInner::Tls(conn) => conn.close(),
        }
    }
}
