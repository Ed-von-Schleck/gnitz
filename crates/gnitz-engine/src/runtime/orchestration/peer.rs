//! Transport-neutral client-connection handle.
//!
//! The dispatch layer (connection loop, request handlers, scan-train
//! forwarders) talks to a `Peer`, not a raw fd, so a transport whose client
//! connections are not file descriptors (a userspace QUIC endpoint reading
//! and writing stream buffers over one UDP socket) can slot in without
//! touching a handler. The handle is an enum, not a trait: connection
//! transports are a closed set, enum dispatch keeps the async methods plain
//! (no boxed futures, no dyn), and the orchestration layer sits above both
//! the reactor and any future transport engine, so the layering stays intact
//! (the reactor keeps its fd-based API and learns nothing about peers).

use std::rc::Rc;

use crate::runtime::reactor::{Reactor, RecvBuf};
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
}

impl Peer {
    pub fn unix(fd: i32, reactor: Rc<Reactor>) -> Peer {
        Peer {
            inner: PeerInner::Unix { fd, reactor },
        }
    }

    /// Next complete inbound frame payload (owned, freed on drop on every
    /// exit path including task cancellation), or `None` on disconnect.
    pub async fn recv(&self) -> Option<RecvBuf> {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.recv(*fd).await,
        }
    }

    pub async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_buffer(*fd, buf).await,
        }
    }

    pub async fn send_slot(&self, slot: W2mSlot) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_slot(*fd, slot).await,
        }
    }

    pub async fn send_hello_ack(&self) -> i32 {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.send_hello_ack(*fd).await,
        }
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
        }
    }

    pub fn close(&self) {
        match &self.inner {
            PeerInner::Unix { fd, reactor } => reactor.close_fd(*fd),
        }
    }
}
