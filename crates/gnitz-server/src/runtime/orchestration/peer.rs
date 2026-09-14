//! Transport-neutral client-connection handle. Both transports deframe into one
//! [`ClientConn`]; only sending and closing dispatch on the transport.

use std::cell::RefCell;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use crate::runtime::reactor::{ClientConn, Reactor, RecvBuf, SendBody};
use crate::runtime::tls::TlsShared;
use crate::runtime::w2m::W2mSlot;
use crate::runtime::wire::COALESCE_MAX_BYTES;
use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};

/// Transport-neutral handle to one client connection. Owned by the
/// connection task; handlers borrow it to send replies.
pub struct Peer {
    conn: Rc<ClientConn>,
    transport: Transport,
    /// Replies written but not yet sent, concatenated so a run of pipelined
    /// requests leaves as one send. `None` when nothing is pending.
    egress: RefCell<Option<PooledSendBuf>>,
}

enum Transport {
    /// AF_UNIX stream connection: the reactor sends on the socket directly.
    Unix(Rc<Reactor>),
    /// TLS 1.3 over TCP; record I/O lives in `runtime::tls`.
    Tls(Rc<TlsShared>),
}

impl Peer {
    pub fn unix(fd: OwnedFd, reactor: Rc<Reactor>) -> Peer {
        let conn = reactor.client_conn(fd);
        reactor.register_conn(&conn, None);
        Peer {
            conn,
            transport: Transport::Unix(reactor),
            egress: RefCell::new(None),
        }
    }

    pub fn tls(tls: Rc<TlsShared>) -> Peer {
        Peer {
            conn: Rc::clone(tls.conn()),
            transport: Transport::Tls(tls),
            egress: RefCell::new(None),
        }
    }

    /// Next complete inbound frame payload (owned, freed on drop on every
    /// exit path including task cancellation), or `None` on disconnect.
    pub async fn recv(&self) -> Option<RecvBuf> {
        self.conn.recv().await
    }

    /// The next already-deframed frame without parking. `None` means nothing is
    /// queued right now, never that the peer is gone — only [`Self::recv`] says that.
    pub fn try_recv(&self) -> Option<RecvBuf> {
        self.conn.try_recv()
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
        self.send_raw(SendBody::Pooled(buf)).await
    }

    /// Send one owned payload, behind whatever is corked — so nothing can
    /// overtake a reply already written. `< 0` (disconnect or eviction) means
    /// the client is gone.
    async fn send(&self, body: SendBody) -> i32 {
        let rc = self.flush_egress().await;
        if rc < 0 {
            return rc;
        }
        self.send_raw(body).await
    }

    /// The transport send itself.
    async fn send_raw(&self, body: SendBody) -> i32 {
        match &self.transport {
            Transport::Unix(r) => r.send_owned(&self.conn, body).await.0,
            Transport::Tls(t) => t.send_bytes(body.bytes()).await,
        }
    }

    /// Send an owned buffer to the client.
    pub async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        self.send(SendBody::Pooled(buf)).await
    }

    /// Forward a worker W2M ring slot to the client. Holding the slot until the
    /// send completes is what preserves the worker's W2M backpressure, and is
    /// why the send deadline matters most on this path.
    pub async fn send_slot(&self, slot: W2mSlot) -> i32 {
        self.send(SendBody::Slot(slot)).await
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
    pub async fn send_or_close(&self, payload: impl Into<SendBody>) {
        if self.send(payload.into()).await < 0 {
            self.close();
        }
    }

    /// Elevate the per-connection inbound frame ceiling after HELLO.
    /// Must run synchronously before any `.await` in the handshake task
    /// (see [`ClientConn::set_max_payload_len`]).
    pub fn set_max_payload_len(&self, limit: usize) {
        self.conn.set_max_payload_len(limit);
    }

    pub fn close(&self) {
        match &self.transport {
            Transport::Unix(r) => r.close_conn(&self.conn),
            Transport::Tls(t) => t.close(),
        }
    }
}
