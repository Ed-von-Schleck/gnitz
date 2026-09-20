//! Transport-neutral client-connection handle. Both transports deframe into one
//! [`ClientConn`]; only sending and closing dispatch on the transport.

use std::cell::RefCell;
use std::os::fd::OwnedFd;
use std::rc::Rc;

use crate::runtime::reactor::{ClientConn, PeerGone, Plain, Reactor, RecvBuf, SendBody};
use crate::runtime::tls::TlsShared;
use crate::runtime::w2m::W2mSlot;
use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};

/// Ceiling on a concatenation of client-bound frames (coalesced scan heads, corked
/// replies): the copy paid to save per-frame sends. `fanout_coalesced_egress_bench`
/// measures the trade.
pub(crate) const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// Transport-neutral handle to one client connection. Owned by the connection
/// task; handlers borrow it to send replies. A failed send closes the connection,
/// so a `PeerGone` from any method means it is already closed.
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
        reactor.register_conn(&conn, Box::new(Plain));
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

    /// Bytes currently corked.
    pub fn corked_len(&self) -> usize {
        self.egress.borrow().as_ref().map_or(0, |b| b.0.len())
    }

    /// Ship what is corked once it reaches [`COALESCE_MAX_BYTES`], bounding the
    /// accumulator across a long pipelined run.
    pub async fn flush_if_full(&self) -> Result<(), PeerGone> {
        let full = self
            .egress
            .borrow()
            .as_ref()
            .is_some_and(|b| b.0.len() >= COALESCE_MAX_BYTES);
        if full {
            self.flush_egress().await
        } else {
            Ok(())
        }
    }

    /// Write everything corked, as one send. No-op when nothing is pending.
    pub async fn flush_egress(&self) -> Result<(), PeerGone> {
        // Taken, not borrowed across the await: the flushed task re-enters `Peer`.
        let Some(buf) = self.egress.borrow_mut().take() else {
            return Ok(());
        };
        self.send_raw(SendBody::Pooled(buf)).await
    }

    /// Send one worker frame behind whatever is corked. Corking copies it, releasing
    /// its ring slot at once; a frame too large to join the cork goes out alone,
    /// straight from that slot on AF_UNIX. A corked frame reports a gone peer at its
    /// flush, not here.
    pub async fn send(&self, slot: W2mSlot) -> Result<(), PeerGone> {
        let len = slot.frame_bytes().len();
        let corked = self.corked_len();
        if !(corked > 0 && corked + len <= COALESCE_MAX_BYTES) {
            self.flush_egress().await?;
            if len > COALESCE_MAX_BYTES / 2 {
                return self.send_raw(SendBody::Slot(slot)).await;
            }
        }
        self.cork(slot.frame_bytes());
        Ok(())
    }

    /// The transport send itself, and the one funnel every `PeerGone` comes through.
    async fn send_raw(&self, body: SendBody) -> Result<(), PeerGone> {
        let r = match &self.transport {
            Transport::Unix(r) => r.send_owned(&self.conn, body).await,
            Transport::Tls(t) => t.send(body).await,
        };
        if r.is_err() {
            self.close();
        }
        r
    }

    /// Elevate the per-connection inbound frame ceiling after HELLO.
    pub fn set_max_payload_len(&self, limit: usize) {
        self.conn.set_max_payload_len(limit);
    }

    /// Idempotent on both transports.
    pub fn close(&self) {
        match &self.transport {
            Transport::Unix(_) => self.conn.close(),
            Transport::Tls(t) => t.close(),
        }
    }
}
