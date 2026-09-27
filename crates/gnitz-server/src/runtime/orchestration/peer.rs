//! Transport-neutral client-connection handle. Both transports deframe into one
//! [`ClientConn`], which also owns the connection's end; only sending dispatches
//! on the transport.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use crate::runtime::reactor::{ClientConn, PeerGone, Plain, Reactor, RecvBuf, SendBody};
use crate::runtime::tls::TlsShared;
use crate::runtime::w2m::W2mSlot;
use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};

/// Ceiling on a concatenation of client-bound frames (coalesced scan heads, corked
/// replies): the copy paid to save per-frame sends. `fanout_coalesced_egress_bench`
/// measures the trade.
pub(crate) const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// Transport-neutral handle to one client connection. Owned by the connection
/// task; handlers borrow it to send replies. A `PeerGone` from any send or flush
/// means the connection is finished, and every later send or flush refuses too.
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
    /// Arm `conn`'s recv and start serving it: a TLS session under `tls`, the
    /// socket's own bytes otherwise.
    pub fn new(reactor: &Rc<Reactor>, conn: Rc<ClientConn>, tls: Option<&Arc<rustls::ServerConfig>>) -> Peer {
        let transport = match tls {
            None => {
                reactor.register_conn(&conn, Box::new(Plain::new()));
                Transport::Unix(Rc::clone(reactor))
            }
            Some(cfg) => Transport::Tls(TlsShared::start(Rc::clone(reactor), Rc::clone(&conn), Arc::clone(cfg))),
        };
        Peer {
            conn,
            transport,
            egress: RefCell::new(None),
        }
    }

    /// The next request, shipping what is corked before parking on the client.
    /// `None` once the client is gone or has sent its last request.
    pub async fn next_request(&self) -> Option<RecvBuf> {
        self.flush_if_full().await.ok()?;
        if let Some(buf) = self.conn.try_recv() {
            return Some(buf);
        }
        self.flush_egress().await.ok()?;
        self.conn.recv().await
    }

    /// Append a reply written by `write`, to leave with whatever is corked
    /// beside it. Synchronous, so a caller holding a W2M ring slot can copy out
    /// of it and release it before any await.
    pub fn cork_with(&self, write: impl FnOnce(&mut Vec<u8>)) {
        let mut e = self.egress.borrow_mut();
        let acc = e.get_or_insert_with(|| PooledSendBuf(acquire_buf()));
        write(&mut acc.0);
    }

    /// Append `frame`'s bytes. See [`Self::cork_with`].
    pub fn cork(&self, frame: &[u8]) {
        self.cork_with(|acc| acc.extend_from_slice(frame));
    }

    /// Bytes currently corked.
    pub fn corked_len(&self) -> usize {
        self.egress.borrow().as_ref().map_or(0, |b| b.0.len())
    }

    /// Ship what is corked once it reaches [`COALESCE_MAX_BYTES`], bounding the
    /// accumulator across a long pipelined run.
    pub async fn flush_if_full(&self) -> Result<(), PeerGone> {
        self.live()?;
        if self.corked_len() < COALESCE_MAX_BYTES {
            return Ok(());
        }
        self.flush_egress().await
    }

    /// Write everything corked, as one send. No-op when nothing is pending.
    pub async fn flush_egress(&self) -> Result<(), PeerGone> {
        self.live()?;
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
        self.live()?;
        let len = slot.frame_bytes().len();
        let corked = self.corked_len();
        if corked == 0 || corked + len > COALESCE_MAX_BYTES {
            self.flush_egress().await?;
            if len > COALESCE_MAX_BYTES / 2 {
                return self.send_raw(SendBody::Slot(slot)).await;
            }
        }
        self.cork(slot.frame_bytes());
        Ok(())
    }

    fn live(&self) -> Result<(), PeerGone> {
        if self.conn.is_gone() {
            Err(PeerGone)
        } else {
            Ok(())
        }
    }

    async fn send_raw(&self, body: SendBody) -> Result<(), PeerGone> {
        match &self.transport {
            Transport::Unix(r) => r.send_owned(&self.conn, body).await,
            Transport::Tls(t) => t.send(body).await,
        }
    }

    pub fn close(&self) {
        self.conn.retire();
    }
}

#[cfg(test)]
#[path = "tests/peer.rs"]
mod tests;
