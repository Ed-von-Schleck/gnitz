//! Transport-neutral client-connection handle: the cork over one
//! [`ClientConn`], whose reactor sends and receives under either transport.

#[cfg(test)]
use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;

use crate::runtime::reactor::{Accepted, ClientConn, PeerGone, Reactor, RecvBuf, SendBody, Session};
use crate::runtime::sync::{select2, Either};
use gnitz_zset::repr::PooledBuf;

/// Ceiling on a concatenation of client-bound frames (coalesced scan heads, corked
/// replies): the copy paid to save per-frame sends. `fanout_coalesced_egress_bench`
/// measures the trade.
const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// Transport-neutral handle to one client connection. Owned by the connection
/// task; handlers borrow it to send replies. A `PeerGone` from any send or flush
/// means the connection is finished, and every later send or flush refuses too.
pub(super) struct Peer {
    reactor: Reactor,
    conn: Rc<ClientConn>,
    /// Replies written but not yet sent, concatenated so a run of pipelined
    /// requests leaves as one send. `None` when nothing is pending.
    egress: Option<PooledBuf>,
}

impl Peer {
    /// Build a connection over `accepted` and arm its recv: under `session`,
    /// or the socket's own bytes with none.
    pub(super) fn new(reactor: &Reactor, accepted: Accepted, session: Option<Rc<dyn Session>>) -> Peer {
        Peer {
            reactor: reactor.clone(),
            conn: reactor.connect(accepted, session),
            egress: None,
        }
    }

    /// See [`ClientConn::admit_full_frames`].
    pub(super) fn admit_full_frames(&self) {
        self.conn.admit_full_frames();
    }

    /// The client's TCP address; `None` on the AF_UNIX socket.
    pub(super) fn peer_addr(&self) -> Option<std::net::SocketAddr> {
        self.conn.peer_addr()
    }

    /// The next request, shipping what is corked before parking on the client.
    /// `None` once the client is gone or has sent its last request. Not
    /// cancel-safe under a session: see [`Reactor::recv`].
    pub(super) async fn next_request(&mut self) -> Option<RecvBuf> {
        self.flush_if_full().await.ok()?;
        // Not `is_readable`: a client that closed its sending side is readable,
        // and is still owed what is corked.
        if let Some(buf) = self.conn.try_recv() {
            return Some(buf);
        }
        self.flush_egress().await.ok()?;
        self.reactor.recv(&self.conn).await
    }

    /// What `waited` resolves to, or `None` once [`Self::next_request`] would
    /// not park. What is corked ships first, so `waited` is raced with no send.
    pub(super) async fn until_request<T>(&mut self, waited: impl Future<Output = T>) -> Result<Option<T>, PeerGone> {
        if !self.conn.is_readable() {
            self.flush_egress().await?;
        }
        Ok(match select2(waited, self.conn.readable()).await {
            Either::A(done) => Some(done),
            Either::B(()) => None,
        })
    }

    /// The cork: replies appended here leave with whatever is corked beside
    /// them.
    pub(super) fn corked(&mut self) -> &mut Vec<u8> {
        &mut (self.egress)
            .get_or_insert_with(|| PooledBuf::with_capacity(COALESCE_MAX_BYTES))
            .0
    }

    /// Append `payload` framed: its length prefix, then its bytes.
    pub(super) fn cork_frame(&mut self, payload: &[u8]) {
        let out = self.corked();
        out.extend_from_slice(&gnitz_wire::frame_len_prefix(payload.len()));
        out.extend_from_slice(payload);
    }

    /// Append `frame`'s bytes.
    pub(super) fn cork(&mut self, frame: &[u8]) {
        self.corked().extend_from_slice(frame);
    }

    /// Bytes currently corked.
    fn corked_len(&self) -> usize {
        self.egress.as_ref().map_or(0, |b| b.0.len())
    }

    /// Ship what is corked once it reaches [`COALESCE_MAX_BYTES`], bounding the
    /// accumulator across a long pipelined run.
    pub(super) async fn flush_if_full(&mut self) -> Result<(), PeerGone> {
        self.live()?;
        if self.corked_len() < COALESCE_MAX_BYTES {
            return Ok(());
        }
        self.flush_egress().await
    }

    /// Write everything corked, as one send. No-op when nothing is pending.
    pub(super) async fn flush_egress(&mut self) -> Result<(), PeerGone> {
        self.live()?;
        let Some(buf) = self.egress.take() else {
            return Ok(());
        };
        self.send_raw(SendBody::Pooled(buf)).await
    }

    /// Send `body` behind whatever is corked: copied into the cork, or alone
    /// and uncopied when it is too large to join it. A corked body reports a
    /// gone peer at its flush, not here.
    pub(super) async fn send(&mut self, mut body: SendBody) -> Result<(), PeerGone> {
        self.live()?;
        let len = body.bytes().len();
        let corked = self.corked_len();
        if corked == 0 || corked + len > COALESCE_MAX_BYTES {
            body.holding(self.flush_egress()).await?;
            if len > COALESCE_MAX_BYTES / 2 {
                return self.send_raw(body).await;
            }
        }
        self.cork(body.bytes());
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
        #[cfg(test)]
        SENDS.with(|n| n.set(n.get() + 1));
        self.reactor.send(&self.conn, body).await
    }

    /// Finish the connection, the client told so.
    pub(super) async fn close(&self) {
        self.reactor.close(&self.conn).await;
    }

    /// Finish the connection now, the client owed nothing more.
    pub(super) fn abort(&self) {
        self.conn.fail();
    }
}

#[cfg(test)]
thread_local! {
    /// Calls of `send_raw` on this thread.
    static SENDS: Cell<u64> = const { Cell::new(0) };
}

#[cfg(test)]
#[path = "tests/peer.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/peer.rs"]
mod bench;
