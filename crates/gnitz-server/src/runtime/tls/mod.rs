//! TLS 1.3 server transport. The recv side decrypts into the connection's
//! `FrameSink` through the session's [`Decrypt`] and never sends; ciphertext leaves only through
//! [`TlsShared::flush_records`], so records reach the socket in emission order.

mod config;
mod tokens;

pub(crate) use config::{CertSource, ResolvedTls, Served, TlsArgs, TlsListener};
pub(crate) use tokens::mint as mint_token;

use std::cell::RefCell;
use std::io::{BufRead, ErrorKind, Write};
use std::rc::Rc;
use std::sync::Arc;

use gnitz_zset::repr::PooledBuf;

use crate::runtime::reactor::{Accepted, Charge, ClientConn, Decrypt, FrameSink, PeerGone, Reactor, RecvEnd, SendBody};
use crate::runtime::sync::{chan, AsyncRwLock, WriteGuard};
use gnitz_wire::Deframer;

/// The most plaintext one TLS record carries.
const TLS_MAX_FRAGMENT: usize = 16 * 1024;
/// A TLS 1.3 record's bytes beyond its plaintext: header, inner content type,
/// AEAD tag.
const TLS_RECORD_OVERHEAD: usize = 5 + 1 + 16;

/// Feed one socket chunk of ciphertext through rustls, deframing the plaintext
/// it yields through `deframer` into `sink`. A peer's close_notify ends the recv
/// side once the plaintext ahead of it is queued.
fn ingest_cipher(
    sess: &mut rustls::ServerConnection,
    mut cipher: &[u8],
    deframer: &mut Deframer<Charge>,
    sink: &mut FrameSink,
) -> Result<(), RecvEnd> {
    while !cipher.is_empty() {
        // An `Err` is rustls's buffer being full, not a failure: the drain below
        // frees it and the loop retries the rest.
        let _ = sess.read_tls(&mut cipher);
        sess.process_new_packets().map_err(|_| RecvEnd::Protocol)?;
        let mut reader = sess.reader();
        loop {
            let n = match reader.fill_buf() {
                Ok([]) => return Err(RecvEnd::PeerClosed),
                Ok(chunk) => {
                    sink.feed(deframer, chunk)?;
                    chunk.len()
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(_) => return Err(RecvEnd::Protocol),
            };
            reader.consume(n);
        }
    }
    Ok(())
}

/// Shared handle to one TLS connection.
pub(crate) struct TlsShared {
    reactor: Reactor,
    /// The socket and its deframed frames; see `ClientConn` for the fd's lifetime.
    conn: Rc<ClientConn>,
    /// The codec state, and nothing else, shared with the recv side. Never
    /// borrowed across an await.
    state: Rc<RefCell<rustls::ServerConnection>>,
    /// Held across every ciphertext extraction and send. `ClientConn::retire` and
    /// `fail` never take it, so an eviction never waits on a stalled send.
    send_lock: AsyncRwLock<TlsSendLock>,
}

/// The tag of a TLS connection's send lock.
enum TlsSendLock {}

impl TlsShared {
    /// Build the connection over `accepted`, arm its recv filter and spawn its
    /// flusher; between them they drive the handshake.
    pub(crate) fn start(reactor: Reactor, accepted: Accepted, cfg: Arc<rustls::ServerConfig>) -> Rc<TlsShared> {
        /// rustls's outgoing-buffer limit: what one `writer().write()` accepts, and
        /// so how much ciphertext one encrypt-and-send turn carries.
        const SEND_BUFFER_BYTES: usize = 256 * 1024;

        let mut sess = rustls::ServerConnection::new(cfg)
            .expect("a session fails to build only on a max_fragment_size TlsArgs::serve never sets");
        sess.set_buffer_limit(Some(SEND_BUFFER_BYTES));
        let state = Rc::new(RefCell::new(sess));
        let (flush_tx, flush_rx) = chan::unbounded::<()>();
        let conn = reactor.connect(accepted, Some(flush_tx), Some(Rc::clone(&state) as Rc<dyn Decrypt>));
        let tls = Rc::new(TlsShared {
            reactor,
            conn,
            state,
            send_lock: AsyncRwLock::default(),
        });
        tls.reactor.spawn(flusher(Rc::clone(&tls), flush_rx));
        tls
    }

    /// The connection this session runs over.
    pub(crate) fn conn(&self) -> &Rc<ClientConn> {
        &self.conn
    }

    /// Send everything rustls has queued, under `_send`, a hold of `send_lock`
    /// that keeps records in emission order. `plaintext`, the bytes handed to
    /// rustls since the last flush, sizes the buffer.
    async fn flush_records(&self, _send: &WriteGuard<TlsSendLock>, plaintext: usize) -> Result<(), PeerGone> {
        let records = plaintext.div_ceil(TLS_MAX_FRAGMENT);
        let mut out = PooledBuf::with_capacity(plaintext + records * TLS_RECORD_OVERHEAD);
        {
            let mut sess = self.state.borrow_mut();
            while sess.wants_write() {
                let _ = sess.write_tls(&mut out.0);
            }
        }
        self.reactor.send_owned(&self.conn, SendBody::Pooled(out)).await
    }

    /// Encrypt and send `body` under one `send_lock` hold; rustls sizes the chunks.
    /// Neither the wait for the lock nor a flush holds a W2M slot `body` is in
    /// once its ring is given back ([`SendBody::holding`]).
    pub(crate) async fn send(&self, mut body: SendBody) -> Result<(), PeerGone> {
        let send = body.holding(self.send_lock.write()).await;
        let len = body.bytes().len();
        let mut off = 0;
        while off < len {
            if self.conn.is_gone() {
                return Err(PeerGone);
            }
            // rustls short-writes only on a full outgoing buffer, which the
            // previous turn's flush emptied: a zero here is a wedged session.
            let Ok(n @ 1..) = self.state.borrow_mut().writer().write(&body.bytes()[off..]) else {
                self.conn.fail();
                return Err(PeerGone);
            };
            off += n;
            if off == len {
                // rustls holds every byte now: release the body — a W2M slot — before the send.
                drop(body);
                return self.flush_records(&send, n).await;
            }
            body.holding(self.flush_records(&send, n)).await?;
        }
        Ok(())
    }
}

/// Ciphertext in, plaintext frames out, inside the recv completion. Never locks
/// `send_lock` and never sends: what rustls queues meanwhile it reports through
/// `owes_egress`, and the reactor wakes the flusher. So a client pipelining
/// pushes ahead of reading its ACKs cannot deadlock it.
impl Decrypt for RefCell<rustls::ServerConnection> {
    fn ingest(&self, cipher: &[u8], deframer: &mut Deframer<Charge>, sink: &mut FrameSink) -> Result<(), RecvEnd> {
        ingest_cipher(&mut self.borrow_mut(), cipher, deframer, sink)
    }

    /// The flusher ships the records rustls queued.
    fn owes_egress(&self) -> bool {
        self.borrow().wants_write()
    }
}

/// Ships what rustls queues outside a send — handshake flights, alerts, the
/// close_notify — and shuts the socket down once the connection is gone.
async fn flusher(tls: Rc<TlsShared>, mut rx: chan::Receiver<()>) {
    loop {
        // The queue is a flag, not a stream: park on the next notification, then
        // drop whatever piled up behind it.
        rx.recv().await;
        while rx.try_recv().is_some() {}
        let gone = tls.conn.is_gone();
        if gone {
            tls.state.borrow_mut().send_close_notify();
        }
        let _ = tls.flush_records(&tls.send_lock.write().await, 0).await;
        if gone {
            tls.conn.shutdown();
            return;
        }
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
