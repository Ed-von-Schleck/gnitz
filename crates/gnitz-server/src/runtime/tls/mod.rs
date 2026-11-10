//! TLS 1.3 server transport. The recv side decrypts as the connection's
//! [`Session`] and never sends; ciphertext leaves only through
//! [`TlsSession::flush_records`], so records reach the socket in emission order.

mod config;
mod tokens;

pub(crate) use config::{CertSource, ResolvedTls, Served, TlsArgs, TlsListener};
pub(crate) use tokens::mint as mint_token;

use std::cell::{Cell, RefCell};
use std::io::{BufRead, ErrorKind, Write};
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Poll, Waker};

use gnitz_zset::repr::PooledBuf;

use crate::runtime::reactor::{Accepted, ClientConn, PeerGone, Reactor, RecvEnd, SendBody, Session};
use crate::runtime::sync::{AsyncRwLock, WriteGuard};

/// The most plaintext one TLS record carries.
const TLS_MAX_FRAGMENT: usize = 16 * 1024;
/// A TLS 1.3 record's bytes beyond its plaintext: header, inner content type,
/// AEAD tag.
const TLS_RECORD_OVERHEAD: usize = 5 + 1 + 16;

/// Feed one socket chunk of ciphertext through rustls, handing the plaintext
/// it yields to `plain`. A peer's close_notify ends the recv side once the
/// plaintext ahead of it is handed over.
fn ingest_cipher(
    sess: &mut rustls::ServerConnection,
    mut cipher: &[u8],
    plain: &mut dyn FnMut(&[u8]) -> Result<(), RecvEnd>,
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
                    plain(chunk)?;
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

/// One TLS connection's codec and send order. It holds neither its connection
/// nor the reactor, which its callers pass in: the connection holds it as its
/// [`Session`].
pub(crate) struct TlsSession {
    /// Never borrowed across an await.
    sess: RefCell<rustls::ServerConnection>,
    /// Held across every ciphertext extraction and send. `ClientConn::retire` and
    /// `fail` never take it, so an eviction never waits on a stalled send.
    send_lock: AsyncRwLock<TlsSendLock>,
    /// The flusher, parked until rustls holds records or the connection is gone.
    flusher: Cell<Option<Waker>>,
}

/// The tag of a TLS connection's send lock.
enum TlsSendLock {}

impl TlsSession {
    /// Build the connection over `accepted`, arm its recv and spawn its
    /// flusher; between them they drive the handshake.
    pub(crate) fn start(
        reactor: &Reactor,
        accepted: Accepted,
        cfg: Arc<rustls::ServerConfig>,
    ) -> (Rc<ClientConn>, Rc<TlsSession>) {
        /// rustls's outgoing-buffer limit: what one `writer().write()` accepts, and
        /// so how much ciphertext one encrypt-and-send turn carries.
        const SEND_BUFFER_BYTES: usize = 256 * 1024;

        let mut sess = rustls::ServerConnection::new(cfg)
            .expect("a session fails to build only on a max_fragment_size TlsArgs::serve never sets");
        sess.set_buffer_limit(Some(SEND_BUFFER_BYTES));
        let tls = Rc::new(TlsSession {
            sess: RefCell::new(sess),
            send_lock: AsyncRwLock::default(),
            flusher: Cell::new(None),
        });
        let conn = reactor.connect(accepted, Some(Rc::clone(&tls) as Rc<dyn Session>));
        reactor.spawn(flusher(reactor.clone(), Rc::clone(&conn), Rc::clone(&tls)));
        (conn, tls)
    }

    fn wake_flusher(&self) {
        if let Some(w) = self.flusher.take() {
            w.wake();
        }
    }

    /// Send everything rustls has queued on `conn`, under `_send`, a hold of
    /// `send_lock` that keeps records in emission order. `plaintext`, the bytes
    /// handed to rustls since the last flush, sizes the buffer.
    async fn flush_records(
        &self,
        reactor: &Reactor,
        conn: &Rc<ClientConn>,
        _send: &WriteGuard<TlsSendLock>,
        plaintext: usize,
    ) -> Result<(), PeerGone> {
        let records = plaintext.div_ceil(TLS_MAX_FRAGMENT);
        let mut out = PooledBuf::with_capacity(plaintext + records * TLS_RECORD_OVERHEAD);
        {
            let mut sess = self.sess.borrow_mut();
            while sess.wants_write() {
                let _ = sess.write_tls(&mut out.0);
            }
        }
        reactor.send_owned(conn, SendBody::Pooled(out)).await
    }

    /// Encrypt and send `body` on `conn` under one `send_lock` hold; rustls
    /// sizes the chunks. Neither the wait for the lock nor a flush holds a W2M
    /// slot `body` is in once its ring is given back ([`SendBody::holding`]).
    pub(crate) async fn send(
        &self,
        reactor: &Reactor,
        conn: &Rc<ClientConn>,
        mut body: SendBody,
    ) -> Result<(), PeerGone> {
        let send = body.holding(self.send_lock.write()).await;
        let len = body.bytes().len();
        let mut off = 0;
        while off < len {
            if conn.is_gone() {
                return Err(PeerGone);
            }
            // rustls short-writes only on a full outgoing buffer, which the
            // previous turn's flush emptied: a zero here is a wedged session.
            let Ok(n @ 1..) = self.sess.borrow_mut().writer().write(&body.bytes()[off..]) else {
                conn.fail();
                return Err(PeerGone);
            };
            off += n;
            if off == len {
                // rustls holds every byte now: release the body — a W2M slot — before the send.
                drop(body);
                return self.flush_records(reactor, conn, &send, n).await;
            }
            body.holding(self.flush_records(reactor, conn, &send, n)).await?;
        }
        Ok(())
    }
}

/// Ciphertext in, plaintext out, inside the recv completion. Never locks
/// `send_lock` and never sends: the flusher ships what rustls queues meanwhile.
/// So a client pipelining pushes ahead of reading its ACKs cannot deadlock it.
impl Session for TlsSession {
    fn ingest(&self, cipher: &[u8], plain: &mut dyn FnMut(&[u8]) -> Result<(), RecvEnd>) -> Result<(), RecvEnd> {
        let mut sess = self.sess.borrow_mut();
        let ingested = ingest_cipher(&mut sess, cipher, plain);
        // Whatever the ingest came to: a refused record queues its alert.
        if sess.wants_write() {
            self.wake_flusher();
        }
        ingested
    }

    /// The flusher sends the close_notify and shuts the socket down.
    fn retired(&self) {
        self.wake_flusher();
    }
}

/// Ships what rustls queues outside a send — handshake flights, alerts, the
/// close_notify — and shuts the socket down once the connection is gone.
async fn flusher(reactor: Reactor, conn: Rc<ClientConn>, tls: Rc<TlsSession>) {
    loop {
        std::future::poll_fn(|cx| {
            if conn.is_gone() || tls.sess.borrow().wants_write() {
                return Poll::Ready(());
            }
            tls.flusher.set(Some(cx.waker().clone()));
            Poll::Pending
        })
        .await;
        let gone = conn.is_gone();
        if gone {
            tls.sess.borrow_mut().send_close_notify();
        }
        let _ = tls
            .flush_records(&reactor, &conn, &tls.send_lock.write().await, 0)
            .await;
        if gone {
            conn.shutdown();
            return;
        }
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
