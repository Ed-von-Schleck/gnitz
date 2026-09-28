//! TLS 1.3 server transport. The recv filter ([`TlsIngress`]) decrypts into the
//! connection's `RecvQueue` and never sends; ciphertext leaves only through
//! [`TlsShared::flush_records`], so records reach the socket in emission order.

mod config;

pub(crate) use config::{TlsArgs, TlsConfig};

use std::cell::RefCell;
use std::io::{BufRead, ErrorKind, Write};
use std::mem::MaybeUninit;
use std::rc::Rc;
use std::sync::Arc;

use gnitz_store::storage::{acquire_buf, PooledBuf};

use crate::runtime::reactor::{
    chan, AsyncRwLock, ClientConn, PeerGone, Reactor, RecvEnd, RecvFilter, RecvQueue, SendBody, WriteGuard,
};

/// Size of the ciphertext window each recv lands in.
const CIPHER_WINDOW_BYTES: usize = 64 * 1024;

/// Feed one socket chunk of ciphertext through rustls, enqueueing the plaintext
/// frames it yields on `q`.
fn ingest_cipher(sess: &mut rustls::ServerConnection, mut cipher: &[u8], q: &mut RecvQueue) -> Result<(), RecvEnd> {
    while !cipher.is_empty() {
        // An `Err` is rustls's buffer being full, not a failure: the drain below
        // frees it and the loop retries the rest.
        let _ = sess.read_tls(&mut cipher);
        sess.process_new_packets().map_err(|_| RecvEnd::Protocol)?;
        feed_decrypted(sess, q)?;
    }
    Ok(())
}

/// Hand every decrypted plaintext chunk rustls holds to `q`. A peer's close_notify
/// ends the recv side here, once the plaintext ahead of it is queued.
fn feed_decrypted(sess: &mut rustls::ServerConnection, q: &mut RecvQueue) -> Result<(), RecvEnd> {
    let mut reader = sess.reader();
    loop {
        let n = match reader.fill_buf() {
            Ok([]) => return Err(RecvEnd::PeerClosed),
            Ok(chunk) => {
                q.feed(chunk)?;
                chunk.len()
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
            Err(_) => return Err(RecvEnd::Protocol),
        };
        reader.consume(n);
    }
}

/// Shared handle to one TLS connection.
pub(crate) struct TlsShared {
    reactor: Rc<Reactor>,
    /// The socket and its deframed frames; see `ClientConn` for the fd's lifetime.
    conn: Rc<ClientConn>,
    /// The codec state, and nothing else. Never borrowed across an await.
    state: RefCell<rustls::ServerConnection>,
    /// Held across every ciphertext extraction and send. Teardown never takes it.
    send_lock: AsyncRwLock,
}

impl TlsShared {
    /// Build the connection, arm its recv filter and spawn its flusher; between them
    /// they drive the handshake.
    pub(crate) fn start(reactor: Rc<Reactor>, conn: Rc<ClientConn>, cfg: Arc<rustls::ServerConfig>) -> Rc<TlsShared> {
        /// rustls's outgoing-buffer limit: what one `writer().write()` accepts, and
        /// so how much ciphertext one encrypt-and-send turn carries.
        const SEND_BUFFER_BYTES: usize = 256 * 1024;

        let mut sess =
            rustls::ServerConnection::new(cfg).expect("server_crypto built a session from this config at boot");
        sess.set_buffer_limit(Some(SEND_BUFFER_BYTES));
        let (flush_tx, flush_rx) = chan::unbounded::<()>();
        let tls = Rc::new(TlsShared {
            reactor,
            conn,
            state: RefCell::new(sess),
            send_lock: AsyncRwLock::default(),
        });
        tls.conn.set_egress_owner(flush_tx);
        let ingress = TlsIngress {
            tls: Rc::clone(&tls),
            cipher: Box::new_uninit_slice(CIPHER_WINDOW_BYTES),
        };
        tls.reactor.register_conn(&tls.conn, Box::new(ingress));
        tls.reactor.spawn(flusher(Rc::clone(&tls), flush_rx));
        tls
    }

    /// Send everything rustls has queued. Callers hold `send_lock`, which is
    /// what keeps records in emission order.
    async fn flush_records(&self, _: &WriteGuard) -> Result<(), PeerGone> {
        let mut out = PooledBuf(acquire_buf());
        {
            let mut sess = self.state.borrow_mut();
            while sess.wants_write() {
                let _ = sess.write_tls(&mut out.0);
            }
        }
        self.reactor.send_owned(&self.conn, SendBody::Pooled(out)).await
    }

    async fn flush_queued(&self) {
        let send = self.send_lock.write().await;
        let _ = self.flush_records(&send).await;
    }

    /// Encrypt and send `body` under one `send_lock` hold; rustls sizes the chunks.
    pub(crate) async fn send(&self, body: SendBody) -> Result<(), PeerGone> {
        let send = self.send_lock.write().await;
        let len = body.bytes().len();
        let mut off = 0;
        while off < len {
            {
                let mut sess = self.state.borrow_mut();
                if self.conn.is_gone() {
                    return Err(PeerGone);
                }
                match sess.writer().write(&body.bytes()[off..]) {
                    Ok(n) if n > 0 => off += n,
                    // rustls short-writes only on a full outgoing buffer, which the
                    // previous turn's flush emptied: a zero here is a wedged session.
                    _ => {
                        self.conn.fail();
                        return Err(PeerGone);
                    }
                }
            }
            if off == len {
                break;
            }
            self.flush_records(&send).await?;
        }
        // rustls holds every byte now: release the body — a W2M slot — before the send.
        drop(body);
        self.flush_records(&send).await
    }
}

/// Ciphertext in, plaintext frames out, inside the recv completion. Never locks
/// `send_lock` and never sends — it only notifies the flusher — so a client
/// pipelining pushes ahead of reading its ACKs cannot deadlock it.
struct TlsIngress {
    tls: Rc<TlsShared>,
    cipher: Box<[MaybeUninit<u8>]>,
}

impl RecvFilter for TlsIngress {
    fn window(&mut self, _q: &mut RecvQueue) -> (*mut u8, u32) {
        (self.cipher.as_mut_ptr().cast::<u8>(), self.cipher.len() as u32)
    }

    fn ingest(&mut self, n: usize, q: &mut RecvQueue) -> Result<(), RecvEnd> {
        // The queue wakes the waiter itself, and only on a completed frame — a
        // large push therefore parks `connection_loop` once per frame, not once
        // per window of ciphertext.
        let mut sess = self.tls.state.borrow_mut();
        // SAFETY: `n` counts bytes the completed recv wrote into the window.
        let cipher = unsafe { self.cipher[..n].assume_init_ref() };
        let result = ingest_cipher(&mut sess, cipher, q);
        if sess.wants_write() {
            self.tls.conn.wake_egress();
        }
        result
    }
}

/// Ships what rustls queues outside a send — handshake flights, alerts, the
/// close_notify — and shuts the socket down once the connection is gone.
async fn flusher(conn: Rc<TlsShared>, mut rx: chan::Receiver<()>) {
    while !conn.conn.is_gone() {
        conn.flush_queued().await;
        // The queue is a flag, not a stream: park on the next notification, then
        // drop whatever piled up behind it.
        rx.recv().await;
        while rx.try_recv().is_some() {}
    }
    conn.state.borrow_mut().send_close_notify();
    conn.flush_queued().await;
    conn.conn.shutdown();
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
