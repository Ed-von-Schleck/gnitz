//! TLS 1.3 server transport. The recv filter ([`TlsIngress`]) decrypts into the
//! connection's `RecvQueue` and never sends; ciphertext leaves only through
//! [`TlsShared::flush_records`], so records reach the socket in emission order.

mod config;
mod listener;

pub(crate) use listener::{setup_tls_listener, TlsCli, TlsListener};

use std::cell::{Cell, RefCell};
use std::io::{ErrorKind, Read, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::sync::Arc;

use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};

use crate::runtime::reactor::{
    chan, shutdown, AsyncMutex, ClientConn, PeerGone, Reactor, RecvEnd, RecvFilter, RecvQueue, SendBody, WriteGuard,
};

/// Size of the ciphertext window each recv lands in.
const CIPHER_WINDOW_BYTES: usize = 64 * 1024;

/// Feed one socket chunk of ciphertext through rustls, enqueueing the plaintext
/// frames it yields on `q`.
fn ingest_cipher(sess: &mut rustls::ServerConnection, mut cipher: &[u8], q: &mut RecvQueue) -> Result<(), RecvEnd> {
    while !cipher.is_empty() {
        // `Ok(0)` is end-of-stream. An `Err` is rustls's buffer being full, not a
        // failure: the drain below frees it and the loop retries the rest.
        if matches!(sess.read_tls(&mut cipher), Ok(0)) {
            return Err(RecvEnd::PeerClosed);
        }
        let io_state = sess.process_new_packets().map_err(|_| RecvEnd::Protocol)?;
        // Before the close test, so plaintext decrypted in this chunk is still
        // enqueued when the peer closed in it too.
        feed_decrypted(sess, q)?;
        if io_state.peer_has_closed() {
            return Err(RecvEnd::PeerClosed);
        }
    }
    Ok(())
}

/// Drain all currently-available decrypted plaintext into `q`'s own write window.
fn feed_decrypted(sess: &mut rustls::ServerConnection, q: &mut RecvQueue) -> Result<(), RecvEnd> {
    loop {
        let (ptr, len) = q.remaining();
        // SAFETY: ptr/len are the queue's own write window (its carry, or an
        // in-flight RecvBuf payload), exclusively ours.
        let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
        match sess.reader().read(slice) {
            Ok(0) => return Err(RecvEnd::PeerClosed), // clean close_notify
            Ok(m) => q.deliver(m)?,
            Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()), // drained; wait
            Err(_) => return Err(RecvEnd::Protocol),
        }
    }
}

/// Live-TLS-connection counter guard: constructed only by
/// [`TlsListener::admit`], so the count and the cap are inseparable, and held in
/// `TlsShared`, so it drops exactly when the session ends.
pub(crate) struct ConnCountGuard(Rc<Cell<u32>>);

impl ConnCountGuard {
    fn new(c: Rc<Cell<u32>>) -> Self {
        c.set(c.get() + 1);
        Self(c)
    }
}

impl Drop for ConnCountGuard {
    fn drop(&mut self) {
        debug_assert!(self.0.get() > 0, "tls conn count underflow");
        self.0.set(self.0.get() - 1);
    }
}

/// Shared handle to one TLS connection. The recv filter holds one too, so
/// `ConnCountGuard` tracks the session until the socket closes.
pub(crate) struct TlsShared {
    reactor: Rc<Reactor>,
    /// The socket and its deframed frames; see `ClientConn` for the fd's lifetime.
    conn: Rc<ClientConn>,
    /// The codec state, and nothing else. Never borrowed across an await.
    state: RefCell<rustls::ServerConnection>,
    /// Decrements the reactor-thread live-connection counter on teardown.
    _conn_guard: ConnCountGuard,
    /// This connection is finished, from whichever end: senders refuse and the
    /// flusher shuts the socket down and exits.
    closed: Cell<bool>,
    /// Held across every ciphertext extraction and send. Teardown never takes it.
    send_mutex: AsyncMutex,
    /// Wakes the flusher task.
    flush_tx: chan::Sender<()>,
}

/// What every accepted TLS socket wants, set here so the accept loop needs no
/// socket-option knowledge. Best-effort: a socket that refuses either works on.
fn set_socket_options(fd: i32) {
    let on: libc::c_int = 1;
    let len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    for (level, opt) in [
        // Small control frames must not pay Nagle's 40 ms batching delay
        // (AF_UNIX has none, so this restores latency parity).
        (libc::IPPROTO_TCP, libc::TCP_NODELAY),
        // Untuned: a silently half-open connection is reaped by the kernel
        // default probing (~2 h) rather than parking a recv forever.
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE),
    ] {
        unsafe {
            libc::setsockopt(fd, level, opt, &on as *const _ as *const libc::c_void, len);
        }
    }
}

impl TlsShared {
    /// Build the connection, arm its recv filter and spawn its flusher; between them
    /// they drive the handshake.
    pub(crate) fn start(
        reactor: Rc<Reactor>,
        fd: OwnedFd,
        cfg: Arc<rustls::ServerConfig>,
        conn_guard: ConnCountGuard,
    ) -> Result<Rc<TlsShared>, rustls::Error> {
        /// rustls's outgoing-buffer limit: what one `writer().write()` accepts, and
        /// so how much ciphertext one encrypt-and-send turn carries.
        const SEND_BUFFER_BYTES: usize = 256 * 1024;

        // `ServerConnection::new` runs first: on failure the moved-in `fd` and
        // `conn_guard` drop here as this frame unwinds — closing the socket and
        // decrementing the count; on success both live in the returned `TlsShared`.
        let mut sess = rustls::ServerConnection::new(cfg)?;
        sess.set_buffer_limit(Some(SEND_BUFFER_BYTES));
        set_socket_options(fd.as_raw_fd());
        let conn = reactor.client_conn(fd);
        let (flush_tx, flush_rx) = chan::unbounded::<()>();
        let tls = Rc::new(TlsShared {
            reactor,
            conn,
            state: RefCell::new(sess),
            _conn_guard: conn_guard,
            closed: Cell::new(false),
            send_mutex: AsyncMutex::default(),
            flush_tx,
        });
        let ingress = TlsIngress {
            tls: Rc::clone(&tls),
            cipher: Box::new_uninit_slice(CIPHER_WINDOW_BYTES),
        };
        tls.reactor.register_conn(&tls.conn, Some(Box::new(ingress)));
        tls.reactor.spawn(flusher(Rc::clone(&tls), flush_rx));
        Ok(tls)
    }

    /// The connection the reactor deframes this session's plaintext into.
    pub(crate) fn conn(&self) -> &Rc<ClientConn> {
        &self.conn
    }

    fn notify_flusher(&self) {
        self.flush_tx.send(());
    }

    /// Send everything rustls has queued. Callers hold `send_mutex`, which is
    /// what keeps records in emission order.
    async fn flush_records(&self, _: &WriteGuard) -> Result<(), PeerGone> {
        let mut out = PooledSendBuf(acquire_buf());
        {
            let mut sess = self.state.borrow_mut();
            while sess.wants_write() {
                let _ = sess.write_tls(&mut out.0);
            }
        }
        self.reactor.send_owned(&self.conn, SendBody::from(out)).await.0
    }

    /// Encrypt and send `bytes` under one `send_mutex` hold; rustls sizes the chunks.
    pub(crate) async fn send_bytes(&self, bytes: &[u8]) -> Result<(), PeerGone> {
        let send = self.send_mutex.lock().await;
        let mut off = 0;
        while off < bytes.len() {
            {
                let mut sess = self.state.borrow_mut();
                if self.closed.get() {
                    return Err(PeerGone);
                }
                match sess.writer().write(&bytes[off..]) {
                    Ok(n) if n > 0 => off += n,
                    // rustls short-writes only on a full outgoing buffer, which the
                    // previous turn's flush emptied: a zero here is a wedged session.
                    _ => return Err(PeerGone),
                }
            }
            self.flush_records(&send).await?;
        }
        Ok(())
    }

    /// Sync and idempotent: the first call queues a close_notify and wakes the flusher.
    pub(crate) fn close(&self) {
        if self.closed.replace(true) {
            return;
        }
        self.state.borrow_mut().send_close_notify();
        self.notify_flusher();
    }
}

/// Ciphertext in, plaintext frames out, inside the recv completion. Never locks
/// `send_mutex` and never sends — it only notifies the flusher — so a client
/// pipelining pushes ahead of reading its ACKs cannot deadlock it.
struct TlsIngress {
    tls: Rc<TlsShared>,
    cipher: Box<[MaybeUninit<u8>]>,
}

impl RecvFilter for TlsIngress {
    fn window(&mut self) -> (*mut u8, u32) {
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
            self.tls.notify_flusher();
        }
        result
    }

    fn on_recv_closed(&mut self) {
        // Half-close so a writer parked on a full sndbuf errors out and releases
        // `send_mutex`; without it teardown could wedge behind a stalled sender.
        shutdown(self.tls.conn.fd());
        self.tls.closed.set(true);
        self.tls.notify_flusher();
    }
}

/// Ships what rustls queues outside a send — handshake flights, alerts, a
/// close_notify — and tears the socket down once either side has closed.
async fn flusher(conn: Rc<TlsShared>, mut rx: chan::Receiver<()>) {
    loop {
        {
            let send = conn.send_mutex.lock().await;
            let _ = conn.flush_records(&send).await;
        } // released before the teardown check and before parking on rx

        // Whatever was queued is out, including a close_notify: finish the socket.
        if conn.closed.get() {
            shutdown(conn.conn.fd());
            return;
        }
        // The queue is a flag, not a stream: park on the next notification, then
        // drop whatever piled up behind it.
        rx.recv().await;
        while rx.try_recv().is_some() {}
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
