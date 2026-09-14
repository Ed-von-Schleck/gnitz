//! TLS 1.3 server transport. A connection's recv filter ([`TlsIngress`]) decrypts
//! into its `RecvQueue` and never sends; ciphertext leaves only under [`SendHeld`],
//! so records reach the socket in emission order.

mod config;
mod listener;

pub(crate) use listener::{setup_tls_listener, TlsCli, TlsListener};

use std::cell::{Cell, RefCell};
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::rc::Rc;
use std::sync::Arc;

use crate::runtime::reactor::{
    chan, shutdown, AsyncMutex, ClientConn, Reactor, RecvFilter, RecvQueue, SendBody, WriteGuard,
};

/// Size of the ciphertext window each recv lands in.
const CIPHER_WINDOW_BYTES: usize = 64 * 1024;

/// Per-connection TLS state, behind `TlsShared::state`. Never borrowed
/// across an await.
struct TlsConn {
    sess: rustls::ServerConnection,
    /// `Peer::close()` ran; senders refuse, flusher tears down.
    closed: bool,
}

impl TlsConn {
    /// Feed one socket chunk of ciphertext through rustls and enqueue whatever
    /// plaintext frames come out on `q`. `Err` ⇒ recv-side teardown.
    ///
    /// An `Err` from `read_tls` is backpressure, not failure, so it is ignored:
    /// the drain below frees the buffer and the loop retries the unconsumed
    /// slice. It terminates because one record always fits that buffer.
    fn ingest_cipher(&mut self, mut cipher: &[u8], q: &mut RecvQueue, fd: i32) -> Result<(), ()> {
        while !cipher.is_empty() {
            // `Ok(0)` = end-of-stream: a `close_notify` was already received,
            // so no further data will ever be read.
            if matches!(self.sess.read_tls(&mut cipher), Ok(0)) {
                return Err(());
            }
            let io_state = self.sess.process_new_packets().map_err(|_| ())?;
            // Before the close test, so already-decrypted plaintext is still
            // enqueued when the peer closed in the same chunk.
            self.feed_decrypted(q, fd)?;
            if io_state.peer_has_closed() {
                return Err(());
            }
        }
        Ok(())
    }

    /// Drain all currently-available decrypted plaintext into `q`.
    /// rustls reads plaintext straight into the deframer's write window — no
    /// intermediate bounce buffer. `Err` ⇒ recv-side teardown (oversize,
    /// zero-len sentinel, cap breach, or a clean/unclean plaintext close).
    fn feed_decrypted(&mut self, q: &mut RecvQueue, fd: i32) -> Result<(), ()> {
        let (mut ptr, mut len) = q.remaining();
        loop {
            // SAFETY: ptr/len are the queue's own write window (its carry, or
            // an in-flight RecvBuf payload), exclusively ours.
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
            // Collapsing WouldBlock into close would kill every live
            // connection; collapsing close into WouldBlock would spin.
            match self.sess.reader().read(slice) {
                Ok(0) => return Err(()), // clean close_notify
                Ok(m) => (ptr, len) = q.deliver(m, fd)?,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()), // drained; wait
                Err(_) => return Err(()),                                     // UnexpectedEof (truncation) etc.
            }
        }
    }
}

/// Live-TLS-connection counter guard: constructed only by
/// [`TlsListener::admit`], which is what makes the count and the cap
/// inseparable. Stored in `TlsShared`, so the count tracks the session lifetime
/// exactly (a session-init failure drops it inside `start`; a live session
/// drops it at full teardown, when the last `Rc<TlsShared>` drops).
pub(crate) struct ConnCountGuard(Rc<Cell<u32>>);

impl ConnCountGuard {
    pub(in crate::runtime::tls) fn new(c: Rc<Cell<u32>>) -> Self {
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
    state: RefCell<TlsConn>,
    /// Decrements the reactor-thread live-connection counter on teardown.
    _conn_guard: ConnCountGuard,
    /// Taken only through [`TlsShared::lock_send`]. Teardown never takes it.
    send_mutex: AsyncMutex,
    /// Ciphertext staging, used under [`SendHeld`].
    cipher_scratch: RefCell<Vec<u8>>,
    /// Wakes the flusher task.
    flush_tx: chan::Sender<()>,
}

/// Proof that this connection's `send_mutex` is held, which every ciphertext
/// extraction and send demands.
struct SendHeld {
    _guard: WriteGuard,
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
        // `ServerConnection::new` runs first: on failure the moved-in `fd` and
        // `conn_guard` drop here as this frame unwinds — closing the socket and
        // decrementing the count; on success both live in the returned `TlsShared`.
        let sess = rustls::ServerConnection::new(cfg)?;
        set_socket_options(fd.as_raw_fd());
        let conn = reactor.client_conn(fd);
        let (flush_tx, flush_rx) = chan::unbounded::<()>();
        let tls = Rc::new(TlsShared {
            reactor,
            conn,
            state: RefCell::new(TlsConn { sess, closed: false }),
            _conn_guard: conn_guard,
            send_mutex: AsyncMutex::default(),
            cipher_scratch: RefCell::new(Vec::new()),
            flush_tx,
        });
        let ingress = TlsIngress {
            tls: Rc::clone(&tls),
            cipher: vec![0u8; CIPHER_WINDOW_BYTES].into_boxed_slice(),
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

    async fn lock_send(&self) -> SendHeld {
        SendHeld { _guard: self.send_mutex.lock().await }
    }

    /// Everything rustls has queued, in the scratch buffer.
    fn extract_ciphertext(&self, _: &SendHeld, c: &mut TlsConn) -> Vec<u8> {
        let mut out = self.cipher_scratch.take();
        out.clear();
        while c.sess.wants_write() {
            let _ = c.sess.write_tls(&mut out);
        }
        out
    }

    /// Send one ciphertext buffer, returning it to `cipher_scratch`.
    async fn send_cipher(&self, _: &SendHeld, cipher: Vec<u8>) -> i32 {
        let (rc, back) = self.reactor.send_owned(&self.conn, SendBody::Cipher(cipher)).await;
        if let SendBody::Cipher(v) = back {
            self.cipher_scratch.replace(v);
        }
        rc
    }

    /// Encrypt and send `bytes` under one `send_mutex` hold; rustls sizes the chunks.
    pub(crate) async fn send_bytes(&self, bytes: &[u8]) -> i32 {
        let send = self.lock_send().await;
        let mut off = 0;
        while off < bytes.len() {
            let cipher = {
                let mut c = self.state.borrow_mut();
                if c.closed || self.conn.recv_closed() {
                    return -1;
                }
                match c.sess.writer().write(&bytes[off..]) {
                    // A queue we cannot drain: kill, don't spin.
                    Ok(0) => return -1,
                    Ok(n) => off += n,
                    Err(_) => return -1,
                }
                self.extract_ciphertext(&send, &mut c)
            }; // state borrow released before the await
            if self.send_cipher(&send, cipher).await < 0 {
                return -1;
            }
        }
        bytes.len() as i32
    }

    /// Sync and idempotent: the first transition of `closed` queues a
    /// close_notify and notifies the flusher, which flushes it, shuts the
    /// socket down and exits.
    pub(crate) fn close(&self) {
        {
            let mut c = self.state.borrow_mut();
            if c.closed {
                return;
            }
            c.closed = true;
            c.sess.send_close_notify();
        }
        self.notify_flusher();
    }
}

/// Ciphertext in, plaintext frames out, inside the recv completion. Never locks
/// `send_mutex` and never sends — it only notifies the flusher — so a client
/// pipelining pushes ahead of reading its ACKs cannot deadlock it.
struct TlsIngress {
    tls: Rc<TlsShared>,
    cipher: Box<[u8]>,
}

impl RecvFilter for TlsIngress {
    fn window(&mut self) -> (*mut u8, u32) {
        (self.cipher.as_mut_ptr(), self.cipher.len() as u32)
    }

    fn ingest(&mut self, n: usize, q: &mut RecvQueue, fd: i32) -> Result<(), ()> {
        // The queue wakes the waiter itself, and only on a completed frame — a
        // large push therefore parks `connection_loop` once per frame, not once
        // per window of ciphertext.
        let mut c = self.tls.state.borrow_mut();
        let result = c.ingest_cipher(&self.cipher[..n], q, fd);
        if c.sess.wants_write() {
            self.tls.notify_flusher();
        }
        result
    }

    fn recv_closed(&mut self, fd: i32) {
        // Half-close so a writer parked on a full sndbuf errors out and releases
        // `send_mutex`; without it teardown could wedge behind a stalled sender.
        shutdown(fd);
        self.tls.notify_flusher();
    }
}

/// Ships what rustls queues outside a send — handshake flights, alerts, a
/// close_notify — and tears the socket down once either side has closed.
async fn flusher(conn: Rc<TlsShared>, mut rx: chan::Receiver<()>) {
    loop {
        {
            let send = conn.lock_send().await;
            let out = {
                let mut c = conn.state.borrow_mut();
                conn.extract_ciphertext(&send, &mut c)
            }; // state borrow released here, before the await
            if out.is_empty() {
                conn.cipher_scratch.replace(out);
            } else {
                let _ = conn.send_cipher(&send, out).await;
            }
        } // released before the teardown checks and before parking on rx

        // Half-close: ends a parked recv and errors out a parked send, so
        // `send_mutex` is always released.
        let closed = conn.state.borrow().closed;
        if closed || conn.conn.recv_closed() {
            shutdown(conn.conn.fd());
        }
        // The local close ran, so nothing more will ever be queued for this
        // connection: exit and release this task's `Rc<TlsShared>`. The socket
        // closes once the recv filter and the `Peer` have released theirs too.
        if closed {
            return;
        }
        // Coalesce a burst of notifications: park on the next, then drain
        // any that piled up so we make exactly one more flush pass, not N.
        rx.recv().await;
        while rx.try_recv().is_some() {}
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
