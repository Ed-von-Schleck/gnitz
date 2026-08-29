//! TLS 1.3 server transport: per-connection rustls session over the
//! reactor's raw stream ops (`recv_raw`/`send_raw`), driving the fd path's
//! `io::RecvQueue` with *decrypted plaintext* (the reactor's CQE-level
//! framing runs on socket bytes, which are ciphertext here).
//! "ZSets over the wire" rides verbatim inside the TLS stream.
//!
//! Three tasks per connection:
//! - the **read pump**: raw recv → `read_tls` → `process_new_packets` →
//!   plaintext fed into the shared `RecvQueue`. Never locks the send
//!   mutex, never sends; on any recv-side death it issues the lock-free
//!   `shutdown` that aborts a parked writer.
//! - the **flusher**: serializes ciphertext extraction *and its send*
//!   under `send_mutex` and issues the lock-free socket shutdown. The fd
//!   itself is closed by `Drop for TlsShared`, when the last holder is gone.
//! - the existing `connection_loop`, consuming via `Peer::recv()` and
//!   sending under the same `send_mutex`.
//!
//! TLS records must arrive in emission order (each record's AEAD nonce is
//! derived from its record number; a reordered record fails to decrypt),
//! and two independent `OP_SEND` SQEs on one fd can complete in either
//! order — so `send_mutex` is held across extraction AND transmission,
//! making it the "at most one `OP_SEND` in flight per fd" guarantee. That
//! in turn means no TLS send may be unbounded: every client-bound send is
//! wrapped in the per-frame eviction deadline (`guard_eviction`), whose
//! expiry fires the lock-free `shutdown` that aborts whichever
//! `send_raw` holds the mutex.
//!
//! The two siblings hold the rest of TLS: `config` builds the rustls
//! `ServerConfig` — operator PEM or minted dev cert, plus the client CA that
//! becomes a required-mTLS verifier — and `listener` holds what `--tls-listen`
//! asks for, the bind refusal guarding it, and the bound listener the executor
//! accepts on. Only `listener`'s three items leave this module.

mod config;
mod listener;

pub(crate) use listener::{setup_tls_listener, TlsCli, TlsListener};

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::runtime::reactor::io::{self, RecvBuf};
use crate::runtime::reactor::{guard_client_egress, mpsc, shutdown, AsyncMutex, Reactor};
use crate::runtime::w2m::W2mSlot;
use gnitz_engine::storage::batch_pool::PooledSendBuf;

/// Per-connection TLS state, behind `TlsShared::state`. Never borrowed
/// across an await.
struct TlsConn {
    sess: rustls::ServerConnection,
    /// The shared inbound half: deframer, completed frames, waiter, and the
    /// recv-closed verdict — identical policy to the fd path, driven here by
    /// `rustls::Reader::read` instead of a recv CQE.
    q: io::RecvQueue,
    /// `Peer::close()` ran; senders refuse, flusher tears down.
    closed: bool,
}

impl TlsConn {
    fn new(sess: rustls::ServerConnection, budget: Rc<io::InboundBudget>) -> Self {
        TlsConn {
            sess,
            q: io::RecvQueue::new(budget),
            closed: false,
        }
    }

    /// Feed one socket chunk of ciphertext through rustls and enqueue whatever
    /// plaintext frames come out. `Err` ⇒ recv-side teardown.
    ///
    /// An `Err` from `read_tls` is backpressure, not failure, so it is ignored:
    /// the drain below frees the buffer and the loop retries the unconsumed
    /// slice. It terminates because one record always fits that buffer.
    fn ingest_cipher(&mut self, mut cipher: &[u8], fd: i32) -> Result<(), ()> {
        while !cipher.is_empty() {
            // `Ok(0)` = end-of-stream: a `close_notify` was already received,
            // so no further data will ever be read.
            if matches!(self.sess.read_tls(&mut cipher), Ok(0)) {
                return Err(());
            }
            let io_state = self.sess.process_new_packets().map_err(|_| ())?;
            // Before the close test, so already-decrypted plaintext is still
            // enqueued when the peer closed in the same chunk.
            self.feed_decrypted(fd)?;
            if io_state.peer_has_closed() {
                return Err(());
            }
        }
        Ok(())
    }

    /// Drain all currently-available decrypted plaintext into the queue.
    /// rustls reads plaintext straight into the deframer's write window — no
    /// intermediate bounce buffer. `Err` ⇒ recv-side teardown (oversize,
    /// zero-len sentinel, cap breach, or a clean/unclean plaintext close).
    fn feed_decrypted(&mut self, fd: i32) -> Result<(), ()> {
        let (mut ptr, mut len) = self.q.remaining();
        loop {
            // SAFETY: ptr/len are the queue's own write window (header buf or
            // in-flight RecvBuf payload), exclusively ours.
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
            // Collapsing WouldBlock into close would kill every live
            // connection; collapsing close into WouldBlock would spin.
            match self.sess.reader().read(slice) {
                Ok(0) => return Err(()), // clean close_notify
                Ok(m) => (ptr, len) = self.q.deliver(m, fd)?,
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

/// Shared handle to one TLS connection, held by the `Peer`, the read pump,
/// and the flusher.
pub(crate) struct TlsShared {
    reactor: Rc<Reactor>,
    /// Owned, so the socket closes exactly when the last holder — `Peer`, read
    /// pump, flusher — is gone. Sound because each of those awaits its io_uring
    /// op to completion (`guard_client_egress` awaits even after evicting), so
    /// no SQE outlives the close.
    fd: OwnedFd,
    state: RefCell<TlsConn>,
    /// Decrements the reactor-thread live-connection counter on teardown.
    _conn_guard: ConnCountGuard,
    /// Serializes ciphertext extraction + its `send_raw` across the flusher
    /// and every `send_bytes` sender — the ≤1-`OP_SEND`-in-flight /
    /// record-order invariant. Teardown never *acquires* it (lock-free
    /// `shutdown` aborts a parked holder instead), so it can
    /// never wedge teardown.
    send_mutex: Rc<AsyncMutex>,
    /// Ciphertext staging buffer reused across sends (capacity retained).
    /// One buffer serves the flusher and every sender because they all
    /// serialize under `send_mutex`; it is unowned whenever the mutex is
    /// free, and the next take clears it.
    cipher_scratch: RefCell<Vec<u8>>,
    /// Wakes the flusher task.
    flush_tx: mpsc::Sender<()>,
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
    /// Build the per-connection state and spawn its read pump + flusher.
    /// The TLS handshake needs no separate phase: the pump's read/process
    /// cycle is the handshake driver and the flusher ships the emitted
    /// flights (no sender contends pre-HELLO — `connection_loop` is parked
    /// in `recv`); plaintext (HELLO) appears once the handshake completes.
    pub(crate) fn start(
        reactor: Rc<Reactor>,
        fd: i32,
        cfg: Arc<rustls::ServerConfig>,
        conn_guard: ConnCountGuard,
    ) -> Result<Rc<TlsShared>, rustls::Error> {
        // `ServerConnection::new` runs first: on failure the moved-in
        // `conn_guard` drops here (decrement) as this frame unwinds; on
        // success it lives in the returned `TlsShared`.
        let sess = rustls::ServerConnection::new(cfg)?;
        set_socket_options(fd);
        let (flush_tx, flush_rx) = mpsc::unbounded::<()>();
        let conn = Rc::new(TlsShared {
            state: RefCell::new(TlsConn::new(sess, Rc::clone(reactor.inbound()))),
            reactor,
            // SAFETY: the accept loop hands this fd over and never touches it
            // again — on the error path above it closes it and returns instead.
            fd: unsafe { OwnedFd::from_raw_fd(fd) },
            _conn_guard: conn_guard,
            send_mutex: Rc::new(AsyncMutex::new()),
            cipher_scratch: RefCell::new(Vec::new()),
            flush_tx,
        });
        conn.reactor.spawn(read_pump(Rc::clone(&conn)));
        conn.reactor.spawn(flusher(Rc::clone(&conn), flush_rx));
        Ok(conn)
    }

    fn fd(&self) -> i32 {
        self.fd.as_raw_fd()
    }

    fn notify_flusher(&self) {
        self.flush_tx.send(());
    }

    /// Next complete inbound frame, or `None` once the recv side is closed
    /// and the queue is drained.
    pub(crate) fn recv(self: &Rc<Self>) -> TlsRecvFuture {
        TlsRecvFuture { conn: Rc::clone(self) }
    }

    /// Take the shared ciphertext scratch and fill it with everything
    /// rustls has queued. Every caller already holds `send_mutex`, so one
    /// buffer serves the flusher and all senders; the paired
    /// [`Self::reclaim_scratch`] returns the capacity after the send.
    fn extract_ciphertext(&self, c: &mut TlsConn) -> Vec<u8> {
        let mut out = self.cipher_scratch.take();
        out.clear();
        while c.sess.wants_write() {
            let _ = c.sess.write_tls(&mut out);
        }
        out
    }

    /// Return the scratch buffer once its send completed. The `try_unwrap`
    /// fails only when a cancelled send parked a keep-alive clone (the
    /// kernel may still be reading the buffer); then the capacity is simply
    /// not reclaimed.
    fn reclaim_scratch(&self, cipher: Rc<Vec<u8>>) {
        if let Ok(v) = Rc::try_unwrap(cipher) {
            self.cipher_scratch.replace(v);
        }
    }

    /// The shared send loop, holding `send_mutex` across ALL its `send_raw`s
    /// (record order). Carries NO timer itself; the deadline is applied by
    /// [`Self::guard_eviction`] around it. rustls does the chunking — it
    /// truncates each write to what its bounded ciphertext queue still holds —
    /// and the awaited `send_raw` is where socket backpressure lands.
    async fn send_bytes(&self, bytes: &[u8]) -> i32 {
        let _g = self.send_mutex.lock().await; // vs pump/flusher; held across all chunks
        let mut off = 0;
        while off < bytes.len() {
            let cipher = {
                let mut c = self.state.borrow_mut();
                if c.closed || c.q.recv_closed() {
                    return -1;
                }
                match c.sess.writer().write(&bytes[off..]) {
                    // A queue we cannot drain: kill, don't spin.
                    Ok(0) => return -1,
                    Ok(n) => off += n,
                    Err(_) => return -1,
                }
                Rc::new(self.extract_ciphertext(&mut c))
            }; // state borrow released before the await
            let rc = self.reactor.send_raw(self.fd(), Rc::clone(&cipher)).await;
            self.reclaim_scratch(cipher);
            if rc < 0 {
                return -1;
            }
        }
        bytes.len() as i32
    }

    /// [`guard_client_egress`] for this connection. The deadline matters more
    /// here than on the fd path: `send_bytes` holds `send_mutex` across its
    /// sends, so a send parked forever on a non-reading client would hold the
    /// mutex forever, wedging the flusher and every other sender. The timer
    /// wraps the whole future including the `lock().await`, so it fires even
    /// while merely blocked on a mutex a stalled peer is holding — the
    /// `shutdown` aborts whichever `send_raw` holds the mutex, the holder drops
    /// its guard, and this send proceeds (to also fail on the shut socket).
    async fn guard_eviction<F: Future<Output = i32>>(&self, what: &str, fut: F) -> i32 {
        guard_client_egress(&self.reactor, self.fd(), what, fut).await
    }

    pub(crate) async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        self.guard_eviction("egress", self.send_bytes(&buf.0)).await
    }

    /// The `slot` is owned here, so its frame bytes stay borrowed (slot
    /// alive, worker W2M backpressure preserved) until the kernel accepts
    /// the last ciphertext byte or the deadline evicts. This is the path
    /// whose stall would otherwise fill the worker's W2M ring and
    /// futex-block the single-threaded worker — a cluster-wide freeze — so
    /// the deadline here protects a *shared* resource.
    pub(crate) async fn send_slot(&self, slot: W2mSlot) -> i32 {
        self.guard_eviction("ring-slot egress", self.send_bytes(slot.frame_bytes()))
            .await
    }

    /// Elevate the per-frame inbound ceiling — the same cap the queue
    /// enforces per frame. Synchronous `RefCell` write.
    pub(crate) fn set_max_payload_len(&self, limit: usize) {
        self.state.borrow_mut().q.set_max_payload_len(limit);
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

/// Future behind `Peer::recv()` on the TLS path: delegates to the shared
/// [`io::RecvQueue`], which hands the caller the charged `RecvBuf` (whose
/// eventual `Drop` refunds the inbound budget), parks in the single waiter
/// slot when empty, and resolves `None` once the recv side is closed and the
/// queue is drained.
pub(crate) struct TlsRecvFuture {
    conn: Rc<TlsShared>,
}

impl Future for TlsRecvFuture {
    type Output = Option<RecvBuf>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.conn.state.borrow_mut().q.poll_recv(cx)
    }
}

impl Drop for TlsRecvFuture {
    fn drop(&mut self) {
        self.conn.state.borrow_mut().q.clear_waiter();
    }
}

/// The read pump — never locks `send_mutex`, never sends: if the pump waited on
/// a sender, a client pipelining pushes ahead of reading its ACKs would
/// deadlock. So it reads unconditionally and only *notifies* the flusher when
/// rustls has control bytes to emit; inbound memory is bounded by the global
/// cap, whose breach closes the connection rather than pausing the read.
async fn read_pump(conn: Rc<TlsShared>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let (b, n) = conn.reactor.recv_raw(conn.fd(), buf).await;
        buf = b;
        if n <= 0 {
            break; // EOF / error / shutdown
        }
        // The queue wakes the waiter itself, and only on a completed frame — a
        // large push therefore parks `connection_loop` once per frame, not once
        // per 64 KiB of ciphertext.
        let (fatal, wants_write) = {
            let mut c = conn.state.borrow_mut(); // never held across an await
            let fatal = c.ingest_cipher(&buf[..n as usize], conn.fd()).is_err();
            if fatal {
                c.q.close();
            }
            (fatal, c.sess.wants_write())
        };
        if wants_write {
            conn.notify_flusher();
        }
        if fatal {
            break;
        }
    }
    // Without this a parked `recv()` would never resolve to `None`.
    conn.state.borrow_mut().q.close();
    // Lock-free: half-close so any writer (a `connection_loop` sender or
    // the flusher) parked in `send_raw` on a full sndbuf is aborted by its
    // error CQE and releases `send_mutex`. This is what makes teardown
    // deadlock-free — without it, a sender parked mid-scan-train while the
    // client has stopped reading pins the mutex and the flusher can never
    // flush or close. Idempotent with the flusher's own shutdown on the
    // local-close path.
    shutdown(conn.fd());
    conn.notify_flusher(); // run teardown
}

/// The flusher — it and the senders serialize ciphertext extraction AND
/// its send under `send_mutex`. Teardown (`shutdown`) is lock-free: it
/// never *acquires* the mutex, so a writer parked in `send_raw` can never
/// wedge it.
async fn flusher(conn: Rc<TlsShared>, mut rx: mpsc::Receiver<()>) {
    loop {
        // Extract AND ship whatever ciphertext rustls has queued: handshake
        // flights, KeyUpdate responses, alerts, a close_notify queued by
        // close(). One pass drains ALL queued ciphertext, so it services
        // any number of coalesced notifications.
        {
            let _g = conn.send_mutex.lock().await;
            let out = {
                let mut c = conn.state.borrow_mut();
                conn.extract_ciphertext(&mut c)
            }; // state borrow released here, before the await
            if out.is_empty() {
                conn.cipher_scratch.replace(out);
            } else {
                // Guarded like every other send: a control-byte send to a
                // non-reading peer must not park forever holding send_mutex.
                let cipher = Rc::new(out);
                let _ = conn
                    .guard_eviction(
                        "control-byte egress",
                        conn.reactor.send_raw(conn.fd(), Rc::clone(&cipher)),
                    )
                    .await;
                conn.reclaim_scratch(cipher);
            }
        } // _g released before the teardown checks and before parking on rx

        // Teardown, lock-free. Half-close once either side has begun it: on
        // a *local* close this drives the pump's parked recv to EOF; on any
        // close it is the mechanism that aborts a parked `send_raw` so
        // `send_mutex` is always eventually released (no teardown
        // deadlock). Idempotent with the pump's own shutdown.
        let (closed, recv_closed) = {
            let c = conn.state.borrow();
            (c.closed, c.q.recv_closed())
        };
        if closed || recv_closed {
            shutdown(conn.fd());
        }
        // The local close ran, so nothing more will ever be queued for this
        // connection: exit and release this task's `Rc<TlsShared>`. The fd is
        // closed by `Drop for TlsShared`, once the pump and the `Peer` have
        // released theirs too.
        if closed {
            return;
        }
        // Coalesce a burst of notifications: park on the next, then drain
        // any that piled up so we make exactly one more flush pass, not N.
        if rx.recv().await.is_none() {
            return;
        }
        while rx.try_recv().is_some() {}
    }
}

#[cfg(test)]
mod tests;
