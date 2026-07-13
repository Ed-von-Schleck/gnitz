//! TLS 1.3 server transport: per-connection rustls session over the
//! reactor's raw stream ops (`recv_raw`/`send_raw`), reusing the fd path's
//! `io::RecvState` deframer over the *decrypted plaintext* (the reactor's
//! CQE-level framing runs on socket bytes, which are ciphertext here).
//! "ZSets over the wire" rides verbatim inside the TLS stream.
//!
//! Three tasks per connection:
//! - the **read pump**: raw recv → `read_tls` → `process_new_packets` →
//!   plaintext fed into the reused `RecvState` deframer. Never locks the
//!   send mutex, never sends; on any recv-side death it issues the
//!   lock-free `posix_io::shutdown` that aborts a parked writer.
//! - the **flusher**: serializes ciphertext extraction *and its send*
//!   under `send_mutex` and owns teardown — the lock-free socket shutdown
//!   and the one `libc::close(fd)`.
//! - the existing `connection_loop`, consuming via `Peer::recv()` and
//!   sending under the same `send_mutex`.
//!
//! TLS records must arrive in emission order (each record's AEAD nonce is
//! derived from its record number; a reordered record fails to decrypt),
//! and two independent `OP_SEND` SQEs on one fd can complete in either
//! order — so `send_mutex` is held across extraction AND transmission,
//! making it the "at most one `OP_SEND` in flight per fd" guarantee. That
//! in turn means no TLS send may be unbounded: every client-bound send is
//! wrapped in the per-frame eviction deadline (`send_guarded` / the
//! flusher's guard), whose expiry fires the lock-free `posix_io::shutdown`
//! that aborts whichever `send_raw` holds the mutex.

pub(crate) mod config;

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::Future;
use std::io::{ErrorKind, Read, Write};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use crate::foundation::posix_io;
use crate::runtime::reactor::io::{self, RecvBuf};
use crate::runtime::reactor::{client_send_timeout, mpsc, select2, AsyncMutex, Either, Reactor};
use crate::runtime::w2m::W2mSlot;
use crate::storage::batch_pool::PooledSendBuf;

/// Per-connection TLS state, behind `TlsShared::state`. Never borrowed
/// across an await.
struct TlsConn {
    sess: rustls::ServerConnection,
    /// Reused fd-path deframer, running over decrypted plaintext.
    recv_state: io::RecvState,
    /// 8 pre-HELLO, elevated by `set_max_payload_len`.
    max_payload_len: usize,
    /// Completed frames awaiting `Peer::recv`; each `RecvBuf`'s RAII
    /// refunds the global inbound counter on drop, so a dirty teardown
    /// that drops the queue reconciles the accounting automatically.
    inbound: VecDeque<RecvBuf>,
    /// Single slot: exactly one `connection_loop` recvs per peer.
    recv_waker: Option<Waker>,
    /// EOF / close_notify / TLS error / cap breach.
    recv_closed: bool,
    /// `Peer::close()` ran; senders refuse, flusher tears down.
    closed: bool,
    /// Pump done; fd may be closed once `closed` too.
    pump_exited: bool,
}

impl TlsConn {
    /// Drain all currently-available decrypted plaintext into the
    /// `RecvState`, enqueuing completed frames. rustls reads plaintext
    /// straight into the `RecvBuf` payload buffer via
    /// `RecvState::remaining()` — no intermediate bounce buffer.
    /// `Err` ⇒ recv-side teardown (oversize, zero-len sentinel, cap
    /// breach, or a clean/unclean plaintext close).
    fn feed_decrypted(&mut self, reactor: &Reactor) -> Result<(), ()> {
        loop {
            let (ptr, len) = self.recv_state.remaining();
            // SAFETY: ptr/len describe the deframer's current write window
            // (header buf or in-flight RecvBuf payload), exclusively ours.
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
            // `reader().read` has exactly four outcomes and they MUST stay
            // distinct: `Ok(n>0)` = plaintext; `Ok(0)` = clean close
            // (`close_notify` received) → tear the recv side down;
            // `Err(WouldBlock)` = no plaintext buffered right now → return,
            // wait for the next socket chunk (do NOT tear down);
            // `Err(UnexpectedEof)` = unclean truncation (TCP EOF, no
            // `close_notify`) → tear down (folded into the catch-all `Err`
            // arm). Collapsing WouldBlock into close would kill every live
            // connection; collapsing close into WouldBlock would spin.
            match self.sess.reader().read(slice) {
                Ok(0) => return Err(()), // clean close_notify
                Ok(m) => match self.recv_state.advance(m) {
                    io::RecvAdvance::NeedMore => {}
                    io::RecvAdvance::HeaderDone => {
                        let plen = self.recv_state.payload_len();
                        if plen > self.max_payload_len {
                            return Err(());
                        }
                        // Charges frame_weight(plen) now, before payload
                        // bytes arrive; None ⇒ cap breach, refused before
                        // malloc (the slow-drip OOM stays closed).
                        let buf = reactor.alloc_inbound_buf(plen).ok_or(())?;
                        self.recv_state.start_payload(buf);
                    }
                    io::RecvAdvance::MessageDone => {
                        self.inbound.push_back(self.recv_state.take_message());
                    }
                    io::RecvAdvance::Disconnect => return Err(()), // len == 0 sentinel
                },
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()), // drained; wait
                Err(_) => return Err(()),                                     // UnexpectedEof (truncation) etc.
            }
        }
    }
}

/// Live-TLS-connection counter guard: `new` increments, `Drop` decrements.
/// Stored in `TlsShared`, so the count tracks the session lifetime exactly
/// (a session-init failure drops it inside `start`; a live session drops it
/// at full teardown, when the last `Rc<TlsShared>` drops).
pub(crate) struct ConnCountGuard(Rc<Cell<u32>>);

impl ConnCountGuard {
    pub(crate) fn new(c: Rc<Cell<u32>>) -> Self {
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
    fd: i32,
    state: RefCell<TlsConn>,
    /// Decrements the reactor-thread live-connection counter on teardown.
    _conn_guard: ConnCountGuard,
    /// Serializes ciphertext extraction + its `send_raw` across the flusher
    /// and every `send_bytes` sender — the ≤1-`OP_SEND`-in-flight /
    /// record-order invariant. Teardown never *acquires* it (lock-free
    /// `posix_io::shutdown` aborts a parked holder instead), so it can
    /// never wedge teardown.
    send_mutex: Rc<AsyncMutex<()>>,
    /// Ciphertext staging buffer reused across sends (capacity retained).
    /// One buffer serves the flusher and every sender because they all
    /// serialize under `send_mutex`; it is empty whenever the mutex is
    /// free.
    cipher_scratch: RefCell<Vec<u8>>,
    /// Wakes the flusher task.
    flush_tx: mpsc::Sender<()>,
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
        let (flush_tx, flush_rx) = mpsc::unbounded::<()>();
        let conn = Rc::new(TlsShared {
            reactor: Rc::clone(&reactor),
            fd,
            _conn_guard: conn_guard,
            state: RefCell::new(TlsConn {
                sess,
                recv_state: io::RecvState::new(),
                max_payload_len: io::HELLO_PRE_HANDSHAKE_LEN,
                inbound: VecDeque::new(),
                recv_waker: None,
                recv_closed: false,
                closed: false,
                pump_exited: false,
            }),
            send_mutex: Rc::new(AsyncMutex::new(())),
            cipher_scratch: RefCell::new(Vec::new()),
            flush_tx,
        });
        reactor.spawn(read_pump(Rc::clone(&conn), Rc::clone(&reactor)));
        reactor.spawn(flusher(Rc::clone(&conn), Rc::clone(&reactor), flush_rx));
        Ok(conn)
    }

    fn closed(&self) -> bool {
        self.state.borrow().closed
    }

    fn recv_closed(&self) -> bool {
        self.state.borrow().recv_closed
    }

    fn pump_exited(&self) -> bool {
        self.state.borrow().pump_exited
    }

    fn wants_write(&self) -> bool {
        self.state.borrow().sess.wants_write()
    }

    fn take_recv_waker(&self) -> Option<Waker> {
        self.state.borrow_mut().recv_waker.take()
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

    /// The shared chunked send loop, holding `send_mutex` across ALL its
    /// `send_raw`s (record order). Carries NO timer itself; the deadline is
    /// applied by `send_guarded` around it. Chunked `writer()` writes keep
    /// rustls's plaintext buffer under its 64 KiB limit and bound transient
    /// ciphertext memory; the awaited `send_raw` is where kernel socket
    /// backpressure lands (exactly the fd path's shape).
    async fn send_bytes(&self, bytes: &[u8]) -> i32 {
        let _g = self.send_mutex.lock().await; // vs pump/flusher; held across all chunks
        let mut off = 0;
        while off < bytes.len() {
            let cipher = {
                let mut c = self.state.borrow_mut();
                if c.closed || c.recv_closed {
                    return -1;
                }
                let end = (off + 32 * 1024).min(bytes.len()); // stay under rustls's
                match c.sess.writer().write(&bytes[off..end]) {
                    // 64 KiB buffer_limit
                    Ok(0) => return -1, // full rustls buffer we can't drain; kill, don't spin
                    Ok(n) => off += n,
                    Err(_) => return -1,
                }
                Rc::new(self.extract_ciphertext(&mut c))
            }; // state borrow released before the await
            let rc = self.reactor.send_raw(self.fd, Rc::clone(&cipher)).await;
            self.reclaim_scratch(cipher);
            if rc < 0 {
                return -1;
            }
        }
        bytes.len() as i32
    }

    /// Wrap a send future in the per-frame eviction deadline. The deadline
    /// wraps EVERY client-bound send, not just `send_slot` — load-bearing,
    /// not fd-parity boilerplate: because `send_bytes` holds `send_mutex`
    /// across its sends, a send parked forever on a non-reading client
    /// would hold the mutex forever, wedging the flusher and every other
    /// sender. The timer wraps the whole future including any
    /// `lock().await`, so it fires even while merely blocked on a mutex a
    /// stalled peer is holding — `posix_io::shutdown(fd)` then aborts
    /// whichever `send_raw` holds the mutex, the holder drops its guard,
    /// and this send proceeds (to also fail on the shut socket). The
    /// per-frame window is ample: every TLS server→client frame is bounded
    /// (control/OK/ACK replies, or one bounded W2M slot-frame — scans
    /// stream as bounded continuations).
    async fn guard_eviction<F: Future<Output = i32>>(&self, what: &str, fut: F) -> i32 {
        let mut fut = std::pin::pin!(fut);
        let deadline = Instant::now() + client_send_timeout();
        match select2(fut.as_mut(), self.reactor.timer(deadline)).await {
            Either::A(rc) => rc,
            Either::B(()) => {
                crate::gnitz_warn!(
                    "tls: client fd={} stalled {what} past {:?}; evicting",
                    self.fd,
                    client_send_timeout(),
                );
                posix_io::shutdown(self.fd); // abort the parked send_raw / unblock the mutex
                fut.await.min(-1)
            }
        }
    }

    pub(crate) async fn send_guarded(&self, bytes: &[u8]) -> i32 {
        self.guard_eviction("egress", self.send_bytes(bytes)).await
    }

    pub(crate) async fn send_buffer(&self, buf: PooledSendBuf) -> i32 {
        self.send_guarded(&buf.0).await
    }

    /// The `slot` is owned here, so its frame bytes stay borrowed (slot
    /// alive, worker W2M backpressure preserved) until the kernel accepts
    /// the last ciphertext byte or the deadline evicts. This is the path
    /// whose stall would otherwise fill the worker's W2M ring and
    /// futex-block the single-threaded worker — a cluster-wide freeze — so
    /// the deadline here protects a *shared* resource.
    pub(crate) async fn send_slot(&self, slot: W2mSlot) -> i32 {
        self.send_guarded(slot.frame_bytes()).await
    }

    /// Elevate the per-frame inbound ceiling — the same cap
    /// `feed_decrypted` enforces per frame. Synchronous `RefCell` write.
    pub(crate) fn set_max_payload_len(&self, limit: usize) {
        self.state.borrow_mut().max_payload_len = limit;
    }

    /// Sync and idempotent: the first transition of `closed` queues a
    /// close_notify and notifies the flusher, which flushes, shuts the
    /// socket down, and (once the pump has exited) closes the fd.
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

/// Future behind `Peer::recv()` on the TLS path: pops the inbound frame
/// queue (handing the caller the charged `RecvBuf`, whose eventual `Drop`
/// refunds the inbound counter), parks in the single `recv_waker` slot
/// when empty, resolves `None` once `recv_closed` and the queue is empty.
pub(crate) struct TlsRecvFuture {
    conn: Rc<TlsShared>,
}

impl Future for TlsRecvFuture {
    type Output = Option<RecvBuf>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut c = self.conn.state.borrow_mut();
        if let Some(buf) = c.inbound.pop_front() {
            return Poll::Ready(Some(buf));
        }
        if c.recv_closed {
            return Poll::Ready(None);
        }
        c.recv_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl Drop for TlsRecvFuture {
    fn drop(&mut self) {
        // Waker hygiene: the single slot must not hold a stale waker after
        // the awaiting task is cancelled.
        self.conn.state.borrow_mut().recv_waker = None;
    }
}

/// The read pump — never locks `send_mutex`, never extracts ciphertext,
/// never sends (a pump-waits-on-sender design re-forms the four-party
/// pipelining deadlock). It reads unconditionally — fd-path parity — so a
/// pipelining client's sends always drain; memory is guarded by the global
/// inbound cap, whose breach closes the connection (never pauses). When
/// rustls has control bytes to emit (handshake flights, KeyUpdate
/// responses, alerts), the pump only *notifies* the flusher.
async fn read_pump(conn: Rc<TlsShared>, reactor: Rc<Reactor>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let (b, n) = reactor.recv_raw(conn.fd, buf).await;
        buf = b;
        if n <= 0 {
            break; // EOF / error / shutdown
        }
        let done = {
            let mut c = conn.state.borrow_mut(); // never held across an await
            let mut cipher = &buf[..n as usize];
            let mut fatal = false;
            while !cipher.is_empty() && !fatal {
                // `read_tls` reads from an in-memory slice, so it does no
                // socket I/O and cannot return a socket error — genuine
                // socket errors/EOF already surfaced as `recv_raw` n<=0
                // above. Its outcomes here are exactly: `Ok(0)` =
                // end-of-stream (a `close_notify` was already received — no
                // more data will ever be read), and `Err(ErrorKind::Other)`
                // = rustls backpressure (its incoming buffer is full and
                // must be drained first). Backpressure is NOT fatal: fall
                // through to `process_new_packets` + `feed_decrypted`,
                // which empties the buffer, then the loop retries the
                // (unconsumed) slice. A single TLS record always fits
                // rustls's buffer, so each pass makes progress (a malformed
                // oversize record fails `process_new_packets` → fatal) and
                // the loop terminates.
                let read = c.sess.read_tls(&mut cipher);
                if matches!(read, Ok(0)) {
                    fatal = true;
                    break;
                }
                // read > 0 (data buffered) OR Err (backpressure): drain now.
                match c.sess.process_new_packets() {
                    Err(_) => fatal = true, // fatal alert queued by rustls
                    Ok(io_state) => {
                        if c.feed_decrypted(&reactor).is_err() {
                            fatal = true;
                        }
                        if io_state.peer_has_closed() {
                            fatal = true;
                        }
                    }
                }
            }
            if fatal {
                c.recv_closed = true;
            }
            fatal
        };
        if let Some(w) = conn.take_recv_waker() {
            w.wake();
        }
        if conn.wants_write() {
            conn.notify_flusher();
        }
        if done {
            break;
        }
    }
    {
        let mut c = conn.state.borrow_mut();
        c.recv_closed = true;
        c.pump_exited = true;
    }
    // Lock-free: half-close so any writer (a `connection_loop` sender or
    // the flusher) parked in `send_raw` on a full sndbuf is aborted by its
    // error CQE and releases `send_mutex`. This is what makes teardown
    // deadlock-free — without it, a sender parked mid-scan-train while the
    // client has stopped reading pins the mutex and the flusher can never
    // flush or close. Idempotent with the flusher's own shutdown on the
    // local-close path.
    posix_io::shutdown(conn.fd);
    if let Some(w) = conn.take_recv_waker() {
        w.wake();
    }
    conn.notify_flusher(); // run teardown
}

/// The flusher — it and the senders serialize ciphertext extraction AND
/// its send under `send_mutex`, and it is the one place the fd is ever
/// closed. Teardown (`posix_io::shutdown`, then `libc::close`) is
/// lock-free — it never *acquires* the mutex, so a writer parked in
/// `send_raw` can never wedge it.
async fn flusher(conn: Rc<TlsShared>, reactor: Rc<Reactor>, mut rx: mpsc::Receiver<()>) {
    let mut shutdown_sent = false;
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
                    .guard_eviction("control-byte egress", reactor.send_raw(conn.fd, Rc::clone(&cipher)))
                    .await;
                conn.reclaim_scratch(cipher);
            }
        } // _g released before the teardown checks and before parking on rx

        // Teardown, lock-free. Half-close once either side has begun it: on
        // a *local* close this drives the pump's parked recv to EOF; on any
        // close it is the mechanism that aborts a parked `send_raw` so
        // `send_mutex` is always eventually released (no teardown
        // deadlock). Idempotent with the pump's own shutdown.
        if (conn.closed() || conn.recv_closed()) && !shutdown_sent {
            posix_io::shutdown(conn.fd);
            shutdown_sent = true;
        }
        // The ONE close — only after the local close ran *and* the pump
        // exited. A bare close neither aborts an in-flight io_uring op nor
        // stops a task from touching a recycled fd number, hence
        // close-once-both-are-done. The fd is unregistered (TLS never calls
        // `register_conn`), so `Reactor::close_fd` would be a no-op — close
        // it directly. When the last `Rc<TlsShared>` drops, queued
        // `RecvBuf`s refund the inbound counter via their own Drop.
        if conn.closed() && conn.pump_exited() {
            // SAFETY: this task is the sole closer, gated on pump exit +
            // local close, so no live SQE references the fd.
            unsafe { libc::close(conn.fd) };
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
