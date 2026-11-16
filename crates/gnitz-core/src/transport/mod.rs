//! Client transport: frames over an AF_UNIX stream socket or TLS 1.3 over TCP
//! (`tls.rs`), the same wire bytes on both. Nothing here waits except
//! [`ClientTransport::connect`].
//!
//! It is the frame layer under a [`Session`](crate::connection::Session), which knows which
//! request a frame answers; this layer knows only frames. A connection of a
//! process that records ([`crate::recording`]) records every frame it queues to
//! send or receives after its HELLO.

use std::borrow::Cow;
use std::io::{IoSlice, Write};
use std::mem::MaybeUninit;
use std::net::Shutdown;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::time::Instant;

use gnitz_wire::{AuthStatus, Deframer, HelloError};

use crate::recording::{Direction, Tap};
use crate::{ClientError, ProtocolError};

mod tls;

/// One connected client transport.
pub struct ClientTransport {
    inner: Inner,
    deframer: Deframer,
    /// Where a socket read lands: the plain socket's carry, the TLS socket's
    /// ciphertext.
    window: Box<[MaybeUninit<u8>]>,
    /// A read ended with bytes of it unfed, so the stream has lost its framing.
    torn: bool,
    queue: OutBuf,
    /// Where this connection records its frames, when its process records.
    tap: Option<Tap>,
}

/// Bytes one socket read can take, short of a read straight into a payload.
const WINDOW_BYTES: usize = 64 * 1024;

enum Inner {
    Unix(UnixStream),
    Tls(Box<tls::TlsInner>),
}

impl Inner {
    /// The non-blocking write core: hand `bytes` to the sink and report how
    /// many of them it accepted. The Unix arm writes them as one `writev`
    /// slice, so a syscall trace tells the transport's writes apart from a
    /// `write` to stdout and a `sendto` to a self-pipe.
    fn write_bytes(&mut self, bytes: &[u8]) -> Result<Option<usize>, ProtocolError> {
        match self {
            Inner::Unix(s) => write_nonblocking(|| (&*s).write_vectored(&[IoSlice::new(bytes)])),
            Inner::Tls(t) => t.write_bytes(bytes),
        }
    }

    /// Ciphertext rustls still holds for the socket. Always false on the Unix
    /// arm, which has no second outbound source beneath the queue.
    fn has_pending_ciphertext(&self) -> bool {
        match self {
            Inner::Unix(_) => false,
            Inner::Tls(t) => t.conn.wants_write(),
        }
    }

    /// Ship queued ciphertext without waiting; a no-op on the Unix arm.
    fn ship_ciphertext(&mut self) -> Result<(), ProtocolError> {
        match self {
            Inner::Unix(_) => Ok(()),
            Inner::Tls(t) => t.ship(),
        }
    }

    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Inner::Unix(s) => s.as_fd(),
            Inner::Tls(t) => t.sock.as_fd(),
        }
    }
}

/// One non-blocking socket write: `EINTR` retried, a zero-byte write refused;
/// `None` where it would block.
fn write_nonblocking(mut write: impl FnMut() -> std::io::Result<usize>) -> Result<Option<usize>, ProtocolError> {
    loop {
        match write() {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
            Ok(n) => return Ok(Some(n)),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    }
}

/// One `recv` into `buf`: the bytes it took, 0 at EOF, or `None` where it
/// would block.
fn recv(fd: RawFd, buf: &mut [MaybeUninit<u8>]) -> Result<Option<usize>, ProtocolError> {
    loop {
        // SAFETY: pointer/len come from a valid &mut [MaybeUninit<u8>].
        let n = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
        if n >= 0 {
            return Ok(Some(n as usize));
        }
        let e = std::io::Error::last_os_error();
        match e.kind() {
            std::io::ErrorKind::Interrupted => {}
            std::io::ErrorKind::WouldBlock => return Ok(None),
            _ => return Err(e.into()),
        }
    }
}

/// A deadline's expiry.
fn timed_out() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::TimedOut, "socket operation timed out")
}

/// What the client waits for on its socket — what [`Host::poll_io`](crate::Host::poll_io)
/// is asked to wait for, and what it hands back: two booleans on the connection.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Interest {
    pub read: bool,
    pub write: bool,
}

impl Interest {
    pub const NONE: Interest = Interest { read: false, write: false };
    pub const READ: Interest = Interest { read: true, write: false };
    pub const WRITE: Interest = Interest { read: false, write: true };

    pub fn is_empty(self) -> bool {
        self == Interest::NONE
    }

    /// The `poll(2)` event mask to wait for.
    fn poll_events(self) -> libc::c_short {
        (if self.read { libc::POLLIN } else { 0 }) | (if self.write { libc::POLLOUT } else { 0 })
    }

    /// What a `poll(2)` wakeup says is ready. `POLLHUP`, `POLLERR` and
    /// `POLLNVAL` fold into `read`: they arrive whether requested or not, and
    /// the read that follows surfaces the EOF, the reset or the `EBADF`; a
    /// driver that ignored them would park forever on a dead peer, or spin on
    /// a closed fd.
    fn from_revents(revents: libc::c_short) -> Interest {
        Interest {
            read: revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR | libc::POLLNVAL) != 0,
            write: revents & libc::POLLOUT != 0,
        }
    }
}

/// One `poll(2)` on `fd` for `want` until `until`, and what is ready; `None`
/// waits untimed. Expiry surfaces as `TimedOut`; `EINTR` is returned, and since
/// `until` is absolute a caller may simply call again.
pub(crate) fn poll_fd(fd: BorrowedFd<'_>, want: Interest, until: Option<Instant>) -> std::io::Result<Interest> {
    poll_fd_or_cancel(fd, want, None, until).map(|ready| ready.expect("only a cancel fd cancels"))
}

/// [`poll_fd`], also polling `cancel` for any event: `None` once it has one.
pub(crate) fn poll_fd_or_cancel(
    fd: BorrowedFd<'_>,
    want: Interest,
    cancel: Option<BorrowedFd<'_>>,
    until: Option<Instant>,
) -> std::io::Result<Option<Interest>> {
    let timeout_ms: libc::c_int = match until {
        None => -1,
        Some(t) => {
            let left = t.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(timed_out());
            }
            // Rounded up: `poll` takes whole milliseconds, and truncating
            // would let it time out before the deadline it was given.
            left.as_nanos().div_ceil(1_000_000).min(i32::MAX as u128) as libc::c_int
        }
    };
    let mut pfds = [
        libc::pollfd {
            fd: fd.as_raw_fd(),
            events: want.poll_events(),
            revents: 0,
        },
        libc::pollfd {
            fd: cancel.map_or(-1, |c| c.as_raw_fd()),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let nfds = 1 + libc::nfds_t::from(cancel.is_some());
    // SAFETY: `nfds` valid pollfds.
    match unsafe { libc::poll(pfds.as_mut_ptr(), nfds, timeout_ms) } {
        rc if rc < 0 => Err(std::io::Error::last_os_error()),
        0 => Err(timed_out()),
        _ if pfds[1].revents != 0 => Ok(None),
        _ => Ok(Some(Interest::from_revents(pfds[0].revents))),
    }
}

/// The stream socket a driver polls: the AF_UNIX socket, or the `TcpStream`
/// under TLS.
impl AsFd for ClientTransport {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.inner.as_fd()
    }
}

impl ClientTransport {
    fn new(inner: Inner) -> Self {
        ClientTransport {
            inner,
            deframer: Deframer::default(),
            window: Box::new_uninit_slice(WINDOW_BYTES),
            torn: false,
            queue: OutBuf::default(),
            tap: None,
        }
    }

    /// Connect to a `tls://` target, or else to an AF_UNIX socket path, and
    /// exchange HELLOs. The TCP connect and the HELLO exchange end no later
    /// than `until`; resolving the TLS host name and connecting the AF_UNIX
    /// socket are blocking calls `until` does not bound.
    pub fn connect(target: &str, until: Instant) -> Result<Self, ClientError> {
        let (mut t, token) = match target.strip_prefix("tls://") {
            Some(rest) => tls::connect_tls(rest, until)?,
            None => (ClientTransport::unix(UnixStream::connect(target)?)?, Vec::new()),
        };
        t.hello(&token, until)?;
        t.tap = Tap::open(target);
        Ok(t)
    }

    /// Wrap a connected AF_UNIX stream socket, which the transport closes on
    /// drop. No HELLO is exchanged.
    pub(crate) fn unix(stream: UnixStream) -> Result<Self, ProtocolError> {
        stream.set_nonblocking(true)?;
        Ok(ClientTransport::new(Inner::Unix(stream)))
    }

    /// Exchange HELLOs, ours carrying `token`, no later than `until`. On TLS
    /// this is also where the handshake completes and certificate failures
    /// surface.
    fn hello(&mut self, token: &[u8], until: Instant) -> Result<(), ClientError> {
        let hello = gnitz_wire::client_hello(token);
        self.enqueue(&hello).map_err(|len| {
            ClientError::from(format!(
                "the HELLO is {len} bytes, past the {}-byte frame ceiling; use a shorter access token",
                gnitz_wire::MAX_FRAME_PAYLOAD
            ))
        })?;
        loop {
            // Every round: a read queues the TLS handshake's next flight.
            self.flush()?;
            let want = Interest { read: true, write: self.wants_write() };
            match poll_fd(self.as_fd(), want, Some(until)) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                polled => polled?,
            };
            let mut frames = Vec::new();
            let read = self.read(|frame| {
                frames.push(frame.into_owned());
                Ok(())
            });
            let reply = match &frames[..] {
                [] => {
                    read?;
                    continue;
                }
                [reply] => reply,
                _ => return Err(ProtocolError::DecodeError("a frame behind the server's HELLO".into()).into()),
            };
            // Ahead of `read`'s own failure: a server refusing the version or
            // the token answers, then hangs up.
            match gnitz_wire::read_server_hello(reply) {
                Ok(AuthStatus::Accepted) => {}
                Ok(AuthStatus::Refused) if token.is_empty() => {
                    return Err(ClientError::from(format!(
                        "authentication failed: the server requires an access token (`token_file=` or {})",
                        tls::TOKEN_ENV
                    )));
                }
                Ok(AuthStatus::Refused) => {
                    return Err(ClientError::from(
                        "authentication failed: the server does not recognise the access token",
                    ));
                }
                Err(e @ HelloError::Malformed) => return Err(ProtocolError::DecodeError(e.to_string()).into()),
                Err(e @ HelloError::Version { .. }) => return Err(ClientError::from(e.to_string())),
            }
            read?;
            return Ok(());
        }
    }

    /// Queue `payload` as one frame behind everything already queued, or
    /// refuse one past [`gnitz_wire::MAX_FRAME_PAYLOAD`] with its length.
    /// Nothing is written here; `flush` ships the queue.
    pub fn enqueue(&mut self, payload: &[u8]) -> Result<(), usize> {
        self.frame(|out| out.extend_from_slice(payload))
    }

    /// Queue the frame whose payload `encode` appends to the buffer it is
    /// handed, behind everything already queued. A payload past
    /// [`gnitz_wire::MAX_FRAME_PAYLOAD`] is taken back out and refused with
    /// its length. Nothing is written here; `flush` ships the queue. A
    /// recording connection records the frame as sent here.
    pub(crate) fn frame(&mut self, encode: impl FnOnce(&mut Vec<u8>)) -> Result<(), usize> {
        let q = &mut self.queue;
        q.compact();
        let at = q.buf.len();
        let body = at + gnitz_wire::FRAME_LEN_PREFIX_BYTES;
        q.buf.resize(body, 0);
        encode(&mut q.buf);
        let len = q.buf.len() - body;
        if len > gnitz_wire::MAX_FRAME_PAYLOAD {
            q.buf.truncate(at);
            if q.is_empty() {
                q.settle();
            }
            return Err(len);
        }
        q.buf[at..body].copy_from_slice(&gnitz_wire::frame_len_prefix(len));
        if let Some(tap) = &self.tap {
            tap.note(Direction::Sent, &q.buf[body..]);
        }
        Ok(())
    }

    /// Write what the fd accepts from the queue cursor. Never parks; what is
    /// left is reported by `wants_write`.
    pub fn flush(&mut self) -> Result<(), ProtocolError> {
        loop {
            self.inner.ship_ciphertext()?;
            if self.inner.has_pending_ciphertext() || self.queue.is_empty() {
                return Ok(());
            }
            match self.inner.write_bytes(self.queue.pending())? {
                Some(n) => self.queue.advance(n),
                None => return Ok(()),
            }
        }
    }

    /// Frame bytes queued and not yet written.
    pub(crate) fn queued_bytes(&self) -> usize {
        self.queue.pending().len()
    }

    /// Bytes queued, or ciphertext pending: the `WRITE` half of a driver's
    /// interest. On TLS the ciphertext can outlast the queue that emptied into
    /// it.
    pub fn wants_write(&self) -> bool {
        !self.queue.is_empty() || self.inner.has_pending_ciphertext()
    }

    /// Shut the socket down so the peer sees EOF, and drop every queued frame.
    /// The fd stays open until the transport drops.
    pub(crate) fn close(&mut self) {
        let _ = match &self.inner {
            Inner::Unix(s) => s.shutdown(Shutdown::Both),
            Inner::Tls(t) => t.sock.shutdown(Shutdown::Both),
        };
        self.queue = OutBuf::default();
    }

    /// One read of the socket, handing `on_frame` every frame it completes.
    /// True when the read filled its window, so more may be waiting.
    pub fn read(
        &mut self,
        mut on_frame: impl FnMut(Cow<'_, [u8]>) -> Result<(), ProtocolError>,
    ) -> Result<bool, ProtocolError> {
        let ClientTransport { inner, deframer, window, torn, tap, .. } = self;
        if *torn {
            return Err(std::io::Error::other("an earlier read was abandoned with bytes unfed").into());
        }
        let fd = inner.as_fd().as_raw_fd();
        let into = match inner {
            Inner::Unix(_) => deframer.window(window),
            Inner::Tls(_) => &mut window[..],
        };
        let len = into.len();
        let Some(n) = recv(fd, into)? else { return Ok(false) };
        let mut feed = |deframer: &mut Deframer, mut src: &[u8]| {
            while let Some((frame, ())) = deframer.feed(&mut src, |_| Ok::<_, ProtocolError>(()))? {
                if let Some(tap) = tap {
                    tap.note(Direction::Received, &frame);
                }
                on_frame(frame)?;
            }
            Ok::<_, ProtocolError>(())
        };
        // Set across the feed, so an error or an unwind out of it leaves it set.
        *torn = true;
        let open = n > 0
            && match inner {
                Inner::Unix(_) => {
                    // SAFETY: the read wrote `n` bytes at the head of the window.
                    let src = unsafe { deframer.landed(window, n) };
                    feed(deframer, src)?;
                    true
                }
                // SAFETY: the read wrote `n` bytes at the head of the window.
                Inner::Tls(t) => t.ingest(unsafe { window[..n].assume_init_ref() }, |plain| feed(deframer, plain))?,
            };
        *torn = false;
        if !open {
            let msg = if deframer.is_mid_frame() {
                "connection closed mid-frame"
            } else {
                "connection closed"
            };
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, msg).into());
        }
        Ok(n == len)
    }
}

// ── Framing: the outbound queue ──────────────────────────────────────────────

/// Capacity the outbound buffer keeps once everything in it is written; a
/// larger allocation is released.
const OUT_RETAIN_BYTES: usize = 1 << 20;

/// The outbound queue: the queued frames back to back, each behind its length
/// prefix, and the partial-write cursor, so a write the socket cut short
/// resumes where it stopped.
#[derive(Default)]
struct OutBuf {
    buf: Vec<u8>,
    /// Bytes of `buf` already written.
    off: usize,
}

impl OutBuf {
    fn is_empty(&self) -> bool {
        self.off == self.buf.len()
    }

    /// The bytes queued and not yet written.
    fn pending(&self) -> &[u8] {
        &self.buf[self.off..]
    }

    /// Advance the cursor by `n` written bytes.
    fn advance(&mut self, n: usize) {
        // Total by construction: `n` is what the sink took of `pending`.
        debug_assert!(n <= self.buf.len() - self.off);
        self.off += n;
        if self.is_empty() {
            self.settle();
        }
    }

    /// Drop the written bytes once they are at least as many as the unwritten
    /// ones, so the unwritten bytes it moves are never more than were written.
    fn compact(&mut self) {
        if self.off > 0 && self.off >= self.buf.len() - self.off {
            self.buf.drain(..self.off);
            self.off = 0;
        }
    }

    /// Empty the buffer, releasing an allocation past `OUT_RETAIN_BYTES`.
    fn settle(&mut self) {
        self.buf.clear();
        self.off = 0;
        if self.buf.capacity() > OUT_RETAIN_BYTES {
            self.buf = Vec::new();
        }
    }
}

#[cfg(test)]
#[path = "tests/transport.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/transport.rs"]
mod bench;
