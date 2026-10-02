//! Client transport: frames over an AF_UNIX stream socket or TLS 1.3 over TCP
//! (`tls.rs`), the same wire bytes on both. Nothing here waits except the HELLO
//! exchange inside [`ClientTransport::connect`].

use std::borrow::Cow;
use std::collections::VecDeque;
use std::io::{IoSlice, Write};
use std::mem::MaybeUninit;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::time::Instant;

use gnitz_wire::{Deframer, FrameLenError, HelloError};

use super::error::ProtocolError;
use crate::ClientError;

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
    queue: OutQueue,
}

/// Bytes one socket read can take, short of a read straight into a payload.
const WINDOW_BYTES: usize = 64 * 1024;

enum Inner {
    Unix(UnixStream),
    Tls(Box<tls::TlsInner>),
}

/// What one non-blocking vectored write accepted.
enum WriteOutcome {
    Written(usize),
    WouldBlock,
}

impl Inner {
    /// The non-blocking write core: hand `slices` to the sink and report how
    /// many bytes it accepted. std's `write_vectored` clamps to `IOV_MAX`
    /// itself, so a caller may pass any number of slices and gets a prefix.
    fn write_slices(&mut self, slices: &[IoSlice<'_>]) -> Result<WriteOutcome, ProtocolError> {
        match self {
            Inner::Unix(s) => write_nonblocking(|| (&*s).write_vectored(slices)),
            Inner::Tls(t) => t.write_slices(slices),
        }
    }

    /// Ciphertext rustls still holds for the socket. Always false on the Unix
    /// arm, which has no second outbound source beneath the queue.
    fn has_pending_ciphertext(&self) -> bool {
        match self {
            Inner::Unix(_) => false,
            Inner::Tls(t) => t.wants_write(),
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
            Inner::Tls(t) => t.as_fd(),
        }
    }
}

/// One non-blocking socket write: `EINTR` retried, `EAGAIN` reported, a zero-byte write refused.
fn write_nonblocking(mut write: impl FnMut() -> std::io::Result<usize>) -> Result<WriteOutcome, ProtocolError> {
    loop {
        match write() {
            Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
            Ok(n) => return Ok(WriteOutcome::Written(n)),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(WriteOutcome::WouldBlock),
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

/// One `poll(2)` on one fd for `events` until `until`; `None` waits untimed.
/// Expiry surfaces as `TimedOut`; `EINTR` is returned, and since `until` is
/// absolute a caller may simply call again.
pub(crate) fn poll_fd(fd: RawFd, events: libc::c_short, until: Option<Instant>) -> std::io::Result<libc::c_short> {
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
    let mut pfd = libc::pollfd { fd, events, revents: 0 };
    // SAFETY: one valid pollfd, count 1.
    match unsafe { libc::poll(&mut pfd, 1, timeout_ms) } {
        rc if rc < 0 => Err(std::io::Error::last_os_error()),
        0 => Err(timed_out()),
        _ => Ok(pfd.revents),
    }
}

impl ClientTransport {
    fn new(inner: Inner) -> Self {
        ClientTransport {
            inner,
            deframer: Deframer::default(),
            window: Box::new_uninit_slice(WINDOW_BYTES),
            torn: false,
            queue: OutQueue::default(),
        }
    }

    /// A `dup` of the underlying stream socket — the AF_UNIX socket, or the
    /// `TcpStream` under TLS. The copy shares the open file description, so it
    /// reports the same readiness, and closing it leaves this transport open.
    pub fn try_clone_fd(&self) -> Result<OwnedFd, ProtocolError> {
        Ok(self.inner.as_fd().try_clone_to_owned()?)
    }

    /// Connect to a `tls://` target, or else to an AF_UNIX socket path, and
    /// exchange HELLOs, all no later than `until`.
    pub fn connect(target: &str, until: Instant) -> Result<Self, ClientError> {
        let mut t = match target.strip_prefix("tls://") {
            Some(rest) => tls::connect_tls(rest, until)?,
            None => ClientTransport::unix(UnixStream::connect(target)?)?,
        };
        t.hello(until)?;
        Ok(t)
    }

    /// Wrap a connected AF_UNIX stream socket, which the transport closes on
    /// drop. No HELLO is exchanged.
    pub(crate) fn unix(stream: UnixStream) -> Result<Self, ProtocolError> {
        stream.set_nonblocking(true)?;
        Ok(ClientTransport::new(Inner::Unix(stream)))
    }

    /// The stream socket every driver polls: the AF_UNIX socket, or the
    /// `TcpStream` under TLS. Never used for framed I/O on the TLS variant.
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.as_fd().as_raw_fd()
    }

    /// Exchange HELLOs, no later than `until`. On TLS this is also where the
    /// handshake completes and certificate failures surface.
    fn hello(&mut self, until: Instant) -> Result<(), ClientError> {
        self.enqueue(gnitz_wire::HELLO.to_vec());
        loop {
            // Every round: a read queues the TLS handshake's next flight.
            self.flush()?;
            let events = if self.wants_write() {
                libc::POLLIN | libc::POLLOUT
            } else {
                libc::POLLIN
            };
            match poll_fd(self.as_raw_fd(), events, Some(until)) {
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                polled => polled.map_err(ProtocolError::from)?,
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
            // Ahead of `read`'s own failure: a server refusing the version
            // answers, then hangs up.
            gnitz_wire::check_hello(reply).map_err(|e| match e {
                HelloError::Malformed => ClientError::from(ProtocolError::DecodeError(e.to_string())),
                HelloError::Version { .. } => ClientError::from(e.to_string()),
            })?;
            read?;
            return Ok(());
        }
    }

    /// Queue an owned frame behind everything already queued. Nothing is
    /// written here; `flush` ships the queue.
    pub(crate) fn enqueue(&mut self, payload: Vec<u8>) {
        self.queue.push(gnitz_wire::frame_len_prefix(payload.len()), payload);
    }

    /// Write what the fd accepts from the queue cursor. Never parks; what is
    /// left is reported by `wants_write`.
    pub(crate) fn flush(&mut self) -> Result<(), ProtocolError> {
        loop {
            self.inner.ship_ciphertext()?;
            if self.inner.has_pending_ciphertext() || self.queue.is_empty() {
                return Ok(());
            }
            let ClientTransport { inner, queue, .. } = self;
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(IOV_MAX_CHUNK.min(2 * queue.len()));
            queue.build_slices(&mut slices);
            match inner.write_slices(&slices)? {
                WriteOutcome::Written(n) => queue.advance(n),
                WriteOutcome::WouldBlock => return Ok(()),
            }
        }
    }

    /// Frame bytes queued and not yet written.
    pub(crate) fn queued_bytes(&self) -> usize {
        self.queue.bytes
    }

    /// Bytes queued, or ciphertext pending: the `WRITE` half of a driver's
    /// interest. On TLS the ciphertext can outlast the queue that emptied into
    /// it.
    pub(crate) fn wants_write(&self) -> bool {
        !self.queue.is_empty() || self.inner.has_pending_ciphertext()
    }

    /// Shut the socket down so the peer sees EOF, and drop every queued frame.
    /// The fd stays open until the transport drops, since a reactor may still
    /// have it registered.
    pub(crate) fn close(&mut self) {
        // SAFETY: `as_raw_fd` is this transport's own open socket.
        let _ = unsafe { libc::shutdown(self.as_raw_fd(), libc::SHUT_RDWR) };
        self.queue = OutQueue::default();
    }

    /// One read of the socket, handing `on_frame` every frame it completes.
    /// True when the read filled its window, so more may be waiting.
    pub(crate) fn read(
        &mut self,
        mut on_frame: impl FnMut(Cow<'_, [u8]>) -> Result<(), ProtocolError>,
    ) -> Result<bool, ProtocolError> {
        let ClientTransport { inner, deframer, window, torn, .. } = self;
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

/// Slices per `flush` chunk: Linux's `UIO_MAXIOV`. std clamps to `IOV_MAX`
/// itself, so this bounds the local slice array rather than the syscall.
const IOV_MAX_CHUNK: usize = 1024;

/// One queue entry: a frame's payload and its own length prefix.
struct QueuedFrame {
    prefix: [u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES],
    payload: Vec<u8>,
}

impl QueuedFrame {
    /// The two slices in wire order.
    fn segments(&self) -> [&[u8]; 2] {
        [&self.prefix, &self.payload]
    }
}

/// The owned outbound queue and its partial-write cursor: one byte offset into
/// the front entry, so a write the socket cut short resumes where it stopped.
#[derive(Default)]
struct OutQueue {
    frames: VecDeque<QueuedFrame>,
    /// Bytes of the front frame already written.
    off: usize,
    /// Bytes queued and not yet written, prefixes included.
    bytes: usize,
}

impl OutQueue {
    fn push(&mut self, prefix: [u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES], payload: Vec<u8>) {
        self.bytes += gnitz_wire::FRAME_LEN_PREFIX_BYTES + payload.len();
        self.frames.push_back(QueuedFrame { prefix, payload });
    }

    fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn len(&self) -> usize {
        self.frames.len()
    }

    /// Slices from the cursor forward, at most `IOV_MAX_CHUNK` of them.
    fn build_slices<'a>(&'a self, out: &mut Vec<IoSlice<'a>>) {
        let mut skip = self.off;
        for f in &self.frames {
            for s in f.segments() {
                if skip >= s.len() {
                    skip -= s.len();
                    continue;
                }
                out.push(IoSlice::new(&s[skip..]));
                skip = 0;
                if out.len() == IOV_MAX_CHUNK {
                    return;
                }
            }
        }
    }

    /// Advance the cursor by `n` written bytes, popping fully-sent frames.
    fn advance(&mut self, n: usize) {
        // Total by construction: `n` is what the sink took from our own slices.
        // Release checks no subtraction, and a wrap here tears the next frame.
        debug_assert!(n <= self.bytes);
        self.bytes -= n;
        self.off += n;
        while let Some(f) = self.frames.front() {
            // Ends the borrow before the pop.
            let total = gnitz_wire::FRAME_LEN_PREFIX_BYTES + f.payload.len();
            if self.off < total {
                break;
            }
            self.off -= total;
            self.frames.pop_front();
        }
    }
}

impl From<FrameLenError> for ProtocolError {
    fn from(e: FrameLenError) -> Self {
        match e {
            FrameLenError::Zero => ProtocolError::DecodeError("zero-length frame".into()),
            FrameLenError::Oversize { len } => ProtocolError::DecodeError(format!(
                "payload length {len} exceeds maximum {} bytes",
                gnitz_wire::MAX_FRAME_PAYLOAD
            )),
            FrameLenError::Alloc { .. } => std::io::Error::from(std::io::ErrorKind::OutOfMemory).into(),
        }
    }
}

#[cfg(test)]
#[path = "tests/transport.rs"]
mod tests;
