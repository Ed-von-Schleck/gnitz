//! Client transport: AF-agnostic framing over a per-transport byte stream.
//!
//! The 4-byte LE length prefix used by every framed message is built and
//! parsed here. [`ClientTransport`] dispatches between the transports: an
//! AF_UNIX stream socket and TLS 1.3 over TCP (`tls.rs`). Both meet at one
//! non-blocking core per direction — [`Inner::read_into`] and
//! [`Inner::write_slices`] — and everything above them is transport-blind:
//! the [`FrameReader`] that turns reads into frames, the owned outbound queue
//! that [`ClientTransport::flush`] drains through a partial-write cursor, and
//! the blocking wrappers ([`ClientTransport::send_frame`],
//! [`ClientTransport::recv_framed`]) that park in `poll(2)` around those
//! cores. Nothing beneath the wrappers ever waits.
//!
//! The fd is `O_NONBLOCK` always; the wrappers emulate blocking under an
//! `until` instant the caller passes, so one deadline covers a whole call
//! however many parks it takes.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

use std::collections::VecDeque;
use std::io::{IoSlice, Write};
use std::mem::MaybeUninit;
use std::ops::Range;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use gnitz_wire::{Deframer, FrameLenError};

use super::error::ProtocolError;

mod tls;

/// The one deadline over a connect: the TCP connect, then the TLS handshake and HELLO
/// exchange, which run as one. Name resolution runs before it, unbounded.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// One connected client transport. All framed I/O goes through these
/// methods; the wire bytes are identical across transports ("ZSets over the
/// wire" rides verbatim inside the TLS stream). On TLS the handshake completes
/// inside the first exchange (HELLO), where certificate failures surface.
pub struct ClientTransport {
    inner: Inner,
    reader: FrameReader,
    queue: OutQueue,
}

enum Inner {
    Unix(UnixStream),
    Tls(Box<tls::TlsInner>),
}

/// What one non-blocking read of the source produced.
#[derive(Clone, Copy)]
enum ReadOutcome {
    /// `n` bytes landed; `drained` says the read returned less than it asked
    /// for, which on a stream socket proves the receive queue is empty.
    Data { n: usize, drained: bool },
    /// The peer closed the stream.
    Eof,
    /// Nothing readable yet.
    WouldBlock,
}

/// What one non-blocking vectored write accepted.
enum WriteOutcome {
    Written(usize),
    WouldBlock,
}

impl Inner {
    /// The non-blocking read core: read what is there into `buf`, once, and
    /// report what happened. Retries `EINTR` — Python signal handlers run on
    /// the main thread during I/O.
    fn read_into(&mut self, buf: &mut [MaybeUninit<u8>]) -> Result<ReadOutcome, ProtocolError> {
        match self {
            Inner::Unix(s) => recv_into(s.as_raw_fd(), buf),
            Inner::Tls(t) => t.read_into(buf),
        }
    }

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

/// One `recv` into possibly-uninitialised storage: the Unix read core, and the
/// TLS arm's ciphertext read.
fn recv_into(fd: RawFd, buf: &mut [MaybeUninit<u8>]) -> Result<ReadOutcome, ProtocolError> {
    loop {
        // SAFETY: pointer/len come from a valid &mut [MaybeUninit<u8>].
        let n = unsafe { libc::recv(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            return match e.kind() {
                std::io::ErrorKind::Interrupted => continue,
                std::io::ErrorKind::WouldBlock => Ok(ReadOutcome::WouldBlock),
                _ => Err(ProtocolError::IoError(e)),
            };
        }
        if n == 0 {
            return Ok(ReadOutcome::Eof);
        }
        let n = n as usize;
        return Ok(ReadOutcome::Data { n, drained: n < buf.len() });
    }
}

/// A deadline's expiry, as a deadlined blocking call reports it: `WouldBlock`.
fn timed_out() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::WouldBlock, "socket operation timed out")
}

/// `poll(2)` on one fd for `events` until `until`; `None` waits untimed.
/// Expiry surfaces as `WouldBlock`, as a deadlined blocking call would.
pub(crate) fn poll_fd(
    fd: RawFd,
    events: libc::c_short,
    until: Option<Instant>,
    retry_eintr: bool,
) -> std::io::Result<libc::c_short> {
    loop {
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
        let rc = unsafe { libc::poll(&mut pfd, 1, timeout_ms) };
        if rc < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted && retry_eintr {
                continue;
            }
            return Err(e);
        }
        if rc == 0 {
            return Err(timed_out());
        }
        return Ok(pfd.revents);
    }
}

impl ClientTransport {
    fn new(inner: Inner) -> Self {
        ClientTransport {
            inner,
            reader: FrameReader::new(gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE),
            queue: OutQueue::default(),
        }
    }

    /// A `dup` of the underlying stream socket — the AF_UNIX socket, or the
    /// `TcpStream` under TLS. The copy shares the open file description, so it
    /// reports the same readiness, and closing it leaves this transport open.
    pub fn try_clone_fd(&self) -> Result<OwnedFd, ProtocolError> {
        Ok(self.inner.as_fd().try_clone_to_owned()?)
    }

    /// Connect to `tls://HOST:PORT[?QUERY]`, or else to an AF_UNIX socket path.
    /// `QUERY` is `&`-separated, each at most once: `ca=PATH` (PEM roots, default
    /// webpki), `cert=PATH` and `key=PATH` (mTLS). `until` bounds a TCP connect.
    pub fn connect(target: &str, until: Option<Instant>) -> Result<Self, ProtocolError> {
        if let Some(rest) = target.strip_prefix("tls://") {
            return tls::connect_tls(rest, until);
        }
        let stream = UnixStream::connect(target)?;
        stream.set_nonblocking(true)?;
        Ok(ClientTransport::new(Inner::Unix(stream)))
    }

    /// Wrap an already-connected AF_UNIX stream socket (the transport closes
    /// it on drop). The payload ceiling stays at the pre-handshake bound until
    /// `mark_established`.
    #[cfg(test)]
    pub(crate) fn from_unix_fd(fd: OwnedFd) -> Self {
        let stream = UnixStream::from(fd);
        stream.set_nonblocking(true).expect("O_NONBLOCK on a fresh socket");
        ClientTransport::new(Inner::Unix(stream))
    }

    /// The stream socket every driver polls: the AF_UNIX socket, or the
    /// `TcpStream` under TLS. Never used for framed I/O on the TLS variant.
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.as_fd().as_raw_fd()
    }

    /// The payload ceiling `recv_framed` enforces: the pre-handshake bound
    /// until `mark_established`, then the established one.
    #[cfg(test)]
    pub(crate) fn max_payload_len(&self) -> usize {
        self.reader.deframer.max_payload_len()
    }

    /// Send one owned frame — `[u32 LE payload_length][payload]` — blocking
    /// behind anything still queued, and no longer than `until`.
    pub fn send_frame(&mut self, payload: Vec<u8>, until: Option<Instant>) -> Result<(), ProtocolError> {
        self.enqueue(payload)?;
        self.flush_blocking(until)
    }

    /// Park until the fd reports `events`, no longer than `until`: expiry
    /// surfaces as `WouldBlock`.
    fn park(&self, events: libc::c_short, until: Option<Instant>) -> Result<(), ProtocolError> {
        poll_fd(self.as_raw_fd(), events, until, true)
            .map(|_| ())
            .map_err(ProtocolError::IoError)
    }

    /// Queue an owned frame behind everything already queued. Nothing is
    /// written here; `flush` / `flush_blocking` ship the queue.
    pub(crate) fn enqueue(&mut self, payload: Vec<u8>) -> Result<(), ProtocolError> {
        let prefix = frame_len_prefix(payload.len())?;
        self.queue.push(prefix, payload);
        Ok(())
    }

    /// Write what the fd accepts from the queue cursor and report whether
    /// anything is still pending — the negation of `wants_write`, which on
    /// TLS includes ciphertext rustls holds after the queue has emptied into
    /// it. Never parks.
    pub(crate) fn flush(&mut self) -> Result<bool, ProtocolError> {
        loop {
            self.inner.ship_ciphertext()?;
            if self.inner.has_pending_ciphertext() {
                return Ok(true);
            }
            if self.queue.is_empty() {
                return Ok(false);
            }
            let ClientTransport { inner, queue, .. } = self;
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(IOV_MAX_CHUNK.min(2 * queue.len()));
            queue.build_slices(&mut slices);
            match inner.write_slices(&slices)? {
                WriteOutcome::Written(n) => queue.advance(n),
                WriteOutcome::WouldBlock => return Ok(true),
            }
        }
    }

    /// `flush` inside the `POLLOUT` park-and-retry loop: returns once nothing
    /// is pending, or with `WouldBlock` at `until` with the cursor intact.
    pub(crate) fn flush_blocking(&mut self, until: Option<Instant>) -> Result<(), ProtocolError> {
        while self.flush()? {
            self.park(libc::POLLOUT, until)?;
        }
        Ok(())
    }

    /// Frame bytes queued and not yet written.
    pub(crate) fn queued_bytes(&self) -> usize {
        self.queue.bytes
    }

    /// Bytes queued, or ciphertext pending: the `WRITE` half of a driver's
    /// interest.
    pub(crate) fn wants_write(&self) -> bool {
        !self.queue.is_empty() || self.inner.has_pending_ciphertext()
    }

    /// Drop every queued frame and the cursor with them.
    pub(crate) fn clear_queue(&mut self) {
        self.queue.clear();
    }

    /// Shut the socket down so the peer sees EOF. The fd stays open until the
    /// transport drops, since a reactor may still have it registered.
    pub(crate) fn shutdown(&self) {
        // SAFETY: `as_raw_fd` is this transport's own open socket.
        let _ = unsafe { libc::shutdown(self.as_raw_fd(), libc::SHUT_RDWR) };
    }

    /// The next complete frame, reading the fd only when `may_read`. Returns
    /// `Pending` once the source is proven drained (or would block) and no
    /// frame can be completed from what is buffered.
    pub(crate) fn next_frame(&mut self, may_read: bool) -> Result<Next, ProtocolError> {
        let ClientTransport { inner, reader, .. } = self;
        reader.next_frame(inner, may_read)
    }

    /// Receive one frame, blocking until it is whole and no longer than
    /// `until`. The ceiling is enforced on the prefix, before any allocation.
    pub fn recv_framed(&mut self, until: Option<Instant>) -> Result<Vec<u8>, ProtocolError> {
        loop {
            match self.next_frame(true)? {
                Next::Frame(f) => return Ok(f),
                Next::Pending => {
                    // A read can queue ciphertext nothing else will send: during the handshake,
                    // the client's Finished flight and the plaintext buffered behind it.
                    let events = if self.flush()? {
                        libc::POLLIN | libc::POLLOUT
                    } else {
                        libc::POLLIN
                    };
                    self.park(events, until)?;
                    self.begin_read();
                }
            }
        }
    }

    /// Forget the drained proof the last read left: after a park the source
    /// may have refilled, so the next `next_frame` must read again rather
    /// than report `Pending` off a stale observation. Every driver calls it
    /// once per wakeup, before its reads.
    pub(crate) fn begin_read(&mut self) {
        self.reader.drained = false;
    }

    /// The HELLO ACK is in hand: raise the inbound ceiling from the pre-handshake
    /// bound to the established one.
    pub(crate) fn mark_established(&mut self) {
        self.reader.deframer.set_max_payload_len(gnitz_wire::MAX_FRAME_PAYLOAD);
    }
}

// ── Framing: the reader ──────────────────────────────────────────────────────

/// Size of the read scratch: large enough that one read serves a whole run
/// of pipelined push ACKs.
const SCRATCH_BYTES: usize = 64 * 1024;

/// What `next_frame` produced.
pub(crate) enum Next {
    Frame(Vec<u8>),
    /// No frame can be completed from what is buffered, and the source is
    /// proven drained or would block.
    Pending,
}

/// Turns reads into frames. A read lands in the scratch unless a payload is in
/// progress, in which case it goes straight into that payload's tail.
struct FrameReader {
    scratch: Box<[MaybeUninit<u8>]>,
    /// The initialised, not-yet-consumed bytes of `scratch`.
    carry: Range<usize>,
    deframer: Deframer<Box<[MaybeUninit<u8>]>>,
    /// A read returned 0. Raised once nothing buffered can advance a frame,
    /// so the frame delivered by the same read is not lost.
    eof: bool,
    /// The last read returned less than it asked for: the source was drained
    /// at that instant. Cleared by `begin_read` at every wakeup, so the
    /// attempt after a park reads again.
    drained: bool,
}

impl FrameReader {
    fn new(max_payload_len: usize) -> Self {
        FrameReader {
            scratch: Box::new_uninit_slice(SCRATCH_BYTES),
            carry: 0..0,
            deframer: Deframer::new(max_payload_len),
            eof: false,
            drained: false,
        }
    }

    fn eof_error(mid_frame: bool) -> ProtocolError {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            if mid_frame {
                "connection closed mid-frame"
            } else {
                "connection closed"
            },
        ))
    }

    fn next_frame(&mut self, inner: &mut Inner, may_read: bool) -> Result<Next, ProtocolError> {
        loop {
            // SAFETY: `carry` covers exactly the bytes a read initialised.
            let mut src = unsafe { self.scratch[self.carry.clone()].assume_init_ref() };
            let before = src.len();
            let frame = self
                .deframer
                .feed(&mut src, |len| Ok::<_, ProtocolError>(Box::new_uninit_slice(len)))?;
            self.carry.start += before - src.len();
            if let Some(b) = frame {
                // SAFETY: the deframer hands a payload out only once every byte is written.
                return Ok(Next::Frame(unsafe { b.assume_init() }.into_vec()));
            }
            if !may_read {
                return Ok(Next::Pending);
            }
            if self.eof {
                return Err(Self::eof_error(self.deframer.is_mid_frame()));
            }
            if self.drained {
                return Ok(Next::Pending);
            }
            match self.read_more(inner)? {
                ReadOutcome::Data { .. } => {}
                ReadOutcome::WouldBlock => return Ok(Next::Pending),
                ReadOutcome::Eof => self.eof = true,
            }
        }
    }

    /// One read, into the payload in progress when there is one, else into the
    /// whole scratch.
    fn read_more(&mut self, inner: &mut Inner) -> Result<ReadOutcome, ProtocolError> {
        debug_assert!(self.carry.is_empty(), "a read would land ahead of carried bytes");
        let outcome = match self.deframer.payload_tail() {
            Some(tail) => {
                let outcome = inner.read_into(tail)?;
                if let ReadOutcome::Data { n, .. } = outcome {
                    // SAFETY: the read initialised `n` bytes at the head of the tail.
                    unsafe { self.deframer.filled(n) };
                }
                outcome
            }
            None => {
                let outcome = inner.read_into(&mut self.scratch[..])?;
                if let ReadOutcome::Data { n, .. } = outcome {
                    self.carry = 0..n;
                }
                outcome
            }
        };
        if let ReadOutcome::Data { drained, .. } = outcome {
            self.drained = drained;
        }
        Ok(outcome)
    }
}

// ── Framing: the outbound queue ──────────────────────────────────────────────

/// Slices per `flush` chunk: Linux's `UIO_MAXIOV`. std clamps to `IOV_MAX`
/// itself, so this bounds the local slice array rather than the syscall. A frame
/// is two slices, so one chunk carries 512 pipelined frames.
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
/// the front entry, so a send that hit its deadline resumes where it stopped.
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

    fn clear(&mut self) {
        self.frames.clear();
        self.off = 0;
        self.bytes = 0;
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
            FrameLenError::Oversize { len, max } => {
                ProtocolError::DecodeError(format!("payload length {len} exceeds maximum {max} bytes"))
            }
        }
    }
}

/// Encode a frame's length prefix, refusing zero and anything past `u32::MAX`: where
/// every framed send of this client is checked.
pub(crate) fn frame_len_prefix(len: usize) -> Result<[u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES], ProtocolError> {
    if len == 0 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty frame (zero is never a legal length)",
        )));
    }
    if len > u32::MAX as usize {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("frame size {len} exceeds u32::MAX wire limit"),
        )));
    }
    Ok((len as u32).to_le_bytes())
}

/// Send HELLO, check the ACK, and mark the transport established. `until` bounds
/// the exchange as a whole — on TLS the handshake included — not each leg.
pub fn hello_handshake(t: &mut ClientTransport, until: Option<Instant>) -> Result<(), ProtocolError> {
    let payload = gnitz_wire::encode_hello_payload(gnitz_wire::wal::WAL_FORMAT_VERSION);
    t.send_frame(payload.to_vec(), until)?;

    let buf = t.recv_framed(until)?;
    if buf.len() == gnitz_wire::HELLO_ACK_PAYLOAD_LEN {
        gnitz_wire::decode_hello_ack(&buf).map_err(|e| ProtocolError::DecodeError(e.into()))?;
        t.mark_established();
        return Ok(());
    }

    // Not an ACK — the server sent a `WireStatus::Error` control block. The frame is
    // well-formed, so its error is the peer's refusal, not a decode failure.
    let ctrl = gnitz_wire::control::peek_control_block(&buf).map_err(|e| ProtocolError::DecodeError(e.into()))?;
    let err = match ctrl.fault() {
        Some(f) if !f.text.is_empty() => f.text,
        _ => "HELLO rejected".into(),
    };
    Err(ProtocolError::ServerRejected(err))
}

#[cfg(test)]
#[path = "tests/transport.rs"]
mod tests;
