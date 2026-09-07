//! Client transport: AF-agnostic framing over a per-transport byte stream.
//!
//! The 4-byte LE length prefix used by every framed message is built and
//! parsed here. [`ClientTransport`] dispatches between the transports: an
//! AF_UNIX stream socket and TLS 1.3 over TCP (`tls.rs`). Both meet at one
//! non-blocking core per direction — [`Inner::read_into`] and
//! [`Inner::write_slices`] — and everything above them is transport-blind:
//! the [`FrameReader`] that turns reads into frames, the owned outbound queue
//! that [`ClientTransport::flush`] drains through a partial-write cursor, and
//! the blocking wrappers ([`ClientTransport::send_parts`],
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
use std::os::fd::AsRawFd;
#[cfg(test)]
use std::os::fd::OwnedFd;
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use super::error::ProtocolError;
use super::message::MessageParts;

mod tls;

/// One bound over everything a connect does: the TCP connect, the TLS
/// handshake, and the HELLO exchange after it.
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// One connected client transport. All framed I/O goes through these
/// methods; the wire bytes are identical across transports ("ZSets over the
/// wire" rides verbatim inside the TLS stream). The inner enum is private:
/// construction goes through `connect`, so a `ClientTransport` is always a
/// fully-handshaken connection, and rustls types stay out of the public API.
pub struct ClientTransport {
    inner: Inner,
    reader: FrameReader,
    queue: OutQueue,
    /// The largest payload this peer accepts, from the HELLO ACK — the server's
    /// own ingress cap until `mark_established` reads the negotiated one.
    /// `Session::submit` refuses past it.
    egress_limit: usize,
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
            Inner::Unix(s) => unix_read_into(s.as_raw_fd(), buf),
            Inner::Tls(t) => t.read_into(buf),
        }
    }

    /// The non-blocking write core: hand `slices` to the sink and report how
    /// many bytes it accepted. std's `write_vectored` clamps to `IOV_MAX`
    /// itself, so a caller may pass any number of slices and gets a prefix.
    fn write_slices(&mut self, slices: &[IoSlice<'_>]) -> Result<WriteOutcome, ProtocolError> {
        match self {
            Inner::Unix(s) => loop {
                match (&*s).write_vectored(slices) {
                    Ok(0) => {
                        return Err(ProtocolError::IoError(std::io::Error::new(
                            std::io::ErrorKind::WriteZero,
                            "writev returned 0",
                        )))
                    }
                    Ok(n) => return Ok(WriteOutcome::Written(n)),
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(WriteOutcome::WouldBlock),
                    Err(e) => return Err(ProtocolError::IoError(e)),
                }
            },
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
            Inner::Tls(t) => t.ship_nonblocking().map(|_| ()),
        }
    }

    fn as_raw_fd(&self) -> RawFd {
        match self {
            Inner::Unix(s) => s.as_raw_fd(),
            Inner::Tls(t) => t.as_raw_fd(),
        }
    }
}

/// `setsockopt(SOL_SOCKET, opt)` with a `c_int` value; a refused option is
/// not an error worth surfacing anywhere this is called.
pub(crate) fn set_sockopt_int(fd: RawFd, opt: libc::c_int, val: libc::c_int) {
    // SAFETY: setsockopt on a valid fd with a properly-sized option value.
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            opt,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

fn set_nonblocking(fd: RawFd) -> std::io::Result<()> {
    // SAFETY: fcntl on a valid fd.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// One `recv` into possibly-uninitialised storage. The Unix read core.
fn unix_read_into(fd: RawFd, buf: &mut [MaybeUninit<u8>]) -> Result<ReadOutcome, ProtocolError> {
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

/// `poll(2)` on one fd for `events` until `until`; `None` waits untimed.
/// Expiry surfaces as `WouldBlock`, as a deadlined blocking call would.
pub(crate) fn poll_fd(
    fd: RawFd,
    events: libc::c_short,
    until: Option<Instant>,
    retry_eintr: bool,
) -> std::io::Result<libc::c_short> {
    let timed_out = || std::io::Error::new(std::io::ErrorKind::WouldBlock, "socket operation timed out");
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
    fn new(inner: Inner) -> Result<Self, ProtocolError> {
        set_nonblocking(inner.as_raw_fd()).map_err(ProtocolError::IoError)?;
        Ok(ClientTransport {
            inner,
            reader: FrameReader::new(gnitz_wire::MAX_FRAME_PAYLOAD_PRE_HANDSHAKE),
            queue: OutQueue::default(),
            egress_limit: gnitz_wire::MAX_FRAME_PAYLOAD_SERVER,
        })
    }

    /// A `dup` of the underlying stream socket — the AF_UNIX socket, or the
    /// `TcpStream` under TLS. The copy shares the open file description, so it
    /// reports the same readiness, and closing it leaves this transport open.
    pub fn try_clone_fd(&self) -> Result<std::os::fd::OwnedFd, ProtocolError> {
        // SAFETY: the transport owns this fd for the whole of the borrow.
        unsafe { std::os::fd::BorrowedFd::borrow_raw(self.as_raw_fd()) }
            .try_clone_to_owned()
            .map_err(ProtocolError::IoError)
    }

    /// Connect to `target`: a literal `tls://HOST:PORT[?insecure|?ca=PATH]`
    /// prefix selects TLS; anything else (including any path containing
    /// `:`) is an AF_UNIX socket path — the prefix is the sole
    /// discriminator.
    pub fn connect(target: &str) -> Result<Self, ProtocolError> {
        if let Some(rest) = target.strip_prefix("tls://") {
            return tls::connect_tls(rest);
        }
        let stream = UnixStream::connect(target).map_err(ProtocolError::IoError)?;
        ClientTransport::new(Inner::Unix(stream))
    }

    /// Wrap an already-connected AF_UNIX stream socket (the transport closes
    /// it on drop). The payload ceiling stays at the pre-handshake bound until
    /// `mark_established`.
    #[cfg(test)]
    pub(crate) fn from_unix_fd(fd: OwnedFd) -> Self {
        ClientTransport::new(Inner::Unix(UnixStream::from(fd))).expect("O_NONBLOCK on a fresh socket")
    }

    /// The stream socket every driver polls: the AF_UNIX socket, or the
    /// `TcpStream` under TLS. Never used for framed I/O on the TLS variant.
    pub fn as_raw_fd(&self) -> RawFd {
        self.inner.as_raw_fd()
    }

    /// The payload ceiling `recv_framed` enforces: the pre-handshake bound
    /// until `mark_established`, then the negotiated one.
    #[cfg(test)]
    pub(crate) fn max_payload_len(&self) -> usize {
        self.reader.max_payload_len
    }

    /// Send a length-prefixed frame: `[u32 LE payload_length][payload]`.
    /// Scripted-peer scaffolding: production paths hand the parts they already
    /// hold to [`Self::send_parts`] instead of flattening into one buffer.
    #[cfg(any(test, feature = "integration"))]
    pub fn send_framed(&mut self, data: &[u8], until: Option<Instant>) -> Result<(), ProtocolError> {
        self.send_parts(MessageParts::single(data.to_vec()), until)
    }

    /// Send one owned frame — a single length prefix over the concatenation of
    /// its parts — blocking behind anything still queued, and no longer than
    /// `until`.
    pub fn send_parts(&mut self, parts: MessageParts, until: Option<Instant>) -> Result<(), ProtocolError> {
        self.enqueue(parts)?;
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
    pub(crate) fn enqueue(&mut self, parts: MessageParts) -> Result<(), ProtocolError> {
        let payload = parts.byte_len();
        let prefix = frame_len_prefix(payload)?;
        self.queue
            .push(prefix, parts, payload + gnitz_wire::FRAME_LEN_PREFIX_BYTES);
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
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(IOV_MAX_CHUNK.min(4 * queue.len()));
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

    /// The payload ceiling this peer advertised in its HELLO ACK.
    pub(crate) fn egress_limit(&self) -> usize {
        self.egress_limit
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
                    self.park(libc::POLLIN, until)?;
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

    /// The HELLO ACK is in hand: its figure replaces the pre-handshake bound in
    /// both directions, clamped — the peer does not get to raise our ceilings.
    pub(crate) fn mark_established(&mut self, server_limit: usize) {
        self.reader.max_payload_len = server_limit.min(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT);
        self.egress_limit = server_limit.min(gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);
    }
}

// ── Framing: the reader ──────────────────────────────────────────────────────

/// Size of the header scratch: large enough that one read serves a whole run
/// of pipelined push ACKs, and the ceiling on the one memcpy `carry` can
/// force.
const SCRATCH_BYTES: usize = 64 * 1024;

/// What `next_frame` produced.
pub(crate) enum Next {
    Frame(Vec<u8>),
    /// No frame can be completed from what is buffered, and the source is
    /// proven drained or would block.
    Pending,
}

/// Turns reads into frames without copying a payload out of a scratch buffer.
/// The header is read into a scratch that may over-read (surplus kept in
/// `carry`); once the length is known the payload is allocated at exactly
/// that size, the carry moves across, and the remainder is read directly into
/// it. The copy is bounded by one read's surplus, never by the frame.
struct FrameReader {
    scratch: Box<[MaybeUninit<u8>]>,
    /// The initialised, not-yet-consumed bytes of `scratch`.
    carry: Range<usize>,
    /// The payload being filled: `len()` of `capacity()` bytes, where the
    /// capacity *is* the frame's declared length — allocated exactly, never grown.
    partial: Option<Vec<u8>>,
    max_payload_len: usize,
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
            partial: None,
            max_payload_len,
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
            if let Some(frame) = self.advance_from_carry()? {
                return Ok(Next::Frame(frame));
            }
            if !may_read {
                return Ok(Next::Pending);
            }
            if self.eof {
                return Err(Self::eof_error(self.partial.is_some() || !self.carry.is_empty()));
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

    /// Consume `carry` into the frame in progress — a header becomes a
    /// payload allocation of exactly its declared length, payload bytes fill
    /// it — and hand the frame out once it is whole.
    fn advance_from_carry(&mut self) -> Result<Option<Vec<u8>>, ProtocolError> {
        loop {
            match self.partial.as_mut() {
                Some(p) => {
                    let take = (p.capacity() - p.len()).min(self.carry.len());
                    // SAFETY: `carry` covers exactly the bytes a read initialised.
                    let src = unsafe { self.scratch[self.carry.start..self.carry.start + take].assume_init_ref() };
                    p.spare_capacity_mut()[..take].write_copy_of_slice(src);
                    // SAFETY: the copy above initialised `take` bytes at the
                    // head of the spare capacity.
                    unsafe { p.set_len(p.len() + take) };
                    self.carry.start += take;
                    if p.len() < p.capacity() {
                        return Ok(None);
                    }
                    return Ok(Some(self.partial.take().unwrap()));
                }
                None => {
                    if self.carry.len() < gnitz_wire::FRAME_LEN_PREFIX_BYTES {
                        return Ok(None);
                    }
                    let hdr = &self.scratch[self.carry.start..self.carry.start + gnitz_wire::FRAME_LEN_PREFIX_BYTES];
                    // SAFETY: `carry` covers exactly the bytes a read initialised.
                    let hdr = unsafe { hdr.assume_init_ref() };
                    let payload_len = parse_frame_len(hdr.try_into().unwrap(), self.max_payload_len)?;
                    self.carry.start += gnitz_wire::FRAME_LEN_PREFIX_BYTES;
                    self.partial = Some(Vec::with_capacity(payload_len));
                }
            }
        }
    }

    /// One read into wherever the next bytes belong: the payload in progress
    /// (the carry is always empty while one is short), else the scratch
    /// behind the carry, which holds at most three header bytes and is
    /// compacted to the front first.
    fn read_more(&mut self, inner: &mut Inner) -> Result<ReadOutcome, ProtocolError> {
        let outcome = match self.partial.as_mut() {
            Some(p) => {
                let outcome = inner.read_into(p.spare_capacity_mut())?;
                if let ReadOutcome::Data { n, .. } = outcome {
                    // SAFETY: the read initialised `n` bytes at the head of the
                    // spare capacity.
                    unsafe { p.set_len(p.len() + n) };
                }
                outcome
            }
            None => {
                if self.carry.start > 0 {
                    let len = self.carry.len();
                    self.scratch.copy_within(self.carry.clone(), 0);
                    self.carry = 0..len;
                }
                let outcome = inner.read_into(&mut self.scratch[self.carry.end..])?;
                if let ReadOutcome::Data { n, .. } = outcome {
                    self.carry.end += n;
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
/// itself, so this bounds the local slice array rather than the syscall.
///
/// Divided by a frame's non-empty segment count it is the **writev quantum**:
/// 512 pipelined frames for a control-only verb, 341 for a warm push, 256 for a
/// cold one. A driver's own request-channel depth does not enter it.
const IOV_MAX_CHUNK: usize = 1024;

/// One queue entry: a frame with its own length prefix, and the byte count of
/// the two together.
struct QueuedFrame {
    prefix: [u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES],
    parts: MessageParts,
    total: usize,
}

impl QueuedFrame {
    /// The segments in wire order, empty ones included: `build_slices` skips
    /// them with the same test that advances its cursor.
    fn segments(&self) -> [&[u8]; 4] {
        let [ctrl, schema, data] = self.parts.segments();
        [&self.prefix, ctrl, schema, data]
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
    fn push(&mut self, prefix: [u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES], parts: MessageParts, total: usize) {
        self.bytes += total;
        self.frames.push_back(QueuedFrame { prefix, parts, total });
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
            let total = f.total;
            if self.off < total {
                break;
            }
            self.off -= total;
            self.frames.pop_front();
        }
    }
}

/// Decode a received length prefix against the per-connection ceiling, refusing
/// the close sentinel. The one enforcement point for every framed recv.
pub(crate) fn parse_frame_len(
    hdr: [u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES],
    max_payload_len: usize,
) -> Result<usize, ProtocolError> {
    let payload_len = u32::from_le_bytes(hdr) as usize;
    if payload_len == 0 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "zero-length close sentinel",
        )));
    }
    if payload_len > max_payload_len {
        return Err(ProtocolError::DecodeError(format!(
            "payload length {payload_len} exceeds maximum {max_payload_len} bytes"
        )));
    }
    Ok(payload_len)
}

/// Encode a frame's length prefix, refusing the close sentinel and anything
/// past `u32::MAX`. The one enforcement point for every framed send.
pub(crate) fn frame_len_prefix(len: usize) -> Result<[u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES], ProtocolError> {
    if len == 0 {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty frame (would collide with the protocol close sentinel)",
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

/// Send HELLO, parse the ACK, and mark the transport established under its
/// payload limit. Returns the server's `published_lsn`, which seeds the client's
/// OCC basis. `timeout` bounds the exchange as a whole, not each leg.
pub fn hello_handshake(t: &mut ClientTransport, timeout: Option<Duration>) -> Result<u64, ProtocolError> {
    let until = timeout.map(|d| Instant::now() + d);
    let payload = gnitz_wire::encode_hello_payload(gnitz_wire::WAL_FORMAT_VERSION as u16);
    t.send_parts(MessageParts::single(payload.to_vec()), until)?;

    let buf = t.recv_framed(until)?;
    if buf.len() == gnitz_wire::HELLO_ACK_PAYLOAD_LEN as usize {
        let ack = gnitz_wire::decode_hello_ack(&buf).map_err(|e| ProtocolError::DecodeError(e.into()))?;
        if ack.magic != gnitz_wire::HELLO_MAGIC {
            return Err(ProtocolError::DecodeError("HELLO ACK magic mismatch".into()));
        }
        t.mark_established(ack.limit_bytes as usize);
        return Ok(ack.published_lsn);
    }

    // Not an ACK — the server sent a STATUS_ERROR control block. The frame is
    // well-formed, so its error is the peer's refusal, not a decode failure.
    let msg = super::message::parse_response_frame(&buf, None)?.message;
    let err = msg.error_text.unwrap_or_else(|| "HELLO rejected".into());
    Err(ProtocolError::ServerRejected(err))
}

#[cfg(test)]
#[path = "tests/transport.rs"]
mod tests;
