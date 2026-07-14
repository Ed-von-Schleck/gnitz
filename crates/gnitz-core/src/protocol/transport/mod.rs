//! Client transport: AF-agnostic framing over a per-transport byte stream.
//!
//! The 4-byte LE length prefix used by every framed message is built and
//! parsed here. [`ClientTransport`] dispatches between the transports: an
//! AF_UNIX stream socket (raw-fd `writev`/`recv` framing below) and TLS 1.3
//! over TCP (`tls.rs`, framing over
//! `rustls::StreamOwned<ClientConnection, TcpStream>`).

use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::io::RawFd;
use std::sync::OnceLock;

use super::error::ProtocolError;

pub mod tls;

/// One connected client transport. All framed I/O goes through these
/// methods; the wire bytes are identical across transports ("ZSets over the
/// wire" rides verbatim inside the TLS stream). The inner enum is private:
/// construction goes through `connect`, so a `ClientTransport` is always a
/// fully-handshaken connection, and rustls types stay out of the public API.
pub struct ClientTransport(Inner);

enum Inner {
    Unix(OwnedFd),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
}

// The sync API surfaces (C handle movable across threads, #[pyclass])
// require the transport to be Send. Concurrent use of one connection is
// still undefined behaviour by the C contract.
const fn assert_send<T: Send>() {}
const _: () = assert_send::<ClientTransport>();

impl ClientTransport {
    /// Connect to `target`: a literal `tls://HOST:PORT[?insecure|?ca=PATH]`
    /// prefix selects TLS; anything else (including any path containing
    /// `:`) is an AF_UNIX socket path — the prefix is the sole
    /// discriminator.
    pub fn connect(target: &str) -> Result<Self, ProtocolError> {
        if let Some(rest) = target.strip_prefix("tls://") {
            return tls::connect_tls(rest);
        }
        let stream = std::os::unix::net::UnixStream::connect(target).map_err(ProtocolError::IoError)?;
        Ok(ClientTransport(Inner::Unix(stream.into())))
    }

    /// Wrap an already-connected AF_UNIX stream fd, taking ownership of it
    /// (the transport closes it on drop).
    #[cfg(test)]
    pub(crate) fn from_unix_fd(fd: RawFd) -> Self {
        use std::os::fd::FromRawFd;
        // SAFETY: the caller transfers exclusive ownership of a valid fd.
        ClientTransport(Inner::Unix(unsafe { OwnedFd::from_raw_fd(fd) }))
    }

    /// The underlying stream socket fd (AF_UNIX socket, or the TcpStream
    /// under `StreamOwned`). For socket-option tweaks in tests and for
    /// `waker()`; never used for framed I/O on the TLS variant.
    pub fn as_raw_fd(&self) -> RawFd {
        match &self.0 {
            Inner::Unix(fd) => fd.as_raw_fd(),
            Inner::Tls(s) => s.sock.as_raw_fd(),
        }
    }

    /// Send a length-prefixed frame: [u32 LE payload_length][payload].
    pub fn send_framed(&mut self, data: &[u8]) -> Result<(), ProtocolError> {
        self.send_framed_iov(&[data])
    }

    /// Send multiple buffers as a single logical frame (one length prefix
    /// over the concatenation).
    pub fn send_framed_iov(&mut self, bufs: &[&[u8]]) -> Result<(), ProtocolError> {
        match &mut self.0 {
            Inner::Unix(fd) => send_framed_iov(fd.as_raw_fd(), bufs),
            Inner::Tls(s) => tls::send_framed_iov(s, bufs),
        }
    }

    /// Send many length-prefixed frames back-to-back. Each frame may consist
    /// of up to [`FRAME_SEGMENTS`] byte segments (one length prefix over their
    /// concatenation), so callers can ship pre-split message blocks without
    /// flattening.
    pub fn send_framed_batch<F: FrameSegments>(&mut self, frames: &[F]) -> Result<(), ProtocolError> {
        match &mut self.0 {
            Inner::Unix(fd) => send_framed_batch(fd.as_raw_fd(), frames),
            Inner::Tls(s) => tls::send_framed_batch(s, frames),
        }
    }

    /// Receive one length-prefixed frame, enforcing `max_payload_len` and
    /// rejecting the zero-length close sentinel.
    pub fn recv_framed(&mut self, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError> {
        match &mut self.0 {
            Inner::Unix(fd) => recv_framed(fd.as_raw_fd(), max_payload_len),
            Inner::Tls(s) => tls::recv_framed(s, max_payload_len),
        }
    }

    /// The connection is validated (HELLO ACK in hand): drop any
    /// connect/handshake deadline so established connections block forever
    /// on reads — the kernel `recv` is the wait, on every transport.
    fn mark_established(&mut self) -> Result<(), ProtocolError> {
        match &self.0 {
            Inner::Unix(_) => Ok(()),
            Inner::Tls(s) => s.sock.set_read_timeout(None).map_err(ProtocolError::IoError),
        }
    }

    /// Handle that unblocks a `recv_framed` parked in another thread.
    pub fn waker(&self) -> Result<TransportWaker, ProtocolError> {
        // SAFETY: dup of a valid fd we own.
        let raw = unsafe { libc::dup(self.as_raw_fd()) };
        if raw < 0 {
            return Err(ProtocolError::IoError(std::io::Error::last_os_error()));
        }
        Ok(TransportWaker(raw))
    }
}

/// One shape for both transports: a dup'd fd of the underlying stream
/// socket (the AF_UNIX socket, or the TcpStream under `StreamOwned`).
/// Dropping the waker is the wake: `shutdown(SHUT_RDWR)` unblocks a
/// kernel-parked read even after the I/O thread has closed its own fd (the
/// integer may already be recycled; the dup keeps the open file description
/// alive), then the dup itself is closed.
pub struct TransportWaker(RawFd);

impl Drop for TransportWaker {
    fn drop(&mut self) {
        // SAFETY: shutdown + close on a kernel-managed dup'd fd we own.
        unsafe {
            libc::shutdown(self.0, libc::SHUT_RDWR);
            libc::close(self.0);
        }
    }
}

/// Read exactly `buf.len()` bytes into possibly-uninitialised storage,
/// retrying on EINTR and handling partial reads. Writing through
/// `*mut c_void` does not require the destination to be initialised; the
/// caller is responsible for declaring the bytes initialised (e.g. via
/// `Vec::set_len`) once this returns successfully.
fn recv_exact_uninit(sock_fd: RawFd, buf: &mut [MaybeUninit<u8>]) -> Result<(), ProtocolError> {
    let mut got = 0usize;
    while got < buf.len() {
        // SAFETY: pointer/len come from a valid &mut [MaybeUninit<u8>] sub-slice.
        let n = unsafe {
            libc::recv(
                sock_fd,
                buf[got..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - got,
                0,
            )
        };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ProtocolError::IoError(e));
        }
        if n == 0 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }
        got += n as usize;
    }
    Ok(())
}

fn recv_exact(sock_fd: RawFd, buf: &mut [u8]) -> Result<(), ProtocolError> {
    // SAFETY: `&mut [u8]` and `&mut [MaybeUninit<u8>]` have identical
    // layout; the bytes are already initialised, so re-narrowing them to
    // `MaybeUninit` and then back is sound.
    let uninit = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len()) };
    recv_exact_uninit(sock_fd, uninit)
}

/// Maximum number of byte segments one `send_framed_batch` frame may carry:
/// control + schema + data blocks.
pub const FRAME_SEGMENTS: usize = 3;

/// A frame for [`ClientTransport::send_framed_batch`]: up to
/// [`FRAME_SEGMENTS`] byte segments sent under ONE length prefix (computed
/// over their concatenation). Zero-length segments are skipped.
pub trait FrameSegments {
    fn segments(&self) -> [&[u8]; FRAME_SEGMENTS];
}

impl FrameSegments for Vec<u8> {
    fn segments(&self) -> [&[u8]; FRAME_SEGMENTS] {
        [self, &[], &[]]
    }
}

impl FrameSegments for &[u8] {
    fn segments(&self) -> [&[u8]; FRAME_SEGMENTS] {
        [self, &[], &[]]
    }
}

impl<T: FrameSegments> FrameSegments for &T {
    fn segments(&self) -> [&[u8]; FRAME_SEGMENTS] {
        (**self).segments()
    }
}

/// Maximum number of payload slices `send_framed_iov` accepts. The +1
/// in the `iovecs` array accounts for the length-prefix slot at index 0.
/// All call sites (control + schema + data) use at most 3 payload
/// slices, so 7 is comfortably above the worst case while staying small
/// enough for stack allocation.
const MAX_PAYLOAD_SLICES: usize = 7;

/// POSIX-mandated minimum is 16; Linux's UIO_MAXIOV is 1024. We fall back
/// to 1024 when sysconf reports "no definite limit" (-1) and refuse to
/// drop below 2 (every framed message uses ≥ one header + one payload).
const IOV_MAX_FALLBACK: usize = 1024;

fn iov_max() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        // SAFETY: sysconf is async-signal-safe and thread-safe.
        let raw = unsafe { libc::sysconf(libc::_SC_IOV_MAX) };
        let max = if raw < 2 { IOV_MAX_FALLBACK } else { raw as usize };
        // `writev` takes `iovcnt` as `c_int`. Clamp so chunks built from
        // this value can never overflow the cast at the call site.
        max.min(i32::MAX as usize)
    })
}

/// Drains `iovecs` via repeated `writev`. Handles partial writes and
/// `IOV_MAX` chunking via a single byte/index cursor. Mutates `iovecs`
/// in place: each call may shrink the iovec at the cursor; do not re-use
/// the slice for another writev.
fn writev_all(sock_fd: RawFd, iovecs: &mut [libc::iovec]) -> Result<(), ProtocolError> {
    let iov_max = iov_max();
    let total = iovecs.len();
    let mut off = 0usize;

    while off < total {
        // Skip zero-length iovecs at the cursor. A chunk whose lengths all
        // sum to zero makes writev return 0, which we'd misread as a closed
        // peer — empty payload slices (e.g. send_framed_iov(&[&[], data]))
        // and trailing empties after the cursor advances both hit this.
        while off < total && iovecs[off].iov_len == 0 {
            off += 1;
        }
        if off >= total {
            break;
        }

        // saturating_add guards 32-bit targets: iov_max is clamped to
        // i32::MAX (~2 GiB), and a `total` near usize::MAX would otherwise
        // wrap. On 64-bit the bound is unreachable, but the extra cost
        // (one CMOV) is irrelevant outside the tight inner loop.
        let end = off.saturating_add(iov_max).min(total);
        let chunk = &iovecs[off..end];

        // SAFETY: every iov_base references caller-owned memory that
        // outlives the call; iov_len matches the backing allocation.
        let n = unsafe { libc::writev(sock_fd, chunk.as_ptr(), chunk.len() as libc::c_int) };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(ProtocolError::IoError(e));
        }
        if n == 0 {
            return Err(ProtocolError::IoError(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "writev returned 0",
            )));
        }

        let mut remaining = n as usize;
        while remaining > 0 && off < total {
            let cur = iovecs[off].iov_len;
            if remaining >= cur {
                remaining -= cur;
                off += 1;
            } else {
                // SAFETY: iov_base + remaining stays within the
                // caller-provided buffer (remaining < cur ≤ iov_len).
                iovecs[off].iov_base =
                    unsafe { (iovecs[off].iov_base as *const u8).add(remaining) as *mut libc::c_void };
                iovecs[off].iov_len -= remaining;
                remaining = 0;
            }
        }
    }
    Ok(())
}

/// Decode and validate a received 4-byte LE length prefix against the
/// per-connection ceiling, rejecting the zero-length close sentinel.
/// Single enforcement point shared by every framed-recv path — the recv
/// mirror of `frame_len_prefix`.
pub(crate) fn parse_frame_len(hdr: [u8; 4], max_payload_len: usize) -> Result<usize, ProtocolError> {
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

/// Encode the 4-byte LE length prefix for a frame, rejecting the two
/// lengths that would corrupt the wire stream: zero (collides with the
/// `recv_framed` close sentinel) and anything above `u32::MAX` (would
/// silently truncate the prefix). Single enforcement point shared by every
/// framed-send path.
pub(crate) fn frame_len_prefix(len: usize) -> Result<[u8; 4], ProtocolError> {
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

/// Send multiple buffers as a single logical message via writev. The
/// length prefix is computed from the sum of `bufs` lengths and sits in
/// `iovecs[0]`; each payload slice maps to `iovecs[i+1]`.
///
/// Uses a fixed `[libc::iovec; 8]` stack array — no `Vec` allocation on
/// the send path; delegates the syscall loop to `writev_all`.
///
/// Rejects empty frames (total length 0) so this path can never emit the
/// 4-byte `[0,0,0,0]` header that `recv_framed` treats as the close
/// sentinel.
pub(crate) fn send_framed_iov(sock_fd: RawFd, bufs: &[&[u8]]) -> Result<(), ProtocolError> {
    assert!(
        bufs.len() <= MAX_PAYLOAD_SLICES,
        "send_framed_iov: {} payload slices exceeds the {}-slot stack iovec array",
        bufs.len(),
        MAX_PAYLOAD_SLICES,
    );

    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let hdr = frame_len_prefix(total)?;

    // Slot 0: length prefix.  Slots 1..=bufs.len(): payload slices.
    let mut iovecs: [libc::iovec; MAX_PAYLOAD_SLICES + 1] = [libc::iovec {
        iov_base: std::ptr::null_mut(),
        iov_len: 0,
    }; MAX_PAYLOAD_SLICES + 1];
    iovecs[0] = libc::iovec {
        iov_base: hdr.as_ptr() as *mut libc::c_void,
        iov_len: hdr.len(),
    };
    for (i, b) in bufs.iter().enumerate() {
        iovecs[1 + i] = libc::iovec {
            iov_base: b.as_ptr() as *mut libc::c_void,
            iov_len: b.len(),
        };
    }
    let iovcnt = 1 + bufs.len();
    writev_all(sock_fd, &mut iovecs[..iovcnt])
}

thread_local! {
    /// Per-thread scratch reused across `send_framed_batch` calls: the
    /// length-prefix bytes and the iovec sequence. Combined into one cell
    /// so the body takes a single borrow instead of two nested ones.
    static SCRATCH: RefCell<(Vec<u8>, Vec<libc::iovec>)> =
        const { RefCell::new((Vec::new(), Vec::new())) };
}

/// Upper bound on retained per-thread scratch. Comfortably above the
/// IO_BATCH_MAX=1024 working set so steady-state callers never reallocate,
/// while still bounding the worst-case footprint after a one-off large
/// batch. `lens` holds 4 bytes per frame (4 KiB at the cap); `iovs` holds
/// `libc::iovec` — 16 bytes on 64-bit Unix (64 KiB at the cap).
const SCRATCH_LENS_CAP: usize = 4 * 1024; // 1024 frames × 4 bytes
const SCRATCH_IOVS_CAP: usize = (1 + FRAME_SEGMENTS) * 1024; // 1024 frames × (prefix + segments)

/// Reclaim scratch capacity that overshot `cap` after a one-off large
/// batch, leaving steady-state buffers (within the working set) untouched
/// so repeat callers never reallocate.
fn shrink_to_cap<T>(v: &mut Vec<T>, cap: usize) {
    if v.capacity() > cap {
        v.clear();
        v.shrink_to(cap);
    }
}

/// Send many length-prefixed frames as a single writev sequence.
/// Each frame contributes one 4-byte LE length iovec plus one iovec per
/// non-empty segment (the prefix covers the segments' concatenation).
///
/// Empty frames are rejected (would collide with the protocol close
/// sentinel). Frames whose length does not fit in `u32` are rejected
/// (would silently truncate the wire prefix).
pub(crate) fn send_framed_batch<F: FrameSegments>(sock_fd: RawFd, frames: &[F]) -> Result<(), ProtocolError> {
    if frames.is_empty() {
        return Ok(());
    }

    SCRATCH.with(|cell| {
        let mut guard = cell.borrow_mut();
        let (lens, iovs) = &mut *guard;
        lens.clear();
        iovs.clear();
        iovs.reserve((1 + FRAME_SEGMENTS) * frames.len());

        // Pass 1: validate and fill the length-prefix buffer. After this
        // resize+copy_from_slice loop returns, `lens` is no longer mutated,
        // so the raw pointer captured in pass 2 remains valid under both
        // Stacked and Tree Borrows.
        lens.resize(4 * frames.len(), 0u8);
        for (i, f) in frames.iter().enumerate() {
            let total: usize = f.segments().iter().map(|s| s.len()).sum();
            let prefix = frame_len_prefix(total)?;
            let off = i * 4;
            lens[off..off + 4].copy_from_slice(&prefix);
        }

        // Pass 2: build the iovec sequence. `lens.as_ptr()` is taken
        // exactly once; the loop reads through it without ever re-borrowing
        // `lens` mutably.
        let lens_ptr = lens.as_ptr();
        for (i, f) in frames.iter().enumerate() {
            iovs.push(libc::iovec {
                // SAFETY: i*4 + 4 ≤ lens.len() per the resize above; `lens`
                // outlives writev_all because `guard` is held across it.
                iov_base: unsafe { lens_ptr.add(i * 4) } as *mut libc::c_void,
                iov_len: 4,
            });
            for seg in f.segments() {
                if !seg.is_empty() {
                    iovs.push(libc::iovec {
                        iov_base: seg.as_ptr() as *mut libc::c_void,
                        iov_len: seg.len(),
                    });
                }
            }
        }

        let res = writev_all(sock_fd, &mut iovs[..]);

        // Bound worst-case retention after a one-off huge batch; steady-state
        // callers within the working set keep their capacity.
        shrink_to_cap(lens, SCRATCH_LENS_CAP);
        shrink_to_cap(iovs, SCRATCH_IOVS_CAP);

        res
    })
}

/// Receive a length-prefixed frame, enforcing `max_payload_len` as the
/// per-connection ceiling. Callers that have completed the HELLO
/// handshake pass the negotiated value (e.g. via
/// `Connection::recv_framed`); pre-handshake or unbounded callers may
/// pass `gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT` for the historical
/// default.
pub(crate) fn recv_framed(sock_fd: RawFd, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut hdr = [0u8; 4];
    recv_exact(sock_fd, &mut hdr)?;
    let payload_len = parse_frame_len(hdr, max_payload_len)?;
    let mut buf: Vec<u8> = Vec::with_capacity(payload_len);
    {
        let spare = buf.spare_capacity_mut();
        recv_exact_uninit(sock_fd, &mut spare[..payload_len])?;
    }
    // SAFETY: recv_exact_uninit wrote exactly `payload_len` bytes into
    // the spare capacity. Declaring those bytes initialised is sound.
    unsafe {
        buf.set_len(payload_len);
    }
    Ok(buf)
}

/// Send the HELLO frame and parse the server's ACK. Returns the
/// server-negotiated per-connection frame payload limit in bytes and the
/// server's durability watermark at connect (`published_lsn`), which seeds the
/// client's OCC basis.
///
/// On version mismatch / auth failure the server replies with a
/// length-prefixed STATUS_ERROR control block (≥ 248 bytes) and closes
/// the connection; this function detects that path via the payload
/// length (`!= HELLO_ACK_PAYLOAD_LEN`) and surfaces the embedded error
/// string.
pub fn hello_handshake(t: &mut ClientTransport) -> Result<(u32, u64), ProtocolError> {
    let payload = gnitz_wire::encode_hello_payload(
        gnitz_wire::WAL_FORMAT_VERSION as u16,
        0, // auth method = none
    );
    t.send_framed(&payload)?;

    // ACK ⇒ 12 bytes, STATUS_ERROR control block ⇒ ≥ 248.
    let buf = t.recv_framed(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT)?;
    if buf.len() == gnitz_wire::HELLO_ACK_PAYLOAD_LEN as usize {
        let ack = gnitz_wire::decode_hello_ack(&buf).map_err(|e| ProtocolError::DecodeError(e.into()))?;
        if ack.magic != gnitz_wire::HELLO_MAGIC {
            return Err(ProtocolError::DecodeError("HELLO ACK magic mismatch".into()));
        }
        if ack.status != gnitz_wire::HELLO_STATUS_OK {
            return Err(ProtocolError::DecodeError(format!(
                "HELLO ACK reported status={}",
                ack.status,
            )));
        }
        t.mark_established()?;
        // Clamp the server-supplied limit to the client's hard ceiling. The
        // negotiated bound is `min(server_limit, MAX_FRAME_PAYLOAD_CLIENT)`,
        // so a compromised or buggy server cannot raise the client's own
        // allocation bound above its hard maximum (honoring the doc-comment
        // on `Connection::max_payload_len`).
        return Ok((
            ack.limit_bytes.min(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT as u32),
            ack.published_lsn,
        ));
    }

    // Not an ACK — the server sent a STATUS_ERROR control block. Surface
    // the embedded error.
    let msg = super::message::parse_response(&buf, None)?;
    let err = msg.error_text.unwrap_or_else(|| "HELLO rejected".into());
    Err(ProtocolError::DecodeError(err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::io::RawFd;

    fn make_socketpair() -> (RawFd, RawFd) {
        let mut fds = [0i32; 2];
        unsafe {
            libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr());
        }
        (fds[0], fds[1])
    }

    const TEST_LIMIT: usize = gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT;

    #[test]
    fn test_transport_loopback() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_transport_medium() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_iov_single_buf() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_iov_multi_buf() {
        let (a, b) = make_socketpair();
        let part1 = b"hello ";
        let part2 = b"world";
        let part3: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
        send_framed_iov(a, &[&part1[..], &part2[..], &part3]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        let mut expected = Vec::new();
        expected.extend_from_slice(part1);
        expected.extend_from_slice(part2);
        expected.extend_from_slice(&part3);
        assert_eq!(received, expected);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_iov_empty_bufs_skipped() {
        let (a, b) = make_socketpair();
        let data = b"payload";
        send_framed_iov(a, &[&[], &data[..], &[]]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_recv_framed_payload_too_large() {
        let (a, b) = make_socketpair();
        let huge: u32 = (TEST_LIMIT + 1) as u32;
        let hdr = huge.to_le_bytes();
        unsafe {
            libc::send(a, hdr.as_ptr() as *const libc::c_void, 4, 0);
        }
        let result = recv_framed(b, TEST_LIMIT);
        assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_recv_framed_enforces_caller_limit() {
        // Caller-bound limit is tighter than the global default — recv must
        // honour the smaller ceiling.
        let (a, b) = make_socketpair();
        let small_limit = 1024usize;
        let just_over: u32 = (small_limit + 1) as u32;
        let hdr = just_over.to_le_bytes();
        unsafe {
            libc::send(a, hdr.as_ptr() as *const libc::c_void, 4, 0);
        }
        let result = recv_framed(b, small_limit);
        assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_writev_interop_with_send_framed() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = vec![42u8; 8192];
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(received, data);
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_iov_max_slices_assertion() {
        // Eight payload slices exceeds MAX_PAYLOAD_SLICES (7); must panic.
        let (a, b) = make_socketpair();
        let r = std::panic::catch_unwind(|| {
            let bufs = [&[1u8][..]; MAX_PAYLOAD_SLICES + 1];
            let _ = send_framed_iov(a, &bufs);
        });
        assert!(r.is_err(), "send_framed_iov must assert on slice overflow");
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    /// Spawn a reader thread that pulls exactly `count` frames off `fd`,
    /// closing it on completion. Returns the join handle.
    fn spawn_reader(fd: RawFd, count: usize) -> std::thread::JoinHandle<Vec<Vec<u8>>> {
        std::thread::spawn(move || {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(recv_framed(fd, TEST_LIMIT).unwrap());
            }
            unsafe {
                libc::close(fd);
            }
            out
        })
    }

    #[test]
    fn test_send_framed_iov_empty_returns_invalid_input() {
        // No slices, and all-empty slices, both sum to 0 → rejected so the
        // close sentinel can never be emitted by this path.
        let (a, b) = make_socketpair();
        let r0 = send_framed_iov(a, &[]);
        let r1 = send_framed_iov(a, &[&[][..], &[][..]]);
        assert!(matches!(r0, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
        assert!(matches!(r1, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_empty_returns_invalid_input() {
        // Inherited via send_framed_iov rejection: send_framed(fd, &[]) must
        // not silently close the peer.
        let (a, b) = make_socketpair();
        let r = send_framed_iov(a, &[&[][..]]);
        assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
        unsafe {
            libc::close(a);
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_batch_empty_input_ok_no_syscall() {
        // Empty input returns Ok before touching the fd. An invalid fd
        // proves no syscall is made (a writev on -1 would yield EBADF).
        assert!(send_framed_batch::<&[u8]>(-1, &[]).is_ok());
    }

    #[test]
    fn test_send_framed_batch_rejects_empty_frame_no_writev() {
        // An empty frame mid-batch must be rejected before any writev — an
        // invalid fd proves no syscall fired (else EBADF, not InvalidInput).
        let frames: [&[u8]; 3] = [b"a", b"", b"c"];
        let r = send_framed_batch(-1, &frames);
        assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
    }

    #[test]
    #[cfg(target_pointer_width = "64")]
    fn test_send_framed_batch_rejects_oversized_frame() {
        // A frame whose length exceeds u32::MAX would truncate the wire
        // prefix. Fabricate a fat pointer with an oversized length; the
        // bytes are never dereferenced (only len/is_empty are read before
        // the rejection), so the dangling base is never accessed.
        let oversized: &[u8] = unsafe {
            std::slice::from_raw_parts(std::ptr::NonNull::<u8>::dangling().as_ptr(), (u32::MAX as usize) + 1)
        };
        let frames = [oversized];
        let r = send_framed_batch(-1, &frames);
        assert!(matches!(r, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::InvalidInput));
    }

    #[test]
    fn test_send_framed_batch_single_frame() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        let reader = spawn_reader(b, 1);
        send_framed_batch(a, &[&data]).unwrap();
        unsafe {
            libc::close(a);
        }
        let frames = reader.join().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0], data);
    }

    #[test]
    fn test_send_framed_batch_many_small_frames_in_order() {
        let (a, b) = make_socketpair();
        let n = 200usize;
        let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("frame-{i}").into_bytes()).collect();
        let reader = spawn_reader(b, n);
        send_framed_batch(a, &expected).unwrap();
        unsafe {
            libc::close(a);
        }
        let got = reader.join().unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn test_send_framed_batch_forces_multiple_writev() {
        // > iov_max()/2 frames ⇒ > iov_max iovecs ⇒ writev_all chunks into
        // at least two writev calls. Receiver must still see every frame
        // intact and in order.
        let (a, b) = make_socketpair();
        let n = iov_max() / 2 + 50;
        let expected: Vec<Vec<u8>> = (0..n).map(|i| format!("f{i:04}").into_bytes()).collect();
        let reader = spawn_reader(b, n);
        send_framed_batch(a, &expected).unwrap();
        unsafe {
            libc::close(a);
        }
        let got = reader.join().unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn test_hello_handshake_clamps_server_limit() {
        // A server advertising an oversized limit (u32::MAX) must not raise the
        // client's negotiated ceiling above MAX_FRAME_PAYLOAD_CLIENT.
        let (a, b) = make_socketpair();
        // Pre-stage the ACK frame on `b` so the handshake on `a` can read it
        // after sending its HELLO (which lands harmlessly in `a`'s recv buffer).
        let ack = gnitz_wire::encode_hello_ack(gnitz_wire::HELLO_STATUS_OK, u32::MAX, 0);
        unsafe {
            libc::send(b, ack.as_ptr() as *const libc::c_void, ack.len(), 0);
        }
        let mut t = ClientTransport::from_unix_fd(a);
        let (limit, _lsn) = hello_handshake(&mut t).unwrap();
        assert_eq!(limit as usize, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT);
        unsafe {
            libc::close(b);
        }
    }

    #[test]
    fn test_hello_handshake_preserves_smaller_limit() {
        // A server limit below the client ceiling passes through unchanged.
        let (a, b) = make_socketpair();
        let small: u32 = 16 * 1024 * 1024;
        let ack = gnitz_wire::encode_hello_ack(gnitz_wire::HELLO_STATUS_OK, small, 7);
        unsafe {
            libc::send(b, ack.as_ptr() as *const libc::c_void, ack.len(), 0);
        }
        let mut t = ClientTransport::from_unix_fd(a);
        let (limit, lsn) = hello_handshake(&mut t).unwrap();
        assert_eq!(limit, small);
        assert_eq!(lsn, 7, "the ACK's published_lsn seeds the client basis");
        unsafe {
            libc::close(b);
        }
    }

    #[test]
    fn test_send_framed_batch_partial_writes_small_sndbuf() {
        // Tiny send buffer relative to the batch forces writev_all to drain
        // across many short writes. Every frame must arrive intact and in
        // order despite the partial-write cursor advancing repeatedly.
        let (a, b) = make_socketpair();
        let small: libc::c_int = 4096;
        unsafe {
            libc::setsockopt(
                a,
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                &small as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
        let n = 500usize;
        // ~256 bytes/frame × 500 ≫ the 4 KiB send buffer.
        let expected: Vec<Vec<u8>> = (0..n)
            .map(|i| {
                let mut v = vec![(i & 0xff) as u8; 256];
                v[0] = (i >> 8) as u8;
                v
            })
            .collect();
        let reader = spawn_reader(b, n);
        send_framed_batch(a, &expected).unwrap();
        unsafe {
            libc::close(a);
        }
        let got = reader.join().unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn test_client_transport_unix_roundtrip() {
        // The enum's Unix arm must be byte-identical to the free functions.
        let (a, b) = make_socketpair();
        let mut t = ClientTransport::from_unix_fd(a);
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        t.send_framed(&data).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(received, data);
        send_framed_iov(b, &[&data]).unwrap();
        let echoed = t.recv_framed(TEST_LIMIT).unwrap();
        assert_eq!(echoed, data);
        unsafe {
            libc::close(b);
        }
    }

    #[test]
    fn test_transport_waker_unblocks_parked_recv() {
        // A recv_framed parked in another thread must return with an error
        // once the waker is dropped (shutdown on the shared description).
        let (a, b) = make_socketpair();
        let t = ClientTransport::from_unix_fd(a);
        let waker = t.waker().unwrap();
        let handle = std::thread::spawn(move || {
            let mut t = t;
            t.recv_framed(TEST_LIMIT)
        });
        std::thread::sleep(std::time::Duration::from_millis(50));
        drop(waker);
        let r = handle.join().unwrap();
        assert!(r.is_err(), "parked recv must be unblocked with an error");
        unsafe {
            libc::close(b);
        }
    }

    #[test]
    fn test_connect_rejects_malformed_tls_targets() {
        // The tls:// prefix is the sole discriminator; these must not be
        // treated as socket paths, and must fail with a parse error.
        for bad in [
            "tls://",                  // no host
            "tls://127.0.0.1",         // no port
            "tls://h:1?insecure&ca=x", // unknown/multiple params
            "tls://h:1?bogus",         // unknown param
            "tls://h:1?insecure?ca=x", // second param
            "tls://[::1:443?insecure", // unterminated bracket
            "tls://h:notaport",        // bad port
        ] {
            let err = ClientTransport::connect(bad).err();
            assert!(
                matches!(err, Some(ProtocolError::DecodeError(_))),
                "{bad} must be rejected with a parse error, got {err:?}",
            );
        }
    }
}
