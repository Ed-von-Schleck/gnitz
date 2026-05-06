//! AF-agnostic framing primitives.
//!
//! The 4-byte LE length prefix used by every framed message is built and
//! parsed here. Per-transport connect logic lives in sibling modules
//! (`unix.rs` for AF_UNIX, future `tcp.rs` for TCP). QUIC streams cannot
//! be passed to `writev` via `RawFd`, so QUIC support requires a separate
//! `AsyncRead`/`AsyncWrite`-based path that is out of scope here.

use std::mem::MaybeUninit;
use std::os::unix::io::RawFd;

use super::error::ProtocolError;

pub mod unix;
pub use unix::connect_unix;

/// Backwards-compatible alias for AF_UNIX clients written before the
/// transport split.
pub fn connect(socket_path: &str) -> Result<RawFd, ProtocolError> {
    unix::connect_unix(socket_path)
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
            if e.kind() == std::io::ErrorKind::Interrupted { continue; }
            return Err(ProtocolError::IoError(e));
        }
        if n == 0 {
            return Err(ProtocolError::IoError(
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "connection closed"),
            ));
        }
        got += n as usize;
    }
    Ok(())
}

fn recv_exact(sock_fd: RawFd, buf: &mut [u8]) -> Result<(), ProtocolError> {
    // SAFETY: `&mut [u8]` and `&mut [MaybeUninit<u8>]` have identical
    // layout; the bytes are already initialised, so re-narrowing them to
    // `MaybeUninit` and then back is sound.
    let uninit = unsafe {
        std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut MaybeUninit<u8>, buf.len())
    };
    recv_exact_uninit(sock_fd, uninit)
}

/// Send a length-prefixed frame: [u32 LE payload_length][payload bytes].
/// Uses writev to send the header and payload in a single syscall.
pub fn send_framed(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    send_framed_iov(sock_fd, &[data])
}

/// Drain `data` via repeated `send` calls, retrying on EINTR and handling
/// partial writes. Used by paths that ship pre-framed bytes (e.g. HELLO),
/// where re-deriving a length prefix via `send_framed` would duplicate
/// work already done by the encoder.
fn send_all_bytes(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    let mut sent = 0usize;
    while sent < data.len() {
        // SAFETY: pointer/len come from a valid &[u8] sub-slice.
        let n = unsafe {
            libc::send(
                sock_fd,
                data[sent..].as_ptr() as *const libc::c_void,
                data.len() - sent,
                0,
            )
        };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted { continue; }
            return Err(ProtocolError::IoError(e));
        }
        if n == 0 {
            return Err(ProtocolError::IoError(
                std::io::Error::new(std::io::ErrorKind::WriteZero, "send returned 0"),
            ));
        }
        sent += n as usize;
    }
    Ok(())
}

/// Maximum number of payload slices `send_framed_iov` accepts. The +1
/// in the `iovecs` array accounts for the length-prefix slot at index 0.
/// All call sites (control + schema + data) use at most 3 payload
/// slices, so 7 is comfortably above the worst case while staying small
/// enough for stack allocation.
const MAX_PAYLOAD_SLICES: usize = 7;

/// Send multiple buffers as a single logical message via writev. The
/// length prefix is computed from the sum of `bufs` lengths and sits in
/// `iovecs[0]`; each payload slice maps to `iovecs[i+1]`.
///
/// Uses a fixed `[libc::iovec; 8]` stack array — no `Vec` allocation on
/// the send path. Partial writes advance an `iov_offset` cursor; the
/// kernel never re-reads consumed iovecs because we resubmit
/// `iovecs[iov_offset..]`.
pub fn send_framed_iov(sock_fd: RawFd, bufs: &[&[u8]]) -> Result<(), ProtocolError> {
    assert!(
        bufs.len() <= MAX_PAYLOAD_SLICES,
        "send_framed_iov: {} payload slices exceeds the {}-slot stack iovec array",
        bufs.len(), MAX_PAYLOAD_SLICES,
    );

    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let hdr = (total as u32).to_le_bytes();

    // Slot 0: length prefix.  Slots 1..=bufs.len(): payload slices.
    let mut iovecs: [libc::iovec; MAX_PAYLOAD_SLICES + 1] = [
        libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 };
        MAX_PAYLOAD_SLICES + 1
    ];
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
    let total_with_hdr = total + hdr.len();
    if total_with_hdr == 0 { return Ok(()); }

    let mut sent = 0usize;
    let mut iov_offset = 0usize;

    while sent < total_with_hdr {
        let iov_slice = &iovecs[iov_offset..iovcnt];
        // SAFETY: every `iov_base` references either the on-stack `hdr`
        // array or a slice held by the caller; both outlive this call.
        let n = unsafe {
            libc::writev(sock_fd, iov_slice.as_ptr(), iov_slice.len() as libc::c_int)
        };
        if n < 0 {
            let e = std::io::Error::last_os_error();
            if e.kind() == std::io::ErrorKind::Interrupted { continue; }
            return Err(ProtocolError::IoError(e));
        }
        if n == 0 {
            return Err(ProtocolError::IoError(
                std::io::Error::new(std::io::ErrorKind::WriteZero, "writev returned 0"),
            ));
        }
        sent += n as usize;

        // Advance past fully-sent iovecs and adjust the partial one in
        // place. Keeping `iov_offset` as a cursor (rather than rewriting
        // `iovecs[0..]`) avoids the kernel rereading consumed entries
        // on resubmit.
        let mut remaining = n as usize;
        while remaining > 0 && iov_offset < iovcnt {
            let cur = iovecs[iov_offset].iov_len;
            if remaining >= cur {
                remaining -= cur;
                iov_offset += 1;
            } else {
                // SAFETY: iov_base points into a valid &[u8] slice;
                // advancing by `remaining` bytes keeps us within the
                // original allocation.
                iovecs[iov_offset].iov_base = unsafe {
                    (iovecs[iov_offset].iov_base as *const u8).add(remaining) as *mut libc::c_void
                };
                iovecs[iov_offset].iov_len -= remaining;
                remaining = 0;
            }
        }
    }
    Ok(())
}

/// Receive a length-prefixed frame, enforcing `max_payload_len` as the
/// per-connection ceiling. Callers that have completed the HELLO
/// handshake pass the negotiated value (e.g. via
/// `Connection::recv_framed`); pre-handshake or unbounded callers may
/// pass `gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT` for the historical
/// default.
pub fn recv_framed(sock_fd: RawFd, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut hdr = [0u8; 4];
    recv_exact(sock_fd, &mut hdr)?;
    let payload_len = u32::from_le_bytes(hdr) as usize;
    if payload_len == 0 {
        return Err(ProtocolError::IoError(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "zero-length close sentinel"),
        ));
    }
    if payload_len > max_payload_len {
        return Err(ProtocolError::DecodeError(format!(
            "payload length {} exceeds maximum {} bytes", payload_len, max_payload_len
        )));
    }
    let mut buf: Vec<u8> = Vec::with_capacity(payload_len);
    {
        let spare = buf.spare_capacity_mut();
        recv_exact_uninit(sock_fd, &mut spare[..payload_len])?;
    }
    // SAFETY: recv_exact_uninit wrote exactly `payload_len` bytes into
    // the spare capacity. Declaring those bytes initialised is sound.
    unsafe { buf.set_len(payload_len); }
    Ok(buf)
}

pub fn close_fd(fd: RawFd) {
    unsafe { libc::close(fd); }
}

/// Send the HELLO frame and parse the server's ACK. Returns the
/// server-negotiated per-connection frame payload limit in bytes.
///
/// On version mismatch / auth failure the server replies with a
/// length-prefixed STATUS_ERROR control block (≥ 248 bytes) and closes
/// the fd; this function detects that path via the length prefix
/// (`!= HELLO_ACK_PAYLOAD_LEN`) and surfaces the embedded error string.
pub fn hello_handshake(sock_fd: RawFd) -> Result<u32, ProtocolError> {
    let frame = gnitz_wire::encode_hello_frame(
        gnitz_wire::WAL_FORMAT_VERSION as u16,
        0, // auth method = none
    );
    send_all_bytes(sock_fd, &frame)?;

    // Inspect the response length prefix. ACK ⇒ 12, control block ⇒ ≥ 248.
    let mut hdr = [0u8; 4];
    recv_exact(sock_fd, &mut hdr)?;
    let payload_len = u32::from_le_bytes(hdr) as usize;
    if payload_len == 0 {
        return Err(ProtocolError::IoError(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "server closed mid-handshake"),
        ));
    }

    if payload_len == gnitz_wire::HELLO_ACK_PAYLOAD_LEN as usize {
        let mut buf = [0u8; gnitz_wire::HELLO_ACK_PAYLOAD_LEN as usize];
        recv_exact(sock_fd, &mut buf)?;
        let ack = gnitz_wire::decode_hello_ack(&buf)
            .map_err(|e| ProtocolError::DecodeError(e.into()))?;
        if ack.magic != gnitz_wire::HELLO_MAGIC {
            return Err(ProtocolError::DecodeError("HELLO ACK magic mismatch".into()));
        }
        if ack.status != gnitz_wire::HELLO_STATUS_OK {
            return Err(ProtocolError::DecodeError(format!(
                "HELLO ACK reported status={}", ack.status,
            )));
        }
        return Ok(ack.limit_bytes);
    }

    // Not an ACK — the server is sending a STATUS_ERROR control block.
    // Read the rest of the frame and surface the embedded error.
    if payload_len > gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT {
        return Err(ProtocolError::DecodeError(format!(
            "HELLO error frame payload {} exceeds client max {}",
            payload_len, gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT,
        )));
    }
    let mut buf: Vec<u8> = Vec::with_capacity(payload_len);
    {
        let spare = buf.spare_capacity_mut();
        recv_exact_uninit(sock_fd, &mut spare[..payload_len])?;
    }
    // SAFETY: recv_exact_uninit wrote exactly `payload_len` bytes.
    unsafe { buf.set_len(payload_len); }
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
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()); }
        (fds[0], fds[1])
    }

    const TEST_LIMIT: usize = gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT;

    #[test]
    fn test_transport_loopback() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed(a, &data).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_transport_medium() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
        send_framed(a, &data).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_send_framed_iov_single_buf() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
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
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_send_framed_iov_empty_bufs_skipped() {
        let (a, b) = make_socketpair();
        let data = b"payload";
        send_framed_iov(a, &[&[], &data[..], &[]]).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_recv_framed_payload_too_large() {
        let (a, b) = make_socketpair();
        let huge: u32 = (TEST_LIMIT + 1) as u32;
        let hdr = huge.to_le_bytes();
        unsafe { libc::send(a, hdr.as_ptr() as *const libc::c_void, 4, 0); }
        let result = recv_framed(b, TEST_LIMIT);
        assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_recv_framed_enforces_caller_limit() {
        // Caller-bound limit is tighter than the global default — recv must
        // honour the smaller ceiling.
        let (a, b) = make_socketpair();
        let small_limit = 1024usize;
        let just_over: u32 = (small_limit + 1) as u32;
        let hdr = just_over.to_le_bytes();
        unsafe { libc::send(a, hdr.as_ptr() as *const libc::c_void, 4, 0); }
        let result = recv_framed(b, small_limit);
        assert!(matches!(result, Err(ProtocolError::DecodeError(ref s)) if s.contains("exceeds maximum")));
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_writev_interop_with_send_framed() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = vec![42u8; 8192];
        send_framed(a, &data).unwrap();
        let received = recv_framed(b, TEST_LIMIT).unwrap();
        assert_eq!(received, data);
        unsafe { libc::close(a); libc::close(b); }
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
        unsafe { libc::close(a); libc::close(b); }
    }
}
