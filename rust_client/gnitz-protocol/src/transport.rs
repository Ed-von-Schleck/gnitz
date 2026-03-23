use std::mem::{size_of, zeroed};
use std::os::unix::io::RawFd;
use crate::error::ProtocolError;

fn io_err() -> ProtocolError {
    ProtocolError::IoError(std::io::Error::last_os_error())
}

/// Read exactly `buf.len()` bytes, retrying on EINTR and handling partial reads.
fn recv_exact(sock_fd: RawFd, buf: &mut [u8]) -> Result<(), ProtocolError> {
    let mut got = 0usize;
    while got < buf.len() {
        // SAFETY: pointer/len come from a valid &mut [u8] sub-slice.
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

pub fn connect(socket_path: &str) -> Result<RawFd, ProtocolError> {
    // SAFETY: All libc calls operate on kernel-managed resources; addr is zeroed
    // then partially filled with the path (length-checked against sun_path capacity).
    unsafe {
        let sock_fd = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0);
        if sock_fd < 0 {
            return Err(io_err());
        }

        let mut addr: libc::sockaddr_un = zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;

        let path_bytes = socket_path.as_bytes();
        if path_bytes.len() >= addr.sun_path.len() {
            libc::close(sock_fd);
            return Err(ProtocolError::DecodeError("socket path too long".into()));
        }
        std::ptr::copy_nonoverlapping(
            path_bytes.as_ptr() as *const libc::c_char,
            addr.sun_path.as_mut_ptr(),
            path_bytes.len(),
        );

        let addr_len = (size_of::<libc::sa_family_t>() + path_bytes.len() + 1) as libc::socklen_t;
        let ret = libc::connect(
            sock_fd,
            &addr as *const libc::sockaddr_un as *const libc::sockaddr,
            addr_len,
        );
        if ret < 0 {
            libc::close(sock_fd);
            return Err(io_err());
        }

        Ok(sock_fd)
    }
}

/// Send a length-prefixed frame: [u32 LE payload_length][payload bytes].
/// Uses writev to send the header and payload in a single syscall.
pub fn send_framed(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    let hdr = (data.len() as u32).to_le_bytes();
    send_all_iov(sock_fd, &[&hdr, data])
}

/// Send multiple buffers as a single logical message via writev, retrying on
/// EINTR and handling partial writes.
pub fn send_framed_iov(sock_fd: RawFd, bufs: &[&[u8]]) -> Result<(), ProtocolError> {
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let hdr = (total as u32).to_le_bytes();
    let mut slices: Vec<&[u8]> = Vec::with_capacity(bufs.len() + 1);
    slices.push(&hdr);
    slices.extend_from_slice(bufs);
    send_all_iov(sock_fd, &slices)
}

/// Low-level scatter-gather send. Drains all bytes from `slices` via writev,
/// handling partial writes and EINTR.
fn send_all_iov(sock_fd: RawFd, slices: &[&[u8]]) -> Result<(), ProtocolError> {
    let total: usize = slices.iter().map(|s| s.len()).sum();
    if total == 0 { return Ok(()); }

    // Build mutable iovec array; we advance through it as bytes are sent.
    let mut iovecs: Vec<libc::iovec> = slices.iter().map(|s| libc::iovec {
        iov_base: s.as_ptr() as *mut libc::c_void,
        iov_len: s.len(),
    }).collect();

    let mut sent = 0usize;
    let mut iov_offset = 0usize; // first iovec that still has unsent bytes

    while sent < total {
        let iov_slice = &iovecs[iov_offset..];
        // SAFETY: iovecs point into valid &[u8] slices that outlive this call.
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

        // Advance past fully-sent iovecs and adjust the partial one.
        let mut remaining = n as usize;
        while remaining > 0 && iov_offset < iovecs.len() {
            let cur = iovecs[iov_offset].iov_len;
            if remaining >= cur {
                remaining -= cur;
                iov_offset += 1;
            } else {
                // SAFETY: iov_base points into a valid &[u8] slice; advancing by
                // `remaining` bytes keeps us within the original allocation.
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

/// Receive a length-prefixed frame. Returns the payload bytes.
pub fn recv_framed(sock_fd: RawFd) -> Result<Vec<u8>, ProtocolError> {
    let mut hdr = [0u8; 4];
    recv_exact(sock_fd, &mut hdr)?;
    let payload_len = u32::from_le_bytes(hdr) as usize;
    if payload_len == 0 {
        return Err(ProtocolError::IoError(
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "zero-length close sentinel"),
        ));
    }
    // Allocate without zeroing — recv_exact will overwrite all bytes.
    let mut buf = Vec::with_capacity(payload_len);
    // SAFETY: recv_exact reads exactly payload_len bytes into buf[0..payload_len].
    // The capacity is sufficient and all bytes will be initialized by recv_exact.
    unsafe { buf.set_len(payload_len); }
    recv_exact(sock_fd, &mut buf)?;
    Ok(buf)
}

pub fn close_fd(fd: RawFd) {
    unsafe { libc::close(fd); }
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

    #[test]
    fn test_transport_loopback() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed(a, &data).unwrap();
        let received = recv_framed(b).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_transport_medium() {
        let (a, b) = make_socketpair();
        // 64 KB payload — fits in Unix socket buffer for single-threaded test
        let data: Vec<u8> = (0u8..=255).cycle().take(64 * 1024).collect();
        send_framed(a, &data).unwrap();
        let received = recv_framed(b).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_send_framed_iov_single_buf() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_framed_iov(a, &[&data]).unwrap();
        let received = recv_framed(b).unwrap();
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
        let received = recv_framed(b).unwrap();
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
        let received = recv_framed(b).unwrap();
        assert_eq!(&received[..], &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_writev_interop_with_send_framed() {
        // send_framed (which now uses writev internally) should be readable
        // by recv_framed, and vice versa — verify backward compat.
        let (a, b) = make_socketpair();
        let data: Vec<u8> = vec![42u8; 8192];
        send_framed(a, &data).unwrap();
        let received = recv_framed(b).unwrap();
        assert_eq!(received, data);
        unsafe { libc::close(a); libc::close(b); }
    }
}
