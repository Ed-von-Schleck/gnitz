use std::mem::{size_of, zeroed};
use std::os::unix::io::RawFd;
use crate::error::ProtocolError;

fn io_err() -> ProtocolError {
    ProtocolError::IoError(std::io::Error::last_os_error())
}

/// Send all bytes, retrying on EINTR and handling partial writes.
fn send_all(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    let mut sent = 0usize;
    while sent < data.len() {
        // SAFETY: sock_fd validity is the caller's invariant; pointer/len come from
        // a valid &[u8] sub-slice so they are always in-bounds and aligned.
        let n = unsafe {
            libc::send(
                sock_fd,
                data[sent..].as_ptr() as *const libc::c_void,
                data.len() - sent,
                libc::MSG_NOSIGNAL,
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

/// Read exactly `buf.len()` bytes, retrying on EINTR and handling partial reads.
fn recv_exact(sock_fd: RawFd, buf: &mut [u8]) -> Result<(), ProtocolError> {
    let mut got = 0usize;
    while got < buf.len() {
        // SAFETY: same as send_all — pointer/len from a valid &mut [u8] sub-slice.
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
/// Two send_all calls avoid allocating a combined buffer for large payloads.
pub fn send_framed(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    let hdr = (data.len() as u32).to_le_bytes();
    send_all(sock_fd, &hdr)?;
    send_all(sock_fd, data)
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
    let mut buf = vec![0u8; payload_len];
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
}
