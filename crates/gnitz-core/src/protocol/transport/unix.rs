//! AF_UNIX-specific connect logic. Kept separate from the AF-agnostic
//! framing so a future TCP/QUIC transport can plug in alongside.

use std::mem::{size_of, zeroed};
use std::os::unix::io::RawFd;

use crate::protocol::error::ProtocolError;

fn io_err() -> ProtocolError {
    ProtocolError::IoError(std::io::Error::last_os_error())
}

/// Open a SOCK_STREAM connection to an AF_UNIX endpoint at `socket_path`.
pub fn connect_unix(socket_path: &str) -> Result<RawFd, ProtocolError> {
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
