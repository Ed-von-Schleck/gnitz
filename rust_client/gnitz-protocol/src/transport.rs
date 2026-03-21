use std::ffi::CString;
use std::mem::{size_of, zeroed};
use std::os::unix::io::RawFd;
use crate::error::ProtocolError;

/// Zero-copy wrapper around an mmap'd memfd.
/// Keeps the mapping and fd alive until dropped.
pub struct MmapBuffer {
    ptr: *const u8,
    len: usize,
    fd:  RawFd,
}

impl MmapBuffer {
    /// An empty buffer (no mmap, no fd).
    fn empty() -> Self {
        Self { ptr: std::ptr::null(), len: 0, fd: -1 }
    }

    pub fn as_slice(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for MmapBuffer {
    fn drop(&mut self) {
        unsafe {
            if self.len > 0 {
                libc::munmap(self.ptr as *mut _, self.len);
            }
            if self.fd >= 0 {
                libc::close(self.fd);
            }
        }
    }
}

// SAFETY: MmapBuffer is read-only (PROT_READ) and the fd is private.
unsafe impl Send for MmapBuffer {}

fn io_err() -> ProtocolError {
    ProtocolError::IoError(std::io::Error::last_os_error())
}

pub fn connect(socket_path: &str) -> Result<RawFd, ProtocolError> {
    unsafe {
        let sock_fd = libc::socket(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0);
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

pub fn send_memfd(sock_fd: RawFd, data: &[u8]) -> Result<(), ProtocolError> {
    unsafe {
        let name = CString::new("gnitz_client").unwrap();
        let mem_fd = libc::memfd_create(name.as_ptr(), 0);
        if mem_fd < 0 {
            return Err(io_err());
        }

        if !data.is_empty() {
            if libc::ftruncate(mem_fd, data.len() as libc::off_t) < 0 {
                libc::close(mem_fd);
                return Err(io_err());
            }
            let ptr = libc::mmap(
                std::ptr::null_mut(),
                data.len(),
                libc::PROT_WRITE,
                libc::MAP_SHARED,
                mem_fd,
                0,
            );
            if ptr == libc::MAP_FAILED {
                libc::close(mem_fd);
                return Err(io_err());
            }
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr as *mut u8, data.len());
            libc::munmap(ptr, data.len());
        }

        let mut dummy: [u8; 1] = [b'G'; 1];
        let mut iov = libc::iovec {
            iov_base: dummy.as_mut_ptr() as *mut libc::c_void,
            iov_len:  1,
        };

        let cmsg_space = libc::CMSG_SPACE(size_of::<libc::c_int>() as libc::c_uint) as usize;
        let mut ctrl_buf = vec![0u8; cmsg_space];

        let mut msg: libc::msghdr = zeroed();
        msg.msg_iov        = &mut iov as *mut libc::iovec;
        msg.msg_iovlen     = 1;
        msg.msg_control    = ctrl_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;

        let cmsg = libc::CMSG_FIRSTHDR(&msg);
        (*cmsg).cmsg_len   = libc::CMSG_LEN(size_of::<libc::c_int>() as libc::c_uint) as _;
        (*cmsg).cmsg_level = libc::SOL_SOCKET;
        (*cmsg).cmsg_type  = libc::SCM_RIGHTS;
        *(libc::CMSG_DATA(cmsg) as *mut libc::c_int) = mem_fd;

        let ret = libc::sendmsg(sock_fd, &msg, 0);
        libc::close(mem_fd);

        if ret < 0 {
            return Err(io_err());
        }

        Ok(())
    }
}

pub fn recv_memfd(sock_fd: RawFd) -> Result<MmapBuffer, ProtocolError> {
    unsafe {
        let cmsg_space = libc::CMSG_SPACE(size_of::<libc::c_int>() as libc::c_uint) as usize;
        let mut ctrl_buf = vec![0u8; cmsg_space];
        let mut recv_byte = [0u8; 1];

        let mut iov = libc::iovec {
            iov_base: recv_byte.as_mut_ptr() as *mut libc::c_void,
            iov_len:  1,
        };

        let mut msg: libc::msghdr = zeroed();
        msg.msg_iov        = &mut iov as *mut libc::iovec;
        msg.msg_iovlen     = 1;
        msg.msg_control    = ctrl_buf.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = cmsg_space as _;

        let ret = libc::recvmsg(sock_fd, &mut msg, 0);
        if ret < 0 {
            return Err(io_err());
        }

        let mut recv_fd: libc::c_int = -1;
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET && (*cmsg).cmsg_type == libc::SCM_RIGHTS {
                let fd = *(libc::CMSG_DATA(cmsg) as *const libc::c_int);
                if recv_fd == -1 {
                    recv_fd = fd;
                } else {
                    libc::close(fd);
                }
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg as *const libc::cmsghdr);
        }

        if recv_fd < 0 {
            return Err(ProtocolError::DecodeError("no file descriptor received".into()));
        }

        let mut stat_buf: libc::stat = zeroed();
        if libc::fstat(recv_fd, &mut stat_buf) < 0 {
            libc::close(recv_fd);
            return Err(io_err());
        }

        let size = stat_buf.st_size as usize;
        if size == 0 {
            libc::close(recv_fd);
            return Ok(MmapBuffer::empty());
        }

        let ptr = libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ,
            libc::MAP_SHARED,
            recv_fd,
            0,
        );
        if ptr == libc::MAP_FAILED {
            libc::close(recv_fd);
            return Err(io_err());
        }

        Ok(MmapBuffer { ptr: ptr as *const u8, len: size, fd: recv_fd })
    }
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
        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_SEQPACKET, 0, fds.as_mut_ptr()); }
        (fds[0], fds[1])
    }

    #[test]
    fn test_transport_loopback() {
        let (a, b) = make_socketpair();
        let data: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        send_memfd(a, &data).unwrap();
        let received = recv_memfd(b).unwrap();
        assert_eq!(received.as_slice(), &data[..]);
        unsafe { libc::close(a); libc::close(b); }
    }

    #[test]
    fn test_transport_empty() {
        let (a, b) = make_socketpair();
        send_memfd(a, &[]).unwrap();
        let received = recv_memfd(b).unwrap();
        assert!(received.is_empty());
        unsafe { libc::close(a); libc::close(b); }
    }
}
