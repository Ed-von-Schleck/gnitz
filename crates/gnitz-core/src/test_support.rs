//! Shared `#[cfg(test)]` scaffolding: the socketpair loopback and the raw
//! framing helpers a scripted peer is written with. They sit at the crate root
//! because the suites that read them are spread over three directories — a
//! `tests` module is private to its parent, so none of them can host the rest.

use std::os::fd::{AsRawFd, OwnedFd};

use crate::protocol::transport::{frame_len_prefix, ClientTransport};

/// Both ends of a connected Unix socketpair — the loopback every framing test
/// runs over.
pub(crate) fn make_socketpair() -> (OwnedFd, OwnedFd) {
    use std::os::fd::FromRawFd;
    let mut fds = [0i32; 2];
    // SAFETY: socketpair fills two fresh fds we take sole ownership of.
    unsafe {
        assert_eq!(
            libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()),
            0
        );
        (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1]))
    }
}

/// A transport over `fd`, established under the client ceiling — the shape
/// every post-handshake connection has.
pub(crate) fn established(fd: OwnedFd) -> ClientTransport {
    let mut t = ClientTransport::from_unix_fd(fd);
    t.mark_established(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT);
    t
}

/// [`make_socketpair`] as a transport pair, both ends established; dropping
/// them closes the fds.
pub(crate) fn make_transport_pair() -> (ClientTransport, ClientTransport) {
    let (a, b) = make_socketpair();
    (established(a), established(b))
}

/// A control-only reply frame carrying `lsn` in `seek_pk` — the terminal a
/// scripted peer answers an uncorrelated request with.
pub(crate) fn reply_ctrl(tid: u64, lsn: u128) -> Vec<u8> {
    crate::protocol::encode_control_frame(tid, 0, 0, lsn, 0, &[])
}

/// `[u32 LE len][payload]`, what a peer writes.
pub(crate) fn framed(payload: &[u8]) -> Vec<u8> {
    let mut v = frame_len_prefix(payload.len()).unwrap().to_vec();
    v.extend_from_slice(payload);
    v
}

/// Raw `send(2)` of all of `bytes` on `fd`: what a scripted peer writes.
pub(crate) fn raw_send(fd: &OwnedFd, bytes: &[u8]) {
    let mut off = 0;
    while off < bytes.len() {
        // SAFETY: valid fd and buffer.
        let n = unsafe {
            libc::send(
                fd.as_raw_fd(),
                bytes[off..].as_ptr() as *const libc::c_void,
                bytes.len() - off,
                0,
            )
        };
        assert!(n > 0, "send failed: {}", std::io::Error::last_os_error());
        off += n as usize;
    }
}

/// Raw `recv(2)` of exactly `buf.len()` bytes on `fd`.
pub(crate) fn raw_read_exact(fd: &OwnedFd, buf: &mut [u8]) {
    let mut off = 0;
    while off < buf.len() {
        // SAFETY: valid fd and buffer.
        let n = unsafe {
            libc::recv(
                fd.as_raw_fd(),
                buf[off..].as_mut_ptr() as *mut libc::c_void,
                buf.len() - off,
                0,
            )
        };
        assert!(n > 0, "recv failed: {}", std::io::Error::last_os_error());
        off += n as usize;
    }
}

/// One length-prefixed frame off `fd`, as a scripted peer reads a request.
pub(crate) fn raw_read_frame(fd: &OwnedFd) -> Vec<u8> {
    let mut hdr = [0u8; 4];
    raw_read_exact(fd, &mut hdr);
    let mut payload = vec![0u8; u32::from_le_bytes(hdr) as usize];
    raw_read_exact(fd, &mut payload);
    payload
}
