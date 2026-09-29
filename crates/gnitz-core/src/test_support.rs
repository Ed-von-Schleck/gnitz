//! Shared `#[cfg(test)]` scaffolding: the socketpair loopback and the raw
//! framing helpers a scripted peer is written with. They sit at the crate root
//! because the suites that read them are spread over three directories — a
//! `tests` module is private to its parent, so none of them can host the rest.

use gnitz_expr::SchemaFacts;
use std::os::fd::{AsRawFd, OwnedFd};

use crate::protocol::transport::ClientTransport;
use crate::Session;

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

/// [`make_socketpair`] as a transport pair; dropping them closes the fds.
pub(crate) fn make_transport_pair() -> (ClientTransport, ClientTransport) {
    let (a, b) = make_socketpair();
    (ClientTransport::from_unix_fd(a), ClientTransport::from_unix_fd(b))
}

/// A scripted peer: the raw far end of a socketpair a [`Session`] runs over.
pub(crate) struct Peer(pub(crate) OwnedFd);

impl Peer {
    /// Write one length-prefixed frame.
    pub(crate) fn send(&self, payload: &[u8]) {
        raw_send(&self.0, &framed(payload));
    }
}

/// A session over one end of a socketpair, and the peer on the other.
pub(crate) fn session_pair() -> (Session, Peer) {
    let (a, b) = make_socketpair();
    (Session::over(ClientTransport::from_unix_fd(a)), Peer(b))
}

/// A control-only reply frame carrying `lsn` in `arg0` — the terminal a
/// scripted peer answers an uncorrelated request with.
pub(crate) fn reply_ctrl(tid: u64, lsn: u64) -> Vec<u8> {
    let hdr = gnitz_wire::control::ControlHeader {
        target_id: tid,
        arg0: lsn,
        ..Default::default()
    };
    crate::encode_frame(hdr, &[], None, None)
}

/// `[u32 LE len][payload]`, what a peer writes.
pub(crate) fn framed(payload: &[u8]) -> Vec<u8> {
    let mut v = gnitz_wire::frame_len_prefix(payload.len()).to_vec();
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

/// The batch as a lone WAL block.
pub(crate) fn encode_wal_block(batch: &crate::ZSetBatch) -> Vec<u8> {
    let mut out = Vec::new();
    gnitz_wire::wal::append_block(&batch.wire_regions(), 0, &mut out);
    out
}

/// A WAL block decoded under `schema` into a fresh batch.
pub(crate) fn decode_wal_block(
    data: &[u8],
    schema: &crate::Schema,
) -> Result<crate::ZSetBatch, crate::protocol::error::ProtocolError> {
    let mut sink = crate::ZSetBatch::new(schema);
    crate::protocol::wal_block::decode_wal_block_into(&mut sink, data, schema)?;
    Ok(sink)
}

/// A STRING/BLOB column region from its values, spilling into `blob`; `None` is
/// a NULL cell, which the region zero-fills.
pub(crate) fn german_col(vals: &[Option<&[u8]>], blob: &mut Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(vals.len() * 16);
    for v in vals {
        out.extend_from_slice(&gnitz_wire::encode_german_string(v.unwrap_or(&[]), blob));
    }
    out
}

/// `schema`'s payload regions holding `regions`, one per payload slot in slot
/// order, each typed as its schema column.
pub(crate) fn payload_of(schema: &crate::Schema, regions: Vec<Vec<u8>>) -> Vec<crate::PayloadColumn> {
    assert_eq!(regions.len(), schema.num_payload_cols(), "one region per payload slot");
    schema
        .payload_columns()
        .zip(regions)
        .map(|((_, _, c), bytes)| {
            let mut col = crate::PayloadColumn::new(c.ty.tc);
            col.bytes = bytes;
            col
        })
        .collect()
}
