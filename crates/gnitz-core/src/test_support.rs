//! Shared `#[cfg(test)]` scaffolding: the socketpair loopback and the
//! framing helpers a scripted peer is written with. They sit at the crate root
//! because the suites that read them are spread over three directories — a
//! `tests` module is private to its parent, so none of them can host the rest.

use gnitz_expr::SchemaFacts;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

use crate::protocol::transport::ClientTransport;
use crate::Session;

/// A scripted peer: the blocking far end of the socketpair a transport runs over.
pub(crate) struct Peer(pub(crate) UnixStream);

impl Peer {
    /// Write one length-prefixed frame.
    pub(crate) fn send(&self, payload: &[u8]) {
        write_frame(&self.0, payload);
    }

    /// Write `bytes` verbatim, however many frames or fragments they hold.
    pub(crate) fn send_bytes(&self, bytes: &[u8]) {
        (&self.0).write_all(bytes).unwrap();
    }

    /// Read one length-prefixed frame.
    pub(crate) fn recv(&self) -> Vec<u8> {
        read_frame(&self.0)
    }
}

/// A transport over one end of a Unix socketpair, and the peer on the other.
pub(crate) fn transport_pair() -> (ClientTransport, Peer) {
    let (a, b) = UnixStream::pair().unwrap();
    (ClientTransport::unix(a).unwrap(), Peer(b))
}

/// [`transport_pair`] with a session over the transport.
pub(crate) fn session_pair() -> (Session, Peer) {
    let (t, peer) = transport_pair();
    (Session::over(t), peer)
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

/// The I/O error kind `r` failed with, if it failed with one.
pub(crate) fn io_kind<T>(r: &Result<T, crate::protocol::error::ProtocolError>) -> Option<std::io::ErrorKind> {
    match r {
        Err(crate::protocol::error::ProtocolError::IoError(e)) => Some(e.kind()),
        _ => None,
    }
}

/// Write one length-prefixed frame to a blocking stream.
pub(crate) fn write_frame(mut w: impl Write, payload: &[u8]) {
    w.write_all(&framed(payload)).unwrap();
    w.flush().unwrap();
}

/// Read one length-prefixed frame off a blocking stream.
pub(crate) fn read_frame(mut r: impl Read) -> Vec<u8> {
    let mut hdr = [0u8; gnitz_wire::FRAME_LEN_PREFIX_BYTES];
    r.read_exact(&mut hdr).unwrap();
    let mut payload = vec![0u8; u32::from_le_bytes(hdr) as usize];
    r.read_exact(&mut payload).unwrap();
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
