//! Shared `#[cfg(test)]` scaffolding: the socketpair loopback, the framing
//! helpers a scripted peer is written with, and the `(pk, v)` fixture. They sit
//! at the crate root because the suites that read them are spread over three
//! directories — a `tests` module is private to its parent, so none of them can
//! host the rest.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use gnitz_wire::{ColumnDef, TypeCode, WireStatus};

use crate::protocol::transport::ClientTransport;
use crate::{BatchAppender, Schema, Session, ZSetBatch};

/// `(pk U64, v <v>)`.
pub(crate) fn kv_schema(v: TypeCode) -> Schema {
    Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", v, false),
        ],
        pk_cols: vec![0],
    }
}

/// `(pk, v, weight)` rows of `kv_schema(I64)`.
pub(crate) fn kv_rows(rows: &[(u64, i64, i64)]) -> ZSetBatch {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b);
    for &(pk, v, w) in rows {
        app.add_row(pk as u128, w).i64_val(v);
    }
    b
}

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

/// A control-only reply frame naming `tid` and carrying `lsn` in `arg0`.
pub(crate) fn reply_ctrl(tid: u64, lsn: u64) -> Vec<u8> {
    let hdr = gnitz_wire::control::ControlHeader {
        target_id: tid,
        arg0: lsn,
        ..Default::default()
    };
    crate::encode_frame(hdr, &[], None, None)
}

/// A fault frame naming `tid`, with `text` as its body.
pub(crate) fn reply_status(tid: u64, status: WireStatus, text: &str) -> Vec<u8> {
    let hdr = gnitz_wire::control::ControlHeader {
        target_id: tid,
        status,
        ..Default::default()
    };
    crate::encode_frame(hdr, text.as_bytes(), None, None)
}

/// Deliver `SIGUSR1` to the calling thread until `stop` is set, with a no-op
/// handler installed without `SA_RESTART` — how CPython's handlers land — so
/// a park in `poll(2)` returns `EINTR`. Repeating rather than one-shot: a
/// single signal that lands before the park is entered leaves the park to
/// block forever, which libtest has no timeout to break.
pub(crate) fn interrupt_self_until(stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    extern "C" fn noop(_: libc::c_int) {}
    // SAFETY: installing a trivial handler for a signal nothing else in the
    // test binary uses.
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = noop as extern "C" fn(libc::c_int) as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        libc::sigaction(libc::SIGUSR1, &sa, std::ptr::null_mut());
    }
    let me = unsafe { libc::pthread_self() };
    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_millis(1));
            // SAFETY: the target is the test thread, which outlives this loop.
            unsafe { libc::pthread_kill(me, libc::SIGUSR1) };
        }
    })
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
pub(crate) fn encode_wal_block(batch: &ZSetBatch) -> Vec<u8> {
    let mut out = Vec::new();
    gnitz_wire::wal::append_block(&batch.wire_regions(), 0, &mut out);
    out
}

/// A WAL block decoded under `schema` into a fresh batch.
pub(crate) fn decode_wal_block(
    data: &[u8],
    schema: &Schema,
) -> Result<ZSetBatch, crate::protocol::error::ProtocolError> {
    let mut sink = ZSetBatch::new(schema);
    crate::protocol::wal_block::decode_wal_block_into(&mut sink, data, schema)?;
    Ok(sink)
}
