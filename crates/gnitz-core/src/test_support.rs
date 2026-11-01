//! Shared `#[cfg(test)]` scaffolding: the socketpair and TLS loopbacks and the
//! sessions and clients over them, the framing helpers a scripted peer is
//! written with, the frames it sends and decodes, and the batch fixtures and
//! their WAL-block codecs. It sits at the crate root because
//! the tests, benchmarks and fuzz targets that read it are spread over several
//! directories — a `tests` module is private to its parent, so none of them can
//! host the rest.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::net::UnixStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_wire::{framed, peek_control_block, ColumnDef, ControlHeader, TypeCode, WireFlags, WireLane, WireStatus};

use crate::connection::Session;
use crate::transport::poll_fd;
use crate::{
    BatchAppender, BlockingHost, ClientError, ClientTransport, GnitzClient, Interest, ProtocolError, RelName, Schema,
    Sent, SubId, ZSetBatch,
};

/// `schema.name`.
pub(crate) fn rel(schema: &str, name: &str) -> RelName {
    RelName::new(schema, name).unwrap()
}

/// `(pk U64, v <v>)`.
pub(crate) fn kv_schema(v: TypeCode) -> Schema {
    Schema::from_parts(
        vec![
            ColumnDef::new("pk", TypeCode::U64, false),
            ColumnDef::new("v", v, false),
        ],
        &[0],
    )
    .unwrap()
}

/// `(pk, v, weight)` rows of `kv_schema(I64)`.
pub(crate) fn kv_rows(rows: &[(u64, i64, i64)]) -> ZSetBatch {
    let schema = kv_schema(TypeCode::I64);
    let mut b = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut b);
    for &(pk, v, w) in rows {
        app.add_row(pk as u128, w).int_val(v);
    }
    b
}

/// `(pk U64, column_1 I64, …)` of `ncols` columns, the payload ones `nullable`.
pub(crate) fn wide_schema(ncols: usize, nullable: bool) -> Arc<Schema> {
    let mut columns = vec![ColumnDef::new("pk", TypeCode::U64, false)];
    columns.extend((1..ncols).map(|i| ColumnDef::new(format!("column_{i}"), TypeCode::I64, nullable)));
    Arc::new(Schema::from_parts(columns, &[0]).unwrap())
}

/// `n` rows of a [`wide_schema`]; with `null_every`, every that-many-th cell
/// is NULL.
pub(crate) fn wide_rows(schema: &Schema, n: usize, null_every: usize) -> ZSetBatch {
    let mut b = ZSetBatch::with_capacity(schema, n);
    let mut app = BatchAppender::new(&mut b);
    for r in 0..n {
        let row = app.add_row(r as u128, 1);
        for c in 1..schema.columns().len() {
            if null_every != 0 && (r + c) % null_every == 0 {
                row.null();
            } else {
                row.int_val(7);
            }
        }
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

/// A rustls server on a loopback thread with a freshly minted `127.0.0.1` cert,
/// reached through the public `tls://…?ca=` target.
pub(crate) struct Loopback<T> {
    pub(crate) target: String,
    thread: std::thread::JoinHandle<T>,
    /// Holds the minted cert's PEM for as long as the target names it.
    _ca_dir: tempfile::TempDir,
}

/// The far end of a TLS connection: a blocking rustls stream.
pub(crate) type ServerEnd = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

/// Complete the server side of the TLS handshake. `StreamOwned` handshakes
/// lazily on first I/O, which a peer that only waits never does.
pub(crate) fn handshake(sock: TcpStream, cfg: Arc<rustls::ServerConfig>) -> ServerEnd {
    let mut end = rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
    while end.conn.is_handshaking() {
        end.conn.complete_io(&mut end.sock).unwrap();
    }
    end
}

/// Answer the client's HELLO, which carries no token.
fn hello(mut end: ServerEnd) -> ServerEnd {
    assert_eq!(read_frame(&mut end), gnitz_wire::HELLO);
    write_frame(&mut end, &gnitz_wire::server_hello(gnitz_wire::AuthStatus::Accepted));
    end
}

impl<T: Send + 'static> Loopback<T> {
    /// `serve` gets the accepted socket (`TCP_NODELAY`, as the server sets it)
    /// and the server config. `rcvbuf` pins the socket's `SO_RCVBUF`, set on the
    /// listener so the window the handshake advertises honours it.
    pub(crate) fn serve(
        rcvbuf: Option<libc::c_int>,
        serve: impl FnOnce(TcpStream, Arc<rustls::ServerConfig>) -> T + Send + 'static,
    ) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let ca_dir = tempfile::tempdir().unwrap();
        let pem = ca_dir.path().join("ca.pem");
        std::fs::write(&pem, cert.cert.pem()).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();
        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key)
            .unwrap();
        cfg.alpn_protocols = vec![gnitz_wire::ALPN_GNITZ.to_vec()];
        let cfg = Arc::new(cfg);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        if let Some(sz) = rcvbuf {
            set_sockopt_int(listener.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF, sz).unwrap();
        }
        let port = listener.local_addr().unwrap().port();
        let thread = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            sock.set_nodelay(true).unwrap();
            serve(sock, cfg)
        });
        Loopback {
            target: format!("tls://127.0.0.1:{port}?ca={}", pem.display()),
            thread,
            _ca_dir: ca_dir,
        }
    }

    /// What `serve` returned.
    pub(crate) fn join(self) -> T {
        self.thread.join().unwrap()
    }
}

/// A TLS transport over [`Loopback`] and its far end, the HELLOs exchanged.
/// `rcvbuf` as [`Loopback::serve`] takes it.
pub(crate) fn tls_pair(rcvbuf: Option<libc::c_int>) -> (ClientTransport, ServerEnd) {
    let lb = Loopback::serve(rcvbuf, |sock, cfg| hello(handshake(sock, cfg)));
    let t = ClientTransport::connect(&lb.target, Instant::now() + gnitz_wire::CONNECT_TIMEOUT).unwrap();
    (t, lb.join())
}

/// One reading pass: `read` until it returns false or fails. The frames it
/// handed out, and how it ended.
pub(crate) fn pass(t: &mut ClientTransport) -> (Vec<Vec<u8>>, Result<(), ProtocolError>) {
    let mut frames = Vec::new();
    loop {
        let read = t.read(|f| {
            frames.push(f.into_owned());
            Ok(())
        });
        match read {
            Ok(true) => {}
            Ok(false) => return (frames, Ok(())),
            Err(e) => return (frames, Err(e)),
        }
    }
}

/// Reading passes, parked on the fd between them, until `n` frames are out.
pub(crate) fn recv_frames(t: &mut ClientTransport, n: usize) -> Vec<Vec<u8>> {
    let mut frames = Vec::new();
    while frames.len() < n {
        poll_fd(t.as_fd(), Interest::READ, None).unwrap();
        let (got, end) = pass(t);
        end.unwrap();
        frames.extend(got);
    }
    frames
}

/// `flush`, parked on the fd between calls, until nothing is left to write.
pub(crate) fn flush_all(t: &mut ClientTransport) {
    loop {
        t.flush().unwrap();
        if !t.wants_write() {
            return;
        }
        poll_fd(t.as_fd(), Interest::WRITE, None).unwrap();
    }
}

/// [`transport_pair`] with a session over the transport.
pub(crate) fn session_pair() -> (Session, Peer) {
    let (t, peer) = transport_pair();
    (Session::over(t), peer)
}

/// A blocking client over [`session_pair`], and the peer.
pub(crate) fn client_pair() -> (GnitzClient, Peer) {
    let (s, peer) = session_pair();
    (GnitzClient::over(s, Box::new(BlockingHost::default())).unwrap(), peer)
}

/// Park until `s`'s fd is ready for `want` (at most 30 s) and say what is.
pub(crate) fn park(s: &Session, want: Interest) -> Interest {
    assert!(!want.is_empty(), "slots pending but nothing to wait for");
    poll_fd(s.as_fd(), want, Some(Instant::now() + Duration::from_secs(30))).expect("poll")
}

/// The driver: step with what `poll` reported, parking on `interest()`, until
/// `last` is answered. Also reports whether both interests were ever armed at
/// once.
pub(crate) fn drive<T>(s: &mut Session, mut last: Sent<T>) -> (Result<T, ClientError>, bool) {
    let mut ready = Interest::WRITE;
    let mut both_armed = false;
    loop {
        s.step(ready);
        if let Some(reply) = last.try_take() {
            return (reply, both_armed);
        }
        let interest = s.interest();
        both_armed |= interest.read && interest.write;
        ready = park(s, interest);
    }
}

/// Step and park until `sent` is answered.
pub(crate) fn await_reply<T>(s: &mut Session, sent: Sent<T>) -> Result<T, ClientError> {
    drive(s, sent).0
}

/// Step `s` on what its socket reports ready, parked between steps, until it owes nothing.
pub(crate) fn step_until_idle(s: &mut Session) {
    while !s.interest().is_empty() {
        s.step(poll_fd(s.as_fd(), s.interest(), None).unwrap());
    }
}

/// One frame payload, without the 4-byte length prefix, as
/// `append_payload` writes it.
pub(crate) fn encode_frame(
    hdr: ControlHeader,
    blob: &[u8],
    schema: Option<&[u8]>,
    data: Option<&ZSetBatch>,
) -> Vec<u8> {
    let mut out = Vec::new();
    crate::connection::append_payload(&mut out, hdr, blob, schema, data);
    out
}

/// A control-only reply frame naming `tid` and carrying `lsn` in `arg0`.
pub(crate) fn reply_ctrl(tid: u64, lsn: u64) -> Vec<u8> {
    let hdr = ControlHeader {
        target_id: tid,
        arg0: lsn,
        ..Default::default()
    };
    encode_frame(hdr, &[], None, None)
}

/// A reply frame carrying `batch` and `lsn` in `arg0`, and no schema block;
/// `cont` sets `continuation`.
pub(crate) fn reply_rows(tid: u64, batch: &ZSetBatch, lsn: u64, cont: bool) -> Vec<u8> {
    let hdr = ControlHeader {
        target_id: tid,
        flags: WireFlags { continuation: cont, ..Default::default() },
        arg0: lsn,
        ..Default::default()
    };
    encode_frame(hdr, &[], None, Some(batch))
}

/// A frame of `view`'s delta train carrying `rows`, if any: a continuation, or
/// with `end` the terminal at the cursor `(tag, tick)`.
pub(crate) fn delta_frame(view: u64, rows: Option<&ZSetBatch>, end: Option<(u64, u64)>) -> Vec<u8> {
    let (tag, tick) = end.unwrap_or_default();
    let hdr = ControlHeader {
        target_id: view,
        flags: WireFlags {
            continuation: end.is_none(),
            ..Default::default()
        },
        arg0: tick,
        arg1: tag,
        ..Default::default()
    };
    encode_frame(hdr, &[], None, rows)
}

/// `view`'s terminal of a delta read or poll: control-only, carrying the cursor
/// `(tag, tick)`.
pub(crate) fn delta_terminal(view: u64, tag: u64, tick: u64) -> Vec<u8> {
    delta_frame(view, None, Some((tag, tick)))
}

/// The frame that opens a pushed train of subscription `sub` to `view`.
pub(crate) fn pushed_marker(view: u64, sub: SubId) -> Vec<u8> {
    let hdr = ControlHeader {
        target_id: view,
        arg0: sub.get(),
        flags: WireFlags {
            lane: WireLane::PushedTrain,
            ..Default::default()
        },
        ..Default::default()
    };
    encode_frame(hdr, &[], None, None)
}

/// One pushed train of subscription `sub` to `view`, framed: its opening frame,
/// `rows` unless there are none, and the terminal at the cursor `end` — or, with
/// `end` an error, a fault carrying that text.
pub(crate) fn pushed_train(view: u64, sub: SubId, rows: &ZSetBatch, end: Result<(u64, u64), &str>) -> Vec<u8> {
    let mut out = framed(&pushed_marker(view, sub));
    let end = match end {
        Ok(cursor) => cursor,
        Err(text) => {
            out.extend(framed(&reply_status(view, WireStatus::Error, text)));
            return out;
        }
    };
    if !rows.is_empty() {
        out.extend(framed(&delta_frame(view, Some(rows), None)));
    }
    out.extend(framed(&delta_frame(view, None, Some(end))));
    out
}

/// The subscription id `id` spells.
pub(crate) fn sub_id(id: u64) -> SubId {
    SubId::new(id).expect("a subscription id is not 0")
}

/// The control header of `request`, which must be a SYNC_PUSHED.
pub(crate) fn sync_header(request: &[u8]) -> ControlHeader {
    let hdr = peek_control_block(request).expect("a control header").hdr;
    assert_eq!(hdr.flags.verb, gnitz_wire::ClientVerb::SyncPushed);
    hdr
}

/// The subscription ids `request`, a SYNC_PUSHED, names as held, ascending.
pub(crate) fn sync_held(request: &[u8]) -> Vec<SubId> {
    let ctrl = peek_control_block(request).expect("a control header");
    assert_eq!(ctrl.hdr.flags.verb, gnitz_wire::ClientVerb::SyncPushed);
    let mut held = gnitz_wire::txn_frame::decode_held(&request[ctrl.blob]).expect("held ids");
    held.sort_unstable();
    held
}

/// The frame that answers the oldest sync: with `refusal`, a fault refusing it.
pub(crate) fn sync_answer(refusal: Option<&str>) -> Vec<u8> {
    let hdr = ControlHeader {
        status: refusal.map_or(WireStatus::Ok, |_| WireStatus::Error),
        flags: WireFlags {
            lane: WireLane::SyncAnswer,
            ..Default::default()
        },
        ..Default::default()
    };
    encode_frame(hdr, refusal.unwrap_or_default().as_bytes(), None, None)
}

/// A fault frame naming `tid`, with `text` as its body.
pub(crate) fn reply_status(tid: u64, status: WireStatus, text: &str) -> Vec<u8> {
    let hdr = ControlHeader {
        target_id: tid,
        status,
        ..Default::default()
    };
    encode_frame(hdr, text.as_bytes(), None, None)
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

/// The I/O error kind `r` failed with, if it failed with one.
pub(crate) fn io_kind<T>(r: &Result<T, ProtocolError>) -> Option<std::io::ErrorKind> {
    match r {
        Err(ProtocolError::IoError(e)) => Some(e.kind()),
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
pub(crate) fn decode_wal_block(data: &[u8], schema: &Schema) -> Result<ZSetBatch, ProtocolError> {
    let mut sink = ZSetBatch::new(schema);
    crate::batch::decode_wal_blocks_into(&mut sink, std::iter::once(data), schema)?;
    Ok(sink)
}
