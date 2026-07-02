# 02 — QUIC Network Transport (quinn-proto, server + client)

## Problem

gnitz clients can reach the server only over an AF_UNIX socket. This plan adds
a network transport: QUIC (RFC 9000) with TLS 1.3, carrying the existing wire
protocol unchanged. "ZSets over the wire" is untouched — the 4-byte-LE
length-prefixed frames (control + schema + data WAL blocks) ride verbatim
inside one QUIC bidirectional stream per connection.

Baseline facts this plan builds on (already in the tree):

- The server dispatch layer addresses a client through the `Peer` handle in
  `crates/gnitz-engine/src/runtime/orchestration/peer.rs` — an enum
  (`PeerInner`) with a `Unix` variant; `connection_loop`, all handlers, and
  the scan-train forwarders take `&Peer` and reply via
  `peer.send_buffer_or_close` / `peer.send_slot_or_close`. `Peer::recv()`
  yields `RecvBuf` frames.
- The reactor exposes one-shot UDP datagram ops: `Reactor::send_udp(fd, Vec<u8>,
  SocketAddr) -> i32` (eagerly submitted; dropping the future is a sound
  fire-and-forget — the op still executes and the CQE handler frees storage)
  and `Reactor::recv_udp(fd, max_datagram) -> Result<UdpDatagram, i32>`, plus
  `foundation::posix_io::{udp_bind, udp_local_addr}`.
- The client's framing (`gnitz-core/src/protocol/transport/mod.rs`) is
  AF-agnostic; only `connect_unix` and the `RawFd` plumbing are
  transport-specific. `gnitz-wire` is pure bytes and needs zero changes.

## Committed decisions

| Decision | Choice |
|---|---|
| QUIC library | `quinn-proto` 0.11 (sans-IO), both server and client |
| TLS stack | rustls 0.23, **ring** provider (`rustls-ring` feature — no cmake/C++ toolchain) |
| Stream model | exactly one client-opened bidirectional stream per connection; frames as today. Enforced server-side by `max_concurrent_bidi_streams(1)`, `max_concurrent_uni_streams(0)` |
| ALPN | `b"gnitz/1"` on both sides (server list non-empty ⇒ mismatch fails the handshake) |
| HELLO | unchanged, first frames on the stream (keeps the `bound_client_id` auth hook and the pre-handshake 8-byte frame cap) |
| QUIC datagrams (RFC 9221) | disabled: `datagram_receive_buffer_size(None)` |
| 0-RTT | not used (replayable DML is a correctness hazard) |
| GSO/ECN | off: `poll_transmit(now, 1, buf)` + `enable_segmentation_offload(false)`; `ecn: None` |
| Idle handling | `max_idle_timeout` 300 s both sides; client `keep_alive_interval` 20 s (keeps the conn alive while the app is inside a blocking call; a >5 min fully idle client gets a clear reconnect error) |
| Flow control | `stream_receive_window` 8 MiB both sides; everything else quinn defaults |
| Datagram sizing | quinn defaults (`max_udp_payload_size` 1472); reactor recv buffers 2048 B |
| Retry tokens / migration | quinn defaults (no forced retry; migration allowed) |
| Server CLI | `--quic-listen=IP:PORT` (port 0 ⇒ ephemeral), `--tls-cert=PEM --tls-key=PEM`; without cert+key a self-signed dev cert is minted (rcgen) for `localhost`/`127.0.0.1`/`::1` and its PEM is written to `<data_dir>/quic_dev_cert.pem`. Bound address written to `<data_dir>/quic_endpoint` (`IP:PORT\n`) after bind |
| Client address syntax | `quic://HOST:PORT`, optional `?insecure` (skip verification) or `?ca=PATH` (PEM root override); default roots = `webpki-roots`. Any non-`quic://` string stays an AF_UNIX path — all four API surfaces (Rust / C / Python sync / Python async) keep their signatures |
| Client I/O model | blocking `std::net::UdpSocket` (connected) driving quinn-proto — no async runtime, no threads added |
| Dependency gating | unconditional deps, no cargo feature (pre-alpha; no compat concerns) |
| Connect deadline | client handshake deadline 10 s |

Dependency additions (versions current as of 2026-07-02):

```toml
# gnitz-engine
quinn-proto = { version = "0.11", default-features = false, features = ["rustls-ring", "bloom"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pki-types = "1"
rcgen = "0.14"
bytes = "1"

# gnitz-core
quinn-proto = { version = "0.11", default-features = false, features = ["rustls-ring"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pki-types = "1"
webpki-roots = "1"
bytes = "1"
```

(quinn-proto's `log`/`tracing` feature is deliberately off; gnitz has its own
logging. `bloom` stays on server-side for address-validation replay
protection. quinn-proto MSRV is 1.85.)

## Design — server

New module tree `crates/gnitz-engine/src/runtime/quic/`:
`mod.rs` (engine state + drive task), `stream.rs` (frame assembly + the
`QuicPeer` glue), `tls.rs` (rustls/rcgen config building).

### tls.rs

```rust
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

pub const ALPN_GNITZ: &[u8] = b"gnitz/1";

/// Build the quinn server crypto config. `cert_key`: Some((cert_pem, key_pem))
/// from the CLI, or None to mint a self-signed dev certificate. When minted,
/// the PEM is also returned so the caller can write <data_dir>/quic_dev_cert.pem.
pub fn server_crypto(
    cert_key: Option<(&str, &str)>,
) -> Result<(Arc<quinn_proto::ServerConfig>, Option<String>), String> {
    let (chain, key, dev_pem) = match cert_key {
        Some((cert_path, key_path)) => {
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
                .map_err(|e| format!("read {cert_path}: {e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("parse {cert_path}: {e}"))?;
            let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| format!("read {key_path}: {e}"))?;
            (chain, key, None)
        }
        None => {
            let ck = rcgen::generate_simple_self_signed(vec![
                "localhost".into(),
                "127.0.0.1".into(),
                "::1".into(),
            ])
            .map_err(|e| format!("dev cert: {e}"))?;
            let pem = ck.cert.pem();
            let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
            (vec![ck.cert.der().clone()], key, Some(pem))
        }
    };
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| e.to_string())?;
    cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
    let quic = quinn_proto::crypto::rustls::QuicServerConfig::try_from(cfg).map_err(|e| e.to_string())?;
    let mut sc = quinn_proto::ServerConfig::with_crypto(Arc::new(quic));
    sc.transport_config(Arc::new(transport_config_server()));
    Ok((Arc::new(sc), dev_pem))
}

fn transport_config_server() -> quinn_proto::TransportConfig {
    let mut t = quinn_proto::TransportConfig::default();
    t.max_concurrent_bidi_streams(1u32.into());
    t.max_concurrent_uni_streams(0u32.into());
    t.datagram_receive_buffer_size(None);
    t.stream_receive_window((8u32 * 1024 * 1024).into());
    t.max_idle_timeout(Some(quinn_proto::IdleTimeout::try_from(Duration::from_secs(300)).unwrap()));
    t.enable_segmentation_offload(false);
    t
}
```

### Engine state and drive task

```rust
pub struct QuicEngine {
    st: RefCell<EngineState>,
    /// Kicked by peer send paths and close; the drive task drains it and
    /// re-runs the poll cycle so freshly written stream data is flushed.
    notify_tx: mpsc::Sender<()>,
}

struct EngineState {
    endpoint: quinn_proto::Endpoint, // Endpoint::new(default, Some(server_config), true, None)
    conns: FxHashMap<quinn_proto::ConnectionHandle, ConnState>,
    scratch: Vec<u8>, // poll_transmit / handle response buffer
}

struct ConnState {
    conn: quinn_proto::Connection,
    stream: Option<StreamState>, // the single accepted bidi stream
    closing: bool,
}

struct StreamState {
    id: quinn_proto::StreamId,
    assembler: FrameAssembler,       // 4-byte-LE prefix framing + max_payload_len cap
    inbound: VecDeque<Vec<u8>>,      // complete frame payloads awaiting Peer::recv
    inbound_bytes: usize,            // watermark accounting (see backpressure below)
    paused: bool,                    // reading suspended at the high-water mark
    readable_pending: bool,          // Readable arrived while paused
    recv_waker: Option<Waker>,
    recv_closed: bool,               // FIN / reset / connection lost
    write_waker: Option<Waker>,      // parked on WriteError::Blocked
}
```

**Inbound backpressure.** The fd path is backpressured by the kernel socket
buffer once the handler stops consuming; the QUIC equivalent must not return
flow-control credit faster than the handler consumes (`Chunks::finalize`
re-credits the peer, and the connection-level `receive_window` default is
unbounded). So chunk-reading pauses at a high-water mark: when
`inbound_bytes >= 8 MiB`, set `paused` and stop calling `read()` (a
`Readable` that arrives meanwhile only sets `readable_pending`). Unread
stream bytes then accumulate in quinn's receive buffer, bounded by the 8 MiB
`stream_receive_window`, and the client's sender blocks — kernel-socket-like
backpressure. `Peer::recv()` decrements `inbound_bytes` on pop; dropping
below 4 MiB clears `paused` and kicks the drive task, which resumes reading
streams whose `readable_pending` is set.

`FrameAssembler` mirrors the reactor's `RecvState` on owned buffers: consume
stream bytes, cut `[len:u32 LE][payload]` frames, reject `len == 0` (close
sentinel) and `len > max_payload_len` (initially
`HELLO_PRE_HANDSHAKE_LEN = 8`, elevated by `Peer::set_max_payload_len` after
HELLO). A violation closes the connection. This is deliberately *stricter*
than the fd path in one corner: the drive task parses stream bytes on
arrival, so a client that pipelines a >8-byte frame behind HELLO before the
ACK gets closed here, while the fd path would leave those bytes unparsed in
the kernel buffer until the cap is elevated. No conforming client pipelines
pre-ACK (`hello_handshake` blocks on the ACK), so the divergence is
intended hardening, not a compatibility break.

The drive task (spawned by the executor when QUIC is enabled) follows the
quinn-proto polling contract — after any `handle_*`/app-I/O, per connection:
**1. `poll_transmit` (drain), 2. `poll_timeout`, 3. `poll_endpoint_events`,
4. `poll`** — with three liveness rules baked into the loop shape:

- **Both long-lived futures are pinned across iterations.** The recv future
  is recreated only after it resolves (dropping it mid-flight tombstones an
  armed recvmsg and silently discards its datagram). The timer future is
  re-armed only when it fires or when the engine's next deadline moves
  *earlier* than the armed one — the `tick_loop_async` pinned-timer rule;
  a fresh timer per iteration would leak an uncancellable kernel Timeout SQE
  (+ `timer_specs` entry until its original deadline) per wakeup. A timer
  that fires at a stale, now-too-early deadline is harmless: the expiry
  check below no-ops and the timer is re-armed at the current deadline.
- **Timeout expiry is checked every iteration**, not only on the timer
  branch: `select2` polls its first argument first, so under a sustained
  datagram flood the timer branch would never win and PTO/loss/idle timers
  of *all* connections would starve. quinn documents spurious
  `handle_timeout` calls as safe no-ops, so an unconditional
  now-vs-deadline check is correct.
- **A notify wake drains the channel** with `try_recv()` before driving —
  the mpsc has counting semantics, and K queued kicks must not buy K
  redundant drive passes.

```rust
pub async fn drive(engine: Rc<QuicEngine>, reactor: Rc<Reactor>, udp_fd: i32,
                   mut notify_rx: mpsc::Receiver<()>, peers_tx: mpsc::Sender<Peer>) {
    let mut recv = Box::pin(reactor.recv_udp(udp_fd, RECV_BUF_LEN)); // 2048
    let far = || Instant::now() + Duration::from_secs(60);           // idle fallback tick
    let mut armed_at = far();
    let mut timer = Box::pin(reactor.timer(armed_at));
    loop {
        let mut timer_fired = false;
        match select2(recv.as_mut(), select2(notify_rx.recv(), timer.as_mut())).await {
            Either::A(dgram) => {
                if let Ok(d) = dgram {
                    engine.handle_datagram(d, &peers_tx, &reactor, udp_fd);
                }
                recv = Box::pin(reactor.recv_udp(udp_fd, RECV_BUF_LEN));
            }
            Either::B(Either::A(_)) => while notify_rx.try_recv().is_some() {},
            Either::B(Either::B(())) => timer_fired = true,
        }
        engine.handle_expired(Instant::now()); // handle_timeout for every conn whose
                                               // poll_timeout() <= now — every iteration
        engine.drive_all(&reactor, udp_fd);    // runs to quiescence, see below
        let want = engine.next_timeout().map_or_else(far, |d| d);
        if timer_fired || want < armed_at {
            armed_at = want;
            timer = Box::pin(reactor.timer(armed_at));
        }
    }
}
```

`handle_datagram` (payload copied into `BytesMut` — bounded at 1472 B by the
endpoint's `max_udp_payload_size` transport parameter; `UdpDatagram.buf` is
already `set_len`-truncated to the received size by the reactor, which is
load-bearing — a 1-RTT short-header packet extends to the end of the
datagram, so trailing padding would fail AEAD verification on every packet):

```rust
let now = Instant::now();
let bm = bytes::BytesMut::from(&d.buf[..]);
let mut st = self.st.borrow_mut();
let mut buf = std::mem::take(&mut st.scratch);
match st.endpoint.handle(now, d.src, None, None, bm, &mut buf) {
    Some(DatagramEvent::ConnectionEvent(ch, ev)) => {
        if let Some(c) = st.conns.get_mut(&ch) { c.conn.handle_event(ev); }
    }
    Some(DatagramEvent::NewConnection(incoming)) => {
        match st.endpoint.accept(incoming, now, &mut buf, None) {
            Ok((ch, conn)) => { st.conns.insert(ch, ConnState::new(conn)); }
            Err(err) => {
                // AcceptError is a struct; its optional response Transmit
                // (payload in `buf`) MUST be sent (e.g. version negotiation).
                if let Some(t) = err.response { send_transmit(&reactor, udp_fd, &buf, &t); }
            }
        }
    }
    Some(DatagramEvent::Response(t)) => send_transmit(&reactor, udp_fd, &buf, &t),
    None => {}
}
st.scratch = { buf.clear(); buf };
```

`drive_all` runs the contract-ordered cycle per connection and **repeats the
whole cycle until a full pass is quiescent** (no transmit emitted, no event
polled). A single pass is not enough: step 4 itself generates transmits —
`Chunks::finalize`'s `ShouldTransmit` (MAX_STREAM_DATA credit),
MAX_STREAMS, STOP_SENDING — and quinn's contract is to send those "as soon
as is reasonable", not at the next external wakeup (which could be the 60 s
fallback while a flow-blocked client stalls mid-frame). One pass:

1. `while let Some(t) = conn.poll_transmit(now, 1, &mut buf)` → copy
   `buf[..t.size]` into a fresh `Vec` and fire-and-forget
   `reactor.send_udp(udp_fd, v, t.destination)` (create the future, drop it —
   the reactor design makes that a sound one-shot; QUIC's loss recovery owns
   reliability).
2. `poll_timeout()` → fold into the engine-wide min deadline.
3. `while let Some(ev) = conn.poll_endpoint_events()` → if
   `ev.is_drained()`, mark the conn for removal after the loop; feed
   `endpoint.handle_event(ch, ev)` and route any returned `ConnectionEvent`
   back into the conn.
4. `while let Some(event) = conn.poll()`:
   - `Event::Stream(StreamEvent::Opened { dir: Dir::Bi })` →
     `conn.streams().accept(Dir::Bi)`; first stream: build `StreamState`,
     construct `Peer::quic(...)`, `peers_tx.send(peer)`. If a stream already
     exists (live **or** finished — stream limits are cumulative, so a
     client may FIN stream 0 and legally open another): protocol violation,
     `conn.close(now, VarInt::from_u32(0x2), ..)`. Never an assert — that
     would be a remotely triggerable debug-build panic.
   - `StreamEvent::Readable { id }` → if `paused`, set `readable_pending`
     and skip; else `conn.recv_stream(id).read(true)`: an `Err(_)` from
     `read` itself (`ClosedStream` from a stale queued event after a
     consumed reset) sets `recv_closed` + wake, no panic path; on `Ok`,
     loop `chunks.next(usize::MAX)` feeding the assembler (queue complete
     frames into `inbound`, wake `recv_waker`, honoring the watermark); on
     `Ok(None)` (FIN) or `Err(ReadError::Reset(_))` from `next` set
     `recv_closed` + wake; **always** `chunks.finalize()` — its
     `ShouldTransmit` is satisfied because the enclosing until-quiescent
     loop re-runs the step-1 transmit drain after this pass.
   - `StreamEvent::Writable { id }` → wake `write_waker`.
   - `StreamEvent::Finished/Stopped` → wake `write_waker` (the send loop
     re-runs and observes the error).
   - `Event::ConnectionLost { .. }` → set `recv_closed`, wake both wakers
     (the connection task exits via `Peer::recv() == None`); keep driving
     until drained.
   - `Event::Connected` / `HandshakeDataReady` / datagram events → nothing.

Frame-limit violation or a `Peer::close()` sets `closing`:
`conn.close(now, VarInt::from_u32(0), Bytes::new())`; the conn keeps being
driven until the `is_drained()` endpoint event removes it.

### QuicPeer (the `PeerInner::Quic` variant)

`peer.rs` gains `Quic(QuicPeer)` where
`QuicPeer { engine: Rc<QuicEngine>, ch: ConnectionHandle, id: StreamId }`.

- `recv()`: future that pops `inbound` (returning
  `RecvBuf::Vec(Vec<u8>)` — `RecvBuf` grows a second, `Vec`-backed variant
  next to the malloc-backed one) or registers `recv_waker`; resolves `None`
  once `recv_closed` and the queue is empty. A pop decrements
  `inbound_bytes`; crossing below the 4 MiB low-water mark clears `paused`
  and kicks the drive task so paused streams resume reading.
- `send_buffer` / `send_slot` / `send_hello_ack`: one shared loop over the
  complete frame bytes (`buf.0`, `slot.frame_bytes()`, the const ACK — all
  already length-prefixed exactly as the fd path sends them):

  ```rust
  let mut off = 0;
  while off < bytes.len() {
      let r = {
          let mut st = self.engine.st.borrow_mut();
          let Some(c) = st.conns.get_mut(&self.ch) else { return -1 };
          c.conn.send_stream(self.id).write(&bytes[off..])
      };
      match r {
          Ok(n) => { off += n; self.engine.kick(); }          // notify drive → flush
          Err(WriteError::Blocked) => WriteBlocked::new(...).await, // parks write_waker
          Err(_) => return -1,                                 // Stopped / ClosedStream
      }
  }
  bytes.len() as i32
  ```

  The W2M slot (`send_slot`) is held across the loop and dropped when it
  returns — worker backpressure is preserved by QUIC stream flow control
  (the peer's 8 MiB receive window) instead of the kernel socket buffer.
  quinn's own send buffering is bounded by `send_window` (10 MB default), so
  a slow client cannot balloon master memory.
- `set_max_payload_len`: sets the assembler cap.
- `close()`: marks `closing` and kicks the drive task.

### Executor / bootstrap / CLI

- `main.rs`: parse `--quic-listen=`, `--tls-cert=`, `--tls-key=` (cert and
  key must be given together, and only with `--quic-listen`); extend
  `HELP_TEXT`. Pass `Option<QuicCli { listen: SocketAddr, cert_key: Option<(String, String)> }>`
  into `runtime::server_main`.
- `bootstrap.rs` (`server_main`, next to `server_create`): when configured,
  `tls::server_crypto(...)`, write `quic_dev_cert.pem` if minted,
  `posix_io::udp_bind(listen, 4 MiB, 4 MiB)`, `udp_local_addr(fd)`, write
  `<data_dir>/quic_endpoint` (`std::fs::write`, `IP:PORT\n`), log
  `Listening on quic://…`. Hand `Option<QuicServerInit { udp_fd, server_config }>`
  to `ServerExecutor::run`.
- `executor.rs`: when `Some`, build the `QuicEngine`
  (`Endpoint::new(Arc::new(EndpointConfig::default()), Some(server_config), true, None)`),
  an `mpsc::unbounded::<Peer>()` incoming-peer channel, and spawn two tasks:
  `quic::drive(...)` and

  ```rust
  async fn quic_accept_loop(shared: Rc<Shared>, mut rx: mpsc::Receiver<Peer>) {
      while let Some(peer) = rx.recv().await {
          let s = Rc::clone(&shared);
          shared.reactor.spawn(connection_loop(peer, s));
      }
  }
  ```

  `connection_loop`, HELLO validation, dispatch, scan trains: **zero changes**.

## Design — client

### ClientTransport

`gnitz-core/src/protocol/transport/mod.rs` gains the dispatch enum; the
framing helpers stay as the Unix implementation:

```rust
pub enum ClientTransport {
    Unix(std::os::fd::OwnedFd),
    Quic(Box<quic::QuicClientTransport>),
}

impl ClientTransport {
    /// `quic://HOST:PORT[?insecure|?ca=PATH]` → QUIC; anything else → AF_UNIX path.
    pub fn connect(target: &str) -> Result<Self, ProtocolError>;
    /// For tests / internal callers that already own a stream fd.
    pub fn from_unix_fd(fd: RawFd) -> Self;

    pub fn send_framed_iov(&mut self, bufs: &[&[u8]]) -> Result<(), ProtocolError>;
    pub fn send_framed_batch<F: AsRef<[u8]>>(&mut self, frames: &[F]) -> Result<(), ProtocolError>;
    pub fn recv_framed(&mut self, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError>;
    /// Handle that unblocks a recv_framed parked in another thread.
    pub fn waker(&self) -> TransportWaker;
}

pub enum TransportWaker {
    Unix(RawFd),              // dup'd fd; wake() = shutdown(SHUT_RDWR), Drop closes
    Quic(Arc<AtomicBool>),    // wake() = store(true); pump loop polls it
}
```

`message.rs` (`send_message*`, `recv_message`) and `connection.rs`
(`Connection` holds `transport: ClientTransport` instead of `sock: OwnedFd`)
swap `sock_fd: RawFd` parameters for `&mut ClientTransport` — mechanical.
`hello_handshake` is rewritten on the generic primitives: send the HELLO via
`send_framed_iov(&[&frame[4..]])` (byte-identical prefix), then one
`recv_framed(MAX_FRAME_PAYLOAD_CLIENT)` whose payload length discriminates
exactly as today (12 ⇒ ACK, anything else ⇒ STATUS_ERROR control block).

### QuicClientTransport (transport/quic.rs)

```rust
pub struct QuicClientTransport {
    socket: std::net::UdpSocket,        // connected to the server addr
    endpoint: quinn_proto::Endpoint,    // Endpoint::new(default, None, true, None)
    ch: quinn_proto::ConnectionHandle,
    conn: quinn_proto::Connection,
    stream: quinn_proto::StreamId,      // the one bidi stream, opened after Connected
    assembler: FrameAssembler,          // shared framing logic (module-local copy)
    frames: VecDeque<Vec<u8>>,
    closed: Arc<AtomicBool>,            // TransportWaker::Quic flag
    recv_buf: Vec<u8>,                  // 2048
    io_buf: Vec<u8>,                    // poll_transmit / handle scratch
}
```

Connect: parse target → resolve `HOST:PORT` via `ToSocketAddrs` (first
address) → bind `0.0.0.0:0` / `[::]:0` per family → `socket.connect(addr)` →
build rustls `ClientConfig` (ring provider, TLS 1.3, ALPN `gnitz/1`) with the
chosen verification mode:

- default: `RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())`;
- `?ca=PATH`: empty store + `add_parsable_certificates(CertificateDer::pem_file_iter(path))`;
- `?insecure`: `.dangerous().with_custom_certificate_verifier(Arc::new(SkipVerify))`
  where `SkipVerify` implements the four `rustls::client::danger::ServerCertVerifier`
  methods (`ServerCertVerified::assertion()`, `HandshakeSignatureValid::assertion()`,
  `supported_verify_schemes()` from
  `provider.signature_verification_algorithms.supported_schemes()`).

→ `QuicClientConfig::try_from(rustls_cfg)` → `quinn_proto::ClientConfig::new`
+ client transport config (idle 300 s, keep-alive 20 s, 8 MiB stream window,
GSO off) → `endpoint.connect(now, cfg, addr, host)` → pump until
`Event::Connected` with `deadline = now + 10 s` (covers both a blackholed
UDP path, which otherwise loops on 250 ms wakeups forever, and slow
handshakes) → `streams().open(Dir::Bi).unwrap()` (fresh quota — cannot
fail).

The pump — the sans-IO heartbeat every send/recv path loops through:

```rust
fn pump(
    &mut self,
    deadline: Option<Instant>, // Some ⇒ hard bound (connect: now + 10 s); None ⇒ wait forever
    mut until: impl FnMut(&mut Self) -> bool,
) -> Result<(), ProtocolError> {
    loop {
        // conn.poll_transmit(now, 1, ..) → socket.send, drained to None.
        // (Endpoint has NO poll_transmit in quinn-proto 0.11 — endpoint-level
        // packets arrive synchronously as DatagramEvent::Response below.)
        // flush_transmits maps ErrorKind::ConnectionRefused → no_server_err,
        // same as the recv path: Linux delivers the ICMP result on send too.
        self.flush_transmits()?;
        self.collect_events()?;                     // poll_endpoint_events→endpoint.handle_event; poll()→streams
        if until(self) { return Ok(()); }
        if self.closed.load(Ordering::Acquire) { return Err(closed_err()); }
        if deadline.is_some_and(|d| Instant::now() >= d) { return Err(timeout_err()); }
        let wait = self.conn.poll_timeout()
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_millis(250))
            .clamp(Duration::from_millis(1), Duration::from_millis(250));
        self.socket.set_read_timeout(Some(wait)).ok();
        match self.socket.recv(&mut self.recv_buf) {
            Ok(n) => {
                let bm = bytes::BytesMut::from(&self.recv_buf[..n]);
                let mut buf = std::mem::take(&mut self.io_buf);
                match self.endpoint.handle(Instant::now(), self.remote, None, None, bm, &mut buf) {
                    Some(DatagramEvent::ConnectionEvent(_, ev)) => self.conn.handle_event(ev),
                    Some(DatagramEvent::Response(t)) => { let _ = self.socket.send(&buf[..t.size]); }
                    _ => {}
                }
                self.io_buf = { buf.clear(); buf };
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
                self.conn.handle_timeout(Instant::now());
            }
            Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
                return Err(no_server_err(&self.remote));   // ICMP port-unreachable on connected UDP
            }
            Err(e) => return Err(ProtocolError::IoError(e)),
        }
    }
}
```

`collect_events` handles `Readable` (chunks → assembler → `frames`, always
`finalize()`), `Writable` (clears the write-blocked flag), and
`ConnectionLost { reason }` — `TimedOut` maps to an explicit
"QUIC connection idle-timed-out (no traffic for 300 s); reconnect" error.

- `send_framed_iov` / `send_framed_batch`: compute the same
  `frame_len_prefix`, then `SendStream::write` loops; on
  `WriteError::Blocked`, `pump(until Writable)`.
- `recv_framed`: `pump(until !frames.is_empty() || recv_closed)`, pop.

### Python / C surfaces

- **C API, Python sync**: no code changes — the connect string flows through
  `GnitzClient::connect` → `Connection::connect` → `ClientTransport::connect`.
  Docstring updates only (`gnitz.h`, `aio.py`).
- **Python async** (`gnitz-py/src/lib.rs`): `PyAsyncTransport::new` builds a
  `ClientTransport` (replacing `SocketGuard`), runs `hello_handshake(&mut t)`,
  takes `t.waker()` (replacing `DupFd`), and moves the transport into
  `async_io_loop`, whose `send_framed_batch(sock_fd, ..)` /
  `recv_framed(sock_fd, ..)` / `recv_scan_response(sock_fd, ..)` calls become
  transport method calls. `PyAsyncTransport::Drop` calls `waker.wake()` (the
  Unix waker keeps today's shutdown-on-drop effect; the QUIC flag makes the
  pump return within ≤250 ms). FIFO pipelining semantics are unchanged — the
  QUIC stream is order-preserving exactly like the fd.

## Test infrastructure

- **gnitz-test-harness**: `start_n` always appends `--quic-listen=127.0.0.1:0`
  (port 0 ⇒ safe under parallel `cargo test`); new
  `pub fn quic_target(&self) -> String` polls `<data_dir>/quic_endpoint`
  (same backoff loop as the socket wait) and returns
  `quic://127.0.0.1:PORT?insecure`; `pub fn quic_ca_target(&self)` returns the
  `?ca=<data_dir>/quic_dev_cert.pem` form.
- **pytest conftest** (`gnitz-py/tests/conftest.py`): the QUIC address must
  be as stable across server restarts as the socket path is (the sock path
  deliberately lives outside the per-restart data dir; an ephemeral QUIC
  port re-read from the fresh data dir would go stale on every
  `_server_guard` restart and cascade errors through the rest of the
  session). So the `_Server` allocates **one** free UDP port at construction
  (bind `127.0.0.1:0`, read it back, close) and passes that same fixed
  `--quic-listen=127.0.0.1:PORT` on *every* spawn, including restarts. A
  `_Server.target` property returns the sock path, or
  `quic://127.0.0.1:PORT?insecure` when `GNITZ_TRANSPORT=quic`. **Both** the
  `server` fixture *and* the `client` fixture (which today dials
  `_srv.sock_path` directly) go through `target` — routing only `server`
  would leave most of the suite on AF_UNIX while claiming QUIC coverage.
  All server spawns must flow through the single `_spawn` helper that
  appends the flag; any additional spawn helper added to this conftest must
  reuse it.
- **Makefile**:

  ```make
  e2e-quic: WORKERS = 4
  e2e-quic: server pyext ## Run the full E2E suite over QUIC
  	cd crates/gnitz-py && GNITZ_TRANSPORT=quic GNITZ_WORKERS=$(WORKERS) \
  		uv run pytest tests/ -v $(if $(K),-k '$(K)')
  ```

  (The `WORKERS = 4` target override matters: the global default is 1, and
  single-worker runs skip the exchange/fanout paths.)

## Edge cases

- **`Incoming` is never dropped** — every `NewConnection` is resolved via
  `accept` (or the `AcceptError.response` transmit is sent); dropping it
  leaks endpoint state by contract.
- **Datagram loss on engine sends** is deliberate fire-and-forget; QUIC loss
  recovery retransmits. The persistent recv future prevents the
  select-cancel datagram drop described in the drive loop.
- **`Chunks::finalize` flow-control credit**: every read path finalizes its
  `Chunks` before returning, and the enclosing drive/pump loop always runs a
  `poll_transmit` pass afterward, satisfying the `ShouldTransmit` contract.
- **Frames larger than the 8 MiB stream window** (up to 64 MB server-bound /
  256 MB client-bound) stream through incrementally: the reader drains as
  long as the consumer keeps up, and the writer parks on
  `WriteError::Blocked` → `Writable`. The watermark pause cannot wedge —
  `paused` requires `inbound_bytes >= 8 MiB`, so the queue is non-empty,
  `connection_loop` consumes strictly FIFO from that queue, and its pops
  cross the low-water mark and resume reading; a partially assembled frame
  therefore always eventually gets its remaining bytes.
- **Zero-length close sentinel**: the assembler rejects `len == 0` frames on
  both sides, matching `recv_framed`/`RecvState`. QUIC connections close via
  `CONNECTION_CLOSE`/FIN, not the sentinel.
- **Worker fork ordering**: the UDP socket is created in `server_main` after
  the workers fork (same position as `server_create`), so no fd inheritance.
- **Server shutdown** keeps L6 semantics: in-flight QUIC connections are not
  drained; clients observe idle timeout / reset. No new graceful-shutdown
  machinery.
- **`quic_endpoint`/`quic_dev_cert.pem` staleness**: written on every boot
  before "GnitzDB ready", truncating any previous content.
- **DNS**: `ToSocketAddrs` resolution, first result; an unresolvable host is
  a connect-time `ProtocolError`.
- **ALPN mismatch / wrong CA / bad cert**: surfaces as a handshake
  `ConnectionLost` within the 10 s connect deadline, mapped to a
  `ProtocolError` carrying the TLS alert text.

## File changes

1. `crates/gnitz-engine/src/runtime/quic/{mod,stream,tls}.rs` (new, ~700
   lines total): engine, drive task, `FrameAssembler`, `QuicPeer` internals,
   TLS/cert building.
2. `crates/gnitz-engine/src/runtime/orchestration/peer.rs`: `PeerInner::Quic`
   variant + `RecvBuf::Vec` variant (~60 lines).
3. `crates/gnitz-engine/src/runtime/orchestration/executor.rs`: optional
   `QuicServerInit` parameter, engine construction, `quic_accept_loop`
   (~50 lines).
4. `crates/gnitz-engine/src/main.rs` + `runtime/bootstrap.rs`: CLI flags,
   UDP bind, endpoint/dev-cert files (~90 lines).
5. `crates/gnitz-core/src/protocol/transport/mod.rs`: `ClientTransport`,
   `TransportWaker`, target parsing (~180 lines delta);
   `transport/quic.rs` (new, ~450 lines): pump, SkipVerify, config building.
6. `crates/gnitz-core/src/protocol/message.rs` + `connection.rs`: `RawFd` →
   `&mut ClientTransport` (mechanical).
7. `crates/gnitz-py/src/lib.rs`: `PyAsyncTransport` re-plumb (~80 lines
   delta). `crates/gnitz-capi/gnitz.h`, `gnitz-py/python/gnitz/aio.py`:
   docstrings.
8. `crates/gnitz-test-harness/src/lib.rs`: quic listener + `quic_target`
   (~60 lines).
9. `crates/gnitz-py/tests/conftest.py`, `tests/test_quic.py` (new),
   `Makefile` (`e2e-quic`).
10. Cargo.toml additions per the dependency table.

## Tests

**Rust unit (engine `quic::tls`)**: dev-cert generation feeds
`QuicServerConfig::try_from` successfully; PEM cert+key written to a tempdir
round-trip through the file-loading path.

**Rust unit (gnitz-core)**: target parsing (`quic://h:p`, `?insecure`,
`?ca=`, bare paths, malformed → error); `FrameAssembler` (split delivery,
pipelined frames, oversize reject, zero-len reject).

**Rust integration (`gnitz-core/tests/quic_client.rs`, via the harness)**:
1. `?insecure` connect + HELLO + `alloc_table_id` roundtrip.
2. `?ca=quic_dev_cert.pem` connect succeeds; default webpki roots against the
   dev cert **fails** with a certificate error.
3. Push → scan roundtrip over QUIC (data integrity, weights).
4. 16 MB push (exceeds the 8 MiB stream window — exercises Blocked/Writable
   on the client and continuous drain on the server).
5. UNIX + QUIC clients concurrently against the same table.
6. Wire-version mismatch HELLO over QUIC → server STATUS_ERROR frame, clean
   close.

**E2E (`test_quic.py`, runs inside the normal suite — the listener is always
on in the test conftest)**: sync + async Python clients over
`quic://…?insecure`; multi-worker scan with continuation frames
(`GNITZ_WORKERS=4` exercises the `send_slot` copy path per worker train);
pipelined async pushes; error surfaces (bad CA path, unreachable port →
clear errors).

**Full-suite sweep**: `make e2e-quic` runs the entire pytest suite over QUIC.
This is the strongest check: the wire protocol is transport-invariant, so
every existing test must pass byte-for-byte semantics unchanged.

## Implementation order

- [ ] 1. `gnitz-core`: `ClientTransport` enum (Unix-only) + `TransportWaker`;
       re-plumb `message.rs`/`connection.rs`/`hello_handshake`/`gnitz-py`
       async loop. Pure refactor; `make test` + `make e2e` green.
- [ ] 2. `gnitz-core`: QUIC client transport (parsing, TLS config, SkipVerify,
       pump, framing) + unit tests. Compiles green; integration deferred to 3.
- [ ] 3. `gnitz-engine`: deps, `quic::tls`, `quic` engine + `QuicPeer` +
       executor/bootstrap/CLI wiring; harness QUIC support; Rust integration
       tests 1–6. `make test` green.
- [ ] 4. E2E: conftest always-on listener + `GNITZ_TRANSPORT`, `test_quic.py`,
       `Makefile` `e2e-quic`. `make e2e` and `make e2e-quic` green.
