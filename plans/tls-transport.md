# TLS Network Transport (TCP + rustls, server + client)

## Problem

gnitz clients can reach the server only over an AF_UNIX socket. This plan
adds the network transport: TCP with TLS 1.3 (rustls), carrying the existing
wire protocol unchanged. "ZSets over the wire" is untouched — the 4-byte-LE
length-prefixed frames (control + schema + data WAL blocks) ride verbatim
inside one TLS-encrypted TCP stream per connection.

TCP + rustls is the committed transport (QUIC/quinn-proto was evaluated and
rejected): the kernel does segmentation, ACKs, retransmission, congestion
control, and flow control, so the engine needs no timer subsystem, no
packetization loop, no credit protocol, and no idle/keep-alive machinery;
TLS records are 16 KiB (an order of magnitude fewer AEAD calls per byte
than 1.4 KB QUIC packets), putting bulk throughput far closer to the
AF_UNIX baseline; the blocking client stays trivially blocking (a kernel
`recv` is the wait — connections idle forever, exactly like AF_UNIX); and a
server restart surfaces as an immediate RST/EOF, not a timeout. rustls's
buffered byte-in/byte-out API (`read_tls`/`process_new_packets`/`reader`/
`writer`/`write_tls`) is the same battle-tested engine `tokio-rustls` and
`StreamOwned` are built on — no TLS record machinery is hand-rolled. What
is given up: QUIC's stream-multiplexing headroom and connection migration,
neither used by any committed design today; pre-alpha rules make adopting
QUIC later free of compatibility cost if that changes.

Baseline facts this plan builds on (all verified against the tree):

- The server dispatch layer addresses a client through the `Peer` handle in
  `crates/gnitz-engine/src/runtime/orchestration/peer.rs` — a struct over a
  `PeerInner` enum with a single `Unix` variant. `connection_loop` takes the
  `Peer` by value; every handler and scan-train forwarder takes `&Peer` and
  replies via `peer.send_buffer_or_close` / `peer.send_slot_or_close`.
  `Peer::recv()` yields `RecvBuf` frames — today a malloc-backed
  `{ptr, len}` struct in `runtime/reactor/io.rs`, freed in `Drop`.
- The reactor's stream path is AF-agnostic: `register_conn`,
  `handle_recv_cqe`, and the send machinery key only on the fd and issue
  `prep_recv`/`prep_send`; nothing is AF_UNIX-specific. The single-listener
  assumption is shallow — `server_fd: Cell<i32>` plus one multishot-accept
  SQE whose `udata(kind, id)` helper already carries an unused `id` field,
  and the accepted-fd path does not care which listener produced it. The
  async toolkit in `runtime/reactor/sync.rs` provides `mpsc`, `select2`,
  and `AsyncMutex`; wakers push a task key onto the run queue (a `wake()`
  never runs the task inline).
- The reactor's fd framing (`RecvState`) runs on the *socket bytes* inside
  the CQE handler. Under TLS the socket bytes are ciphertext, so TLS
  connections bypass that machinery: framing runs on the decrypted stream
  in a per-connection module (below), and the reactor only needs raw
  byte-stream ops.
- The fd path's inbound queue is **unbounded by design**: `handle_recv_cqe`
  re-arms the next header recv unconditionally on `MessageDone`, and the
  `pending_recv` doc comment calls this "load-bearing for TCP flow control"
  — with a bounded slot, a pipelined client deadlocks in a four-party cycle
  (client blocked mid-send-batch ← server stopped reading ←
  `connection_loop` blocked sending a scan train ← train blocked on the
  client, which won't read until its send completes). The TLS path MUST
  reproduce these semantics: an always-reading pump with an unbounded
  frame queue, guarded by the reactor's inbound memory caps (see the
  decisions table) — close-on-violation, a guard against runaway
  pipelining, never a pause.
- The client's framing (`gnitz-core/src/protocol/transport/mod.rs`) is a
  set of AF-agnostic free functions over `sock_fd: RawFd`
  (`send_framed_iov`, `send_framed_batch`, `recv_framed`,
  `hello_handshake`); only `transport/unix.rs::connect_unix` is
  transport-specific. The module header currently declares network
  transports out of scope with a stale rationale — replace it. Frame caps
  live in `gnitz-wire::handshake`: `MAX_FRAME_PAYLOAD_SERVER = 64 MB`
  (server-side accept limit), `MAX_FRAME_PAYLOAD_CLIENT = 256 MB`,
  `HELLO_PAYLOAD_LEN = 8`, `HELLO_ACK_PAYLOAD_LEN = 12`. `gnitz-wire`
  needs zero changes.
- The reactor's UDP datagram ops (`runtime/reactor/udp.rs`, the
  `KIND_UDP_RECV`/`KIND_UDP_CANCEL` kinds and `ReactorShared` UDP fields,
  `Ring::prep_sendmsg`/`prep_recvmsg`, `posix_io::{udp_bind,
  udp_local_addr}`) were built for the abandoned QUIC transport and have
  no other consumer. **This plan deletes them** (no-legacy rule); the
  sockaddr helpers (`sockaddr_from_addr`/`addr_from_sockaddr`) stay — TCP
  bind/local-addr reuse them.

## Committed decisions

| Decision | Choice |
|---|---|
| TLS stack | rustls 0.23, **ring** provider (feature `ring` — no cmake/C++ toolchain), TLS 1.3 only, buffered `ServerConnection`/`ClientConnection` API (the `unbuffered` API buys nothing here: gnitz is std, moves large frames, and the buffered loop is the far more trodden path) |
| ALPN | `b"gnitz/1"` on both sides (server list non-empty ⇒ mismatch fails the handshake) |
| Server I/O model | per-connection, three tasks: a **read pump** (raw recv → `read_tls` → `process_new_packets` → plaintext → frame queue; never locks, never sends), a **flusher** (the only task that may *park* on the send mutex: TLS control bytes, close_notify, socket shutdown, the one `close(fd)`), and the existing `connection_loop` consuming via `Peer::recv()`. No drive task, no timers, no flow-control protocol — the kernel owns all of that |
| Client I/O model | blocking `std::net::TcpStream` + `rustls::StreamOwned<ClientConnection, TcpStream>` — no async runtime, no threads, no pump. A blocking read IS the wait |
| HELLO | unchanged, first frames on the stream (keeps the `bound_client_id` auth hook and the pre-handshake 8-byte frame cap) |
| Idle handling | none — TCP connections idle forever (kernel keepalive off, matching AF_UNIX). No keep-alive, no idle timeout, no divergence |
| Inbound bound | unbounded frame queue per connection (fd-path parity; bounded-slot designs deadlock pipelined clients, see baseline facts) guarded by the reactor's inbound memory accounting: `frame_weight(len) = len.max(64)` per held frame, against a per-connection ceiling (`INBOUND_CAP_PER_CONN`, 1 GiB) and a global ceiling (`GNITZ_INBOUND_MEM_BYTES`, default 4 GiB) — constants in `reactor/io.rs`, counter `Cell`s in `ReactorShared`, created if not already present; the TLS pump increments/decrements the same counters the Unix path uses. Breach **closes** the connection with a log line, never pauses |
| Nagle | `TCP_NODELAY` on every TCP socket, both sides (AF_UNIX has no Nagle; without this, small control frames pay 40 ms batching delays) |
| Connection limit | `MAX_TLS_CONNS = 64`: the accept path closes the fd immediately (before any TLS work) when the live TLS-connection count is at the cap |
| Server CLI | `--tls-listen=IP:PORT` (port 0 ⇒ ephemeral), `--tls-cert=PEM --tls-key=PEM`; without cert+key a self-signed dev cert is minted (rcgen) for `localhost`/`127.0.0.1`/`::1` (rcgen 0.14 emits real IP SANs; rustls verifies IP `ServerName`s against them) and its PEM is written to `<data_dir>/tls_dev_cert.pem` with a log line noting the identity is ephemeral, regenerated every boot. Bound address written **atomically** (write `tls_endpoint.tmp`, `rename`) to `<data_dir>/tls_endpoint` (`IP:PORT\n`) after bind |
| Exposure posture | the listener is **unauthenticated** — `bound_client_id` is a real hook but nothing populates it today, so anyone reaching the port has full DDL/DML/scan. Boot logs a loud warning when binding a non-loopback address; HELP_TEXT states it plainly. The client prints a one-time stderr warning when `?insecure` targets a non-loopback host (refusing would block LAN dev testing — the dev cert has only loopback SANs, so `?ca=` can't cover it) |
| Client address syntax | `tls://HOST:PORT`, optional `?insecure` (skip verification) or `?ca=PATH` (PEM root override); default roots = `webpki-roots`. Any non-`tls://` string stays an AF_UNIX path — all four API surfaces (Rust / C / Python sync / Python async) keep their signatures |
| Connect deadline | `TcpStream::connect_timeout(10 s)`; `set_read_timeout(Some(10 s))` for the handshake + HELLO exchange, cleared to `None` (block forever) after the ACK |
| Dependency gating | unconditional deps, no cargo feature (pre-alpha; no compat concerns) |

Dependency additions (versions current as of 2026-07-02: rustls 0.23.41,
rustls-pki-types 1.15, rcgen 0.14.8, webpki-roots 1.0.8):

```toml
# gnitz-engine
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pki-types = "1"
rcgen = "0.14"

# gnitz-core
rustls = { version = "0.23", default-features = false, features = ["ring", "std"] }
rustls-pki-types = "1"
webpki-roots = "1"
```

(Effective toolchain floor becomes Rust 1.88 via rcgen 0.14.8. No
quinn-proto, no `bytes`.)

## Design — reactor additions & deletions

**Delete** (dead code once QUIC is abandoned): `runtime/reactor/udp.rs`
(~446 lines incl. tests), `KIND_UDP_RECV`/`KIND_UDP_CANCEL` and their
dispatch arms, the four `ReactorShared` UDP fields,
`Ring::prep_sendmsg`/`prep_recvmsg`, `posix_io::{udp_bind, udp_local_addr}`
and their unit tests. Keep `sockaddr_from_addr`/`addr_from_sockaddr`.

**Add** three small primitives:

- `posix_io::tcp_bind(addr: SocketAddr) -> i32`: socket(`SOCK_STREAM`) +
  `SO_REUSEADDR` + bind (via `sockaddr_from_addr`) + `listen(backlog)` +
  `O_NONBLOCK`, mirroring `server_create`; and
  `posix_io::tcp_local_addr(fd) -> Option<SocketAddr>` (getsockname via
  `addr_from_sockaddr`) for port-0 discovery.
- **Multi-listener accept**: `attach_server_fd` generalizes to
  `attach_listener(fd)` callable per listener; the multishot-accept SQE
  carries the **listener fd in the udata `id` field** (today always 0), the
  KIND_ACCEPT handler pushes `(conn_fd, listener_fd)` onto `accept_queue`
  and re-arms the specific listener from the udata on `CQE_F_MORE == 0`;
  `Reactor::accept()` resolves `(i32, i32)`. `server_fd: Cell<i32>` is
  replaced by the udata round-trip. The EMFILE/ENFILE backoff becomes
  per-listener: today one `accept_backoff_armed` flag re-arms the single
  `server_fd`, but fd exhaustion is global — both multishot accepts cancel
  in the same window, the second CQE is dropped, and one listener stays
  permanently deaf. Keep a pending-re-arm set that the single 50 ms
  backoff timer drains, re-arming every listed listener. The accept unit
  tests (`reactor/mod.rs:3338-3371`) update for the `(fd, listener)`
  queue shape.
- **Raw stream ops** (TLS ciphertext bypasses the framed `RecvState`
  machinery): `Reactor::recv_raw(fd, buf: Vec<u8>) -> (Vec<u8>, i32)` — a
  one-shot recv into the caller's buffer, resolving with the byte count
  (≤ 0 = EOF/error); the buffer round-trips so the pump reuses one 64 KiB
  allocation forever. `recv_raw` gets its own completion kind
  `KIND_RAW_RECV` (+ a cancel kind) with per-fd park/waker/tombstone maps
  mirroring the deleted UDP-recv machinery — it must NOT ride `KIND_RECV`,
  whose handler silently drops CQEs for fds absent from `conns` (TLS fds
  never call `register_conn`). `Reactor::send_raw(fd, Vec<u8>) -> i32` —
  sends the whole buffer, looping partial `OP_SEND` completions like the
  existing send path (which already loops partials), riding the existing
  `KIND_SEND`/send-waker routing (its absent-`conns`-key lookup is a
  benign no-op) with a new `SendAlive::Vec(Vec<u8>)` keep-alive variant
  replacing the deleted `UdpOp`. Both follow the UDP ops' drop discipline
  (drop = AsyncCancel + tombstone); `prep_sendmsg`/`prep_recvmsg` deletion
  hits both the `Ring` trait and the `IoUringRing` impl.

## Design — server

New module `crates/gnitz-engine/src/runtime/tls/`: `mod.rs` (conn state,
read pump, `TlsPeer`), `frame.rs` (`FrameAssembler`), `config.rs`
(rustls/rcgen building).

### config.rs

Identical to the audited QUIC-plan `tls.rs` minus the quinn wrapper: load
PEM chain/key via `rustls-pki-types` `PemObject`
(`CertificateDer::pem_file_iter`, `PrivateKeyDer::from_pem_file`) or mint
the rcgen dev cert (`generate_simple_self_signed`, field `signing_key`,
PKCS#8 via `serialize_der`); build
`rustls::ServerConfig::builder_with_provider(ring::default_provider())
.with_protocol_versions(&[&TLS13])?.with_no_client_auth()
.with_single_cert(chain, key)?`, set `alpn_protocols = [b"gnitz/1"]`.
Returns `(Arc<rustls::ServerConfig>, Option<String /*dev PEM*/>)`.

### frame.rs

`FrameAssembler` mirrors the reactor's `RecvState` on owned buffers:
consume plaintext bytes, cut `[len:u32 LE][payload]` frames, reject
`len == 0` (close sentinel) and `len > max_payload_len` (initially
`HELLO_PRE_HANDSHAKE_LEN = 8`, elevated by `Peer::set_max_payload_len`
after HELLO — `run_hello_handshake` elevates synchronously *before*
sending the ACK, so no conforming client's frame can hit the 8-byte cap).
A violation closes the connection. Deliberately *stricter* than the fd
path in one corner: the pump parses stream bytes on arrival, so a client
that pipelines a >8-byte frame behind HELLO before the ACK gets closed
here, while the fd path leaves those bytes unparsed in the kernel buffer
until the cap is elevated. No conforming client pipelines pre-ACK
(`hello_handshake` blocks on the ACK) — intended hardening, not a
compatibility break.

### Connection state and read pump

```rust
struct TlsConn {
    sess: rustls::ServerConnection,
    assembler: FrameAssembler,
    inbound: VecDeque<Vec<u8>>, // complete frame payloads awaiting Peer::recv
    inbound_bytes: usize,       // Σ frame_weight(len); reactor cap accounting
    recv_waker: Option<Waker>,  // single slot: exactly one connection_loop recvs
    recv_closed: bool,          // EOF / close_notify / TLS error / cap breach
    closed: bool,               // Peer::close() ran; senders refuse, flusher tears down
    pump_exited: bool,          // pump done; fd may be closed once `closed` too
}

struct TlsShared {
    reactor: Rc<Reactor>,
    fd: i32,
    state: RefCell<TlsConn>,
    send_mutex: AsyncMutex,      // serializes ciphertext extraction + send_raw
    flush_tx: mpsc::Sender<()>,  // wakes the flusher task
    conn_guard: TlsConnGuard,    // RAII: decrements the live-conn counter on Drop
}
```

Two lifecycle flags in `TlsConn` drive teardown: `closed` (set once by
`Peer::close()`) and `pump_exited` (set by the pump on its way out).
`write_tls` dequeues *sequenced* encrypted records — extracting ciphertext
and then failing to send it holes the record stream and kills the session
— so ALL ciphertext extraction happens under `send_mutex`, in exactly two
places: a sender's per-chunk drain, and the flusher.

The single `recv_waker` slot encodes the same invariant as the fd path:
exactly one `connection_loop` task recvs and sends per peer (strictly
sequential — recv → handle → reply; scan trains send from the same task).
Sends never park: `send_raw` completes when the kernel accepts the bytes,
so outbound backpressure is the kernel socket buffer, exactly like the fd
path (a W2M slot held across the await preserves worker backpressure
identically).

The read pump — never locks `send_mutex`, never extracts ciphertext, never
sends. A sender mid-large-frame parks on kernel sndbuf space, which needs
the client to read, which (for a pipelining client) needs the pump to keep
reading — any pump-waits-on-sender design re-forms the four-party
deadlock. When rustls has control bytes to emit (handshake flights,
session tickets, KeyUpdate responses, alerts), the pump only *notifies*
the flusher:

```rust
async fn read_pump(conn: Rc<TlsShared>, reactor: Rc<Reactor>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let (b, n) = reactor.recv_raw(conn.fd, buf).await;
        buf = b;
        if n <= 0 { break; }                                   // EOF / error / shutdown
        let done = {
            let mut c = conn.state.borrow_mut();               // never held across await
            let mut cipher = &buf[..n as usize];
            let mut fatal = false;
            while !cipher.is_empty() && !fatal {
                match c.sess.read_tls(&mut cipher) {
                    Ok(0) | Err(_) => fatal = true,
                    // process after EACH read_tls, per the rustls docs —
                    // never let the deframer accumulate a whole chunk
                    Ok(_) => match c.sess.process_new_packets() {
                        Err(_) => fatal = true,                // fatal alert queued by rustls
                        Ok(io) => {
                            let mut pt = [0u8; 32 * 1024];
                            loop {                             // drain ALL plaintext
                                match c.sess.reader().read(&mut pt) {
                                    Ok(0) => { fatal = true; break; }     // clean close
                                    Ok(m) => c.feed_assembler(&pt[..m]),  // frames → inbound,
                                                                          // cap + len checks
                                    Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                                    Err(_) => { fatal = true; break; }
                                }
                            }
                            if io.peer_has_closed() { fatal = true; }
                        }
                    },
                }
            }
            if fatal { c.recv_closed = true; }
            fatal
        };
        if let Some(w) = conn.take_recv_waker() { w.wake(); }
        if conn.wants_write() { conn.notify_flusher(); }
        if done { break; }
    }
    {
        let mut c = conn.state.borrow_mut();
        c.recv_closed = true;
        c.pump_exited = true;
    }
    if let Some(w) = conn.take_recv_waker() { w.wake(); }
    conn.notify_flusher();                                     // run teardown
}
```

The flusher — the only task that may *park* on `send_mutex`, and the one
place the fd is ever closed:

```rust
async fn flusher(conn: Rc<TlsShared>, reactor: Rc<Reactor>, mut rx: mpsc::Receiver<()>) {
    let mut shutdown_sent = false;
    loop {
        let closed_seen = conn.closed();          // sampled before the flush, so a
                                                  // close_notify queued by close() ships
        let out = {
            let _g = conn.send_mutex.lock().await;
            let mut c = conn.state.borrow_mut();
            let mut out = Vec::new();
            while c.sess.wants_write() { let _ = c.sess.write_tls(&mut out); }
            out
        };
        if !out.is_empty() { let _ = reactor.send_raw(conn.fd, out).await; }
        if closed_seen && !shutdown_sent {
            posix_io::shutdown_rdwr(conn.fd);     // completes the pump's in-flight recv (EOF)
            shutdown_sent = true;
        }
        if conn.closed() && conn.pump_exited() {
            posix_io::close_fd(conn.fd);          // the ONE close; TlsShared's guard Drop
            return;                               // decrements the live-conn counter
        }
        if rx.recv().await.is_none() { return; }
    }
}
```

**Teardown converges from either direction.** `Peer::close()` stays a
**sync** fn (its many call sites are unchanged): the first transition of
`closed` calls `sess.send_close_notify()` under the borrow and notifies
the flusher; repeat calls are no-ops. Local close: flusher flushes the
close_notify → `shutdown(SHUT_RDWR)` (new `posix_io` helper) completes the
pump's in-flight `recv_raw` with EOF → pump exits and notifies → flusher
sees `closed && pump_exited` → `close(fd)`. Remote EOF/error: pump exits
and notifies → `connection_loop` drains the queue, gets `None`, calls
`peer.close()` at its exit (it always does) → same convergence. Notes that
make this airtight: `Reactor::close_fd` is a no-op for unregistered fds
and a plain `libc::close` neither completes an in-flight io_uring recv nor
prevents a task from touching a recycled fd number — hence shutdown-first,
close-once-both-tasks-are-done. In-flight `send_raw` buffers are
reactor-owned keep-alives until their CQE, and senders check `closed`
under the borrow in the same poll as submission, so nothing can submit on
a recycled fd. The live-connection counter is RAII (`TlsConnGuard` built
at accept, decrement in `Drop` when the last `Rc<TlsShared>` goes away) —
a panicking task can never leak a `MAX_TLS_CONNS` slot.

The TLS handshake needs no separate phase: the pump's read/process cycle
*is* the handshake driver and the flusher ships the emitted flights (no
sender contends pre-HELLO — `connection_loop` is parked in `recv`);
plaintext (HELLO) appears once the handshake completes, and
NewSessionTickets queue in the same `process_new_packets` that consumes
the client Finished and ship the same way. Control bytes queued while a
sender holds the mutex are never lost: the sender's own post-chunk drain
picks them up, and the pump's notify makes the flusher sweep whatever
remains after the mutex is released. The pump reads unconditionally —
fd-path parity — so a pipelining client's sends always drain (no
four-party deadlock); memory is guarded by the inbound caps, whose breach
sets `recv_closed`, logs fd and accounted backlog, and lets teardown
reclaim the queue. Guard the chunked send loop against a zero-length
`writer().write` acceptance (drain, then retry) so a full rustls buffer
can never spin it.

### TlsPeer (the `PeerInner::Tls` variant)

`peer.rs` gains `Tls(Rc<TlsShared>)`.

- `recv()`: future that pops `inbound` (returning `RecvBuf::Vec(Vec<u8>)` —
  `RecvBuf`, today a malloc-backed `{ptr, len}` struct in
  `runtime/reactor/io.rs`, becomes a two-variant enum with
  `as_slice`/`Drop` dispatch), decrementing `inbound_bytes`; parks in
  `recv_waker` when empty; resolves `None` once `recv_closed` and the
  queue is empty.
- `send_buffer` / `send_slot` / `send_hello_ack`: one shared loop over the
  complete frame bytes (already length-prefixed exactly as the fd path
  sends them):

  ```rust
  async fn send_bytes(&self, bytes: &[u8]) -> i32 {
      let _g = self.conn.send_mutex.lock().await;   // vs pump's handshake flushes
      let mut off = 0;
      while off < bytes.len() {
          let cipher = {
              let mut c = self.conn.state.borrow_mut();
              if c.closed || c.recv_closed { return -1; }
              let end = (off + 32 * 1024).min(bytes.len());   // stay under rustls's
              match c.sess.writer().write(&bytes[off..end]) { // 64 KiB buffer_limit
                  Ok(n) => off += n,
                  Err(_) => return -1,
              }
              let mut out = Vec::new();
              while c.sess.wants_write() { let _ = c.sess.write_tls(&mut out); }
              out
          };
          if self.reactor.send_raw(self.fd(), cipher).await < 0 { return -1; }
      }
      bytes.len() as i32
  }
  ```

  Chunked writes keep rustls's plaintext buffer under its default 64 KiB
  limit and bound transient ciphertext memory; the awaited `send_raw` is
  where kernel backpressure lands.
- `set_max_payload_len`: sets the assembler cap (synchronous `RefCell`
  write).
- `close()`: sync and idempotent — the first transition of `closed` calls
  `sess.send_close_notify()` under the borrow and notifies the flusher,
  which flushes, shuts the socket down, and (once the pump has exited)
  closes the fd — the teardown protocol above. No call site changes.

### Executor / bootstrap / CLI

- `main.rs`: parse `--tls-listen=`, `--tls-cert=`, `--tls-key=` (cert and
  key together, and only with `--tls-listen`); extend `HELP_TEXT`
  (including the unauthenticated-listener statement). Pass
  `Option<TlsCli { listen: SocketAddr, cert_key: Option<(String, String)> }>`
  into `runtime::server_main`.
- `bootstrap.rs` (`server_main`, next to `server_create` — after the worker
  fork, so no fd inheritance): when configured, `config::server_crypto(...)`,
  write `tls_dev_cert.pem` if minted, `tcp_bind(listen)`,
  `tcp_local_addr(fd)`, write `<data_dir>/tls_endpoint` atomically
  (tmp + rename), log `Listening on tls://…` plus the loud
  unauthenticated-listener warning when non-loopback. Hand
  `Option<TlsServerInit { listen_fd, server_config }>` to
  `ServerExecutor::run`.
- `executor.rs`: when `Some`, `reactor.attach_listener(tls_fd)` next to the
  unix listener; `accept_loop` matches the listener fd of each accepted
  `(fd, listener)` pair: unix → today's path verbatim; TLS → if the live
  count is at `MAX_TLS_CONNS`, `posix_io::close(fd)` and log; else set
  `TCP_NODELAY`, build `rustls::ServerConnection::new(cfg)`, create the
  `TlsShared` (constructing the RAII live-conn guard), spawn `read_pump`
  and `flusher`, wrap `Peer::tls(...)`, spawn `connection_loop`.
  `connection_loop`, HELLO validation, dispatch, scan trains: **zero
  changes**.

## Design — client

### ClientTransport

`gnitz-core/src/protocol/transport/mod.rs` gains the dispatch enum; the
framing free functions stay as the Unix implementation:

```rust
pub enum ClientTransport {
    Unix(std::os::fd::OwnedFd),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, std::net::TcpStream>>),
}

impl ClientTransport {
    /// `tls://HOST:PORT[?insecure|?ca=PATH]` → TLS; anything else → AF_UNIX path.
    pub fn connect(target: &str) -> Result<Self, ProtocolError>;
    /// For tests / internal callers that already own a stream fd.
    pub fn from_unix_fd(fd: RawFd) -> Self;

    pub fn send_framed_iov(&mut self, bufs: &[&[u8]]) -> Result<(), ProtocolError>;
    pub fn send_framed_batch<F: AsRef<[u8]>>(&mut self, frames: &[F]) -> Result<(), ProtocolError>;
    pub fn recv_framed(&mut self, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError>;
    /// Handle that unblocks a recv_framed parked in another thread.
    pub fn waker(&self) -> TransportWaker;
}

/// One shape for both transports: a dup'd fd (the unix socket, or the
/// TcpStream under StreamOwned via as_raw_fd); wake() = shutdown(SHUT_RDWR)
/// — unblocks a kernel-parked read either way. Drop = shutdown + close,
/// a true drop-in for today's DupFd (whose Drop does both): the gnitz-py
/// teardown wakes the I/O thread by DROPPING the waker, so a Drop that
/// only closed the dup would leave that thread parked forever.
pub struct TransportWaker(RawFd);
```

- Unix variant: today's free functions, verbatim.
- Tls variant: `send_framed_*` coalesce the length prefix + payload (and,
  in `send_framed_batch`, all frames) into a single buffered `write_all` —
  each separate `writer().write` emits its own TLS record, so naive
  per-piece writes would cost two records (2 AEAD ops + 2×~22 B) per small
  control frame; one write lets rustls fragment into full 16 KiB records
  internally. `recv_framed` = `read_exact` a 4-byte prefix,
  validate against `max_payload_len` and the zero-length sentinel exactly
  like the fd path, `read_exact` the payload. `StreamOwned` drives the
  handshake implicitly on first use and handles partial record I/O
  internally — the blocking `TcpStream` is the pump. A rustls handshake
  failure (bad cert, ALPN mismatch, wrong CA) surfaces as an
  `io::Error` wrapping the rustls alert on that first read/write, mapped to
  a `ProtocolError` carrying the alert text; the 10 s read timeout bounds a
  fully-silent stalled handshake (SO_RCVTIMEO is per-read, so a byte-drip
  server can stretch it — accepted for this posture, no total-deadline
  machinery), and it is cleared after the HELLO ACK.
- Connect: parse target — host extraction handles bracketed IPv6
  (`tls://[::1]:PORT`: strip the brackets for `ServerName`/verification,
  keep them for `ToSocketAddrs`; otherwise split on the trailing `:port`)
  → resolve via `ToSocketAddrs` (first address) →
  `TcpStream::connect_timeout(addr, 10 s)` → `set_nodelay(true)` →
  `set_read_timeout(Some(10 s))` → rustls `ClientConfig` (ring provider,
  TLS 1.3, ALPN `gnitz/1`) with the chosen verification mode — default
  `RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned())`;
  `?ca=PATH` empty store + `add_parsable_certificates(pem_file_iter)`;
  `?insecure` a `SkipVerify` `ServerCertVerifier` (assertion() stubs +
  `supported_verify_schemes` from the provider), with the one-time stderr
  warning when the host is non-loopback → `ServerName::try_from(host)`
  (DNS or IP — both supported) → `ClientConnection::new` → `StreamOwned`.
- A server restart or crash surfaces as EOF/`ECONNRESET` on the next call —
  immediately, like AF_UNIX. No idle timeout exists on either side.
- **Thread-safety**: the sync surfaces (C handle movable across threads,
  `#[pyclass]`) require `ClientTransport: Send` — `StreamOwned`,
  `ClientConnection`, and `TcpStream` are `Send`; pin it with a
  compile-time `assert_send::<ClientTransport>()`. Concurrent use of one
  connection was already UB by the C contract; under TLS it is a data race
  on the session state machine rather than benign fd interleaving —
  restate the warning in `gnitz.h`.

`message.rs` (`send_message*`, `recv_message`) and `connection.rs`
(`Connection` holds `transport: ClientTransport` instead of `sock:
OwnedFd`) swap `sock_fd: RawFd` parameters for `&mut ClientTransport` —
mechanical (the audited call-site inventory: four `message.rs` functions,
~9 `connection.rs` sites, the gnitz-py loop). `hello_handshake` is
rewritten on the generic primitives: send the HELLO via
`send_framed_iov(&[&frame[4..]])` (byte-identical prefix), then one
`recv_framed(MAX_FRAME_PAYLOAD_CLIENT)` whose payload length discriminates
exactly as today (12 ⇒ ACK, anything else ⇒ STATUS_ERROR control block).

### Python / C surfaces

- **C API, Python sync**: no code changes — the connect string flows
  through `GnitzClient::connect` → `Connection::connect` →
  `ClientTransport::connect`. Docstring updates only (`gnitz.h`, `aio.py`).
- **Python async** (`gnitz-py/src/lib.rs`): `PyAsyncTransport::new` builds a
  `ClientTransport` (replacing `SocketGuard`), runs `hello_handshake(&mut
  t)`, takes `t.waker()` (replacing `DupFd` — same dup-fd shutdown
  mechanism, now transport-generic), and moves the transport into
  `async_io_loop`, whose `send_framed_batch(sock_fd, ..)` /
  `recv_framed(sock_fd, ..)` / `recv_scan_response(sock_fd, ..)` calls
  become transport method calls. `PyAsyncTransport::Drop` keeps today's
  shutdown-on-drop unblocking. FIFO pipelining semantics are unchanged —
  TLS-over-TCP is order-preserving exactly like the fd, and the server's
  always-reading pump preserves the fd path's pipelining liveness.

## Test infrastructure

- **gnitz-test-harness**: `start_n` always appends
  `--tls-listen=127.0.0.1:0` (port 0 ⇒ safe under parallel `cargo test`);
  new `pub fn tls_target(&self) -> String` polls `<data_dir>/tls_endpoint`
  (same backoff loop as the socket wait; the atomic rename makes existence
  imply complete content, so one unconditional read follows) and returns
  `tls://127.0.0.1:PORT?insecure`; `pub fn tls_ca_target(&self)` returns
  the `?ca=<data_dir>/tls_dev_cert.pem` form; new `pub fn restart(&mut
  self)` reads the bound port from `tls_endpoint`, kills the server, and
  respawns it with the same data dir and an explicit
  `--tls-listen=127.0.0.1:<same port>`. Per-boot rcgen dev-cert minting
  costs single-digit milliseconds — negligible against fork/SAL setup for
  the ~236 existing `ServerHandle` call sites; stated so the always-on
  listener is a deliberate tradeoff, not a silent one.
- **pytest conftest** (`gnitz-py/tests/conftest.py`): the TLS address must
  be as stable across server restarts as the socket path (the sock path
  deliberately lives outside the per-restart data dir). The `_Server`
  allocates **one** free TCP port at construction (bind `127.0.0.1:0`,
  read it back, close) and passes that same fixed
  `--tls-listen=127.0.0.1:PORT` on *every* spawn, including restarts. A
  `_Server.target` property returns the sock path, or
  `tls://127.0.0.1:PORT?insecure` when `GNITZ_TRANSPORT=tls` (the env
  check follows the existing `if x := os.environ.get(...)` idiom in
  `_spawn`). **Both** the `server` fixture *and* the `client` fixture
  (which today dials `_srv.sock_path` directly) go through `target`; for
  the same reason `_seamed_server` (conftest.py:170) and
  `relay_lowspace_server` (conftest.py:240) switch from `s.sock_path` to
  `s.target`, or their five dependent fixtures silently stay on AF_UNIX.
  Every `_Server` spawn flows through the single `_spawn` helper; the
  standalone `subprocess.Popen` helpers in `test_crash_recovery.py`,
  `test_server_args.py`, etc. are transport-agnostic and intentionally
  untouched. Two robustness fixes to `_spawn` while touching it: the
  readiness loop gains a `proc.poll() is not None` fail-fast that raises
  with the captured stderr (today a boot failure — e.g. the pre-allocated
  port stolen — blind-waits 10 s and raises a generic error that cascades
  the session), and `_Server.__init__` wraps the port-probe + first spawn
  in a bounded retry (≤ 3 fresh ports). A port stolen *between restarts*
  still fails fast with stderr in hand — the stable-target requirement
  forbids re-porting mid-session.
- **Makefile** (plus `.PHONY` and the `##` help comment):

  ```make
  e2e-tls: WORKERS = 4
  e2e-tls: server pyext ## Run the full E2E suite over TLS
  	cd crates/gnitz-py && GNITZ_TRANSPORT=tls GNITZ_WORKERS=$(WORKERS) \
  		uv run pytest tests/ -v $(if $(K),-k '$(K)')
  ```

  The `test` and `clippy` recipes gain `gnitz-core/integration` next to
  the existing `gnitz-sql/integration` feature flag.

## Edge cases

- **Pipelined clients cannot deadlock**: the read pump reads
  unconditionally (fd-path parity), so a client blocked mid-send-batch
  always drains; the server's scan-train sends land in the kernel socket
  buffer and complete once the client finishes sending and reads. Memory
  is guarded by the inbound caps (close, never pause).
- **close_notify / EOF / RST**: inbound clean close (`reader()` returns
  `Ok(0)` / `peer_has_closed()`), EOF, and RST all set `recv_closed`;
  `connection_loop` exits via `Peer::recv() == None` and calls
  `peer.close()`. Outbound close sends a best-effort close_notify. A
  truncation attack (TCP FIN without close_notify) at worst ends a session
  early — the framing already rejects partial frames.
- **TLS protocol errors**: `process_new_packets` returns `Err` and queues
  the fatal alert; the pump's flush sends it, then the connection closes.
  No panic path; a garbage-spewing client costs one session.
- **Zero-length close sentinel**: the assembler and the client's
  `recv_framed` reject `len == 0`, matching the fd path. TLS connections
  close via close_notify/FIN, not the sentinel.
- **Big frames both directions** (64 MB server-bound / 256 MB
  client-bound): chunked `writer()` writes bound rustls buffering; the
  kernel socket buffer + awaited `send_raw` provide backpressure; the
  pump's 64 KiB reads assemble arbitrarily large frames (bounded by
  `max_payload_len`, plus the hard cap on queued frames).
- **Worker fork ordering**: the TCP listener is created in `server_main`
  after the workers fork (same position as `server_create`), so no fd
  inheritance.
- **Server shutdown** keeps today's semantics: in-flight connections see
  RST/EOF; clients get an immediate, clear error on their next call. No
  new graceful-shutdown machinery.
- **`tls_endpoint`/`tls_dev_cert.pem` staleness**: rewritten on every boot
  before "GnitzDB ready" (endpoint file atomically); a `?ca=` pin must
  re-read the dev cert after a restart.
- **DNS**: `ToSocketAddrs` resolution, first result; an unresolvable host
  is a connect-time `ProtocolError`.
- **EMFILE / accept errors**: the existing accept-error handling covers the
  TCP listener via the same KIND_ACCEPT path, with the per-listener
  backoff re-arm from the reactor section (the single-flag backoff would
  leave one of two exhausted listeners permanently deaf).

## File changes

1. `crates/gnitz-engine/src/runtime/tls/{mod,frame,config}.rs` (new, ~520
   lines total): conn state, read pump, flusher, `TlsPeer` internals,
   `FrameAssembler`, TLS/cert building.
2. `crates/gnitz-engine/src/runtime/reactor/`: delete `udp.rs` + UDP
   kinds/fields/preps (−~450 lines); add `recv_raw`/`send_raw`
   (`KIND_RAW_RECV` machinery + `SendAlive::Vec`, ~170) and multi-listener
   accept incl. per-listener EMFILE backoff (~50). `posix_io.rs`: delete
   `udp_bind`/`udp_local_addr`, add `tcp_bind`/`tcp_local_addr`/
   `shutdown_rdwr` (~50).
3. `crates/gnitz-engine/src/runtime/orchestration/peer.rs` +
   `runtime/reactor/io.rs`: `PeerInner::Tls` variant; `RecvBuf` becomes a
   two-variant enum (malloc-backed / `Vec`-backed) with `as_slice`/`Drop`
   dispatch (~60 lines).
4. `crates/gnitz-engine/src/runtime/orchestration/executor.rs`: optional
   `TlsServerInit` parameter, accept routing + connection cap, task
   spawning (~60 lines).
5. `crates/gnitz-engine/src/main.rs` + `runtime/bootstrap.rs`: CLI flags,
   bind, endpoint/dev-cert files, warnings (~100 lines).
6. `crates/gnitz-core/src/protocol/transport/mod.rs`: `ClientTransport`,
   `TransportWaker`, target parsing (~180 lines delta); `transport/tls.rs`
   (new, ~250 lines): connect, verifier modes, framed I/O over
   `StreamOwned`.
7. `crates/gnitz-core/src/protocol/message.rs` + `connection.rs`: `RawFd`
   → `&mut ClientTransport` (mechanical).
8. `crates/gnitz-py/src/lib.rs`: `PyAsyncTransport` re-plumb (~80 lines
   delta). `crates/gnitz-capi/gnitz.h`, `gnitz-py/python/gnitz/aio.py`:
   docstrings.
9. `crates/gnitz-test-harness/src/lib.rs`: TLS listener, `tls_target`,
   `restart` (~90 lines).
10. `crates/gnitz-py/tests/conftest.py`, `tests/test_tls.py` (new);
    `Makefile` (`e2e-tls`, feature flags, `.PHONY`).
11. Cargo.toml changes per the dependency table; gnitz-core gains
    `[features] integration = []`.

## Tests

**Rust unit (engine `tls::config`)**: dev-cert generation feeds
`rustls::ServerConfig` successfully; PEM cert+key written to a tempdir
round-trip through the file-loading path.

**Rust unit (engine `tls::frame`)**: `FrameAssembler` — split delivery,
pipelined frames, oversize reject, zero-len reject, hard-cap accounting
(64-byte floor; cap trips after ~`CAP/64` one-byte frames with a
test-lowered cap).

**Rust unit (gnitz-core)**: target parsing (`tls://h:p`, `?insecure`,
`?ca=`, bare paths, malformed → error); compile-time
`assert_send::<ClientTransport>()`.

**Rust integration (`gnitz-core/tests/tls_client.rs` — new directory; gate
with `#![cfg(feature = "integration")]` plus `[features] integration = []`
in gnitz-core/Cargo.toml, mirroring gnitz-sql's harness tests)**:
1. `?insecure` connect + HELLO + `alloc_table_id` roundtrip.
2. `?ca=tls_dev_cert.pem` connect succeeds; default webpki roots against
   the dev cert **fails** with a certificate error; ALPN mismatch and
   wrong-CA both surface as `ProtocolError`s carrying the alert text
   within the 10 s handshake timeout.
3. Push → scan roundtrip over TLS (data integrity, weights).
4. 16 MB push and a large multi-frame scan (exercises chunked
   `writer()` sends, kernel backpressure, pump reassembly).
5. UNIX + TLS clients concurrently against the same table.
6. Wire-version mismatch HELLO over TLS → server STATUS_ERROR frame, clean
   close.
7. Restart: connect, `srv.restart()` (same port), assert the old client's
   next call fails fast (EOF/RST-derived error, well under 5 s) and a
   fresh connect succeeds.
8. Pipelining liveness: with `SO_SNDBUF`/`SO_RCVBUF` pinned small on both
   ends (default autotuned buffers can absorb the whole exchange and
   false-green the test), one connection sends >16 MB of pushes pipelined
   ahead of a scan whose response exceeds the shrunken buffers, then reads
   everything — the four-party deadlock shape. The body runs under a
   watchdog (thread + join-with-timeout) so a deadlock fails the test
   instead of hanging the suite.
9. IPv6: `tls://[::1]:PORT?insecure` parses and connects;
   `?ca=tls_dev_cert.pem` over IPv6 verifies against the dev cert's `::1`
   IP SAN.

**E2E (`test_tls.py`, runs inside the normal suite — the listener is
always on in the test conftest)**: sync + async Python clients over
`tls://…?insecure`; multi-worker scan with continuation frames
(`GNITZ_WORKERS=4` exercises the `send_slot` path per worker train);
pipelined async pushes; error surfaces (bad CA path, unreachable port,
server restart → clear errors).

**Full-suite sweep**: `make e2e-tls` runs the entire pytest suite over
TLS. Coverage is two-tier by design: `test_tls.py` runs inside the
*default* `make e2e` (continuous smoke), while `e2e-tls` is the on-demand
deep sweep — the wire protocol is transport-invariant, so every existing
test must pass byte-for-byte semantics unchanged. When the implementation
lands, record the `make e2e-tls` wall-clock next to `make e2e`'s and a
64 MB push throughput number (expectation: within small factors of
AF_UNIX — 16 KiB records, kernel TCP; AF_UNIX remains the local fast
path).

## Implementation order

- [ ] 1. `gnitz-core`: `ClientTransport` enum (Unix-only) + `TransportWaker`;
       re-plumb `message.rs`/`connection.rs`/`hello_handshake`/`gnitz-py`
       async loop. Pure refactor; `make test` + `make e2e` green.
- [ ] 2. `gnitz-core`: TLS client transport (parsing, rustls config,
       SkipVerify, `StreamOwned` framed I/O) + unit tests. Compiles green;
       integration deferred to 3.
- [ ] 3. `gnitz-engine`: delete the UDP reactor ops; add
       `recv_raw`/`send_raw`, multi-listener accept, `tcp_bind`; deps,
       `runtime/tls`, `PeerInner::Tls`, executor/bootstrap/CLI wiring;
       harness TLS support; Rust integration tests 1–9. `make test` green.
- [ ] 4. E2E: conftest always-on listener + `GNITZ_TRANSPORT`,
       `test_tls.py`, `Makefile` `e2e-tls`. `make e2e` and `make e2e-tls`
       green.
