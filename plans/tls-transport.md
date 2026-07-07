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
| Server I/O model | per-connection, three tasks: a **read pump** (raw recv → `read_tls` → `process_new_packets` → plaintext fed into a reused `io::RecvState` deframer; never locks, never sends; on any recv-side death it issues the lock-free `posix_io::shutdown` that aborts a parked writer), a **flusher** (serializes ciphertext extraction under `send_mutex`, and owns teardown — the lock-free socket shutdown and the one `libc::close(fd)`), and the existing `connection_loop` consuming via `Peer::recv()` and sending under the same `send_mutex`. No drive task, no flow-control protocol; the only timer is the per-connection handshake watchdog — the kernel owns everything else |
| Client I/O model | blocking `std::net::TcpStream` + `rustls::StreamOwned<ClientConnection, TcpStream>` — no async runtime, no threads, no pump. A blocking read IS the wait |
| HELLO | unchanged, first frames on the stream (keeps the `bound_client_id` auth hook and the pre-handshake 8-byte frame cap) |
| Idle handling | no engine-level idle timeout — established connections idle indefinitely (a blocking client's kernel `recv` is the wait). Keepalive is set on every TCP socket, both sides, with **tuned intervals** (`SO_KEEPALIVE` + `TCP_KEEPIDLE=120`, `TCP_KEEPINTVL=15`, `TCP_KEEPCNT=6` — dead-peer detection ≈ 210 s), not the kernel default (`TCP_KEEPIDLE` = 7200 s / 2 h). This bounds the one liveness gap AF_UNIX lacks — a silent half-open drop (NAT rebind / physical link loss) that would otherwise hang a client's post-handshake `recv` (its read timeout is cleared to `None`) forever *and*, server-side, pin one of the scarce `MAX_TLS_CONNS = 64` slots for two hours. Tuning is safe: TCP keepalive never reaps a *live* idle peer — the peer's kernel ACKs probes with zero application involvement, so probes only fail against a genuinely dead host or a partition longer than the ~210 s window (in which case the client simply reconnects). No engine machinery — the kernel drives the probes |
| Handshake timeout | 10 s server-side watchdog per accepted TLS connection (reactor timer). rustls has no socket timeouts, so a peer that completes the TCP handshake but stalls the TLS handshake would park its read pump forever and hold one of the `MAX_TLS_CONNS` slots; on expiry, if still `is_handshaking()`, the watchdog drives recv-side teardown. This is a stuck-connection guard, not a security boundary (the listener is unauthenticated) |
| Inbound bound | unbounded frame queue per connection (fd-path parity; bounded-slot designs deadlock pipelined clients, see baseline facts) guarded by the reactor's **global** inbound-memory counter (`total_inbound_bytes: Rc<Cell<usize>>` + `global_cap` in `ReactorShared`, shared with the Unix path). The TLS path **reuses the fd path's `io::RecvState`/`RecvBuf` accounting verbatim** rather than re-implementing it: the counter is charged `frame_weight(len) = len.max(64)` at the **header-parse point** (`RecvAdvance::HeaderDone`) — *before* the payload buffer is `malloc`'d and before any payload byte arrives — with the cap checked first so a breach is refused before allocation; `RecvBuf`'s `Drop` refunds via `saturating_sub` on the *only* release paths (consume by `Peer::recv`, or teardown dropping the queued `VecDeque<RecvBuf>`). This closes the slow-drip OOM (a client that declares a 64 MB frame then dribbles it is charged the full 64 MB at header time, not on completion) and needs **no** manual `inbound_bytes` field, `release_inbound_memory`, or `TlsShared::Drop` reconciliation — RAII on `RecvBuf` reclaims everything. **No per-connection sub-cap** — a global ceiling must shed *some* connection under pressure, and a per-conn cap neither tightens the OOM bound nor avoids disconnecting a legitimate deep-pipelining client. Breach **closes** the connection with a log line, never pauses |
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
and their unit tests. Keep `sockaddr_from_addr`/`addr_from_sockaddr` (TCP
bind/local-addr reuse them). The `ReactorShared.ring`-first drop-order
SAFETY INVARIANT comment names `udp_recv_ops` among the userspace buffers
it protects; **retarget it** to the new `KIND_RAW_RECV` op/keep-alive
map(s) (below), which — like every added field — are declared **below**
`ring`, or ring teardown frees the buffers the kernel still points at.

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
- **Raw stream ops** (the reactor's CQE-level `RecvState` deframes *socket
  bytes*, which under TLS are ciphertext — so the raw ops carry undeframed
  bytes and a second `RecvState` instance deframes the decrypted plaintext
  in the pump, below): `Reactor::recv_raw(fd, buf: Vec<u8>) -> (Vec<u8>, i32)`
  — a one-shot recv into the caller's buffer, resolving with the byte count
  (≤ 0 = EOF/error); the buffer round-trips so the pump reuses one 64 KiB
  allocation forever. `recv_raw` gets its own completion kind
  `KIND_RAW_RECV` (+ a cancel kind) with per-fd park/waker/tombstone maps
  mirroring the deleted UDP-recv machinery — it must NOT ride `KIND_RECV`,
  whose handler silently drops CQEs for fds absent from `conns` (TLS fds
  never call `register_conn`). `Reactor::send_raw(fd, cipher: Rc<Vec<u8>>) -> i32`
  — sends the whole buffer, looping partial `OP_SEND` completions like the
  existing send path (which already loops partials), riding the existing
  `KIND_SEND`/send-waker routing (its absent-`conns`-key lookup is a
  benign no-op) with a new **`SendAlive::RcVec(Rc<Vec<u8>>)`** keep-alive
  variant replacing the deleted `UdpOp`. It **must be `Rc`-wrapped, not an
  owned `SendAlive::Vec(Vec<u8>)`**: `send_buf_inner` `.clone()`s the
  keep-alive on *every* partial-write chunk (`_alive: Some(alive.clone())`
  per re-submitted SQE), so an owned `Vec` would deep-copy the entire
  ciphertext buffer once per chunk — every existing variant is `Rc`/`Static`
  precisely to keep that clone O(1). Both follow the UDP ops' drop discipline
  (drop = AsyncCancel + tombstone); `prep_sendmsg`/`prep_recvmsg` deletion
  hits both the `Ring` trait and the `IoUringRing` impl.
- **Inbound buffer allocation (shared with the fd path)**: the TLS pump
  charges and allocates inbound frames through the *same* accounting the fd
  handler uses — no new counter API, no `release_inbound_memory`. Factor the
  fd handler's `HeaderDone` body into `Reactor::alloc_inbound_buf(plen:
  usize) -> Option<RecvBuf>` and call it from both sites:

  ```rust
  pub(crate) fn alloc_inbound_buf(&self, plen: usize) -> Option<RecvBuf> {
      let w = io::frame_weight(plen);
      if self.inner.total_inbound_bytes.get() + w > self.inner.global_cap.get() {
          return None; // refuse before malloc — no overshoot
      }
      let pbuf = unsafe { libc::malloc(plen) as *mut u8 };
      if pbuf.is_null() {
          return None;
      }
      // RecvBuf::new charges frame_weight(plen); its Drop refunds.
      Some(io::RecvBuf::new(pbuf, plen, Rc::clone(&self.inner.total_inbound_bytes)))
  }
  ```

  `handle_recv_cqe`'s `HeaderDone` arm becomes a call to this helper (its
  `plen > max_payload_len` guard stays at the call site, since the per-conn
  cap differs by path). Because every completed frame is a `RecvBuf` whose
  `Drop` refunds the counter, the queued `VecDeque<RecvBuf>` self-reconciles
  when a `TlsConn` is dropped at teardown — no manual reconciliation, no
  `TlsShared::Drop` counter logic. Expose `RecvState`, `RecvBuf`,
  `RecvBuf::new`, and `RecvAdvance` as `pub(crate)` for the `tls` module
  (today `pub(super)` within `reactor`).

## Design — server

New module `crates/gnitz-engine/src/runtime/tls/`: `mod.rs` (conn state,
read pump, `TlsPeer`) and `config.rs` (rustls/rcgen building). There is **no**
`frame.rs`: TLS reuses the reactor's `io::RecvState` deframer over the decrypted
plaintext (below), so no bespoke frame assembler exists.

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

### Plaintext framing — reuse `io::RecvState`

TLS does **not** re-implement frame assembly. The decrypted plaintext is fed
into a per-connection `io::RecvState` — the identical deframer the fd path runs
on socket bytes — so `[len:u32 LE][payload]` cutting, the `len == 0` close
sentinel, the `len > max_payload_len` cap, header-time inbound-memory charging,
and `RecvBuf` RAII refund are all inherited verbatim, not duplicated. This
closes a slow-drip OOM the fd path is already immune to: `RecvState` charges
`frame_weight(len)` and `malloc`s the payload at `RecvAdvance::HeaderDone`
(refusing before allocation if the cap would breach), so a client that declares
a 64 MB frame and then dribbles the payload one byte per second is charged the
full 64 MB up front — it can never accumulate uncounted in-flight bytes.

`max_payload_len` starts at `HELLO_PRE_HANDSHAKE_LEN = 8`, elevated by
`Peer::set_max_payload_len` after HELLO — `run_hello_handshake` elevates
synchronously *before* sending the ACK, so no conforming client's frame can hit
the 8-byte cap. `TlsConn::feed_decrypted` drives the `RecvState` from the rustls
plaintext reader and returns `Result<(), ()>`: `Err` on any violation — oversize
`len`, the zero-len sentinel, or an inbound-cap breach — which the pump turns
into recv-side teardown. It also achieves one-fewer-copy decryption: rustls reads
plaintext straight into the `RecvBuf` payload buffer via `RecvState::remaining()`,
with no intermediate stack bounce buffer.

```rust
impl TlsConn {
    /// Drain all currently-available decrypted plaintext into the RecvState,
    /// enqueuing completed frames. Err ⇒ recv-side teardown (oversize, zero-len
    /// sentinel, cap breach, or a rustls read error / clean close).
    fn feed_decrypted(&mut self, reactor: &Reactor) -> Result<(), ()> {
        loop {
            let (ptr, len) = self.recv_state.remaining();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, len as usize) };
            match self.sess.reader().read(slice) {
                Ok(0) => return Err(()),                       // clean TLS close
                Ok(m) => match self.recv_state.advance(m) {
                    io::RecvAdvance::NeedMore => {}
                    io::RecvAdvance::HeaderDone => {
                        let plen = self.recv_state.payload_len();
                        if plen > self.max_payload_len {
                            return Err(());
                        }
                        // Charges frame_weight(plen) now, before payload bytes
                        // arrive; None ⇒ cap breach, refused before malloc.
                        let buf = reactor.alloc_inbound_buf(plen).ok_or(())?;
                        self.recv_state.start_payload(buf);
                    }
                    io::RecvAdvance::MessageDone => {
                        self.inbound.push_back(self.recv_state.take_message());
                    }
                    io::RecvAdvance::Disconnect => return Err(()), // len == 0 sentinel
                },
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(()),
                Err(_) => return Err(()),
            }
        }
    }
}
```

Deliberately *stricter* than the fd path in one corner: the pump parses stream
bytes on arrival, so a client that pipelines a >8-byte frame behind HELLO before
the ACK gets closed here, while the fd path leaves those bytes unparsed in the
kernel buffer until the cap is elevated. No conforming client pipelines pre-ACK
(`hello_handshake` blocks on the ACK) — intended hardening, not a compatibility
break.

### Connection state and read pump

```rust
struct TlsConn {
    sess: rustls::ServerConnection,
    recv_state: io::RecvState,     // reused fd-path deframer over decrypted plaintext
    max_payload_len: usize,        // 8 pre-HELLO, elevated by set_max_payload_len
    inbound: VecDeque<RecvBuf>,    // completed frames awaiting Peer::recv; RecvBuf
                                   //   RAII refunds the inbound counter on drop
    recv_waker: Option<Waker>,     // single slot: exactly one connection_loop recvs
    recv_closed: bool,             // EOF / close_notify / TLS error / cap breach
    closed: bool,                  // Peer::close() ran; senders refuse, flusher tears down
    pump_exited: bool,             // pump done; fd may be closed once `closed` too
}

struct TlsShared {
    reactor: Rc<Reactor>,
    fd: i32,
    state: RefCell<TlsConn>,
    send_mutex: Rc<AsyncMutex<()>>, // serializes ciphertext extraction + send_raw
    flush_tx: mpsc::Sender<()>,     // wakes the flusher task
    conn_guard: TlsConnGuard,       // RAII: decrements the live-conn counter on Drop
}
```

No `impl Drop for TlsShared` is needed for inbound-memory reconciliation: each
queued frame is a `RecvBuf` whose own `Drop` refunds `frame_weight(len)`, so a
dirty teardown that drops the `TlsConn` (and its `VecDeque<RecvBuf>`) with frames
unread reclaims the counter automatically — the same RAII the fd path relies on.
`TlsConnGuard` (a distinct field) still handles the live-connection count on drop.

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
                            // Deframe all available plaintext straight into the
                            // reused RecvState (zero-copy into RecvBuf); Err on a
                            // violation — oversize, zero-len sentinel, cap breach,
                            // or a clean plaintext close — tears the recv side down.
                            if c.feed_decrypted(&reactor).is_err() {
                                fatal = true;
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
    // Lock-free: half-close so any writer (a `connection_loop` sender or the
    // flusher) parked in `send_raw` on a full sndbuf is aborted by its error
    // CQE and releases `send_mutex`. This is what makes teardown deadlock-free
    // — without it, a sender parked mid-scan-train while the client has
    // stopped reading pins the mutex and the flusher can never flush or close.
    // Idempotent with the flusher's own shutdown on the local-close path.
    // `posix_io::shutdown` (SHUT_RDWR, EINTR-looped, ENOTCONN-tolerant) already
    // exists — its doc comment describes exactly this ring-slot eviction use.
    posix_io::shutdown(conn.fd);
    if let Some(w) = conn.take_recv_waker() { w.wake(); }
    conn.notify_flusher();                                     // run teardown
}
```

The flusher — it and the senders serialize ciphertext extraction under
`send_mutex`, and it is the one place the fd is ever closed. Teardown
(`posix_io::shutdown`, then `libc::close`) is lock-free — it never *acquires*
the mutex, so a writer parked in `send_raw` can never wedge it:

```rust
async fn flusher(conn: Rc<TlsShared>, reactor: Rc<Reactor>, mut rx: mpsc::Receiver<()>) {
    let mut shutdown_sent = false;
    loop {
        // Extract and ship whatever ciphertext rustls has queued: handshake
        // flights, tickets, KeyUpdate, alerts, a close_notify queued by
        // close(). The send mutex is held only for the synchronous `write_tls`
        // extraction — teardown never *waits* on it. One pass drains ALL queued
        // ciphertext, so it services any number of coalesced notifications.
        let out = {
            let _g = conn.send_mutex.lock().await;
            let mut c = conn.state.borrow_mut();
            let mut out = Vec::new();
            while c.sess.wants_write() { let _ = c.sess.write_tls(&mut out); }
            out
        };
        if !out.is_empty() { let _ = reactor.send_raw(conn.fd, Rc::new(out)).await; }

        // Teardown, lock-free. Half-close once either side has begun it: on a
        // *local* close this drives the pump's parked recv to EOF; on any close
        // it is the mechanism that aborts a parked `send_raw` so `send_mutex`
        // is always eventually released (no teardown deadlock). Idempotent with
        // the pump's own shutdown.
        if (conn.closed() || conn.recv_closed()) && !shutdown_sent {
            posix_io::shutdown(conn.fd);
            shutdown_sent = true;
        }
        // The ONE close — only after the local close ran *and* the pump exited.
        // A bare close neither aborts an in-flight io_uring op nor stops a task
        // from touching a recycled fd number, hence close-once-both-are-done.
        // The fd is unregistered (TLS never calls `register_conn`), so
        // `Reactor::close_fd` would be a no-op — close it directly with the
        // codebase's raw idiom. `TlsShared`'s guard Drop (when the last
        // `Rc<TlsShared>` drops) then decrements the live-conn counter; queued
        // `RecvBuf`s refund the inbound counter via their own Drop.
        if conn.closed() && conn.pump_exited() {
            unsafe { libc::close(conn.fd) };
            return;
        }
        // Coalesce a burst of notifications: park on the next, then drain any
        // that piled up so we make exactly one more flush pass, not N.
        if rx.recv().await.is_none() { return; }
        while rx.try_recv().is_some() {}
    }
}
```

**Teardown converges from either direction, and cannot deadlock.**
`Peer::close()` stays a **sync** fn (its many call sites are unchanged):
the first transition of `closed` calls `sess.send_close_notify()` under the
borrow and notifies the flusher; repeat calls are no-ops. *Local close:*
the flusher flushes the close_notify → `posix_io::shutdown`
completes the pump's in-flight `recv_raw` with EOF → the pump exits
and notifies → the flusher sees `closed && pump_exited` → `libc::close(fd)`.
*Remote EOF / error / cap-breach:* the pump sets `recv_closed`/`pump_exited`
**and issues `posix_io::shutdown` itself** → `connection_loop` drains the queue,
gets `None`, calls `peer.close()` at its exit (it always does) → same
convergence.

The load-bearing property is that **`posix_io::shutdown` is lock-free and every
recv-side death issues it.** A `connection_loop` sender (or the flusher) can
be parked in `send_raw` on a full kernel sndbuf when the client has stopped
reading, holding `send_mutex` across the await; if teardown had to *acquire*
that mutex before it could shut the socket down, it would wedge forever (the
socket never closes → `send_raw` never errors → the mutex never releases →
the fd + `MAX_TLS_CONNS` slot leak, with keepalive the only, ~210 s
backstop). Issuing `posix_io::shutdown` **without** the mutex breaks exactly this
cycle: the kernel aborts the parked `send_raw` with an error CQE, the writer
returns `-1` and drops its `send_mutex` guard, and both the flusher's flush
and the final `libc::close(fd)` proceed. Because the mutex is therefore never held
indefinitely, the flusher's brief lock-extract-release step can never wedge.

Notes that keep the rest airtight: the TLS fd is never `register_conn`'d, so
`Reactor::close_fd` would be a no-op on it — the flusher closes it directly with
raw `libc::close` (the codebase idiom), and only after shutdown + pump-exit,
because a bare close neither completes an in-flight io_uring recv nor prevents a
task from touching a recycled fd number. In-flight `send_raw` buffers are
reactor-owned `Rc` keep-alives until their CQE, and senders check `closed`
under the borrow in the same poll as submission, so nothing can submit on a
recycled fd. The live-connection counter is reclaimed by `TlsConnGuard`'s Drop
and the residual inbound-memory weight by each queued `RecvBuf`'s Drop, both
firing when the last `Rc<TlsShared>` (and hence the `TlsConn`) goes away — on
normal close, and on reactor teardown, which drops every task future. **Panics
are not isolated:** the reactor's `poll_task` has no `catch_unwind`, so a panic
in any handler unwinds the whole reactor thread exactly as it does on the fd path
today — not a per-connection slot leak, a process-level failure. RAII is the
recovery for *clean* teardown, not a panic firewall; no `Drop for Peer` is added
(it would change the fd path's explicit-close semantics for no gain).

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
four-party deadlock); memory is guarded by the global inbound cap, whose
breach (in `feed_decrypted`, via `alloc_inbound_buf` returning `None`) sets
`recv_closed`, logs fd and accounted backlog, and lets teardown reclaim the
queue. Guard the chunked send loop against a zero-length `writer().write`:
treat `Ok(0)` on a non-empty slice as fatal (`return -1`) so a full rustls
buffer can never spin it.

### TlsPeer (the `PeerInner::Tls` variant)

`peer.rs` gains `Tls(Rc<TlsShared>)`.

- `recv()`: future that pops `inbound: VecDeque<RecvBuf>` and returns the
  `RecvBuf` directly (the same malloc-backed `{ptr, len}` type in
  `runtime/reactor/io.rs` the fd path already yields — **no two-variant enum
  change**; the TLS payload buffers are `malloc`'d by `alloc_inbound_buf`
  exactly like the fd path's). Popping hands the caller the `RecvBuf`, whose
  eventual `Drop` refunds the inbound counter — no manual decrement. Parks in
  `recv_waker` when empty; resolves `None` once `recv_closed` and the queue is
  empty.
- `send_buffer` / `send_hello_ack`: call the shared `send_bytes` loop over the
  complete frame bytes (already length-prefixed exactly as the fd path sends
  them). No deadline — fd parity (`Reactor::send_buffer`/`send_hello_ack` are
  un-guarded; they pin no shared worker resource):

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
                  Ok(0) => return -1,   // full rustls buffer we can't drain; kill, don't spin
                  Ok(n) => off += n,
                  Err(_) => return -1,
              }
              let mut out = Vec::new();
              while c.sess.wants_write() { let _ = c.sess.write_tls(&mut out); }
              out
          };
          if self.reactor.send_raw(self.fd(), Rc::new(cipher)).await < 0 { return -1; }
      }
      bytes.len() as i32
  }
  ```

  Chunked writes keep rustls's plaintext buffer under its default 64 KiB
  limit and bound transient ciphertext memory; the awaited `send_raw` is
  where kernel backpressure lands.
- `send_slot`: the W2M ring-slot egress. It **must** re-impose the fd path's
  per-frame client-send deadline (`Reactor::send_slot`, `conn.rs:160`) — the
  unified `send_bytes` loop drops it, and a client that simply stops reading
  (never triggering recv-side EOF/error) would otherwise park the send in
  `send_raw` forever, holding the `W2mSlot` across the await. That pins the
  worker's W2M ring, whose fill synchronously futex-blocks the single-threaded
  worker — a cluster-wide freeze. The slot's frame bytes stay borrowed (hence
  the slot alive, and W2M backpressure preserved) until the kernel accepts the
  last ciphertext byte; on stall, `posix_io::shutdown` aborts the parked
  `send_raw`, releasing both the `send_mutex` and — once the awaited future
  returns — the slot:

  ```rust
  async fn send_slot(&self, slot: W2mSlot) -> i32 {
      let mut send_fut = std::pin::pin!(self.send_bytes(slot.frame_bytes()));
      let deadline = Instant::now() + client_send_timeout();   // pub(crate) from reactor
      match select2(send_fut.as_mut(), self.reactor.timer(deadline)).await {
          Either::A(rc) => rc,
          Either::B(()) => {
              gnitz_warn!("tls: client fd={} stalled ring-slot egress past {:?}; evicting",
                          self.fd(), client_send_timeout());
              posix_io::shutdown(self.fd());   // abort the parked send_raw → release slot
              send_fut.await.min(-1)
          }
      }
  }
  ```
- `set_max_payload_len`: sets `TlsConn.max_payload_len` (synchronous `RefCell`
  write) — the same cap `feed_decrypted` enforces per frame.
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
  count is at `MAX_TLS_CONNS`, `unsafe { libc::close(fd) }` and log; else set
  `TCP_NODELAY` and tuned keepalive (`posix_io::set_keepalive`) on the
  accepted fd, build
  `rustls::ServerConnection::new(cfg)`, create the `TlsShared` (constructing
  the RAII live-conn guard), spawn `read_pump` and `flusher`, spawn the
  **handshake watchdog** (below), wrap `Peer::tls(...)`, spawn
  `connection_loop`. `connection_loop`, HELLO validation, dispatch, scan
  trains: **zero changes**.

  The handshake watchdog is one small task per accepted TLS connection:

  ```rust
  let wd = Rc::downgrade(&conn);        // Weak<TlsShared> — see below
  let wd_reactor = Rc::clone(&reactor);
  reactor.spawn(async move {
      wd_reactor.timer(Instant::now() + Duration::from_secs(10)).await;
      let Some(wd) = wd.upgrade() else { return; };   // already torn down
      let stalled = {
          let c = wd.state.borrow();
          !c.closed && !c.recv_closed && c.sess.is_handshaking()
      };
      if stalled {
          gnitz_warn!("tls: handshake timed out (fd={}), dropping", wd.fd);
          // Same lock-free recv-side teardown the pump runs on error:
          // set recv_closed, posix_io::shutdown(fd), wake the recv waiter,
          // notify the flusher. The pump exits on the resulting EOF and
          // connection_loop closes via the normal convergence.
          wd.begin_recv_teardown();
      }
  });
  ```

  The watchdog holds a **`Weak<TlsShared>`, not a strong `Rc`**: it lives in the
  reactor's timer queue for the full 10 s, and a strong clone would keep
  `TlsShared` (and its `TlsConnGuard`) alive that whole time even for a
  connection that closed in 5 ms — so 64 rapid connect/disconnects would pin all
  `MAX_TLS_CONNS` slots for 10 s and wedge new connections. With `Weak`, a
  closed connection's guard drops immediately; the watchdog `upgrade()`s to a
  no-op when the connection is already gone.

  `begin_recv_teardown` factors the pump's exit actions (set `recv_closed`,
  `posix_io::shutdown`, wake recv waiter, notify flusher) so the pump and the
  watchdog share one path; it does **not** set `pump_exited` (only the pump
  does, on its actual exit).

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
- Tls variant: `send_framed_*` write the length prefix + payload(s) **directly
  into rustls's internal plaintext buffer** via `conn.writer().write_all`, then
  flush once by looping `conn.write_tls(&mut sock)` until `!conn.wants_write()`.
  This bypasses `StreamOwned`'s `Write` impl, which flushes (`complete_io`) on
  *every* `write` call — so a naive prefix-then-payload pair emits two TLS
  records (2 AEAD ops + 2×~22 B overhead) per small control frame — **and**
  avoids the coalescing `Vec` the prior draft allocated per send (a real cost
  for a large `send_framed_batch`). Writing straight into `writer()` lets rustls
  fragment the buffered plaintext into full 16 KiB records at flush time:

  ```rust
  // ClientTransport::Tls { stream } — StreamOwned exposes .conn and .sock.
  let prefix = (total as u32).to_le_bytes();
  stream.conn.writer().write_all(&prefix)?;
  for buf in bufs {                         // send_framed_batch loops all frames
      stream.conn.writer().write_all(buf)?;
  }
  while stream.conn.wants_write() {
      stream.conn.write_tls(&mut stream.sock)?;   // blocking TcpStream drains it
  }
  ```

  `recv_framed` = `read_exact` a 4-byte prefix,
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
  → resolve via `ToSocketAddrs` and try `connect_timeout` on **each** address
  in turn (dual-stack fallback — first-address-only would hang 10 s then fail
  when `localhost` resolves to `[::1]` first but the server listens only on
  `127.0.0.1`; keep the last error to report if all fail):

  ```rust
  let mut last_err = None;
  let stream = target.to_socket_addrs()?
      .find_map(|addr| match TcpStream::connect_timeout(&addr, Duration::from_secs(10)) {
          Ok(s) => Some(s),
          Err(e) => { last_err = Some(e); None }
      })
      .ok_or_else(|| ProtocolError::IoError(last_err.unwrap()))?;
  ```

  → `set_nodelay(true)` → tuned keepalive (raw `setsockopt` on the stream fd —
  std `TcpStream` has no keepalive setter; `SO_KEEPALIVE` + `TCP_KEEPIDLE=120`
  / `TCP_KEEPINTVL=15` / `TCP_KEEPCNT=6`, the same values the server sets; this
  bounds a silent half-open drop that would otherwise hang the post-handshake
  `recv` forever, since its read timeout is cleared below) →
  `set_read_timeout(Some(10 s))` → rustls `ClientConfig`
  (ring provider,
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
- **Python async** (`gnitz-py/src/lib.rs`): the async loop's per-request calls
  (`send_framed_batch` / `recv_framed` / `recv_scan_response`) become
  `ClientTransport` method calls (replacing the `SocketGuard` + raw-fd free
  functions), and `t.waker()` replaces `DupFd` (same dup-fd shutdown mechanism,
  now transport-generic). `PyAsyncTransport::Drop` keeps today's
  shutdown-on-drop unblocking. FIFO pipelining semantics are unchanged —
  TLS-over-TCP is order-preserving exactly like the fd, and the server's
  always-reading pump preserves the fd path's pipelining liveness.

  **Connect and HELLO move off the event-loop thread.** Today `new` runs
  `connect` + `hello_handshake` synchronously on the caller's thread (the
  asyncio loop thread), spawning the I/O thread only afterward. That is
  harmless for AF_UNIX (an instant local connect) but under TLS it blocks the
  loop for the full `connect_timeout` (up to 10 s on an unreachable host) plus
  the TLS handshake — a multi-second event-loop freeze. So `new` instead spawns
  the I/O thread immediately and returns at once; the thread's **first** action
  is `ClientTransport::connect(target)` + `hello_handshake`, after which it
  signals the loop via the existing result-callback plumbing:
  `set_result_fn(negotiated_max_payload_len)` on success (the thread then drains
  the request channel as today) or `set_exception_fn(err)` on failure (the
  thread exits; queued requests fail). `aio.py`'s `_ConnectContext.__await__`
  returns a real loop future resolved by that first signal instead of the
  current `_immediate_return`, so `await connect(...)` / `async with
  connect(...)` awaits the connection without blocking the loop, and a connect
  error surfaces as an exception on the await. A `Drop` before the ready signal
  (client cancels the connect) sets the thread's shutdown flag so the in-flight
  blocking connect is abandoned. Requests enqueued before the ready signal
  simply wait in the channel behind the connect — the same FIFO ordering as a
  post-connect enqueue.

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
  is guarded by the global inbound cap (close, never pause).
- **Teardown under write backpressure cannot deadlock**: if a client stops
  reading while the server is mid-send (sender parked in `send_raw`, holding
  `send_mutex`) and teardown is triggered (cap breach, TLS error, or EOF),
  the pump/flusher issue the lock-free `posix_io::shutdown` that aborts the
  parked send and releases the mutex — never *acquiring* the mutex to shut down.
  A client that stalls a *scan train* without ever triggering recv-side death
  (it just stops reading) is caught by the `send_slot` client-send deadline,
  which fires the same lock-free `posix_io::shutdown` on expiry — the fd path's
  W2M-ring eviction, reproduced. See the teardown-convergence argument above.
- **close_notify / EOF / RST**: inbound clean close (`reader()` returns
  `Ok(0)` / `peer_has_closed()`), EOF, and RST all set `recv_closed`;
  `connection_loop` exits via `Peer::recv() == None` and calls
  `peer.close()`. Outbound close sends a best-effort close_notify. A
  truncation attack (TCP FIN without close_notify) at worst ends a session
  early — the framing already rejects partial frames.
- **TLS protocol errors**: `process_new_packets` returns `Err` and queues
  the fatal alert; the pump sets `recv_closed` and notifies the flusher,
  which ships the alert best-effort before the socket is shut down (a
  non-reading peer may miss it — acceptable, same posture as close_notify).
  No panic path; a garbage-spewing client costs one session.
- **Half-open / dead peer**: tuned keepalive (both sides,
  `TCP_KEEPIDLE=120`/`TCP_KEEPINTVL=15`/`TCP_KEEPCNT=6`) reaps a silently-dropped
  TCP connection whose peer sends no RST in ≈ 210 s — the one liveness gap
  AF_UNIX does not have, and (server-side) the guard that stops a silent
  half-open from pinning one of the 64 slots for the kernel-default 2 hours.
  Keepalive never reaps a *live* idle connection: the peer's kernel ACKs probes
  with no application involvement, so only a genuinely dead host or a partition
  longer than ~210 s (client then reconnects) is reaped. On the server the
  handshake watchdog covers the pre-HELLO window that keepalive's idle timer
  would not shorten.
- **Zero-length close sentinel**: the reused `RecvState`
  (`RecvAdvance::Disconnect`) and the client's `recv_framed` reject `len == 0`,
  matching the fd path. TLS connections close via close_notify/FIN, not the
  sentinel.
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
- **DNS**: `ToSocketAddrs` resolution, every returned address tried in turn
  (dual-stack fallback); an unresolvable host — or one where every address
  fails to connect — is a connect-time `ProtocolError`.
- **EMFILE / accept errors**: the existing accept-error handling covers the
  TCP listener via the same KIND_ACCEPT path, with the per-listener
  backoff re-arm from the reactor section (the single-flag backoff would
  leave one of two exhausted listeners permanently deaf).

## File changes

1. `crates/gnitz-engine/src/runtime/tls/{mod,config}.rs` (new, ~430
   lines total): conn state, read pump (`feed_decrypted` over a reused
   `io::RecvState`), flusher, `TlsPeer` internals, TLS/cert building. No
   `frame.rs` — the deframer is reused, not rebuilt.
2. `crates/gnitz-engine/src/runtime/reactor/`: delete `udp.rs` + UDP
   kinds/fields/preps (−~450 lines); retarget the `ring`-first drop-order
   SAFETY INVARIANT comment off `udp_recv_ops` onto the new raw-recv maps;
   add `recv_raw`/`send_raw` (`KIND_RAW_RECV` machinery + `SendAlive::RcVec(Rc<Vec<u8>>)`,
   ~170), `alloc_inbound_buf` factored out of the `handle_recv_cqe`
   `HeaderDone` arm and shared with the TLS pump (~10), `client_send_timeout`
   made `pub(crate)`, and multi-listener accept incl. per-listener EMFILE
   backoff (~50). `posix_io.rs`: delete `udp_bind`/`udp_local_addr`, add
   `tcp_bind` (incl. `SO_REUSEADDR`), `tcp_local_addr`, and `set_keepalive`
   (~55); the existing `shutdown` (SHUT_RDWR) is reused as-is.
3. `crates/gnitz-engine/src/runtime/orchestration/peer.rs` +
   `runtime/reactor/io.rs`: `PeerInner::Tls(Rc<TlsShared>)` variant; expose
   `RecvState`/`RecvBuf`/`RecvBuf::new`/`RecvAdvance` as `pub(crate)` for the
   `tls` module. `RecvBuf` stays a single malloc-backed variant — no enum
   change (~30 lines).
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

**Rust unit (engine `tls`, `feed_decrypted` over a reused `RecvState`)**: feed
a `TlsConn` decrypted plaintext (via a test rustls loopback or a direct
`RecvState` harness) — split delivery, pipelined frames, oversize reject,
zero-len reject, and header-time charging (cap trips after ~`CAP/64` one-byte
frames with a test-lowered cap; and a declared-but-undelivered oversize frame
is charged/refused at `HeaderDone`, before any payload byte, proving the
slow-drip OOM is closed).

**Rust unit (engine `tls`)**: inbound-counter reclaim on dirty drop — enqueue
several completed `RecvBuf`s into a `TlsConn`'s `inbound` (charging
`total_inbound_bytes`), then drop the `TlsConn` *without* popping them; assert
`total_inbound_bytes` returns to its pre-connection value (proves the queued
`RecvBuf`s' own `Drop` reconciles the counter — no manual reconciliation).

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
10. Teardown under write backpressure (the deadlock this plan closes): with
    `SO_SNDBUF`/`SO_RCVBUF` pinned small and the inbound cap test-lowered,
    one connection stops reading mid-scan-train while pipelining an
    inbound-cap-breaching burst; assert the server closes the connection
    promptly (well under the 10 s watchdog) and that after reap the live-conn
    count *and* `total_inbound_bytes` return to their pre-connection values
    (no deadlock, no counter leak). Under the same thread + join-with-timeout
    watchdog as test 8.
11. Handshake watchdog: open a raw `TcpStream`, complete only the TCP
    handshake, send no TLS bytes; assert the server drops the connection and
    frees its `MAX_TLS_CONNS` slot within ~10–12 s (a fresh `?insecure`
    connect afterward, with the cap set to 1, succeeds). Watchdog does **not**
    pin the slot: with the cap at 1, open a connection, complete HELLO, and
    close it *cleanly* well under 10 s; a second connect immediately afterward
    succeeds (proves the `Weak` watchdog released the slot on close, not 10 s
    later).
12. Client-send-timeout eviction (the `send_slot` W2M-ring guard): with
    `GNITZ_CLIENT_SEND_TIMEOUT_MS` test-lowered and `SO_SNDBUF`/`SO_RCVBUF`
    pinned small, one connection issues a multi-worker scan whose response
    exceeds the buffers, then **stops reading entirely** (sending nothing, so
    no recv-side death and no inbound-cap breach — only the send deadline can
    fire). Assert the server evicts it within the lowered timeout, the W2M
    slot and live-conn count are reclaimed, and a concurrent second client is
    unaffected (no cluster freeze). Same thread + join-with-timeout watchdog as
    test 8.

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
       harness TLS support; Rust integration tests 1–12. `make test` green.
- [ ] 4. E2E: conftest always-on listener + `GNITZ_TRANSPORT`,
       `test_tls.py`, `Makefile` `e2e-tls`. `make e2e` and `make e2e-tls`
       green.
