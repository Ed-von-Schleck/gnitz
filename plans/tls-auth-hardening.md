# TLS client authentication (required mTLS) + exposure hardening

## Problem

The TLS network transport ships a **fully unauthenticated** listener. TLS 1.3
gives confidentiality, integrity, and *server* authentication (the client
verifies the server cert), but **zero client authentication**: anyone who
completes the TLS handshake and the HELLO (magic + wire version) gets full
DDL/DML/scan. That is acceptable only for a loopback-bound, opt-in listener.
This plan makes a **network-reachable** listener safe by authenticating clients
with **required mTLS** (X.509 client certificates verified against an operator
CA) and adding the exposure hardening a reachable listener needs: a bind refusal
that makes an unauthenticated public bind impossible-by-accident, a global
connection cap, a pre-auth first-frame deadline, and a fail-closed client
`?insecure`.

The prerequisite TLS transport already exists. Facts this plan depends on,
verified against the source (line anchors are current):

- **Server config** — `runtime::tls::config::server_crypto(cert_key:
  Option<(&str, &str)>) -> Result<(Arc<rustls::ServerConfig>, Option<String>),
  String>` builds a TLS 1.3 / ring `ServerConfig`, chain ending in
  `.with_no_client_auth()` (`config.rs:52`), then sets `alpn_protocols =
  [ALPN_GNITZ]` and `send_tls13_tickets = 0`. `cert_key = None` mints an rcgen
  dev cert (SANs `localhost`/`127.0.0.1`/`::1`) and returns its public PEM.
  Callers: `bootstrap.rs:733`, and the tests `config.rs:76,103,111` +
  `tls/tests.rs:15`.
- **Server session** — `runtime::tls::mod.rs`: `TlsShared` owns `TlsConn { sess:
  rustls::ServerConnection, recv_state, max_payload_len, inbound, recv_waker,
  recv_closed, closed, pump_exited }`. `TlsShared::start(reactor, fd, cfg)`
  builds it and spawns `read_pump` + `flusher`. The pump drives the handshake
  and deframes plaintext via `feed_decrypted`; the flusher is the sole
  `libc::close(fd)`, gated on `closed() && pump_exited()`. `Peer::close()` →
  `TlsShared::close()` queues a `close_notify` and drives that teardown. Imports:
  `use std::cell::RefCell;` (no `Cell` yet).
- **Accept path** — `orchestration/executor.rs`: `accept_loop` awaits
  `reactor.accept() -> (fd, listener)` (io_uring **multishot** accept — no peer
  sockaddr). `AcceptCtx { unix_fd, tls: Option<(i32, Arc<ServerConfig>)> }`
  (`:283-286`). The TLS branch (`:302-317`) runs `set_nodelay`/`set_keepalive`,
  `TlsShared::start`, `Peer::tls`, and `spawn(connection_loop(peer, s))`.
  `ServerExecutor::run(... tls: Option<(i32, Arc<ServerConfig>)>)` (`:187`)
  attaches the listener via `if let Some((tls_fd, _)) = &tls { … }` (`:205-207`)
  and builds `AcceptCtx { unix_fd, tls }` (`:208-211`). `Shared.reactor` is
  `pub` (`:88`). `executor.rs:26` imports `use crate::runtime::tls::TlsShared;`.
- **Connection loop** — `connection_loop(peer, shared)` (`:334-357`) recvs the
  first frame, runs `run_hello_handshake` (validates magic + wire version,
  elevates the payload ceiling, sends the ACK; `:362-395`), then loops
  `handle_message` (`:735`). `run_hello_handshake` returns
  `HelloOutcome::Pass(Option<u64>)` hardcoded `None`; `handle_message` carries a
  dormant `if let Some(bound) = bound_client_id` check (`:748-761`) that never
  fires. All dead — removed here (§ Server 6).
- **Reactor primitives** — `reactor.timer(deadline: Instant) -> impl
  Future<Output=()>` submits a Timeout SQE and **cancels it on future drop**
  (`futures.rs:56-77`). `select2(a, b) -> Either<A,B>` stack-pins both owned
  futures and drops the loser (`sync.rs:592-611`); both are re-exported from
  `runtime::reactor` (`mod.rs:75`). `TlsRecvFuture::Drop` clears the recv waker,
  so racing `peer.recv()` under `select2` and dropping it is clean
  (`tls/mod.rs:352-358`).
- **Bootstrap** — `TlsCli { listen: SocketAddr, cert_key: Option<(String,
  String)> }` (`bootstrap.rs:338-341`). `setup_tls_listener(data_dir, cli)`
  (`:731-758`) calls `server_crypto`, persists the dev PEM, binds via
  `tcp_bind`, publishes `<data_dir>/tls_endpoint`, and today only **warns** on a
  non-loopback bind (`:750-757`). `server_main(... tls_cli: Option<TlsCli>)` maps
  it into `tls_init` and calls `ServerExecutor::run(..., tls_init)` (`:712-724`).
  Bootstrap reaches the executor via `crate::runtime::executor::…`.
- **CLI** — `main.rs:88-152` parses `--tls-listen`/`--tls-cert`/`--tls-key` into
  `tls_listen/tls_cert/tls_key`, then builds `TlsCli` via a `match (tls_listen,
  tls_cert, tls_key)` with a requires-`--tls-listen` guard and a
  cert-xor-key error (`:137-152`); `HELP_TEXT` documents them.
- **Client** — `gnitz-core/.../transport/tls.rs`: `parse_target(rest)`
  (`:51-94`) splits on the **first `?`** and takes **exactly one** param
  (`insecure` | `ca=PATH`, literal to end-of-string). `build_client_config`
  (`:168-197`) process-caches `Default`/`Insecure` in `OnceLock`s (setting
  `alpn_protocols` in the `cached_config` helper, `:153`) and builds `Ca(PATH)`
  fresh (setting `alpn_protocols` inline, `:193`); every arm ends
  `.with_no_client_auth()`. A non-loopback `?insecure` prints a one-time stderr
  warning but proceeds (`:257-269`). `is_loopback_host(host)` tests the literal
  string: `host == "localhost"` or an `IpAddr` that `is_loopback()` (`:96-98`).
- **`client_id`** — a wire control-block column (`gnitz-wire/control.rs`),
  **not** in HELLO (HELLO is a fixed 8-byte `[magic u32][version u16][flags
  u16]`, `gnitz-wire/handshake.rs`). The client **self-assigns** it
  (`connection.rs:29-33`, `PID<<32 | counter`). The server never registers or
  validates it and echoes it back verbatim; access is by `target_id`, placement
  by PK-hash, reply routing by `request_id`. `client_id` gates no resource — this
  is why no identity is bound to it (Non-goals).

## Committed decisions

| Decision | Choice |
|---|---|
| Auth mechanism | **Required mTLS client certificates** (X.509). The mTLS handshake *is* the authentication — a client that cannot present a CA-signed cert never completes the handshake, so it never reaches HELLO. Auth rides *inside* the TLS handshake, so the wire protocol (fixed 8-byte HELLO) is untouched. No shared secret: each client's private key stays on the client |
| Trust anchor | `--tls-client-ca=PEM`: the CA (chain) signing acceptable client certs. Presence **enables required mTLS**; absence keeps `.with_no_client_auth()` |
| Verifier | `rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::new(rustls::crypto::ring::default_provider())).build()?` → `Arc<dyn ClientCertVerifier>`, installed via `.with_client_cert_verifier(verifier)`. Built **without** `.allow_unauthenticated()`, so the default `AnonymousClientPolicy::Deny` makes client auth mandatory (a no-cert handshake fails with `CertificateRequired`). webpki verifies the full chain to the configured root, validity dates, CA-as-leaf rejection, and (via CertificateVerify) private-key possession |
| **No identity binding** | The server does **not** parse the client cert subject/CN and does **not** populate `bound_client_id`. `client_id` authorizes nothing (Problem); binding it is theater and would pull in an X.509-subject parser for zero gain. The dead `bound_client_id` plumbing is removed (§ Server 6). Security boundary = *authenticated (CA-signed) vs not* |
| Bind refusal | Binding a **non-loopback** address with neither `--tls-client-ca` nor `--allow-unauthenticated` **aborts boot** (`return 1`, clear message), replacing today's warning. Loopback binds stay turnkey. `--allow-unauthenticated` is the single, loud escape hatch |
| Global connection cap | `--tls-max-conns=N` (default **256**). The accept path closes a newly-accepted fd immediately (before any TLS work) when the live TLS-connection count is at the cap. A reactor-thread `Rc<Cell<u32>>` counter incremented by an RAII guard stored in `TlsShared`, decremented on its drop |
| First-frame (pre-auth) deadline | The **first** frame (HELLO) must arrive within `GNITZ_TLS_HELLO_TIMEOUT_MS` (default **15 000**) of accept, or the connection is torn down. Implemented by racing the first `peer.recv()` against `reactor.timer()` in `connection_loop` (§ First-frame deadline) — **no** separate task, `Weak`, or flag. Covers both a stalled TLS handshake and a completed-handshake-no-HELLO squat: in both, the first frame never deframes so `recv()` never resolves. 15 s comfortably exceeds the client's ~10 s post-connect handshake+HELLO budget, so legitimate slow-link clients are not reaped |
| Client `?insecure` fail-closed | The client **refuses** `?insecure` against a non-loopback host unless `GNITZ_TLS_INSECURE=1` (the connect returns a `ProtocolError` before any socket work). When the override *is* set, a one-time stderr warning is still emitted (observability). Loopback `?insecure` stays allowed |
| Client cert presentation | `tls://HOST:PORT?cert=PATH&key=PATH[&…]` — client loads its cert chain + key and installs them via `.with_client_auth_cert(chain, key)?`. Requires the multi-param grammar rewrite (§ Client). `cert` and `key` must appear together |
| 0-RTT lock | Server `max_early_data_size` stays `0` and client `enable_early_data` stays `false` (both rustls defaults; no ticketer is installed, and `send_tls13_tickets=0` emits none). A unit test asserts both fields |

## Non-goals (explicitly dropped — do not implement)

- **No CN→`client_id` binding, no X.509-subject parsing, no `x509-parser` dep.**
  `client_id` is a self-assigned tag that authorizes nothing. `rustls-webpki`
  does not expose the subject publicly (`cert.rs` `subject` is `pub(crate)`), and
  `x509-parser` is **not compiled** today (`cargo tree -e features -i
  x509-parser` prints nothing — rcgen's `ring` feature uses only
  `x509-parser?/verify`, which the `?` leaves disabled), so binding would add a
  real parser dependency for no gain. Revisit identity only when per-client
  **authorization** is designed — that design dictates the identity
  representation (and must derive it from the cert, never from `client_id`).
- **No per-IP connection cap.** Breaks NAT deployments (many clients share one
  source IP), redundant with the global cap for bounding total connections, no
  meaningful per-IP fairness when every client is CA-authenticated, and it would
  cost a `getpeername` per accept + a per-IP map.
- **No idle timeout.** Targets *authenticated* slot-holders (operator-issued,
  revocable) — a weak threat; the global cap + kernel `SO_KEEPALIVE` (set on both
  ends) + the per-frame `client_send_timeout` already bound dead/stalled peers.
- **No mid-connection deadline.** The first-frame deadline covers pre-auth; after
  HELLO the loop uses a plain `peer.recv().await` (no per-frame receive
  deadline).

## Accepted residuals (document in `HELP_TEXT`/operator notes; do not fix here)

- **EKU-absent client certs authenticate.** webpki's `KeyUsage::client_auth()` is
  *RequiredIfPresent*: a CA-signed leaf with valid dates and **no** EKU extension
  is accepted. So *any* leaf the configured CA signs can act as a gnitz client.
  Guidance: use a **dedicated** client-auth CA (don't reuse a CA that signs other
  cert kinds), or issue client certs with an explicit `clientAuth` EKU.
- **Coarse revocation only.** No CRL/OCSP is wired; a leaked client key is valid
  until the operator rotates the CA / drops the anchor, which invalidates all
  clients at once.
- **Loopback trusts every local UID.** `is_loopback()` means "not
  network-reachable," not "safe": any local user (or a container sharing the net
  namespace) can connect to `127.0.0.1:port` unauthenticated. This matches
  Postgres/Redis convention. The always-present AF_UNIX listener is likewise
  unauthenticated, gated only by filesystem permissions.
- **Pre-auth connection squatting (availability only).** The single cap counts
  pre-auth and authenticated connections together, so a single source completing
  TCP+TLS but never sending HELLO can hold slots (each for up to the first-frame
  deadline) and lock out legitimate clients at a low connection rate. Acceptable
  for the trusted-network/operator-CA posture and network-layer mitigable
  (firewall rate-limit). If the port is ever exposed to an untrusted network, add
  a pre-auth slot reservation or a handshake rate limit — out of scope here.

## Design — server

### 1. Crypto config (`config::server_crypto`)

Add `client_ca: Option<&str>` (return type unchanged). Replace the fixed
`.with_no_client_auth()` (`config.rs:52`) with an auth step chosen by `client_ca`:

```rust
// after `.with_protocol_versions(&[&rustls::version::TLS13]).map_err(...)?`
// let builder = <ConfigBuilder<ServerConfig, WantsVerifier>>;
let auth = match client_ca {
    Some(path) => {
        use rustls::pki_types::pem::PemObject;
        let mut roots = rustls::RootCertStore::empty();
        let (added, _) = roots.add_parsable_certificates(
            CertificateDer::pem_file_iter(path)
                .map_err(|e| format!("tls client-ca {path:?}: {e}"))?
                .filter_map(Result::ok),
        );
        if added == 0 {
            return Err(format!("tls client-ca {path:?}: no usable certificates"));
        }
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            std::sync::Arc::new(roots),
            std::sync::Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(|e| format!("tls client-ca {path:?}: verifier build failed: {e}"))?;
        builder.with_client_cert_verifier(verifier)
    }
    None => builder.with_no_client_auth(),
};
let mut cfg = auth.with_single_cert(chain, key).map_err(|e| format!("tls cert/key rejected: {e}"))?;
cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
cfg.send_tls13_tickets = 0;
```

Verified (rustls 0.23.41): `builder_with_provider(Arc<RootCertStore>,
Arc<CryptoProvider>) -> ClientCertVerifierBuilder`; `.build() -> Result<Arc<dyn
ClientCertVerifier>, rustls::server::VerifierBuilderError>`;
`with_client_cert_verifier(Arc<dyn ClientCertVerifier>)` on
`ConfigBuilder<ServerConfig, WantsVerifier>`. The `Arc<dyn ClientCertVerifier>`
is never *named* here (it flows from `.build()` into
`.with_client_cert_verifier`), so no `use` is needed; if ever named, import
`rustls::server::danger::ClientCertVerifier`. Do **not** add a `debug_assert` on
`max_early_data_size` in the builder — assert it in a unit test.

**Update the four existing callers** to pass the new arg:
`config.rs:76` `server_crypto(None, None)`; `config.rs:103`
`server_crypto(Some((cert,key)), None)`; `config.rs:111` `server_crypto(Some((…)),
None)`; `tls/tests.rs:15` `config::server_crypto(None, None)`.

### 2. Deriving `client_id` — not done

No identity is derived from the cert. `run_hello_handshake` gains no auth logic
beyond the dead-code removal in §6.

### 3. Global connection cap + RAII guard (`tls/mod.rs` + `executor.rs`)

In `tls/mod.rs`, add `use std::cell::Cell;` and:

```rust
/// Live-TLS-connection counter guard: `new` increments, `Drop` decrements.
/// Stored in `TlsShared`, so the count tracks the session lifetime exactly
/// (a session-init failure drops it inside `start`; a live session drops it at
/// full teardown).
pub(crate) struct ConnCountGuard(Rc<Cell<u32>>);
impl ConnCountGuard {
    pub(crate) fn new(c: Rc<Cell<u32>>) -> Self { c.set(c.get() + 1); Self(c) }
}
impl Drop for ConnCountGuard {
    fn drop(&mut self) {
        debug_assert!(self.0.get() > 0, "tls conn count underflow");
        self.0.set(self.0.get() - 1);
    }
}
```

`TlsShared` gains a field `_conn_guard: ConnCountGuard`; `TlsShared::start` takes
it as a new final parameter and stores it. `ServerConnection::new(cfg)?` runs
first in `start`, so if it fails the moved-in guard drops (decrement) as
`start`'s frame unwinds; on success it lives in the returned `TlsShared`.

In `executor.rs`, define the listener bundle and thread the counter:

```rust
/// TLS listener runtime inputs. Defined here; returned by
/// `bootstrap::setup_tls_listener`, so it is `pub(crate)`.
pub(crate) struct TlsListener {
    pub fd: i32,
    pub cfg: std::sync::Arc<rustls::ServerConfig>,
    pub max_conns: u32,
}
```

Type changes (all four sites — the plan's earlier tuple form is gone):
- `ServerExecutor::run(..., tls: Option<TlsListener>)` (`:187`).
- `if let Some(tl) = &tls { reactor.attach_listener(tl.fd); }` (`:205-207`).
- `AcceptCtx { unix_fd: i32, tls: Option<TlsListener>, tls_conn_count:
  Rc<Cell<u32>> }` (`:283-286`).
- In `run`, `let tls_conn_count = Rc::new(Cell::new(0u32));` and build
  `AcceptCtx { unix_fd: server_fd, tls, tls_conn_count }` (`:208-211`).

Extend `executor.rs`'s `use crate::runtime::tls::` to `{TlsShared,
ConnCountGuard}`.

Accept-path TLS branch (`:302-317`) — cap check before any TLS work:

```rust
Some(tl) if listener == tl.fd => {
    if ctx.tls_conn_count.get() >= tl.max_conns {
        gnitz_warn!("tls: connection cap {} reached; closing fd={fd}", tl.max_conns);
        unsafe { libc::close(fd) };                     // freshly-accepted fd we own
        continue;
    }
    posix_io::set_nodelay(fd);
    posix_io::set_keepalive(fd);
    let guard = ConnCountGuard::new(Rc::clone(&ctx.tls_conn_count));
    match TlsShared::start(Rc::clone(&shared.reactor), fd, Arc::clone(&tl.cfg), guard) {
        Ok(conn) => {
            let peer = Peer::tls(conn);
            let deadline = Instant::now() + tls_hello_timeout();
            shared.reactor.spawn(connection_loop(peer, Rc::clone(&shared), Some(deadline)));
        }
        Err(e) => {
            // `guard` was moved into `start`; it already dropped (decrementing)
            // inside `start`'s frame on the error path.
            gnitz_warn!("tls: session init failed for fd={fd}: {e}");
            unsafe { libc::close(fd) };
        }
    }
}
```

No TOCTOU: on the single-threaded reactor there is no `.await` between the cap
check and `ConnCountGuard::new` (only synchronous `set_nodelay`/`set_keepalive`/
`start`), so the count cannot go stale between them.

### 4. Bind refusal (`bootstrap.rs`)

`TlsCli` gains `client_ca: Option<String>`, `allow_unauthenticated: bool`,
`max_conns: u32`. `setup_tls_listener` returns `Result<TlsListener, String>`
(import `use crate::runtime::executor::TlsListener;`), and `server_main` threads
that `TlsListener` into `ServerExecutor::run`. Before `tcp_bind`:

```rust
if !cli.listen.ip().is_loopback() && cli.client_ca.is_none() && !cli.allow_unauthenticated {
    return Err(format!(
        "refusing to bind a non-loopback TLS listener {} without client authentication; \
         pass --tls-client-ca=PEM or --allow-unauthenticated",
        cli.listen,
    ));
}
```

`cli.listen.ip().is_loopback()` is the correct conservative test: `0.0.0.0`/`::`
(bind-all), IPv4-mapped `::ffff:127.0.0.1`, and any specific LAN/link-local IP
are all non-loopback → refused. Pass `cli.client_ca.as_deref()` into
`server_crypto`. Replace the existing non-loopback *warning* (`:750-757`): keep a
softened warning only on the `--allow-unauthenticated` escape-hatch path (a
deliberately-unauthenticated non-loopback bind should still be loud). If both
`--tls-client-ca` and `--allow-unauthenticated` are passed, the CA wins (mTLS
enabled) and `--allow-unauthenticated` is a silent no-op — acceptable.

### 5. CLI (`main.rs`)

Add locals `tls_client_ca: Option<String>`, `allow_unauthenticated: bool`
(bare-flag `--allow-unauthenticated`), `tls_max_conns: Option<u32>` (`Option` so
"unset" is distinguishable; reject non-numeric with the existing error-exit
pattern). Replace the `match (tls_listen, tls_cert, tls_key)` block (`:137-152`)
with:

```rust
let tls_cli = match tls_listen {
    None => {
        if tls_cert.is_some() || tls_key.is_some() || tls_client_ca.is_some()
            || allow_unauthenticated || tls_max_conns.is_some()
        {
            eprintln!("Error: --tls-* flags require --tls-listen");
            return; // mirror the existing error-exit at main.rs:137-152
        }
        None
    }
    Some(listen) => {
        let cert_key = match (tls_cert, tls_key) {
            (None, None) => None,
            (Some(cert), Some(key)) => Some((cert, key)),
            _ => { eprintln!("Error: --tls-cert and --tls-key must be given together"); return; }
        };
        Some(runtime::TlsCli {
            listen,
            cert_key,
            client_ca: tls_client_ca,
            allow_unauthenticated,
            max_conns: tls_max_conns.unwrap_or(256),
        })
    }
};
```

Update `HELP_TEXT`: replace the "fully UNAUTHENTICATED" block with docs for
`--tls-client-ca=PEM` (enables required mTLS), `--allow-unauthenticated` (the
escape hatch — what it exposes, incl. the loopback-trusts-local-UIDs residual),
and `--tls-max-conns=N` (default 256).

### 6. Remove the dead `bound_client_id` plumbing (`executor.rs`)

- `HelloOutcome::Pass(Option<u64>)` → `HelloOutcome::Pass` (unit). Drop the `let
  bound_client_id: Option<u64> = None;` line + comment (`:384-394`);
  `run_hello_handshake` returns `HelloOutcome::Pass` on success.
- `connection_loop`: drop the `bound_client_id` local (see § First-frame
  deadline for its new body).
- `handle_message`: drop the `bound_client_id: Option<u64>` param and the whole
  `if let Some(bound) = bound_client_id { … }` block (`:748-761`); update the
  call site.

## First-frame deadline (`executor.rs::connection_loop`)

`connection_loop` gains a `first_frame_deadline: Option<Instant>` parameter (TLS
passes `Some`, AF_UNIX passes `None` — so the mature loopback path is behaviorally
unchanged). Race the **first** recv against the deadline; everything after HELLO
is unchanged:

```rust
async fn connection_loop(peer: Peer, shared: Rc<Shared>, first_frame_deadline: Option<Instant>) {
    let first = match first_frame_deadline {
        Some(deadline) => match select2(peer.recv(), shared.reactor.timer(deadline)).await {
            Either::A(opt) => opt,
            Either::B(()) => { peer.close(); return; }   // no HELLO in time → normal close cascade
        },
        None => peer.recv().await,
    };
    let buf = match first {
        Some(b) => b,
        None => { peer.close(); return; }
    };
    match run_hello_handshake(&peer, buf.as_slice()).await {
        HelloOutcome::Pass => {}
        HelloOutcome::Reject => { peer.close(); return; }
    }
    loop {
        let buf = match peer.recv().await {
            Some(v) => v,
            None => break,
        };
        handle_message(&peer, buf.as_slice(), &shared).await;
    }
    peer.close();
}
```

Add the helper + imports in `executor.rs` (`select2`, `Either` are re-exported
from `runtime::reactor`):

```rust
fn tls_hello_timeout() -> std::time::Duration {
    static T: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *T.get_or_init(|| std::time::Duration::from_millis(
        std::env::var("GNITZ_TLS_HELLO_TIMEOUT_MS").ok()
            .and_then(|s| s.parse().ok()).unwrap_or(15_000)))
}
```

The AF_UNIX accept site (`:298`) becomes `connection_loop(peer, s, None)`.

**Why `peer.close()` suffices (no new teardown path):** on timeout,
`peer.close()` → `TlsShared::close()` sets `closed`, queues `close_notify`,
notifies the flusher. The flusher ships `close_notify` (fast: pre-HELLO the
server's socket send buffer is empty, so the small write does not park), then
half-closes the socket; the pump's parked `recv_raw` aborts and sets
`pump_exited`; the flusher's next pass closes the fd (`closed() &&
pump_exited()`). This is the exact cascade a client EOF triggers today. `Drop` of
the losing `peer.recv()` clears the recv waker; `Drop` of the losing
`reactor.timer()` cancels its Timeout SQE — so on the happy path (HELLO arrives)
the timer is cancelled immediately and no per-connection timer lingers (no
churn-DoS accumulation). The `ConnCountGuard` in `TlsShared` decrements when the
last `Rc<TlsShared>` drops at end of teardown.

## Design — client (`gnitz-core/.../transport/tls.rs`)

### Grammar rewrite (multi-param)

Keep the `HOST:PORT` / bracketed-IPv6 split. Rewrite the query parse (everything
after the first `?`):

- **Empty query** (`tls://h:1?`) → error (as today), *unless* you special-case
  `is_empty()` to `Default`; choose **error** to preserve current behavior
  (`parse_target("h:1?")` errors today). State it in the doc comment.
- Split the non-empty query on `&`; a resulting empty element (leading/trailing/
  doubled `&`, e.g. `?insecure&`) is an error.
- Each element ∈ `insecure` | `ca=PATH` | `cert=PATH` | `key=PATH` (match via
  `strip_prefix`; a `PATH` may contain `=` — taken literally — but not `&`).
  Anything else, or empty `PATH`, is an error.
- **Verification mode** → `Verify`: at most one of `insecure` / `ca=PATH`;
  neither → `Default`; both → error.
- **Client auth** → `Target.client_auth: Option<(String, String)>`: `cert=PATH`
  and `key=PATH` both-or-neither; one alone → error.
- **Duplicates** → error, including a repeated bare `insecure` (track with a
  `seen_insecure` bool) and a repeated `ca`/`cert`/`key`.

`Target` gains `client_auth: Option<(String, String)>`. Update the `parse_target`
tests: existing single-param cases hold; add `?cert=/c&key=/k`,
`?ca=/x&cert=/c&key=/k`, `?insecure&cert=/c&key=/k`, `?cert=/p=q&key=/k` (value
with `=` OK), and error cases `?cert=/c` (alone), `?insecure&ca=/x`,
`?cert=/c&cert=/d`, `?insecure&insecure`, `?insecure&` (trailing `&`).

### `?insecure` fail-closed (retain a warning on override)

Replace the `WARN_ONCE` block (`:257-269`) with, evaluated right after
`parse_target`, before any socket work:

```rust
if matches!(target.verify, Verify::Insecure) && !is_loopback_host(&target.host) {
    if std::env::var("GNITZ_TLS_INSECURE").as_deref() != Ok("1") {
        return Err(decode_err(
            "refusing ?insecure against a non-loopback host; set GNITZ_TLS_INSECURE=1 to override",
        ));
    }
    static WARN_ONCE: std::sync::Once = std::sync::Once::new();
    WARN_ONCE.call_once(|| eprintln!(
        "gnitz: WARNING: server certificate verification disabled by GNITZ_TLS_INSECURE=1 \
         for a non-loopback host — the connection is open to man-in-the-middle interception"));
}
```

`is_loopback_host` tests the literal host string, so a remote hostname that
*resolves* to a loopback IP does not satisfy it — DNS-rebinding cannot flip the
check.

### Installing the client cert (compile-correct)

`build_client_config` takes `client_auth: Option<&(String, String)>` (**by
reference** — a by-value `Option<(String,String)>` is moved by the `match` and
breaks the multi-arm `FnOnce` closure + the `client_auth.is_none()` reads).
Call site: `build_client_config(&target.verify, target.client_auth.as_ref())`.

```rust
fn build_client_config(
    verify: &Verify,
    client_auth: Option<&(String, String)>,
) -> Result<Arc<rustls::ClientConfig>, ProtocolError> {
    // Installs client auth (or not) and always sets ALPN. Reached from the
    // WantsClientCert state that BOTH `.with_root_certificates(roots)` and
    // `.dangerous().with_custom_certificate_verifier(..)` return.
    let finish = |b /* ConfigBuilder<ClientConfig, WantsClientCert> */|
        -> Result<rustls::ClientConfig, ProtocolError> {
        let mut cfg = match client_auth {
            Some((cert, key)) => {
                use rustls::pki_types::pem::PemObject;
                let chain = CertificateDer::pem_file_iter(cert)
                    .map_err(|e| decode_err(format!("tls client cert {cert:?}: {e}")))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| decode_err(format!("tls client cert {cert:?}: {e}")))?;
                if chain.is_empty() { return Err(decode_err(format!("tls client cert {cert:?}: empty"))); }
                let key = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
                    .map_err(|e| decode_err(format!("tls client key {key:?}: {e}")))?;
                b.with_client_auth_cert(chain, key)
                    .map_err(|e| decode_err(format!("tls client cert/key rejected: {e}")))?
            }
            None => b.with_no_client_auth(),
        };
        cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
        Ok(cfg)
    };

    // Cache Default/Insecure only when there is NO client cert (a per-connection
    // cert must not be baked into a shared process-global config). Ca is always
    // per-connect; with client_auth, all modes are per-connect.
    match (verify, client_auth) {
        (Verify::Default, None) => cached_config(&DEFAULT_CFG, || {
            let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            finish(config_builder()?.with_root_certificates(roots))
        }),
        (Verify::Insecure, None) => cached_config(&INSECURE_CFG, || {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            finish(config_builder()?.dangerous().with_custom_certificate_verifier(Arc::new(SkipVerify(provider))))
        }),
        (Verify::Default, Some(_)) => Ok(Arc::new(finish(
            config_builder()?.with_root_certificates(
                rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()))
        )?)),
        (Verify::Insecure, Some(_)) => Ok(Arc::new(finish(config_builder()?
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipVerify(Arc::new(rustls::crypto::ring::default_provider())))))?)),
        (Verify::Ca(path), _) => {
            use rustls::pki_types::pem::PemObject;
            let mut store = rustls::RootCertStore::empty();
            let (added, _) = store.add_parsable_certificates(
                CertificateDer::pem_file_iter(path)
                    .map_err(|e| decode_err(format!("tls ca bundle {path:?}: {e}")))?
                    .filter_map(Result::ok));
            if added == 0 { return Err(decode_err(format!("tls ca bundle {path:?}: no usable certificates"))); }
            Ok(Arc::new(finish(config_builder()?.with_root_certificates(store))?))
        }
    }
}
```

(`cached_config` already sets `alpn_protocols`; `finish` also sets it, which is
idempotent — harmless. `DEFAULT_CFG`/`INSECURE_CFG` statics and `config_builder`
are unchanged.) Verified: `with_client_auth_cert(self, Vec<CertificateDer<'static>>,
PrivateKeyDer<'static>) -> Result<ClientConfig, rustls::Error>`
(`client/builder.rs:146`); both `with_root_certificates` and the dangerous
verifier setter return `ConfigBuilder<ClientConfig, WantsClientCert>`.

## File changes

1. `crates/gnitz-engine/src/runtime/tls/config.rs`: `server_crypto` gains
   `client_ca: Option<&str>` + `WebPkiClientVerifier` install; the three test
   callers get `, None`.
2. `crates/gnitz-engine/src/runtime/tls/mod.rs`: `use std::cell::Cell;`;
   `ConnCountGuard`; `TlsShared._conn_guard` field + `start` param.
3. `crates/gnitz-engine/src/runtime/orchestration/executor.rs`: `TlsListener`
   struct (`pub(crate)`); `ServerExecutor::run`/`attach_listener`
   destructure/`AcceptCtx` retyped + `tls_conn_count`; accept-path cap check +
   guard; `connection_loop` `first_frame_deadline` param + `select2`;
   `tls_hello_timeout`; extend `use …tls::{TlsShared, ConnCountGuard}` and import
   `select2`/`Either`; **remove** dead `bound_client_id` plumbing (HelloOutcome
   variant, `handle_message` param + check, call site).
4. `crates/gnitz-engine/src/runtime/bootstrap.rs`: `TlsCli` gains `client_ca`/
   `allow_unauthenticated`/`max_conns`; `setup_tls_listener` returns
   `TlsListener` (import it), bind-refusal check, `client_ca` → `server_crypto`;
   `server_main` threads `TlsListener` to `ServerExecutor::run`.
5. `crates/gnitz-engine/src/main.rs`: `--tls-client-ca`,
   `--allow-unauthenticated`, `--tls-max-conns` flags + requires-listen guard;
   `TlsCli` construction restructure; `HELP_TEXT`.
6. `crates/gnitz-core/src/protocol/transport/tls.rs`: `parse_target` multi-param
   rewrite + `Target.client_auth`; `?insecure` fail-closed (with override
   warning); `build_client_config(verify, client_auth)` by-reference +
   `with_client_auth_cert`.
7. `crates/gnitz-test-harness/`: add `rcgen = { version = "0.14",
   default-features = false, features = ["crypto","pem","ring"] }` to
   `Cargo.toml`; `start_mtls(workers)` mints a **client CA** (rcgen
   `CertificateParams` with `IsCa::Ca(BasicConstraints::Unconstrained)`,
   self-signed) and a **leaf** signed by it (`params.signed_by(&leaf_key,
   &ca_cert, &ca_key)`), writes the CA public PEM + the leaf cert/key PEMs into
   the data dir, spawns with `--tls-client-ca=<ca.pem>`, and exposes
   `mtls_target()` → `tls://IP:PORT?cert=<leaf.pem>&key=<leafkey.pem>&ca=<server
   dev cert>`. The server keeps its minted dev **server** cert; only the client
   CA is added.
8. `crates/gnitz-py/tests/test_tls.py`, `Makefile`: an mTLS variant if desired.

## Tests

**Rust unit** (`config.rs`, `tls/tests.rs`):
- `server_crypto(None, Some(ca_pem_path))` builds a config (mint a CA to a
  tempfile); a bad path errors cleanly.
- 0-RTT lock: assert `server_cfg.max_early_data_size == 0` and
  `client_cfg.enable_early_data == false`.
- mTLS `handshaken_pair` variant: server config with a client verifier + a client
  config presenting a CA-signed leaf → handshake completes,
  `server.peer_certificates()` is `Some`.
- `parse_target` cases above (in `tls.rs`'s `#[cfg(test)]`).

**Rust integration** (`gnitz-core/tests/tls_client.rs`, gated as existing tests):
1. mTLS roundtrip: CA-signed client cert → connect + push + scan succeed.
2. No client cert vs an mTLS-required server → connect fails with a
   `ProtocolError` carrying the handshake alert.
3. Client cert from an untrusted CA → rejected.
4. Bind refusal: non-loopback `--tls-listen`, no `--tls-client-ca`, no
   `--allow-unauthenticated` → server exits non-zero with the refusal message
   (captured stderr); the same listen **with** `--allow-unauthenticated` boots.
5. Global cap: `--tls-max-conns=2` → a 3rd concurrent connection is closed
   immediately; after one closes, a new connect succeeds.
6. First-frame deadline: with `GNITZ_TLS_HELLO_TIMEOUT_MS=500`, a raw TCP
   connection that sends no HELLO (and one that stalls mid-handshake) is closed
   ~0.5 s later; a normal client is unaffected.
7. `?insecure` fail-closed: `tls://<non-loopback>?insecure` without
   `GNITZ_TLS_INSECURE=1` returns a `ProtocolError` before connecting; with the
   env set it proceeds (loopback `?insecure` still works — existing coverage).

**E2E** (`gnitz-py`): an mTLS-enabled server variant; a client presenting a cert.

## Implementation order

- [ ] 1. `gnitz-core`: `parse_target` multi-param rewrite + `Target.client_auth`;
       `?insecure` fail-closed (+override warning); `build_client_config` by-ref +
       `with_client_auth_cert`. Parse/unit tests green.
- [ ] 2. `gnitz-engine`: `server_crypto(client_ca)` + `WebPkiClientVerifier` (+
       4 caller updates); `TlsCli`/`TlsListener` threading; bind refusal; CLI
       flags + HELP; remove dead `bound_client_id` plumbing. Integration tests
       1–4 green.
- [ ] 3. `gnitz-engine`: `ConnCountGuard` + global cap; `connection_loop`
       `select2` first-frame deadline + `tls_hello_timeout`. Integration tests
       5–6 green.
- [ ] 4. Harness `rcgen` dep + `start_mtls` + E2E variant. `make verify` +
       `make e2e` green.
