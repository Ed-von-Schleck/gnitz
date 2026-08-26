//! TLS 1.3 over TCP client transport: rustls (`ring` provider) over a
//! `std::net::TcpStream`, driven record by record by the transport's own
//! non-blocking cores rather than through `rustls::StreamOwned`. Nothing here
//! reads plaintext except [`TlsInner::read_into`] and nothing writes it except
//! [`TlsInner::write_slices`], so no path can bypass the frame reader and
//! desync the stream. The wire protocol rides verbatim inside the TLS stream
//! ("ZSets over the wire").

use std::io::{IoSlice, Read, Write};
use std::mem::MaybeUninit;
use std::net::{IpAddr, TcpStream, ToSocketAddrs};
use std::ops::Range;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::ClientConnection;

use super::super::error::ProtocolError;
use super::{set_sockopt_int, ClientTransport, Inner, ReadOutcome, WriteOutcome};

/// Connect + TLS-handshake + HELLO-exchange deadline: the per-address connect
/// timeout, the handshake's kernel read timeout, then the transport deadline
/// `mark_established` clears after the ACK.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

fn decode_err(msg: impl Into<String>) -> ProtocolError {
    ProtocolError::DecodeError(msg.into())
}

/// Server-certificate verification mode, from the target's single
/// optional param.
enum Verify {
    /// Default webpki roots (`webpki-roots` bundle).
    Default,
    /// `?ca=PATH`: PEM root override.
    Ca(String),
    /// `?insecure`: skip verification entirely.
    Insecure,
}

/// Parsed `HOST:PORT[?QUERY]` (the part after the `tls://` prefix).
struct Target {
    host: String,
    port: u16,
    verify: Verify,
    /// `?cert=PATH&key=PATH`: the client's own cert chain + private key for
    /// mTLS. Both-or-neither.
    client_auth: Option<(String, String)>,
}

/// Parse the target grammar: split on the **first** `?`; the left is
/// `HOST:PORT` (bracketed IPv6 supported), the right is an `&`-separated
/// query of `insecure` | `ca=PATH` | `cert=PATH` | `key=PATH` params (each
/// `PATH` taken literally to the next `&`, so it may contain `=` but not
/// `&`). Verification mode: at most one of `insecure` / `ca=` (both →
/// error); neither → default webpki roots. Client auth: `cert=` and `key=`
/// are both-or-neither. A duplicate of any param, an unrecognised param, an
/// empty `PATH`, or an empty query element (a stray `&`, or a bare trailing
/// `?` whose whole query is one empty element) is a connect-time error.
fn parse_target(rest: &str) -> Result<Target, ProtocolError> {
    let (hostport, query) = match rest.split_once('?') {
        Some((hp, q)) => (hp, Some(q)),
        None => (rest, None),
    };

    let mut seen_insecure = false;
    let mut ca: Option<String> = None;
    let mut cert: Option<String> = None;
    let mut key: Option<String> = None;
    // Accept exactly one non-empty value for a `name=PATH` param.
    let take = |slot: &mut Option<String>, name: &str, path: &str| -> Result<(), ProtocolError> {
        if path.is_empty() {
            return Err(decode_err(format!("tls target: empty `{name}=` path")));
        }
        if slot.is_some() {
            return Err(decode_err(format!("tls target: duplicate `{name}=`")));
        }
        *slot = Some(path.to_string());
        Ok(())
    };
    if let Some(q) = query {
        // One empty element rejects a stray `&` and a bare trailing `?` (whose
        // whole query splits to a single empty element) alike.
        for elem in q.split('&') {
            if elem.is_empty() {
                return Err(decode_err("tls target: empty query element (stray `&` or bare `?`)"));
            } else if elem == "insecure" {
                if seen_insecure {
                    return Err(decode_err("tls target: duplicate `insecure`"));
                }
                seen_insecure = true;
            } else if let Some(path) = elem.strip_prefix("ca=") {
                take(&mut ca, "ca", path)?;
            } else if let Some(path) = elem.strip_prefix("cert=") {
                take(&mut cert, "cert", path)?;
            } else if let Some(path) = elem.strip_prefix("key=") {
                take(&mut key, "key", path)?;
            } else {
                return Err(decode_err(format!(
                    "tls target param {elem:?} not recognised \
                     (expected `insecure`, `ca=PATH`, `cert=PATH`, or `key=PATH`)"
                )));
            }
        }
    }

    let verify = match (seen_insecure, ca) {
        (true, Some(_)) => return Err(decode_err("tls target: `insecure` and `ca=` are mutually exclusive")),
        (true, None) => Verify::Insecure,
        (false, Some(path)) => Verify::Ca(path),
        (false, None) => Verify::Default,
    };
    let client_auth = match (cert, key) {
        (Some(c), Some(k)) => Some((c, k)),
        (None, None) => None,
        (Some(_), None) => return Err(decode_err("tls target: `cert=` requires `key=`")),
        (None, Some(_)) => return Err(decode_err("tls target: `key=` requires `cert=`")),
    };

    let (host, port_str) = if let Some(bracketed) = hostport.strip_prefix('[') {
        // Bracketed IPv6: [::1]:PORT. Strip the brackets — ServerName and
        // (host, port) resolution both take the bare address.
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| decode_err(format!("tls target {hostport:?}: unterminated `[`")))?;
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| decode_err(format!("tls target {hostport:?}: expected `:PORT` after `]`")))?;
        (host, port)
    } else {
        hostport
            .rsplit_once(':')
            .ok_or_else(|| decode_err(format!("tls target {hostport:?}: expected HOST:PORT")))?
    };
    if host.is_empty() {
        return Err(decode_err(format!("tls target {hostport:?}: empty host")));
    }
    let port: u16 = port_str
        .parse()
        .map_err(|_| decode_err(format!("tls target {hostport:?}: invalid port {port_str:?}")))?;
    Ok(Target {
        host: host.to_string(),
        port,
        verify,
        client_auth,
    })
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost" || host.parse::<IpAddr>().map(|ip| ip.is_loopback()).unwrap_or(false)
}

/// `?insecure` certificate verifier: accepts any server certificate.
/// Confidentiality/integrity of the stream remain; server authentication
/// is dropped — dev/test use only.
#[derive(Debug)]
struct SkipVerify(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for SkipVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Build a verification-mode-specific `ClientConfig` (TLS 1.3 + ring +
/// `gnitz/1` ALPN). Fallible `OnceLock` caching: `get_or_init` alone cannot
/// propagate a builder error, so a miss builds first and publishes after
/// (the benign race loser drops its copy).
fn cached_config(
    cell: &OnceLock<Arc<rustls::ClientConfig>>,
    build: impl FnOnce() -> Result<rustls::ClientConfig, ProtocolError>,
) -> Result<Arc<rustls::ClientConfig>, ProtocolError> {
    if let Some(cfg) = cell.get() {
        return Ok(Arc::clone(cfg));
    }
    // The `build` closure sets ALPN (via `finish`), so there is nothing to add.
    let cfg = build()?;
    Ok(Arc::clone(cell.get_or_init(|| Arc::new(cfg))))
}

fn config_builder() -> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ProtocolError> {
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| decode_err(format!("tls config: {e}")))
}

/// Resolve the immutable `ClientConfig` for a verification mode + optional
/// client-auth cert. The `Default` and `Insecure` configs with NO client
/// cert are pure values (the webpki root store alone is ~150 anchors), so
/// they are built once per process and shared — connection-churny callers
/// (one connection per test) skip the rebuild. `?ca=PATH` reads a file and a
/// client cert is per-connection, so those stay per-connect (a per-connection
/// cert must never be baked into a shared process-global config).
///
/// `client_auth` is taken by reference: a by-value `Option<(String, String)>`
/// would be moved by the `match` below and break the `finish` closure's reads.
fn build_client_config(
    verify: &Verify,
    client_auth: Option<&(String, String)>,
) -> Result<Arc<rustls::ClientConfig>, ProtocolError> {
    static DEFAULT_CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    static INSECURE_CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();

    // Installs client auth (or not) and always sets ALPN. Reached from the
    // `WantsClientCert` state that BOTH `.with_root_certificates(roots)` and
    // `.dangerous().with_custom_certificate_verifier(..)` return.
    let finish = |b: rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>|
     -> Result<rustls::ClientConfig, ProtocolError> {
        let mut cfg = match client_auth {
            Some((cert, key)) => {
                use rustls::pki_types::pem::PemObject;
                let chain = CertificateDer::pem_file_iter(cert)
                    .map_err(|e| decode_err(format!("tls client cert {cert:?}: {e}")))?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| decode_err(format!("tls client cert {cert:?}: {e}")))?;
                if chain.is_empty() {
                    return Err(decode_err(format!("tls client cert {cert:?}: empty")));
                }
                let key_der = rustls::pki_types::PrivateKeyDer::from_pem_file(key)
                    .map_err(|e| decode_err(format!("tls client key {key:?}: {e}")))?;
                b.with_client_auth_cert(chain, key_der)
                    .map_err(|e| decode_err(format!("tls client cert/key rejected: {e}")))?
            }
            None => b.with_no_client_auth(),
        };
        cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
        Ok(cfg)
    };

    // Verify-mode-specific builder, up to the shared `WantsClientCert` state
    // that both `.with_root_certificates` and the dangerous verifier setter
    // return — so each verifier is constructed exactly once.
    let verifier_stage = |v: &Verify| -> Result<
        rustls::ConfigBuilder<rustls::ClientConfig, rustls::client::WantsClientCert>,
        ProtocolError,
    > {
        Ok(match v {
            Verify::Default => config_builder()?.with_root_certificates(rustls::RootCertStore::from_iter(
                webpki_roots::TLS_SERVER_ROOTS.iter().cloned(),
            )),
            Verify::Insecure => config_builder()?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipVerify(Arc::new(
                    rustls::crypto::ring::default_provider(),
                )))),
            Verify::Ca(path) => {
                use rustls::pki_types::pem::PemObject;
                let mut store = rustls::RootCertStore::empty();
                let (added, _skipped) = store.add_parsable_certificates(
                    CertificateDer::pem_file_iter(path)
                        .map_err(|e| decode_err(format!("tls ca bundle {path:?}: {e}")))?
                        .filter_map(Result::ok),
                );
                if added == 0 {
                    return Err(decode_err(format!("tls ca bundle {path:?}: no usable certificates")));
                }
                config_builder()?.with_root_certificates(store)
            }
        })
    };

    // Share the immutable Default/Insecure configs only when there is no
    // per-connection client cert; `?ca=PATH` and any client-auth config are
    // built fresh (a per-connection cert must never be baked into a shared
    // process-global config).
    match (verify, client_auth) {
        (Verify::Default, None) => cached_config(&DEFAULT_CFG, || finish(verifier_stage(verify)?)),
        (Verify::Insecure, None) => cached_config(&INSECURE_CFG, || finish(verifier_stage(verify)?)),
        _ => Ok(Arc::new(finish(verifier_stage(verify)?)?)),
    }
}

/// Connect to `rest` (the target after the `tls://` prefix): TCP with a
/// per-address timeout (dual-stack fallback), TCP_NODELAY + SO_KEEPALIVE,
/// then the TLS 1.3 handshake driven to completion under the 10 s read
/// timeout (a handshake failure — bad cert, ALPN mismatch, wrong CA —
/// surfaces here as a `ProtocolError` carrying the rustls alert text).
pub(super) fn connect_tls(rest: &str) -> Result<ClientTransport, ProtocolError> {
    let target = parse_target(rest)?;

    // `?insecure` fail-closed: refuse to drop server authentication against a
    // non-loopback host unless explicitly overridden. Evaluated before any
    // socket work. `is_loopback_host` tests the literal host string, so a
    // remote name that merely *resolves* to a loopback IP cannot flip this
    // (DNS-rebinding safe).
    if matches!(target.verify, Verify::Insecure) && !is_loopback_host(&target.host) {
        if std::env::var("GNITZ_TLS_INSECURE").as_deref() != Ok("1") {
            return Err(decode_err(
                "refusing ?insecure against a non-loopback host; set GNITZ_TLS_INSECURE=1 to override",
            ));
        }
        static WARN_ONCE: std::sync::Once = std::sync::Once::new();
        WARN_ONCE.call_once(|| {
            eprintln!(
                "gnitz: WARNING: server certificate verification disabled by GNITZ_TLS_INSECURE=1 \
                 for a non-loopback host — the connection is open to man-in-the-middle interception"
            );
        });
    }

    // Resolve and try every returned address in turn (first-address-only
    // would hang the full timeout then fail when `localhost` resolves to
    // `[::1]` first but the server listens only on `127.0.0.1`).
    let mut last_err: Option<std::io::Error> = None;
    let stream = (target.host.as_str(), target.port)
        .to_socket_addrs()
        .map_err(ProtocolError::IoError)?
        .find_map(|addr| match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => Some(s),
            Err(e) => {
                last_err = Some(e);
                None
            }
        })
        .ok_or_else(|| match last_err.take() {
            Some(e) => ProtocolError::IoError(e),
            None => decode_err(format!(
                "tls target {}:{}: no addresses resolved",
                target.host, target.port
            )),
        })?;

    stream.set_nodelay(true).map_err(ProtocolError::IoError)?;
    // Bare `SO_KEEPALIVE` (kernel-default probing, ~2 h) so a silently
    // half-open connection is eventually reaped rather than parking an
    // established connection's untimed read forever. std's `TcpStream` has
    // no keepalive setter; no interval tuning, matching the server.
    set_sockopt_int(stream.as_raw_fd(), libc::SO_KEEPALIVE, 1);
    // Bounds the blocking handshake; SO_RCVTIMEO is per-read, so a byte-drip
    // server can stretch it — accepted for this posture.
    stream
        .set_read_timeout(Some(CONNECT_TIMEOUT))
        .map_err(ProtocolError::IoError)?;

    let cfg = build_client_config(&target.verify, target.client_auth.as_ref())?;
    // `ClientConnection::new` needs ServerName<'static>: the owned
    // TryFrom<String> conversion (accepts a DNS name or an IP literal —
    // the un-bracketed host).
    let server_name = ServerName::try_from(target.host.clone())
        .map_err(|e| decode_err(format!("tls server name {:?}: {e}", target.host)))?;
    let mut conn = ClientConnection::new(cfg, server_name).map_err(|e| decode_err(format!("tls: {e}")))?;
    let mut sock = stream;

    // Drive the handshake to completion now so certificate/ALPN failures
    // surface at connect time with the alert text, not on the first frame.
    while conn.is_handshaking() {
        conn.complete_io(&mut sock)
            .map_err(|e| decode_err(format!("tls handshake with {}:{}: {e}", target.host, target.port)))?;
    }
    // From here the transport's own deadline bounds the HELLO exchange over
    // the non-blocking fd; the kernel timeout has done its job.
    sock.set_read_timeout(None).map_err(ProtocolError::IoError)?;

    let mut t = ClientTransport::new(Inner::Tls(Box::new(TlsInner {
        conn,
        sock,
        cipher: Box::new_uninit_slice(CIPHER_BYTES),
        unread: 0..0,
        plain_avail: 0,
        eof: false,
        sock_drained: false,
    })))?;
    t.set_deadline(Some(CONNECT_TIMEOUT));
    Ok(t)
}

/// One socket read's worth of ciphertext. rustls's own `read_tls` asks the
/// source for at most 4 KiB at a time, so feeding it straight from the
/// socket would cost one `recv` per 4 KiB of a reply; reading into this
/// buffer once and handing rustls slices of it keeps the syscall count at
/// one per socket-buffer's worth, as on the Unix arm.
const CIPHER_BYTES: usize = 64 * 1024;

/// The TLS arm's state: the rustls connection and the socket beneath it,
/// plus what the last pump learned. `StreamOwned` is deliberately not used —
/// its `Read` would be a second plaintext path around the frame reader.
pub(super) struct TlsInner {
    conn: ClientConnection,
    sock: TcpStream,
    /// Ciphertext off the socket; `unread` is the part rustls has not taken.
    cipher: Box<[MaybeUninit<u8>]>,
    unread: Range<usize>,
    /// Decrypted bytes rustls holds that `reader()` has not handed out.
    plain_avail: usize,
    /// The peer closed the stream: a `close_notify`, or a TCP EOF.
    eof: bool,
    /// The last socket read returned less than it asked for.
    sock_drained: bool,
}

impl TlsInner {
    pub(super) fn as_raw_fd(&self) -> RawFd {
        self.sock.as_raw_fd()
    }

    pub(super) fn wants_write(&self) -> bool {
        self.conn.wants_write()
    }

    /// Ship what the socket takes of rustls's queued ciphertext, reporting
    /// whether any went. `WouldBlock` leaves the rest in `sendable_tls`, where
    /// `wants_write` reports it for a driver's `WRITE` interest.
    pub(super) fn ship_nonblocking(&mut self) -> Result<bool, ProtocolError> {
        let mut progress = false;
        while self.conn.wants_write() {
            match self.conn.write_tls(&mut self.sock) {
                Ok(0) => {
                    return Err(ProtocolError::IoError(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "tls write returned 0",
                    )))
                }
                Ok(_) => progress = true,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(ProtocolError::IoError(e)),
            }
        }
        Ok(progress)
    }

    /// The TLS write core. `writer().write_vectored` encrypts into rustls's
    /// bounded ciphertext buffer and returns 0 once it is full; shipping is
    /// what frees the space. `WouldBlock` means neither rustls nor the socket
    /// took anything.
    pub(super) fn write_slices(&mut self, slices: &[IoSlice<'_>]) -> Result<WriteOutcome, ProtocolError> {
        loop {
            let n = self
                .conn
                .writer()
                .write_vectored(slices)
                .map_err(ProtocolError::IoError)?;
            if n > 0 {
                self.ship_nonblocking()?;
                return Ok(WriteOutcome::Written(n));
            }
            if !self.ship_nonblocking()? {
                return Ok(WriteOutcome::WouldBlock);
            }
        }
    }

    /// The TLS read core: ship any ciphertext the read path queued (an alert;
    /// partially is fine), hand out decrypted plaintext, feed rustls the
    /// ciphertext already buffered, and read the socket only once both are
    /// exhausted — which is what keeps rustls's "received plaintext buffer
    /// full" refusal unreachable.
    pub(super) fn read_into(&mut self, buf: &mut [MaybeUninit<u8>]) -> Result<ReadOutcome, ProtocolError> {
        loop {
            self.ship_nonblocking()?;
            if self.plain_avail > 0 {
                let want = buf.len().min(self.plain_avail);
                let dst = &mut buf[..want];
                dst.fill(MaybeUninit::new(0));
                // SAFETY: just zeroed, so the slice is initialised.
                let dst = unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr() as *mut u8, want) };
                let n = self.conn.reader().read(dst).map_err(ProtocolError::IoError)?;
                self.plain_avail -= n;
                return Ok(ReadOutcome::Data {
                    n,
                    drained: self.plain_avail == 0 && self.unread.is_empty() && self.sock_drained,
                });
            }
            if !self.unread.is_empty() {
                let raw = &self.cipher[self.unread.clone()];
                // SAFETY: `unread` covers exactly the bytes a socket read initialised.
                let mut src = unsafe { std::slice::from_raw_parts(raw.as_ptr() as *const u8, raw.len()) };
                let taken = self.conn.read_tls(&mut src).map_err(ProtocolError::IoError)?;
                self.unread.start += taken;
                match self.conn.process_new_packets() {
                    Ok(st) => {
                        self.plain_avail = st.plaintext_bytes_to_read();
                        if st.peer_has_closed() {
                            self.eof = true;
                        }
                    }
                    Err(e) => {
                        let _ = self.ship_nonblocking();
                        return Err(ProtocolError::IoError(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("tls: {e}"),
                        )));
                    }
                }
                continue;
            }
            if self.eof {
                return Ok(ReadOutcome::Eof);
            }
            match super::unix_read_into(self.sock.as_raw_fd(), &mut self.cipher)? {
                ReadOutcome::Data { n, drained } => {
                    self.unread = 0..n;
                    self.sock_drained = drained;
                }
                ReadOutcome::Eof => self.eof = true,
                ReadOutcome::WouldBlock => return Ok(ReadOutcome::WouldBlock),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::{Interest, Reply, Request, Session};
    use crate::protocol::transport::{framed, hello_handshake, poll_fd};
    use std::net::TcpListener;
    use std::sync::mpsc;

    /// A `rustls::ServerConnection` over a loopback `TcpStream` on a helper
    /// thread: mints a self-signed cert, answers the HELLO with a real ACK so
    /// the client's `mark_established` runs, then hands the connection to
    /// `script`. The client reaches it through the fully public
    /// `ClientTransport::connect("tls://127.0.0.1:{port}?insecure")`.
    struct Loopback {
        target: String,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    /// The far end as the script sees it: a blocking rustls stream. One
    /// `write` with several frames in it puts them in as few records as
    /// rustls makes of the run — one, below 16 KiB.
    type ServerEnd = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

    fn write_frame(end: &mut ServerEnd, payload: &[u8]) {
        end.write_all(&framed(payload)).unwrap();
        end.flush().unwrap();
    }

    fn read_frame(end: &mut ServerEnd) -> Vec<u8> {
        let mut hdr = [0u8; 4];
        end.read_exact(&mut hdr).unwrap();
        let mut payload = vec![0u8; u32::from_le_bytes(hdr) as usize];
        end.read_exact(&mut payload).unwrap();
        payload
    }

    impl Loopback {
        fn start(script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
            Self::spawn(None, true, script)
        }

        /// `rcvbuf` pins the accepted socket's `SO_RCVBUF` (inherited from
        /// the listener) so the peer's window bounds what the client can push
        /// unread.
        fn start_with_rcvbuf(rcvbuf: Option<libc::c_int>, script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
            Self::spawn(rcvbuf, true, script)
        }

        /// Completes the TLS handshake and then never answers the HELLO,
        /// holding the socket open until `hold` returns.
        fn start_silent(hold: impl FnOnce() + Send + 'static) -> Self {
            Self::spawn(None, false, move |_end| hold())
        }

        fn spawn(
            rcvbuf: Option<libc::c_int>,
            ack_hello: bool,
            script: impl FnOnce(ServerEnd) + Send + 'static,
        ) -> Self {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let key = rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();
            let mut cfg =
                rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                    .with_protocol_versions(&[&rustls::version::TLS13])
                    .unwrap()
                    .with_no_client_auth()
                    .with_single_cert(vec![cert.cert.der().clone()], key)
                    .unwrap();
            cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
            let cfg = Arc::new(cfg);
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            if let Some(sz) = rcvbuf {
                set_sockopt_int(listener.as_raw_fd(), libc::SO_RCVBUF, sz);
            }
            let port = listener.local_addr().unwrap().port();
            let thread = std::thread::spawn(move || {
                let (sock, _) = listener.accept().unwrap();
                let mut end = rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
                // `StreamOwned` handshakes lazily on first I/O; a silent
                // script never does any, so finish it here.
                while end.conn.is_handshaking() {
                    end.conn.complete_io(&mut end.sock).unwrap();
                }
                if ack_hello {
                    let hello = read_frame(&mut end);
                    assert_eq!(hello.len(), gnitz_wire::HELLO_PAYLOAD_LEN as usize);
                    // `encode_hello_ack` frames the ACK itself.
                    let ack = gnitz_wire::encode_hello_ack(
                        gnitz_wire::HELLO_STATUS_OK,
                        gnitz_wire::MAX_FRAME_PAYLOAD_SERVER as u32,
                        0,
                    );
                    end.write_all(&ack).unwrap();
                    end.flush().unwrap();
                }
                script(end);
            });
            Loopback {
                target: format!("tls://127.0.0.1:{port}?insecure"),
                thread: Some(thread),
            }
        }

        fn connect(&self) -> ClientTransport {
            let mut t = ClientTransport::connect(&self.target).unwrap();
            hello_handshake(&mut t).unwrap();
            assert_eq!(t.max_payload_len(), gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);
            t
        }

        fn join(mut self) {
            self.thread.take().unwrap().join().unwrap();
        }
    }

    fn ctrl_reply(tid: u64, lsn: u128) -> Vec<u8> {
        crate::protocol::encode_control_frame(tid, 0, 0, lsn, 0, &[])
    }

    #[test]
    fn loopback_frame_split_across_two_records() {
        let payload: Vec<u8> = (0u8..=255).cycle().take(3000).collect();
        let p = payload.clone();
        let lb = Loopback::start(move |mut end| {
            let bytes = framed(&p);
            end.write_all(&bytes[..1000]).unwrap();
            end.flush().unwrap();
            std::thread::sleep(Duration::from_millis(50));
            end.write_all(&bytes[1000..]).unwrap();
            end.flush().unwrap();
        });
        let mut t = lb.connect();
        assert_eq!(t.recv_framed().unwrap(), payload);
        lb.join();
    }

    #[test]
    fn loopback_one_record_two_frames_one_step_completes_both_slots() {
        let lb = Loopback::start(|mut end| {
            read_frame(&mut end);
            read_frame(&mut end);
            let mut both = framed(&ctrl_reply(0, 1));
            both.extend(framed(&ctrl_reply(0, 2)));
            end.write_all(&both).unwrap();
            end.flush().unwrap();
        });
        let mut s = Session::from_transport(lb.connect());
        let req = ctrl_reply(0, 0);
        let a = s.submit(Request::Uncorrelated(req.clone())).unwrap();
        let b = s.submit(Request::Uncorrelated(req)).unwrap();
        assert!(s.step(Interest::WRITE).unwrap().is_empty());
        assert!(s.interest().read && !s.interest().write);
        // Park once; the one readable wakeup must complete both.
        let done = loop {
            poll_fd(s.as_raw_fd(), libc::POLLIN, None, true).unwrap();
            let d = s.step(Interest::READ).unwrap();
            if !d.is_empty() {
                break d;
            }
        };
        let ids: Vec<_> = done.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![a, b]);
        for (id, r) in done {
            let Reply::Train(t) = r.unwrap() else { panic!("train") };
            assert_eq!(t.terminal.seek_pk, if id == a { 1 } else { 2 });
        }
        assert_eq!(
            s.interest(),
            Interest::NONE,
            "nothing left in carry or in rustls's plaintext"
        );
        lb.join();
    }

    #[test]
    fn loopback_two_back_to_back_records_come_out_of_one_step() {
        let lb = Loopback::start(|mut end| {
            read_frame(&mut end);
            read_frame(&mut end);
            // Two records: one frame each, flushed separately.
            write_frame(&mut end, &ctrl_reply(0, 1));
            write_frame(&mut end, &ctrl_reply(0, 2));
        });
        let mut s = Session::from_transport(lb.connect());
        let req = ctrl_reply(0, 0);
        s.submit(Request::Uncorrelated(req.clone())).unwrap();
        s.submit(Request::Uncorrelated(req)).unwrap();
        s.step(Interest::WRITE).unwrap();
        // Let both records land before the one step reads.
        std::thread::sleep(Duration::from_millis(100));
        poll_fd(s.as_raw_fd(), libc::POLLIN, None, true).unwrap();
        let done = s.step(Interest::READ).unwrap();
        assert_eq!(done.len(), 2, "both frames from one step");
        assert_eq!(s.interest(), Interest::NONE);
        lb.join();
    }

    #[test]
    fn loopback_large_reply_spanning_many_records_is_intact() {
        // A 1 MiB reply is 64+ records: the ciphertext buffer is refilled
        // many times, records straddle refills, and the payload lands in one
        // exact allocation.
        let payload: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
        let p = payload.clone();
        let lb = Loopback::start(move |mut end| write_frame(&mut end, &p));
        let mut t = lb.connect();
        set_sockopt_int(t.as_raw_fd(), libc::SO_RCVBUF, 256 * 1024);
        assert_eq!(t.recv_framed().unwrap(), payload);
        lb.join();
    }

    #[test]
    fn loopback_step_write_can_empty_the_queue_with_ciphertext_still_pending() {
        // A frame under rustls's 64 KiB limit is taken by one `write_vectored`
        // — the queue empties — while the socket, with both buffers pinned
        // below the frame and a peer that is not yet reading, takes only part
        // of the ciphertext. `interest()` must still report WRITE for the
        // rest, and only a later step(WRITE) puts the frame's last bytes on
        // the wire.
        let frame: Vec<u8> = (0u8..=255).cycle().take(60 * 1024).collect();
        let expect = frame.clone();
        let (tx, rx) = mpsc::channel::<()>();
        let lb = Loopback::start_with_rcvbuf(Some(8 * 1024), move |mut end| {
            rx.recv().unwrap();
            assert_eq!(read_frame(&mut end), expect);
            write_frame(&mut end, &ctrl_reply(0, 9));
        });
        let t = lb.connect();
        set_sockopt_int(t.as_raw_fd(), libc::SO_SNDBUF, 8 * 1024);
        let mut s = Session::from_transport(t);
        let slot = s.submit(Request::Uncorrelated(frame)).unwrap();
        assert!(s.step(Interest::WRITE).unwrap().is_empty());
        assert!(s.queue_is_empty(), "rustls took the whole frame");
        assert!(
            s.interest().write,
            "ciphertext still in sendable_tls: the queue alone is not the predicate"
        );
        tx.send(()).unwrap();
        let mut ready = Interest::WRITE;
        let reply = loop {
            let mut d = s.step(ready).unwrap();
            if let Some(i) = d.iter().position(|(id, _)| *id == slot) {
                break d.swap_remove(i).1.unwrap();
            }
            let rev = poll_fd(s.as_raw_fd(), s.interest().poll_events(), None, true).unwrap();
            ready = Interest::from_revents(rev);
        };
        let Reply::Train(t) = reply else { panic!("train") };
        assert_eq!(t.terminal.seek_pk, 9);
        lb.join();
    }

    #[test]
    fn loopback_deadline_over_tls_never_tears_a_frame() {
        // A >64 KiB frame (so rustls forces mid-frame flushes) under a short
        // deadline to a peer that drains only after a delay, followed by a
        // small one: the server parses two well-formed frames.
        let big: Vec<u8> = (0u8..=255).cycle().take(400 * 1024).collect();
        let expect = big.clone();
        let lb = Loopback::start(move |mut end| {
            std::thread::sleep(Duration::from_millis(400));
            assert_eq!(read_frame(&mut end), expect);
            assert_eq!(read_frame(&mut end), b"after");
        });
        let mut t = lb.connect();
        set_sockopt_int(t.as_raw_fd(), libc::SO_SNDBUF, 16 * 1024);
        t.set_deadline(Some(Duration::from_millis(100)));
        // The big frame is sent once; on expiry its remainder is queue state,
        // and the small frame that follows queues behind it.
        let expired = match t.send_framed(&big) {
            Ok(()) => false,
            Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => true,
            Err(e) => panic!("{e}"),
        };
        assert!(expired, "the deadline must have fired");
        loop {
            match t.send_framed(b"after") {
                Ok(()) => break,
                Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("{e}"),
            }
        }
        lb.join();
    }

    #[test]
    fn loopback_established_connection_idle_past_connect_timeout_still_reads() {
        let lb = Loopback::start(|mut end| {
            std::thread::sleep(CONNECT_TIMEOUT + Duration::from_millis(500));
            write_frame(&mut end, b"still here");
        });
        let mut t = lb.connect();
        assert_eq!(t.recv_framed().unwrap(), b"still here");
        lb.join();
    }

    #[test]
    fn loopback_silent_peer_fails_the_hello_at_the_deadline_once() {
        // A peer that completes the TLS handshake and then says nothing:
        // the HELLO read fails at CONNECT_TIMEOUT, not at twice it.
        let (tx, rx) = mpsc::channel::<()>();
        let lb = Loopback::start_silent(move || rx.recv().unwrap_or(()));
        let mut t = ClientTransport::connect(&lb.target).unwrap();
        let t0 = std::time::Instant::now();
        assert!(matches!(
            hello_handshake(&mut t),
            Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock
        ));
        let took = t0.elapsed();
        assert!(
            took >= CONNECT_TIMEOUT && took < CONNECT_TIMEOUT + Duration::from_secs(2),
            "{took:?}"
        );
        tx.send(()).unwrap();
        lb.join();
    }

    #[test]
    fn loopback_clean_close_notify_surfaces_as_eof() {
        let lb = Loopback::start(|mut end| {
            write_frame(&mut end, b"last");
            end.conn.send_close_notify();
            end.flush().unwrap();
        });
        let mut t = lb.connect();
        assert_eq!(t.recv_framed().unwrap(), b"last");
        assert!(
            matches!(t.recv_framed(), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        lb.join();
    }

    #[test]
    fn parse_target_accepts_all_forms() {
        let t = parse_target("db.example.com:5433").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("db.example.com", 5433));
        assert!(matches!(t.verify, Verify::Default));
        assert!(t.client_auth.is_none());

        let t = parse_target("127.0.0.1:1?insecure").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("127.0.0.1", 1));
        assert!(matches!(t.verify, Verify::Insecure));
        assert!(t.client_auth.is_none());

        let t = parse_target("[::1]:65535?ca=/some/dir/cert.pem").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("::1", 65535));
        assert!(matches!(t.verify, Verify::Ca(ref p) if p == "/some/dir/cert.pem"));
        assert!(t.client_auth.is_none());

        // Client auth: cert+key, default verification.
        let t = parse_target("h:5?cert=/c&key=/k").unwrap();
        assert_eq!(
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
            Some(("/c", "/k"))
        );
        assert!(matches!(t.verify, Verify::Default));

        // Client auth + explicit CA.
        let t = parse_target("h:5?ca=/x&cert=/c&key=/k").unwrap();
        assert!(matches!(t.verify, Verify::Ca(ref p) if p == "/x"));
        assert_eq!(
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
            Some(("/c", "/k"))
        );

        // Client auth + insecure.
        let t = parse_target("h:5?insecure&cert=/c&key=/k").unwrap();
        assert!(matches!(t.verify, Verify::Insecure));
        assert_eq!(
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
            Some(("/c", "/k"))
        );

        // A `PATH` may contain `=` (taken literally to the next `&`).
        let t = parse_target("h:5?cert=/p=q&key=/k").unwrap();
        assert_eq!(
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
            Some(("/p=q", "/k"))
        );
    }

    #[test]
    fn parse_target_rejects_malformed() {
        for bad in [
            "",                        // empty
            "hostonly",                // no port
            ":443",                    // empty host
            "h:0x1f",                  // non-numeric port
            "h:99999",                 // port out of u16 range
            "h:443?",                  // empty query (bare trailing `?`)
            "h:443?ca=",               // empty CA path
            "h:443?insecure=1",        // unknown param
            "h:443?Insecure",          // params are case-sensitive
            "[::1]443",                // missing `:` after bracket
            "[::1:443",                // unterminated bracket
            "h:443?cert=/c",           // cert without key
            "h:443?key=/k",            // key without cert
            "h:443?insecure&ca=/x",    // insecure + ca mutually exclusive
            "h:443?cert=/c&cert=/d",   // duplicate cert
            "h:443?ca=/x&ca=/y",       // duplicate ca
            "h:443?insecure&insecure", // duplicate insecure
            "h:443?insecure&",         // trailing `&` (empty element)
            "h:443?&insecure",         // leading `&` (empty element)
        ] {
            assert!(
                parse_target(bad).is_err(),
                "{bad:?} must be rejected by the target parser",
            );
        }
    }

    #[test]
    fn loopback_detection() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("example.com"));
        assert!(!is_loopback_host("10.0.0.7"));
    }
}
