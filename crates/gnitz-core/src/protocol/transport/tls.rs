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
#[path = "tests/tls.rs"]
mod tests;
