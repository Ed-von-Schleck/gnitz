//! TLS 1.3 over TCP client transport: rustls (`ring` provider) over a
//! blocking `std::net::TcpStream` via `rustls::StreamOwned`. No async
//! runtime, no threads, no pump — a blocking read IS the wait. The wire
//! protocol rides verbatim inside the TLS stream ("ZSets over the wire").

use std::io::{IoSlice, Read, Write};
use std::net::{IpAddr, TcpStream, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConnection, StreamOwned};

use super::super::error::ProtocolError;
use super::{frame_len_prefix, parse_frame_len, ClientTransport, Inner};

/// Connect + TLS-handshake + HELLO-exchange deadline. Cleared to `None`
/// (block forever) by `hello_handshake` after the ACK.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

type TlsStream = StreamOwned<ClientConnection, TcpStream>;

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

/// Parsed `HOST:PORT[?PARAM]` (the part after the `tls://` prefix).
struct Target {
    host: String,
    port: u16,
    verify: Verify,
}

/// Parse the target grammar: split on the **first** `?`; the left is
/// `HOST:PORT` (bracketed IPv6 supported), the right is at most one param
/// — `insecure` or `ca=PATH` (taken literally to end-of-string), mutually
/// exclusive. Anything else is a connect-time error.
fn parse_target(rest: &str) -> Result<Target, ProtocolError> {
    let (hostport, param) = match rest.split_once('?') {
        Some((hp, p)) => (hp, Some(p)),
        None => (rest, None),
    };
    let verify = match param {
        None => Verify::Default,
        Some("insecure") => Verify::Insecure,
        Some(p) => match p.strip_prefix("ca=") {
            Some(path) if !path.is_empty() => Verify::Ca(path.to_string()),
            _ => {
                return Err(decode_err(format!(
                    "tls target param {p:?} not recognised (expected `insecure` or `ca=PATH`)"
                )))
            }
        },
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
    let mut cfg = build()?;
    cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
    Ok(Arc::clone(cell.get_or_init(|| Arc::new(cfg))))
}

fn config_builder() -> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ProtocolError> {
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| decode_err(format!("tls config: {e}")))
}

/// Resolve the immutable `ClientConfig` for a verification mode. The
/// `Default` and `Insecure` configs are pure values (the webpki root store
/// alone is ~150 anchors), so they are built once per process and shared —
/// connection-churny callers (one connection per test) skip the rebuild.
/// `?ca=PATH` reads a file, so it stays per-connect.
fn build_client_config(verify: &Verify) -> Result<Arc<rustls::ClientConfig>, ProtocolError> {
    static DEFAULT_CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    static INSECURE_CFG: OnceLock<Arc<rustls::ClientConfig>> = OnceLock::new();
    match verify {
        Verify::Default => cached_config(&DEFAULT_CFG, || {
            let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            Ok(config_builder()?.with_root_certificates(roots).with_no_client_auth())
        }),
        Verify::Insecure => cached_config(&INSECURE_CFG, || {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            Ok(config_builder()?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipVerify(provider)))
                .with_no_client_auth())
        }),
        Verify::Ca(path) => {
            use rustls::pki_types::pem::PemObject;
            let certs =
                CertificateDer::pem_file_iter(path).map_err(|e| decode_err(format!("tls ca bundle {path:?}: {e}")))?;
            let mut store = rustls::RootCertStore::empty();
            let (added, _skipped) = store.add_parsable_certificates(certs.filter_map(Result::ok));
            if added == 0 {
                return Err(decode_err(format!("tls ca bundle {path:?}: no usable certificates")));
            }
            let mut cfg = config_builder()?.with_root_certificates(store).with_no_client_auth();
            cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
            Ok(Arc::new(cfg))
        }
    }
}

/// Bare `SO_KEEPALIVE` (kernel-default probing, ~2 h) so a silently
/// half-open connection is eventually reaped rather than hanging the
/// post-handshake `recv` forever (its read timeout is cleared after the
/// HELLO ACK). std's `TcpStream` has no keepalive setter, so raw
/// `setsockopt`; no interval tuning, matching the server.
fn set_keepalive(stream: &TcpStream) {
    let on: libc::c_int = 1;
    // SAFETY: setsockopt on a valid fd with a properly-sized option value.
    unsafe {
        libc::setsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Connect to `rest` (the target after the `tls://` prefix): TCP with a
/// per-address timeout (dual-stack fallback), TCP_NODELAY + SO_KEEPALIVE,
/// then the TLS 1.3 handshake driven to completion under the 10 s read
/// timeout (a handshake failure — bad cert, ALPN mismatch, wrong CA —
/// surfaces here as a `ProtocolError` carrying the rustls alert text).
pub(super) fn connect_tls(rest: &str) -> Result<ClientTransport, ProtocolError> {
    let target = parse_target(rest)?;

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
    set_keepalive(&stream);
    // Bounds the handshake + HELLO exchange; `mark_established` clears it
    // after the ACK. SO_RCVTIMEO is per-read, so a byte-drip server can
    // stretch it — accepted for this posture.
    stream
        .set_read_timeout(Some(CONNECT_TIMEOUT))
        .map_err(ProtocolError::IoError)?;

    if matches!(target.verify, Verify::Insecure) && !is_loopback_host(&target.host) {
        // One-time warning: `?insecure` drops server authentication,
        // acceptable only for dev/test loopback use.
        static WARN_ONCE: std::sync::Once = std::sync::Once::new();
        WARN_ONCE.call_once(|| {
            eprintln!(
                "gnitz: WARNING: tls://{}:… with ?insecure skips server certificate \
                 verification on a non-loopback host — the connection is open to \
                 man-in-the-middle interception",
                target.host,
            );
        });
    }
    let cfg = build_client_config(&target.verify)?;
    // `ClientConnection::new` needs ServerName<'static>: the owned
    // TryFrom<String> conversion (accepts a DNS name or an IP literal —
    // the un-bracketed host).
    let server_name = ServerName::try_from(target.host.clone())
        .map_err(|e| decode_err(format!("tls server name {:?}: {e}", target.host)))?;
    let conn = ClientConnection::new(cfg, server_name).map_err(|e| decode_err(format!("tls: {e}")))?;
    let mut tls = StreamOwned::new(conn, stream);

    // Drive the handshake to completion now so certificate/ALPN failures
    // surface at connect time with the alert text, not on the first frame.
    while tls.conn.is_handshaking() {
        tls.conn
            .complete_io(&mut tls.sock)
            .map_err(|e| decode_err(format!("tls handshake with {}:{}: {e}", target.host, target.port)))?;
    }

    Ok(ClientTransport(Inner::Tls(Box::new(tls))))
}

/// Drain `slices` (`remaining` bytes total) into rustls's plaintext sink as
/// ONE vectored write sequence, flushing ciphertext to the socket whenever
/// rustls's outgoing buffer fills. Post-handshake rustls encrypts every
/// `writer()` call into records immediately — there is no flush-time
/// coalescing — so the vectored write is what lets a 4-byte length prefix
/// share a 16 KiB record with its payload (and small pipelined frames share
/// records with each other) instead of paying one record + AEAD seal per
/// slice.
fn write_all_vectored(
    s: &mut TlsStream,
    mut slices: &mut [IoSlice<'_>],
    mut remaining: usize,
) -> Result<(), ProtocolError> {
    while remaining > 0 {
        let n = s.conn.writer().write_vectored(slices).map_err(ProtocolError::IoError)?;
        if n == 0 {
            // Outgoing rustls buffer full: drain ciphertext to the socket, retry.
            flush_tls(s)?;
            continue;
        }
        IoSlice::advance_slices(&mut slices, n);
        remaining -= n;
    }
    Ok(())
}

/// Ship all ciphertext rustls has queued. The blocking `TcpStream` is the
/// pump; kernel socket-buffer backpressure lands here.
fn flush_tls(s: &mut TlsStream) -> Result<(), ProtocolError> {
    while s.conn.wants_write() {
        s.conn.write_tls(&mut s.sock).map_err(ProtocolError::IoError)?;
    }
    Ok(())
}

/// Send one length-prefixed frame built from `bufs` (one prefix over the
/// concatenation), as a single flush.
pub(super) fn send_framed_iov(s: &mut TlsStream, bufs: &[&[u8]]) -> Result<(), ProtocolError> {
    let total: usize = bufs.iter().map(|b| b.len()).sum();
    let prefix = frame_len_prefix(total)?;
    let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(1 + bufs.len());
    slices.push(IoSlice::new(&prefix));
    slices.extend(bufs.iter().map(|b| IoSlice::new(b)));
    write_all_vectored(s, &mut slices, 4 + total)?;
    flush_tls(s)
}

/// Send many length-prefixed frames as one vectored write sequence, flushed
/// once at the end (plus intermediate flushes whenever rustls's buffer
/// fills), so small frames coalesce into full records.
pub(super) fn send_framed_batch<F: AsRef<[u8]>>(s: &mut TlsStream, frames: &[F]) -> Result<(), ProtocolError> {
    if frames.is_empty() {
        return Ok(());
    }
    let mut total = 0usize;
    let mut prefixes = Vec::with_capacity(frames.len());
    for f in frames {
        let len = f.as_ref().len();
        prefixes.push(frame_len_prefix(len)?);
        total += 4 + len;
    }
    let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(2 * frames.len());
    for (f, prefix) in frames.iter().zip(&prefixes) {
        slices.push(IoSlice::new(prefix));
        slices.push(IoSlice::new(f.as_ref()));
    }
    write_all_vectored(s, &mut slices, total)?;
    flush_tls(s)
}

/// Receive one length-prefixed frame: `read_exact` the 4-byte prefix,
/// validate it exactly like the fd path (shared `parse_frame_len`), then
/// fill the payload without zero-initialising it — `take + read_to_end`
/// reads into uninitialised spare capacity, mirroring the fd path's
/// `recv_exact_uninit`. `StreamOwned`'s `Read` impl handles TLS record I/O
/// internally.
pub(super) fn recv_framed(s: &mut TlsStream, max_payload_len: usize) -> Result<Vec<u8>, ProtocolError> {
    let mut hdr = [0u8; 4];
    s.read_exact(&mut hdr).map_err(ProtocolError::IoError)?;
    let payload_len = parse_frame_len(hdr, max_payload_len)?;
    let mut buf = Vec::with_capacity(payload_len);
    let got = std::io::Read::by_ref(s)
        .take(payload_len as u64)
        .read_to_end(&mut buf)
        .map_err(ProtocolError::IoError)?;
    if got < payload_len {
        return Err(ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "connection closed mid-frame",
        )));
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_target_accepts_all_forms() {
        let t = parse_target("db.example.com:5433").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("db.example.com", 5433));
        assert!(matches!(t.verify, Verify::Default));

        let t = parse_target("127.0.0.1:1?insecure").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("127.0.0.1", 1));
        assert!(matches!(t.verify, Verify::Insecure));

        let t = parse_target("[::1]:65535?ca=/some/dir/cert.pem").unwrap();
        assert_eq!((t.host.as_str(), t.port), ("::1", 65535));
        assert!(matches!(t.verify, Verify::Ca(ref p) if p == "/some/dir/cert.pem"));
    }

    #[test]
    fn parse_target_rejects_malformed() {
        for bad in [
            "",                 // empty
            "hostonly",         // no port
            ":443",             // empty host
            "h:0x1f",           // non-numeric port
            "h:99999",          // port out of u16 range
            "h:443?ca=",        // empty CA path
            "h:443?insecure=1", // unknown param
            "h:443?Insecure",   // params are case-sensitive
            "[::1]443",         // missing `:` after bracket
            "[::1:443",         // unterminated bracket
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
