//! TLS 1.3 over TCP: rustls driven by the transport's own non-blocking cores.
//! Not `rustls::StreamOwned`: its `Read` would be a plaintext path around the
//! deframer.

use std::io::{self, BufRead, IoSlice, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::sync::Arc;
use std::time::Instant;

use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, ClientConnection, RootCertStore};

use super::super::error::ProtocolError;
use super::{timed_out, write_nonblocking, ClientTransport, Inner, WriteOutcome};

/// Parsed `HOST:PORT[?QUERY]` (the part after the `tls://` prefix).
struct Target {
    host: String,
    port: u16,
    /// `ca=PATH`: PEM root override; `None` verifies against the webpki roots.
    ca: Option<String>,
    /// `cert=PATH&key=PATH`: the client's chain and key, for mTLS.
    client_auth: Option<(String, String)>,
}

fn bad_target(msg: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("tls target: {msg}"))
}

/// Parse `HOST:PORT[?QUERY]`, the target after `tls://`. `QUERY` is
/// `&`-separated `ca=PATH`, `cert=PATH` and `key=PATH`, each at most once.
fn parse_target(rest: &str) -> io::Result<Target> {
    let (hostport, query) = match rest.split_once('?') {
        Some((hp, q)) => (hp, Some(q)),
        None => (rest, None),
    };
    let (mut ca, mut cert, mut key) = (None, None, None);
    for elem in query.into_iter().flat_map(|q| q.split('&')) {
        let (slot, path) = match elem.split_once('=') {
            Some(("ca", p)) => (&mut ca, p),
            Some(("cert", p)) => (&mut cert, p),
            Some(("key", p)) => (&mut key, p),
            _ => {
                return Err(bad_target(format!(
                    "param {elem:?} not recognised (expected `ca=`, `cert=` or `key=`)"
                )))
            }
        };
        if path.is_empty() {
            return Err(bad_target(format!("empty path in {elem:?}")));
        }
        if slot.replace(path.to_string()).is_some() {
            return Err(bad_target(format!("duplicate {elem:?}")));
        }
    }
    let client_auth = match (cert, key) {
        (Some(c), Some(k)) => Some((c, k)),
        (None, None) => None,
        (Some(_), None) => return Err(bad_target("`cert=` requires `key=`")),
        (None, Some(_)) => return Err(bad_target("`key=` requires `cert=`")),
    };

    let (host, port_str) = if let Some(bracketed) = hostport.strip_prefix('[') {
        // Bracketed IPv6: [::1]:PORT. Strip the brackets — ServerName and
        // (host, port) resolution both take the bare address.
        let (host, after) = bracketed
            .split_once(']')
            .ok_or_else(|| bad_target(format!("{hostport:?}: unterminated `[`")))?;
        let port = after
            .strip_prefix(':')
            .ok_or_else(|| bad_target(format!("{hostport:?}: expected `:PORT` after `]`")))?;
        (host, port)
    } else {
        hostport
            .rsplit_once(':')
            .ok_or_else(|| bad_target(format!("{hostport:?}: expected HOST:PORT")))?
    };
    if host.is_empty() {
        return Err(bad_target(format!("{hostport:?}: empty host")));
    }
    let port: u16 = port_str
        .parse()
        .map_err(|_| bad_target(format!("{hostport:?}: invalid port {port_str:?}")))?;
    Ok(Target {
        host: host.to_string(),
        port,
        ca,
        client_auth,
    })
}

fn bad_file(path: &str, e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("tls {path:?}: {e}"))
}

/// The `ClientConfig` for one connection, built on every connect so a replaced
/// `ca=`/`cert=`/`key=` file (the server's dev cert is re-minted each boot) is read afresh.
fn build_client_config(target: &Target) -> io::Result<Arc<ClientConfig>> {
    let roots = match &target.ca {
        None => RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        Some(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .and_then(|it| it.collect::<Result<Vec<_>, _>>())
                .map_err(|e| bad_file(path, e))?;
            let mut roots = RootCertStore::empty();
            for cert in certs {
                roots.add(cert).map_err(|e| bad_file(path, e))?;
            }
            if roots.is_empty() {
                return Err(bad_file(path, "no certificates"));
            }
            roots
        }
    };
    let builder = ClientConfig::builder().with_root_certificates(roots);
    let mut cfg = match &target.client_auth {
        None => builder.with_no_client_auth(),
        Some((cert, key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
                .map_err(|e| bad_file(cert, e))?;
            let key_der = PrivateKeyDer::from_pem_file(key).map_err(|e| bad_file(key, e))?;
            builder.with_client_auth_cert(chain, key_der).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("tls client cert/key rejected: {e}"),
                )
            })?
        }
    };
    cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
    Ok(Arc::new(cfg))
}

/// The plaintext rustls holds unsent before it refuses more, which bounds what
/// one `flush` moves out of the queue.
const SEND_BUFFER_BYTES: usize = 1 << 20;

/// The TCP connect to `rest` (the target after the `tls://` prefix), no later
/// than `until`. No TLS handshake is run.
pub(super) fn connect_tls(rest: &str, until: Instant) -> Result<ClientTransport, ProtocolError> {
    let target = parse_target(rest)?;
    let cfg = build_client_config(&target)?;
    let server_name = ServerName::try_from(target.host.clone())
        .map_err(|e| bad_target(format!("server name {:?}: {e}", target.host)))?;

    // Every resolved address in turn — `localhost` may resolve to [::1] ahead of
    // the 127.0.0.1 a server listens on — each on an equal share of the time left.
    let addrs: Vec<SocketAddr> = (target.host.as_str(), target.port).to_socket_addrs()?.collect();
    let mut last_err = bad_target(format!("{}:{}: no addresses resolved", target.host, target.port));
    let sock = 'connect: {
        for (i, addr) in addrs.iter().enumerate() {
            let share = until.saturating_duration_since(Instant::now()) / (addrs.len() - i) as u32;
            if share.is_zero() {
                last_err = timed_out();
                break;
            }
            match TcpStream::connect_timeout(addr, share) {
                Ok(s) => break 'connect s,
                Err(e) => last_err = e,
            }
        }
        return Err(last_err.into());
    };
    sock.set_nodelay(true)?;
    // Bare SO_KEEPALIVE: a silently half-open connection is eventually reaped rather
    // than parking an untimed read forever.
    set_sockopt_int(sock.as_raw_fd(), libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
    sock.set_nonblocking(true)?;
    let mut conn = ClientConnection::new(cfg, server_name)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("tls: {e}")))?;
    conn.set_buffer_limit(Some(SEND_BUFFER_BYTES));
    Ok(ClientTransport::new(Inner::Tls(Box::new(TlsInner { conn, sock }))))
}

/// The TLS arm's state: the rustls connection and the socket beneath it.
pub(super) struct TlsInner {
    conn: ClientConnection,
    sock: TcpStream,
}

impl TlsInner {
    pub(super) fn as_fd(&self) -> BorrowedFd<'_> {
        self.sock.as_fd()
    }

    pub(super) fn wants_write(&self) -> bool {
        self.conn.wants_write()
    }

    /// Ship what the socket takes of rustls's queued ciphertext; the rest stays queued,
    /// reported by `wants_write`.
    pub(super) fn ship(&mut self) -> Result<(), ProtocolError> {
        while self.conn.wants_write() {
            if let WriteOutcome::WouldBlock = write_nonblocking(|| self.conn.write_tls(&mut self.sock))? {
                break;
            }
        }
        Ok(())
    }

    /// The TLS write core: rustls takes a prefix of `slices`, held as plaintext
    /// until the handshake completes.
    pub(super) fn write_slices(&mut self, slices: &[IoSlice<'_>]) -> Result<WriteOutcome, ProtocolError> {
        debug_assert!(!self.conn.wants_write());
        write_nonblocking(|| self.conn.writer().write_vectored(slices))
    }

    /// Decrypt one read's ciphertext, handing `plain` every chunk it yields.
    /// False once the peer's close_notify is reached, every plaintext byte
    /// before it handed out.
    pub(super) fn ingest(
        &mut self,
        mut cipher: &[u8],
        mut plain: impl FnMut(&[u8]) -> Result<(), ProtocolError>,
    ) -> Result<bool, ProtocolError> {
        while !cipher.is_empty() {
            // rustls takes nothing more from a peer that has closed.
            if self.conn.read_tls(&mut cipher)? == 0 {
                return Ok(false);
            }
            if let Err(e) = self.conn.process_new_packets() {
                // Last gasp: the alert rustls queued for the peer.
                let _ = self.ship();
                return Err(io::Error::new(io::ErrorKind::InvalidData, format!("tls: {e}")).into());
            }
            let mut reader = self.conn.reader();
            loop {
                let took = match reader.fill_buf() {
                    Ok([]) => return Ok(false),
                    Ok(chunk) => {
                        plain(chunk)?;
                        chunk.len()
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                };
                reader.consume(took);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
#[path = "tests/tls.rs"]
mod tests;
