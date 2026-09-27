//! The TLS listener from argv to bound socket: the `--tls-*` flags, the rustls
//! `ServerConfig`, and the TCP bind.

use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::Arc;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// The `--tls-*` flags as given, before any is checked against another.
#[derive(Default)]
pub(crate) struct TlsArgs {
    pub listen: Option<SocketAddr>,
    pub cert: Option<String>,
    pub key: Option<String>,
    pub client_ca: Option<String>,
    pub allow_unauthenticated: bool,
}

/// A TLS listener the flags asked for and the rustls config it serves, not yet
/// bound.
pub(crate) struct TlsConfig {
    listen: SocketAddr,
    cfg: Arc<rustls::ServerConfig>,
    /// The minted dev certificate's public PEM, published at bind.
    dev_pem: Option<String>,
}

impl TlsArgs {
    /// Check the flags against each other and build the rustls config: `None`
    /// when no TLS listener was asked for.
    pub(crate) fn resolve(self) -> Result<Option<TlsConfig>, String> {
        let Some(listen) = self.listen else {
            if self.cert.is_some() || self.key.is_some() || self.client_ca.is_some() || self.allow_unauthenticated {
                return Err("--tls-* flags require --tls-listen".to_string());
            }
            return Ok(None);
        };
        let cert_key = match (&self.cert, &self.key) {
            (None, None) => None,
            (Some(c), Some(k)) => Some((c.as_str(), k.as_str())),
            _ => return Err("--tls-cert and --tls-key must be given together".to_string()),
        };
        if !listen.ip().is_loopback() && self.client_ca.is_none() {
            if !self.allow_unauthenticated {
                return Err(format!(
                    "refusing to bind a non-loopback TLS listener {listen} without client authentication; \
                     pass --tls-client-ca=PEM or --allow-unauthenticated",
                ));
            }
            gnitz_warn!(
                "TLS listener on NON-LOOPBACK address {listen} with --allow-unauthenticated and NO client \
                 authentication: anyone who can reach this port gets full DDL/DML/scan access. Prefer \
                 --tls-client-ca=PEM (required mTLS). Note: even a loopback bind trusts every local UID.",
            );
        }
        let (cfg, dev_pem) = server_crypto(cert_key, self.client_ca.as_deref())?;
        Ok(Some(TlsConfig { listen, cfg, dev_pem }))
    }
}

impl TlsConfig {
    /// Bind the TCP listener and publish its address to `<data_dir>/tls_endpoint`,
    /// and a minted dev certificate's public PEM to `<data_dir>/tls_dev_cert.pem`.
    pub(crate) fn bind(self, data_dir: &str) -> Result<(OwnedFd, Arc<rustls::ServerConfig>), String> {
        use gnitz_foundation::posix_io::set_sockopt_int;

        let listener =
            TcpListener::bind(self.listen).map_err(|e| format!("failed to bind TLS listener {}: {e}", self.listen))?;
        let fd = listener.as_raw_fd();
        // Both are inherited by every accepted socket. Keepalive lets the kernel
        // reap a half-open peer that would otherwise park its recv forever.
        set_sockopt_int(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1);
        set_sockopt_int(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1);
        // A negative backlog re-listens at `net.core.somaxconn`.
        if unsafe { libc::listen(fd, -1) } < 0 {
            return Err(format!(
                "failed to widen the TLS listener backlog: {}",
                std::io::Error::last_os_error()
            ));
        }
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("failed to set the TLS listener non-blocking: {e}"))?;
        // Not `self.listen`, whose port may be 0.
        let bound = listener
            .local_addr()
            .map_err(|e| format!("failed to read the bound TLS address: {e}"))?;
        if let Some(pem) = self.dev_pem {
            let path = format!("{data_dir}/tls_dev_cert.pem");
            gnitz_store::storage::publish_file_sync(data_dir, "tls_dev_cert.pem", pem.as_bytes())
                .map_err(|e| format!("failed to publish {path}: {e}"))?;
            gnitz_info!(
                "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
                 public PEM at {path}"
            );
        }
        gnitz_store::storage::publish_file_sync(data_dir, "tls_endpoint", format!("{bound}\n").as_bytes())
            .map_err(|e| format!("failed to publish {data_dir}/tls_endpoint: {e}"))?;
        gnitz_info!("Listening on tls://{}", bound);
        Ok((OwnedFd::from(listener), self.cfg))
    }
}

/// The server config, and the public PEM of the dev certificate minted when no
/// operator `cert_key` is given; its private key never leaves this process.
/// `client_ca` makes a client certificate chaining to it mandatory.
pub(super) fn server_crypto(
    cert_key: Option<(&str, &str)>,
    client_ca: Option<&str>,
) -> Result<(Arc<rustls::ServerConfig>, Option<String>), String> {
    let (chain, key, dev_pem) = match cert_key {
        Some((cert_path, key_path)) => {
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
                .and_then(|it| it.collect())
                .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?;
            let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| format!("tls key {key_path:?}: {e}"))?;
            (chain, key, None)
        }
        None => {
            let ck = rcgen::generate_simple_self_signed(vec![
                "localhost".to_string(),
                "127.0.0.1".to_string(),
                "::1".to_string(),
            ])
            .map_err(|e| format!("tls dev-cert mint failed: {e}"))?;
            let chain = vec![ck.cert.der().clone()];
            let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
            (chain, key, Some(ck.cert.pem()))
        }
    };

    let builder = rustls::ServerConfig::builder();
    let auth = match client_ca {
        Some(path) => {
            let mut roots = rustls::RootCertStore::empty();
            roots.add_parsable_certificates(
                CertificateDer::pem_file_iter(path)
                    .map_err(|e| format!("tls client-ca {path:?}: {e}"))?
                    .filter_map(Result::ok),
            );
            let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| format!("tls client-ca {path:?}: verifier build failed: {e}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut cfg = auth
        .with_single_cert(chain, key)
        .map_err(|e| format!("tls cert/key rejected: {e}"))?;
    cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
    // Connections are long-lived: resumption buys nothing.
    cfg.send_tls13_tickets = 0;
    // Early data is replayable.
    cfg.max_early_data_size = 0;
    let cfg = Arc::new(cfg);
    // Every accepted connection builds a session from this config.
    rustls::ServerConnection::new(Arc::clone(&cfg)).map_err(|e| format!("tls config rejected: {e}"))?;
    Ok((cfg, dev_pem))
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
