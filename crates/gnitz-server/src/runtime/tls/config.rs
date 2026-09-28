//! The `--tls-*` flags and the rustls `ServerConfig` they resolve to.

use std::net::SocketAddr;
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

/// What `--tls-*` resolved to: the address to bind, the rustls config its
/// connections are served under, and a minted dev certificate's public PEM.
pub(crate) struct TlsConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) cfg: Arc<rustls::ServerConfig>,
    pub(crate) dev_pem: Option<String>,
}

impl TlsArgs {
    /// Check the flags against each other and build the rustls config: `None`
    /// when no TLS listener was asked for.
    pub(crate) fn resolve(self) -> Result<Option<TlsConfig>, String> {
        let Some(listen) = self.listen else {
            if self.cert.is_some() || self.key.is_some() || self.client_ca.is_some() || self.allow_unauthenticated {
                return Err(
                    "--tls-cert, --tls-key, --tls-client-ca and --allow-unauthenticated require --tls-listen"
                        .to_string(),
                );
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
                 --tls-client-ca=PEM (required mTLS).",
            );
        }
        let (cfg, dev_pem) = server_crypto(cert_key, self.client_ca.as_deref())?;
        Ok(Some(TlsConfig { listen, cfg, dev_pem }))
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
            let cas = CertificateDer::pem_file_iter(path)
                .and_then(|it| it.collect::<Result<Vec<_>, _>>())
                .map_err(|e| format!("tls client-ca {path:?}: {e}"))?;
            let mut roots = rustls::RootCertStore::empty();
            for ca in cas {
                roots.add(ca).map_err(|e| format!("tls client-ca {path:?}: {e}"))?;
            }
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
