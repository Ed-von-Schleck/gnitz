//! The `--tls-*` flags and the rustls `ServerConfig` they resolve to.

use std::net::SocketAddr;
use std::sync::Arc;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// The `--tls-*` flags argv parsing checked against each other: a TLS
/// listener was asked for, and `--tls-cert`/`--tls-key` come as a pair.
pub(crate) struct TlsArgs {
    pub listen: SocketAddr,
    pub cert_key: Option<(String, String)>,
    pub client_ca: Option<String>,
    pub allow_unauthenticated: bool,
}

/// The names the minted dev certificate is valid for.
const DEV_CERT_NAMES: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// What `--tls-*` resolved to: the address to bind, the rustls config its
/// connections are served under, and a minted dev certificate's public PEM.
pub(crate) struct TlsConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) cfg: Arc<rustls::ServerConfig>,
    pub(crate) dev_pem: Option<String>,
}

impl TlsArgs {
    /// A listener on `listen` with every other flag at its default.
    #[cfg(test)]
    pub(crate) fn on(listen: &str) -> TlsArgs {
        TlsArgs {
            listen: listen.parse().unwrap(),
            cert_key: None,
            client_ca: None,
            allow_unauthenticated: false,
        }
    }

    /// Apply the bind policy and build the rustls config. With no `cert_key` a
    /// dev certificate is minted, whose private key never leaves this process;
    /// `client_ca` makes a client certificate chaining to it mandatory.
    ///
    /// Reads the PEM files and binds nothing, so every refusal here precedes the
    /// data directory.
    pub(crate) fn resolve(self) -> Result<TlsConfig, String> {
        let listen = self.listen;
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
        // The dev certificate names loopback alone. A wildcard bind is still
        // reachable through loopback; a specific address is not.
        if self.cert_key.is_none() && !listen.ip().is_loopback() {
            if !listen.ip().is_unspecified() {
                return Err(format!(
                    "refusing to bind the TLS listener to {listen} without --tls-cert and --tls-key: the dev \
                     certificate names {} only, so no client dialling {} could verify it",
                    DEV_CERT_NAMES.join(", "),
                    listen.ip(),
                ));
            }
            gnitz_warn!(
                "TLS listener on {listen} serves the dev certificate, minted at every boot for {} only: a \
                 client dialling any other address cannot verify it. Pass --tls-cert=PEM and --tls-key=PEM.",
                DEV_CERT_NAMES.join(", "),
            );
        }

        let (chain, key, dev_pem) = match &self.cert_key {
            Some((cert_path, key_path)) => {
                let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
                    .and_then(|it| it.collect())
                    .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?;
                let key = PrivateKeyDer::from_pem_file(key_path).map_err(|e| format!("tls key {key_path:?}: {e}"))?;
                (chain, key, None)
            }
            None => {
                let ck = rcgen::generate_simple_self_signed(DEV_CERT_NAMES.map(String::from).to_vec())
                    .map_err(|e| format!("tls dev-cert mint failed: {e}"))?;
                let chain = vec![ck.cert.der().clone()];
                let key = PrivateKeyDer::Pkcs8(ck.signing_key.serialize_der().into());
                (chain, key, Some(ck.cert.pem()))
            }
        };

        let builder = rustls::ServerConfig::builder();
        let auth = match &self.client_ca {
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
        Ok(TlsConfig { listen, cfg: Arc::new(cfg), dev_pem })
    }
}
