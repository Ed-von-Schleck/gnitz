//! rustls `ServerConfig` construction: operator-supplied PEM cert/key, or an
//! auto-minted (rcgen) self-signed dev certificate for loopback use.
//!
//! Why auto-mint rather than committing a static dev cert+key to the repo: the
//! rcgen key exists only in this boot's memory, so there is no long-lived dev
//! private key to leak, or to be shared by every install. Only the public PEM
//! is persisted, for `?ca=` clients.

use std::sync::Arc;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build the server-side TLS configuration. `cert_key` = operator PEM
/// paths; `None` mints a self-signed dev cert for
/// `localhost`/`127.0.0.1`/`::1` (rcgen emits real IP SANs) and returns its
/// public PEM for persistence. The dev private key is never written to
/// disk — it is serialized in-memory for rustls and dropped.
///
/// `client_ca` = PEM path of the CA (chain) that signs acceptable client
/// certificates. `Some` **enables required mTLS**: the handshake installs a
/// webpki client-cert verifier built *without* `.allow_unauthenticated()`, so
/// a client that cannot present a cert chaining to this CA (valid dates, and
/// proven private-key possession via CertificateVerify) never completes the
/// handshake. `None` keeps `.with_no_client_auth()` (server-auth only).
pub(super) fn server_crypto(
    cert_key: Option<(&str, &str)>,
    client_ca: Option<&str>,
) -> Result<(Arc<rustls::ServerConfig>, Option<String>), String> {
    let (chain, key, dev_pem) = match cert_key {
        Some((cert_path, key_path)) => {
            use rustls::pki_types::pem::PemObject;
            let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert_path)
                .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("tls cert {cert_path:?}: {e}"))?;
            if chain.is_empty() {
                return Err(format!("tls cert {cert_path:?}: no certificates found"));
            }
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

    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("tls config: {e}"))?;
    // Required-mTLS when a client CA is configured; server-auth-only otherwise.
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
            // Built WITHOUT `.allow_unauthenticated()`, so the default
            // `AnonymousClientPolicy::Deny` makes a client cert mandatory (a
            // no-cert handshake fails with `CertificateRequired`).
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots),
                Arc::new(rustls::crypto::ring::default_provider()),
            )
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
    // gnitz connections are long-lived, so resumption tickets buy nothing, and
    // emitting none removes the post-handshake `NewSessionTicket` send that
    // would otherwise race connection_loop's HELLO-ACK under `send_mutex`.
    // 0-RTT stays off (`max_early_data_size` defaults to 0): once a future auth
    // layer gives a session DML authority, replayable early data is replayable DML.
    cfg.send_tls13_tickets = 0;
    Ok((Arc::new(cfg), dev_pem))
}

#[cfg(test)]
#[path = "tests/config.rs"]
mod tests;
