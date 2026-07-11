//! rustls `ServerConfig` construction: operator-supplied PEM cert/key, or an
//! auto-minted (rcgen) self-signed dev certificate for loopback use.
//!
//! Why auto-mint rather than committing a static dev cert+key to the repo: a
//! committed private key is the exact "key on disk" smell this design
//! otherwise avoids — CI secret-scanners flag it, and it invites misuse —
//! whereas rcgen mints an ephemeral key that never touches disk (only the
//! public PEM is persisted, for `?ca=` clients).

use std::sync::Arc;

use gnitz_wire::ALPN_GNITZ;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

/// Build the server-side TLS configuration. `cert_key` = operator PEM
/// paths; `None` mints a self-signed dev cert for
/// `localhost`/`127.0.0.1`/`::1` (rcgen emits real IP SANs) and returns its
/// public PEM for persistence. The dev private key is never written to
/// disk — it is serialized in-memory for rustls and dropped.
pub(crate) fn server_crypto(
    cert_key: Option<(&str, &str)>,
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

    let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| format!("tls config: {e}"))?
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .map_err(|e| format!("tls cert/key rejected: {e}"))?;
    cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
    // Load-bearing, not cosmetic (the field defaults to 2): gnitz connections
    // are long-lived, so resumption tickets add nothing, and disabling
    // NewSessionTicket emission removes the most common post-handshake
    // control-bytes race — tickets shipped by the flusher concurrently with
    // connection_loop's HELLO-ACK send. (The send_mutex-across-send
    // discipline is still required for handshake flights, KeyUpdate
    // responses, and alerts.) 0-RTT stays off (`max_early_data_size`
    // defaults to 0): once a future auth layer gives a session DML
    // authority, replayable early data would be replayable DML.
    cfg.send_tls13_tickets = 0;
    Ok((Arc::new(cfg), dev_pem))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn dev_cert_mint_builds_server_config() {
        let (cfg, dev_pem) = server_crypto(None).expect("dev-cert mint must succeed");
        assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
        let pem = dev_pem.expect("mint path must return the public PEM");
        assert!(pem.contains("BEGIN CERTIFICATE"));
        assert!(
            !pem.contains("PRIVATE KEY"),
            "the dev private key must never be exported"
        );
    }

    #[test]
    fn pem_cert_key_roundtrip_through_file_loading() {
        // Mint a cert+key with rcgen, write both as PEM to a tempdir, and
        // load them back through the operator file path.
        let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::File::create(&cert_path)
            .unwrap()
            .write_all(ck.cert.pem().as_bytes())
            .unwrap();
        std::fs::File::create(&key_path)
            .unwrap()
            .write_all(ck.signing_key.serialize_pem().as_bytes())
            .unwrap();

        let (cfg, dev_pem) = server_crypto(Some((cert_path.to_str().unwrap(), key_path.to_str().unwrap())))
            .expect("PEM cert+key must load");
        assert!(dev_pem.is_none(), "operator path must not mint a dev cert");
        assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
    }

    #[test]
    fn bad_pem_paths_error_cleanly() {
        assert!(server_crypto(Some(("/nonexistent/cert.pem", "/nonexistent/key.pem"))).is_err());
    }
}
