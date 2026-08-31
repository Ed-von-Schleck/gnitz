use super::*;
use std::io::Write;

#[test]
fn dev_cert_mint_builds_server_config() {
    let (cfg, dev_pem) = server_crypto(None, None).expect("dev-cert mint must succeed");
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
    // 0-RTT stays off: replayable early data would be replayable DML once an
    // auth layer grants authority. `gnitz-core`'s transport tests assert the
    // client-side mirror.
    assert_eq!(cfg.max_early_data_size, 0, "0-RTT early data must be disabled");
    assert_eq!(cfg.send_tls13_tickets, 0, "and no resumption ticket is issued");
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

    let (cfg, dev_pem) = server_crypto(Some((cert_path.to_str().unwrap(), key_path.to_str().unwrap())), None)
        .expect("PEM cert+key must load");
    assert!(dev_pem.is_none(), "operator path must not mint a dev cert");
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
}

#[test]
fn client_ca_builds_required_mtls_config() {
    // Mint a CA cert, write its public PEM, and feed it as the client CA:
    // the dev server cert is minted, mTLS verification is enabled.
    let ca = rcgen::generate_simple_self_signed(vec!["client-ca".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("client_ca.pem");
    std::fs::File::create(&ca_path)
        .unwrap()
        .write_all(ca.cert.pem().as_bytes())
        .unwrap();

    let (cfg, dev_pem) = server_crypto(None, Some(ca_path.to_str().unwrap())).expect("mTLS config must build");
    assert!(
        dev_pem.is_some(),
        "dev server cert still minted alongside the client CA"
    );
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
}

#[test]
fn missing_pem_paths_error_cleanly() {
    assert!(server_crypto(Some(("/nonexistent/cert.pem", "/nonexistent/key.pem")), None).is_err());
    assert!(server_crypto(None, Some("/nonexistent/ca.pem")).is_err());
}
