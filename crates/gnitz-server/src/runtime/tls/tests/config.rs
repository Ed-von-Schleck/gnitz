use super::*;

#[test]
fn dev_cert_mint_builds_server_config() {
    let (cfg, dev_pem) = server_crypto(None, None).expect("dev-cert mint must succeed");
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
    assert_eq!(cfg.max_early_data_size, 0, "0-RTT early data must be disabled");
    assert_eq!(cfg.send_tls13_tickets, 0, "and no resumption ticket is issued");
    assert!(cfg
        .crypto_provider()
        .cipher_suites
        .iter()
        .all(|s| s.version() == &rustls::version::TLS13));
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
    std::fs::write(&cert_path, ck.cert.pem()).unwrap();
    std::fs::write(&key_path, ck.signing_key.serialize_pem()).unwrap();

    let (cfg, dev_pem) = server_crypto(Some((cert_path.to_str().unwrap(), key_path.to_str().unwrap())), None)
        .expect("PEM cert+key must load");
    assert!(dev_pem.is_none(), "operator path must not mint a dev cert");
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
}

#[test]
fn client_ca_config_builds_beside_the_minted_cert() {
    let ca = rcgen::generate_simple_self_signed(vec!["client-ca".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("client_ca.pem");
    std::fs::write(&ca_path, ca.cert.pem()).unwrap();

    let (cfg, dev_pem) = server_crypto(None, Some(ca_path.to_str().unwrap())).expect("mTLS config must build");
    assert!(
        dev_pem.is_some(),
        "dev server cert still minted alongside the client CA"
    );
    assert_eq!(cfg.alpn_protocols, vec![ALPN_GNITZ.to_vec()]);
}

#[test]
fn client_ca_with_a_malformed_section_is_refused() {
    let ca = rcgen::generate_simple_self_signed(vec!["client-ca".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ca_path = dir.path().join("client_ca.pem");
    std::fs::write(
        &ca_path,
        format!(
            "{}-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n",
            ca.cert.pem()
        ),
    )
    .unwrap();
    let path = ca_path.to_str().unwrap();

    let Err(e) = server_crypto(None, Some(path)) else {
        panic!("a malformed section is refused")
    };
    assert!(e.contains(path) && e.contains("base64"), "{e}");
}

#[test]
fn missing_pem_paths_error_cleanly() {
    assert!(server_crypto(Some(("/nonexistent/cert.pem", "/nonexistent/key.pem")), None).is_err());
    assert!(server_crypto(None, Some("/nonexistent/ca.pem")).is_err());
}

fn args_on(listen: &str) -> TlsArgs {
    TlsArgs {
        listen: Some(listen.parse().unwrap()),
        ..TlsArgs::default()
    }
}

#[test]
fn resolve_refuses_an_unauthenticated_public_bind() {
    for public in ["0.0.0.0:0", "[::]:0", "[::ffff:127.0.0.1]:0"] {
        let Err(e) = args_on(public).resolve() else {
            panic!("{public} is refused")
        };
        assert!(e.contains("refusing to bind"), "{e}");
    }
    let hatch = TlsArgs {
        allow_unauthenticated: true,
        ..args_on("0.0.0.0:0")
    };
    assert!(hatch.resolve().expect("the escape hatch admits it").is_some());
    assert!(args_on("127.0.0.1:0").resolve().expect("loopback is turnkey").is_some());
}

#[test]
fn resolve_rejects_inconsistent_flags() {
    let half = TlsArgs {
        cert: Some("c.pem".into()),
        ..args_on("127.0.0.1:0")
    };
    assert_eq!(
        half.resolve().err().as_deref(),
        Some("--tls-cert and --tls-key must be given together")
    );
    let no_listen = TlsArgs {
        allow_unauthenticated: true,
        ..TlsArgs::default()
    };
    assert_eq!(
        no_listen.resolve().err().as_deref(),
        Some("--tls-cert, --tls-key, --tls-client-ca and --allow-unauthenticated require --tls-listen")
    );
    assert!(TlsArgs::default().resolve().unwrap().is_none());
}
