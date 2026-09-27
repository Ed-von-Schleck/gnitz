use super::*;
use std::io::Write;

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
        Some("--tls-* flags require --tls-listen")
    );
    assert!(TlsArgs::default().resolve().unwrap().is_none());
}

/// `bind` publishes the bound endpoint and the dev PEM, and every accepted
/// socket inherits the listening socket's options.
#[test]
fn bind_publishes_both_files_and_accepted_sockets_inherit_the_options() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let tls = args_on("127.0.0.1:0").resolve().unwrap().unwrap();
    let (fd, _cfg) = tls.bind(data_dir).expect("bind");

    let endpoint = std::fs::read_to_string(dir.path().join("tls_endpoint")).unwrap();
    let addr: SocketAddr = endpoint.trim().parse().expect("a connectable address");
    assert_ne!(addr.port(), 0);
    let pem = std::fs::read_to_string(dir.path().join("tls_dev_cert.pem")).unwrap();
    assert!(pem.contains("BEGIN CERTIFICATE"));

    let _client = std::net::TcpStream::connect(addr).unwrap();
    let mut pfd = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    assert_eq!(unsafe { libc::poll(&mut pfd, 1, 5_000) }, 1, "the connect is pending");
    let accepted = unsafe { libc::accept(fd.as_raw_fd(), std::ptr::null_mut(), std::ptr::null_mut()) };
    assert!(accepted >= 0, "accept: {}", std::io::Error::last_os_error());
    // SAFETY: a fresh fd from `accept`, owned by nothing else.
    let accepted = unsafe { <OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(accepted) };
    let opt = |level, name| {
        let mut v: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let rc = unsafe { libc::getsockopt(accepted.as_raw_fd(), level, name, (&raw mut v).cast(), &mut len) };
        assert_eq!(rc, 0);
        v
    };
    assert_eq!(opt(libc::IPPROTO_TCP, libc::TCP_NODELAY), 1);
    assert_eq!(opt(libc::SOL_SOCKET, libc::SO_KEEPALIVE), 1);
}
