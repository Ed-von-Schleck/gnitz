use super::*;
use crate::runtime::tls::TlsArgs;

/// The AF_UNIX bind replaces only a stale socket: a regular file at the path
/// and a socket a live server answers on each refuse the boot, untouched.
#[test]
fn bind_unix_replaces_only_a_stale_socket() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let path_str = path.to_str().unwrap();

    std::fs::write(&path, b"data").unwrap();
    let e = bind_unix(path_str).expect_err("a regular file refuses");
    assert!(e.contains("is not a socket"), "{e}");
    assert_eq!(std::fs::read(&path).unwrap(), b"data", "and survives");
    std::fs::remove_file(&path).unwrap();

    let live = bind_unix(path_str).expect("a free path binds");
    let e = bind_unix(path_str).expect_err("a live socket refuses");
    assert!(e.contains("running server"), "{e}");

    drop(live);
    bind_unix(path_str).expect("a stale socket is replaced");
}

/// `bind_listeners` publishes the bound endpoint and the dev PEM after the
/// AF_UNIX listener, every accepted TLS socket inherits the listening socket's
/// options, and `clear_published` removes both files, tolerating their absence.
#[test]
fn bind_listeners_publishes_the_tls_files_and_accepted_sockets_inherit_the_options() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let sock = dir.path().join("s.sock");
    let tls = TlsArgs::on("127.0.0.1:0").resolve().unwrap();
    let mut listeners = bind_listeners(data_dir, sock.to_str().unwrap(), Some(tls)).expect("bind");

    let endpoint = std::fs::read_to_string(dir.path().join(TLS_ENDPOINT_FILE)).unwrap();
    let addr: SocketAddr = endpoint.trim().parse().expect("a connectable address");
    assert_ne!(addr.port(), 0);
    let pem = std::fs::read_to_string(dir.path().join(TLS_DEV_CERT_FILE)).unwrap();
    assert!(pem.contains("BEGIN CERTIFICATE"));

    assert_eq!(listeners.len(), 2);
    assert!(listeners[0].tls.is_none() && listeners[1].tls.is_some());
    let tcp = TcpListener::from(listeners.pop().unwrap().fd);
    let _client = std::net::TcpStream::connect(addr).unwrap();
    let (accepted, _) = tcp.accept().unwrap();
    assert!(accepted.nodelay().unwrap());
    let mut keepalive: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let rc = unsafe {
        libc::getsockopt(
            accepted.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            (&raw mut keepalive).cast(),
            &mut len,
        )
    };
    assert_eq!(rc, 0);
    assert_eq!(keepalive, 1);

    clear_published(data_dir).unwrap();
    clear_published(data_dir).unwrap();
    for name in [TLS_ENDPOINT_FILE, TLS_DEV_CERT_FILE] {
        assert!(!dir.path().join(name).exists(), "{name} is removed");
    }
}

/// The TCP socket binds first, so a boot that fails it exposes neither the
/// AF_UNIX socket nor a TLS file.
#[test]
fn a_failed_tcp_bind_exposes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("s.sock");
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let tls = TlsArgs::on(&taken.local_addr().unwrap().to_string()).resolve().unwrap();
    assert!(bind_listeners(dir.path().to_str().unwrap(), sock.to_str().unwrap(), Some(tls)).is_err());
    for path in [
        sock,
        dir.path().join(TLS_ENDPOINT_FILE),
        dir.path().join(TLS_DEV_CERT_FILE),
    ] {
        assert!(!path.exists(), "{}", path.display());
    }
}
