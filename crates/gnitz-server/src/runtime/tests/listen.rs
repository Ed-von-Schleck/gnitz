use super::*;
use crate::runtime::tls::{CertSource, TlsArgs, TlsListener};

/// The AF_UNIX bind replaces only a stale socket: a regular file at the path
/// and a socket a live server answers on each refuse the boot, untouched.
#[test]
fn bind_unix_replaces_only_a_stale_socket() {
    let bind_unix = |path| bind_unix(path, Instant::now());
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

/// Every accepted TLS socket inherits the listening socket's options, and
/// `clear_published` tolerates an absent endpoint.
#[test]
fn bind_listeners_publishes_the_endpoint_and_accepted_sockets_inherit_the_options() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let sock = dir.path().join("s.sock");
    let tls = TlsListener::open(TlsArgs::on("127.0.0.1:0").resolve().unwrap(), data_dir).unwrap();
    let mut listeners = bind_listeners(data_dir, sock.to_str().unwrap(), Some(tls)).expect("bind");

    let endpoint = std::fs::read_to_string(dir.path().join(TLS_ENDPOINT_FILE)).unwrap();
    let addr: SocketAddr = endpoint.trim().parse().expect("a connectable address");
    assert_ne!(addr.port(), 0);

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
    assert!(!dir.path().join(TLS_ENDPOINT_FILE).exists(), "the endpoint is removed");
}

/// The TCP socket binds first, so a boot that fails it exposes neither the
/// AF_UNIX socket nor the endpoint.
#[test]
fn a_failed_tcp_bind_exposes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let sock = dir.path().join("s.sock");
    // TEST-NET-1: no host holds it, so the bind fails at once, without the grace.
    let off_host = TlsArgs {
        cert: CertSource::Minted(vec!["192.0.2.1".try_into().unwrap()]),
        allow_unauthenticated: true,
        ..TlsArgs::on("192.0.2.1:9")
    };
    let tls = TlsListener::open(off_host.resolve().unwrap(), data_dir).unwrap();
    let e = bind_listeners(data_dir, sock.to_str().unwrap(), Some(tls))
        .err()
        .expect("an address off the host refuses");
    assert!(e.contains("failed to bind TLS listener"), "{e}");
    for path in [sock, dir.path().join(TLS_ENDPOINT_FILE)] {
        assert!(!path.exists(), "{}", path.display());
    }
}

/// An address a dying server still holds is waited for: the boot binds each
/// transport once it comes free within the grace. The AF_UNIX socket is held
/// past the TCP bind, so its own wait runs too.
#[test]
fn an_address_freed_within_the_grace_is_bound() {
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();
    let path = dir.path().join("s.sock");
    let held_unix = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let held_tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = held_tcp.local_addr().unwrap();
    let tls = TlsListener::open(TlsArgs::on(&addr.to_string()).resolve().unwrap(), data_dir).unwrap();
    let release = std::thread::spawn(move || {
        for held in [Box::new(held_tcp) as Box<dyn Send>, Box::new(held_unix)] {
            std::thread::sleep(Duration::from_millis(50));
            drop(held);
        }
    });
    let listeners = bind_listeners(data_dir, path.to_str().unwrap(), Some(tls)).expect("both addresses come free");
    assert_eq!(listeners.len(), 2);
    release.join().unwrap();
}
