use super::*;
use crate::runtime::tls::{TlsArgs, TlsListener, TLS_CERT_FILE};

/// A grace no test waits out.
const NO_GRACE: Duration = Duration::ZERO;

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
    let mut listeners = bind_listeners(data_dir, sock.to_str().unwrap(), Some(tls), NO_GRACE).expect("bind");

    let endpoint = std::fs::read_to_string(dir.path().join(TLS_ENDPOINT_FILE)).unwrap();
    let addr: SocketAddr = endpoint.trim().parse().expect("a connectable address");
    assert_ne!(addr.port(), 0);
    let pem = std::fs::read_to_string(dir.path().join(TLS_CERT_FILE)).unwrap();
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
    assert!(!dir.path().join(TLS_ENDPOINT_FILE).exists(), "the endpoint is removed");
    assert!(dir.path().join(TLS_CERT_FILE).exists(), "the identity is kept");
}

/// The TCP socket binds first, so a boot that fails it exposes neither the
/// AF_UNIX socket nor the endpoint.
#[test]
fn a_failed_tcp_bind_exposes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("s.sock");
    let taken = TcpListener::bind("127.0.0.1:0").unwrap();
    let args = TlsArgs::on(&taken.local_addr().unwrap().to_string()).resolve().unwrap();
    let tls = TlsListener::open(args, dir.path().to_str().unwrap()).unwrap();
    assert!(bind_listeners(
        dir.path().to_str().unwrap(),
        sock.to_str().unwrap(),
        Some(tls),
        NO_GRACE
    )
    .is_err());
    for path in [sock, dir.path().join(TLS_ENDPOINT_FILE)] {
        assert!(!path.exists(), "{}", path.display());
    }
}

/// An address a dying server still holds is waited for: the boot binds it
/// once it comes free within the grace, on both transports.
#[test]
fn an_address_freed_within_the_grace_is_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let held_unix = std::os::unix::net::UnixListener::bind(&path).unwrap();
    let held_tcp = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = held_tcp.local_addr().unwrap();
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        drop((held_unix, held_tcp));
    });
    let deadline = Instant::now() + ADDRESS_GRACE;
    bind_tcp(addr, deadline).expect("the TCP address comes free");
    bind_unix(path.to_str().unwrap(), deadline).expect("the socket comes free");
    release.join().unwrap();
}
