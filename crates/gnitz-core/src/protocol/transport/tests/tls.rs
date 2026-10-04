use super::*;
use crate::protocol::transport::poll_fd;
use crate::test_support::{flush_all, framed, io_kind, pass, read_frame, recv_frames, write_frame};
use crate::ClientError;
use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_wire::CONNECT_TIMEOUT;
use std::io::ErrorKind;
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

/// A rustls server on a loopback thread with a freshly minted `127.0.0.1` cert,
/// reached through the public `tls://…?ca=` target.
struct Loopback<T> {
    target: String,
    thread: std::thread::JoinHandle<T>,
    /// Holds the minted cert's PEM for as long as the target names it.
    _ca_dir: tempfile::TempDir,
}

/// The far end of a connection: a blocking rustls stream.
pub(in crate::protocol::transport) type ServerEnd = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

/// Complete the server side of the TLS handshake. `StreamOwned` handshakes
/// lazily on first I/O, which a peer that only waits never does.
fn handshake(sock: TcpStream, cfg: Arc<rustls::ServerConfig>) -> ServerEnd {
    let mut end = rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
    while end.conn.is_handshaking() {
        end.conn.complete_io(&mut end.sock).unwrap();
    }
    end
}

/// Answer the client's HELLO.
fn hello(mut end: ServerEnd) -> ServerEnd {
    assert_eq!(read_frame(&mut end), gnitz_wire::HELLO);
    write_frame(&mut end, &gnitz_wire::HELLO);
    end
}

/// Read and discard until the client closes: a peer that never answers.
fn drain(mut r: impl std::io::Read) {
    let _ = std::io::copy(&mut r, &mut std::io::sink());
}

impl<T: Send + 'static> Loopback<T> {
    /// `serve` gets the accepted socket (`TCP_NODELAY`, as the server sets it)
    /// and the server config. `rcvbuf` pins the socket's `SO_RCVBUF`, set on the
    /// listener so the window the handshake advertises honours it.
    fn serve(
        rcvbuf: Option<libc::c_int>,
        serve: impl FnOnce(TcpStream, Arc<rustls::ServerConfig>) -> T + Send + 'static,
    ) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
        let ca_dir = tempfile::tempdir().unwrap();
        let pem = ca_dir.path().join("ca.pem");
        std::fs::write(&pem, cert.cert.pem()).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();
        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert.cert.der().clone()], key)
            .unwrap();
        cfg.alpn_protocols = vec![ALPN_GNITZ.to_vec()];
        let cfg = Arc::new(cfg);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        if let Some(sz) = rcvbuf {
            set_sockopt_int(listener.as_raw_fd(), libc::SOL_SOCKET, libc::SO_RCVBUF, sz).unwrap();
        }
        let port = listener.local_addr().unwrap().port();
        let thread = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            sock.set_nodelay(true).unwrap();
            serve(sock, cfg)
        });
        Loopback {
            target: format!("tls://127.0.0.1:{port}?ca={}", pem.display()),
            thread,
            _ca_dir: ca_dir,
        }
    }

    fn join(self) -> T {
        self.thread.join().unwrap()
    }
}

/// A connected transport and its far end, the HELLOs exchanged. `rcvbuf` as
/// [`Loopback::serve`] takes it.
pub(in crate::protocol::transport) fn pair(rcvbuf: Option<libc::c_int>) -> (ClientTransport, ServerEnd) {
    let lb = Loopback::serve(rcvbuf, |sock, cfg| hello(handshake(sock, cfg)));
    let t = ClientTransport::connect(&lb.target, Instant::now() + CONNECT_TIMEOUT).unwrap();
    (t, lb.join())
}

/// The client profile: TLS 1.3 alone, and early data off, so that once a
/// future auth layer gives a session DML authority, replayable early data
/// cannot become replayable DML. gnitz-server's TLS config tests assert the
/// server-side half.
#[test]
fn client_config_profile() {
    let cfg = build_client_config(&parse_target("127.0.0.1:1").unwrap()).expect("client config");
    assert!(!cfg.enable_early_data, "client 0-RTT early data must be off");
    assert!(cfg
        .crypto_provider()
        .cipher_suites
        .iter()
        .all(|s| s.version() == &rustls::version::TLS13));
}

#[test]
fn ca_file_with_an_unparsable_certificate_is_refused() {
    // Valid PEM around bytes that are no certificate: refused, not skipped
    // for the good one beside it.
    let ca = rcgen::generate_simple_self_signed(vec!["127.0.0.1".into()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ca.pem");
    let pem = format!(
        "{}-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        ca.cert.pem()
    );
    std::fs::write(&path, pem).unwrap();
    let target = parse_target(&format!("127.0.0.1:1?ca={}", path.display())).unwrap();
    assert!(build_client_config(&target).is_err());
}

#[test]
fn one_wakeup_drains_every_frame_rustls_holds() {
    // Two frames in one record and a third in the next: one readable wakeup
    // and one reading pass bring out all three.
    let (mut t, mut peer) = pair(None);
    let mut both = framed(b"one");
    both.extend(framed(b"two"));
    peer.write_all(&both).unwrap();
    peer.flush().unwrap();
    write_frame(&mut peer, b"three");
    poll_fd(t.as_raw_fd(), libc::POLLIN, None).unwrap();
    let (got, end) = pass(&mut t);
    end.unwrap();
    assert_eq!(got, [b"one".as_slice(), b"two", b"three"]);
    drop(peer);
    let (got, end) = pass(&mut t);
    assert!(got.is_empty());
    assert_eq!(io_kind(&end), Some(ErrorKind::UnexpectedEof));
}

#[test]
fn large_reply_spanning_many_records_is_intact() {
    // Many 16 KiB records: the window is refilled repeatedly and records
    // straddle the refills.
    let payload: Vec<u8> = (0u8..=255).collect::<Vec<_>>().repeat(1024);
    let (mut t, mut end) = pair(None);
    let p = payload.clone();
    let writer = std::thread::spawn(move || write_frame(&mut end, &p));
    assert_eq!(recv_frames(&mut t, 1), [payload]);
    writer.join().unwrap();
}

#[test]
fn flush_can_empty_the_queue_with_ciphertext_still_pending() {
    // 60 KiB fits rustls's send buffer but not the pinned socket buffers: the
    // queue empties into rustls while its ciphertext still waits on the socket.
    let frame: Vec<u8> = (0u8..=255).collect::<Vec<_>>().repeat(240);
    let (mut t, mut end) = pair(Some(8 * 1024));
    set_sockopt_int(t.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 8 * 1024).unwrap();
    t.enqueue(frame.clone());
    t.flush().unwrap();
    assert_eq!(t.queued_bytes(), 0, "rustls took the whole frame");
    assert!(t.wants_write(), "the queue alone is not the predicate");
    let reader = std::thread::spawn(move || assert_eq!(read_frame(&mut end), frame));
    flush_all(&mut t);
    reader.join().unwrap();
}

#[test]
fn a_frame_larger_than_the_send_buffer_leaves_its_tail_queued() {
    // The frame outgrows rustls's send buffer, so one flush leaves its tail in
    // the queue, and the next frame goes out behind it.
    let big: Vec<u8> = (0u8..=255)
        .collect::<Vec<_>>()
        .repeat((SEND_BUFFER_BYTES + 256 * 1024) / 256);
    let (mut t, mut end) = pair(None);
    set_sockopt_int(t.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 16 * 1024).unwrap();
    t.enqueue(big.clone());
    t.flush().unwrap();
    assert!(t.queued_bytes() > 0, "the tail is queue state, not rustls's");
    let reader = std::thread::spawn(move || {
        assert_eq!(read_frame(&mut end), big);
        assert_eq!(read_frame(&mut end), b"after");
    });
    t.enqueue(b"after".to_vec());
    flush_all(&mut t);
    reader.join().unwrap();
}

#[test]
fn silent_peer_fails_the_connect_at_the_one_deadline() {
    // A peer that stalls the TLS handshake, or completes it and then never
    // answers the HELLO: either way the one deadline bounds the whole connect
    // — once, not once per phase.
    let silent_tcp = Loopback::serve(None, |sock, _| drain(sock));
    let silent_tls = Loopback::serve(None, |sock, cfg| drain(handshake(sock, cfg)));
    for lb in [silent_tcp, silent_tls] {
        let d = Duration::from_millis(300);
        let t0 = Instant::now();
        let r = ClientTransport::connect(&lb.target, t0 + d);
        assert!(matches!(
            r,
            Err(ClientError::Protocol(ProtocolError::IoError(ref e))) if e.kind() == ErrorKind::TimedOut
        ));
        let took = t0.elapsed();
        assert!(took >= d && took < 2 * d, "{took:?}");
        lb.join();
    }
}

#[test]
fn another_versions_hello_is_the_refusal_when_the_close_rides_the_same_read() {
    // The refusing HELLO and the close_notify in one flight: the read that
    // hands the HELLO out also fails, and the version is what is reported.
    let lb = Loopback::serve(None, |sock, cfg| {
        let mut end = handshake(sock, cfg);
        assert_eq!(read_frame(&mut end), gnitz_wire::HELLO);
        let mut other = gnitz_wire::HELLO;
        other[4] ^= 1;
        end.conn.writer().write_all(&framed(&other)).unwrap();
        end.conn.send_close_notify();
        end.flush().unwrap();
    });
    match ClientTransport::connect(&lb.target, Instant::now() + CONNECT_TIMEOUT) {
        Err(ClientError::Refused(f)) => assert!(f.text.contains("version mismatch"), "{}", f.text),
        r => panic!("expected a version refusal, got {:?}", r.err()),
    }
    lb.join();
}

#[test]
fn close_notify_with_bytes_behind_it_surfaces_eof() {
    // The last frame, the close_notify and bytes past it, all in one socket
    // read: the frame comes out, then EOF — never a spin on bytes past the
    // close that rustls refuses.
    let (t, mut end) = pair(None);
    write_frame(&mut end, b"last");
    end.conn.send_close_notify();
    end.flush().unwrap();
    end.sock.write_all(&[0u8; 16 * 1024]).unwrap();
    drop(end);
    let (done_tx, done_rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut t = t;
        let (frames, end) = pass(&mut t);
        done_tx.send((frames, io_kind(&end))).unwrap();
    });
    let (frames, end) = done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the read past close_notify must not spin");
    assert_eq!(frames, [b"last".to_vec()]);
    assert_eq!(end, Some(ErrorKind::UnexpectedEof));
    reader.join().unwrap();
}

#[test]
fn parse_target_accepts_all_forms() {
    type Parsed<'a> = (&'a str, u16, Option<&'a str>, Option<(&'a str, &'a str)>);
    let cases: [(&str, Parsed); 4] = [
        ("db.example.com:5433", ("db.example.com", 5433, None, None)),
        (
            "[::1]:65535?ca=/some/dir/cert.pem",
            ("::1", 65535, Some("/some/dir/cert.pem"), None),
        ),
        ("h:5?cert=/c&key=/k", ("h", 5, None, Some(("/c", "/k")))),
        // A `PATH` may contain `=`, taken literally to the next `&`.
        ("h:5?ca=/x&cert=/p=q&key=/k", ("h", 5, Some("/x"), Some(("/p=q", "/k")))),
    ];
    for (input, want) in cases {
        let t = parse_target(input).unwrap();
        let got = (
            t.host.as_str(),
            t.port,
            t.ca.as_deref(),
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        );
        assert_eq!(got, want, "{input:?}");
    }
}

#[test]
fn parse_target_rejects_malformed() {
    for bad in [
        "",                      // empty
        "hostonly",              // no port
        ":443",                  // empty host
        "h:0x1f",                // non-numeric port
        "h:99999",               // port out of u16 range
        "h:443?",                // empty query (bare trailing `?`)
        "h:443?ca=",             // empty CA path
        "h:443?key=",            // empty key path
        "h:443?CA=/x",           // params are case-sensitive
        "[::1]443",              // missing `:` after bracket
        "[::1:443",              // unterminated bracket
        "h:443?cert=/c",         // cert without key
        "h:443?key=/k",          // key without cert
        "h:443?cert=/c&cert=/d", // duplicate cert
        "h:443?ca=/x&ca=/y",     // duplicate ca
        "h:443?ca=/x&",          // trailing `&` (empty element)
        "h:443?&ca=/x",          // leading `&` (empty element)
    ] {
        assert!(
            parse_target(bad).is_err(),
            "{bad:?} must be rejected by the target parser"
        );
    }
}
