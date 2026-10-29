use super::*;
use crate::test_support::{
    flush_all, handshake, io_kind, pass, read_frame, recv_frames, tls_pair, write_frame, Loopback,
};
use crate::{poll_fd, ClientError, Interest};
use gnitz_wire::{framed, CONNECT_TIMEOUT};
use std::io::ErrorKind;
use std::os::fd::AsFd;
use std::sync::mpsc;
use std::time::Duration;

/// Read and discard until the client closes: a peer that never answers.
fn drain(mut r: impl std::io::Read) {
    let _ = std::io::copy(&mut r, &mut std::io::sink());
}

/// The client profile: TLS 1.3 alone, and early data off, so that the access
/// token the handshake sends never rides replayable early data. gnitz-server's
/// TLS config tests assert the server-side half.
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
    let (mut t, mut peer) = tls_pair(None);
    let mut both = framed(b"one");
    both.extend(framed(b"two"));
    peer.write_all(&both).unwrap();
    peer.flush().unwrap();
    write_frame(&mut peer, b"three");
    poll_fd(t.as_fd(), Interest::READ, None).unwrap();
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
    let (mut t, mut end) = tls_pair(None);
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
    let (mut t, mut end) = tls_pair(Some(8 * 1024));
    set_sockopt_int(t.as_fd().as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 8 * 1024).unwrap();
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
    let (mut t, mut end) = tls_pair(None);
    set_sockopt_int(t.as_fd().as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 16 * 1024).unwrap();
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
    let (t, mut end) = tls_pair(None);
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
    type Parsed<'a> = (
        &'a str,
        u16,
        Option<&'a str>,
        Option<(&'a str, &'a str)>,
        Option<&'a str>,
    );
    let cases: [(&str, Parsed); 5] = [
        ("db.example.com:5433", ("db.example.com", 5433, None, None, None)),
        (
            "[::1]:65535?ca=/some/dir/cert.pem",
            ("::1", 65535, Some("/some/dir/cert.pem"), None, None),
        ),
        ("h:5?cert=/c&key=/k", ("h", 5, None, Some(("/c", "/k")), None)),
        // A `PATH` may contain `=`, taken literally to the next `&`.
        (
            "h:5?ca=/x&cert=/p=q&key=/k",
            ("h", 5, Some("/x"), Some(("/p=q", "/k")), None),
        ),
        ("h:5?token_file=/t&ca=/x", ("h", 5, Some("/x"), None, Some("/t"))),
    ];
    for (input, want) in cases {
        let t = parse_target(input).unwrap();
        let got = (
            t.host.as_str(),
            t.port,
            t.ca.as_deref(),
            t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
            t.token_file.as_deref(),
        );
        assert_eq!(got, want, "{input:?}");
    }
}

#[test]
fn parse_target_rejects_malformed() {
    for bad in [
        "",                                  // empty
        "hostonly",                          // no port
        ":443",                              // empty host
        "h:0x1f",                            // non-numeric port
        "h:99999",                           // port out of u16 range
        "h:443?",                            // empty query (bare trailing `?`)
        "h:443?ca=",                         // empty CA path
        "h:443?key=",                        // empty key path
        "h:443?CA=/x",                       // params are case-sensitive
        "[::1]443",                          // missing `:` after bracket
        "[::1:443",                          // unterminated bracket
        "h:443?cert=/c",                     // cert without key
        "h:443?key=/k",                      // key without cert
        "h:443?cert=/c&cert=/d",             // duplicate cert
        "h:443?ca=/x&ca=/y",                 // duplicate ca
        "h:443?ca=/x&",                      // trailing `&` (empty element)
        "h:443?&ca=/x",                      // leading `&` (empty element)
        "h:443?token_file=",                 // empty token path
        "h:443?token_file=/a&token_file=/b", // duplicate token file
        "h:443?token=gnitz_x",               // a token is never written into the target
    ] {
        assert!(
            parse_target(bad).is_err(),
            "{bad:?} must be rejected by the target parser"
        );
    }
}

/// A `token_file=` is read whole, its surrounding whitespace dropped; a missing
/// one or one past the token limit is refused naming the file.
#[test]
fn a_token_file_is_read_trimmed_and_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let file = |name: &str, body: &[u8]| {
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        path.display().to_string()
    };
    let ok = file("ok", b"  gnitz_0123\n");
    let target = parse_target(&format!("h:1?token_file={ok}")).unwrap();
    assert_eq!(token(&target).unwrap(), b"gnitz_0123");

    let long = file("long", &[b'x'; gnitz_wire::MAX_TOKEN_BYTES + 1]);
    for path in [long, "/nonexistent/token".to_string()] {
        let e = token(&parse_target(&format!("h:1?token_file={path}")).unwrap()).unwrap_err();
        assert!(e.to_string().contains(&path), "{e}");
    }
}

/// A token the server refuses fails the connect with that verdict, and the token
/// rides the client's HELLO verbatim.
#[test]
fn a_refused_token_fails_the_connect() {
    let lb = Loopback::serve(None, |sock, cfg| {
        let mut end = handshake(sock, cfg);
        let hello = read_frame(&mut end);
        write_frame(&mut end, &gnitz_wire::server_hello(gnitz_wire::AuthStatus::Refused));
        end.conn.send_close_notify();
        end.flush().unwrap();
        hello
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, "gnitz_feed\n").unwrap();
    let target = format!("{}&token_file={}", lb.target, path.display());
    match ClientTransport::connect(&target, Instant::now() + CONNECT_TIMEOUT) {
        Err(ClientError::Refused(f)) => assert!(f.text.contains("does not recognise"), "{}", f.text),
        r => panic!("expected the token refused, got {:?}", r.err()),
    }
    assert_eq!(lb.join(), gnitz_wire::client_hello(b"gnitz_feed"));
}
