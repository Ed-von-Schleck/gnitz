use super::*;
use crate::connection::{Interest, Reply, Request, Session};
use crate::protocol::transport::{hello_handshake, poll_fd, CONNECT_TIMEOUT};
use crate::test_support::{framed, reply_ctrl};
use std::io::Read;
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

/// A rustls server on a loopback thread with a freshly minted `127.0.0.1` cert,
/// reached through the public `tls://…?ca=` target.
struct Loopback {
    target: String,
    thread: Option<std::thread::JoinHandle<()>>,
    /// Holds the minted cert's PEM for as long as the target names it.
    _ca_dir: tempfile::TempDir,
}

/// The far end as the script sees it: a blocking rustls stream. One
/// `write` with several frames in it puts them in as few records as
/// rustls makes of the run — one, below 16 KiB.
type ServerEnd = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

/// How far the loopback peer goes before its script runs.
#[derive(Clone, Copy, PartialEq)]
enum Peer {
    /// Completes the TLS handshake and acknowledges the HELLO.
    AckHello,
    /// Completes the TLS handshake, then says nothing.
    SilentTls,
    /// Accepts the TCP connection and never starts TLS.
    SilentTcp,
}

fn write_frame(end: &mut ServerEnd, payload: &[u8]) {
    end.write_all(&framed(payload)).unwrap();
    end.flush().unwrap();
}

fn read_frame(end: &mut ServerEnd) -> Vec<u8> {
    let mut hdr = [0u8; 4];
    end.read_exact(&mut hdr).unwrap();
    let mut payload = vec![0u8; u32::from_le_bytes(hdr) as usize];
    end.read_exact(&mut payload).unwrap();
    payload
}

impl Loopback {
    fn start(script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
        Self::spawn(None, Peer::AckHello, |end| script(end.unwrap()))
    }

    /// `rcvbuf` pins the accepted socket's `SO_RCVBUF` (inherited from
    /// the listener) so the peer's window bounds what the client can push
    /// unread.
    fn start_with_rcvbuf(rcvbuf: Option<libc::c_int>, script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
        Self::spawn(rcvbuf, Peer::AckHello, |end| script(end.unwrap()))
    }

    /// A peer that goes as far as `peer` says and then never answers,
    /// holding the socket open until `hold` returns.
    fn start_silent(peer: Peer, hold: impl FnOnce() + Send + 'static) -> Self {
        Self::spawn(None, peer, move |_end| hold())
    }

    /// `script` gets the rustls stream, or `None` for a `SilentTcp` peer.
    fn spawn(rcvbuf: Option<libc::c_int>, peer: Peer, script: impl FnOnce(Option<ServerEnd>) + Send + 'static) -> Self {
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
            set_sockopt_int(listener.as_raw_fd(), libc::SO_RCVBUF, sz);
        }
        let port = listener.local_addr().unwrap().port();
        let thread = std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            if peer == Peer::SilentTcp {
                script(None);
                drop(sock);
                return;
            }
            let mut end = rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
            // `StreamOwned` handshakes lazily on first I/O; a silent
            // script never does any, so finish it here.
            while end.conn.is_handshaking() {
                end.conn.complete_io(&mut end.sock).unwrap();
            }
            if peer == Peer::AckHello {
                let hello = read_frame(&mut end);
                assert_eq!(hello.len(), gnitz_wire::HELLO_PAYLOAD_LEN as usize);
                // `encode_hello_ack` frames the ACK itself.
                let ack = gnitz_wire::encode_hello_ack(gnitz_wire::MAX_FRAME_PAYLOAD_SERVER as u32, 0);
                end.write_all(&ack).unwrap();
                end.flush().unwrap();
            }
            script(Some(end));
        });
        Loopback {
            target: format!("tls://127.0.0.1:{port}?ca={}", pem.display()),
            thread: Some(thread),
            _ca_dir: ca_dir,
        }
    }

    fn connect(&self) -> ClientTransport {
        let until = Some(Instant::now() + CONNECT_TIMEOUT);
        let mut t = ClientTransport::connect(&self.target, until).unwrap();
        hello_handshake(&mut t, until).unwrap();
        assert_eq!(t.max_payload_len(), gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);
        t
    }

    fn join(mut self) {
        self.thread.take().unwrap().join().unwrap();
    }
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
fn loopback_frame_split_across_two_records() {
    let payload: Vec<u8> = (0u8..=255).cycle().take(3000).collect();
    let p = payload.clone();
    let lb = Loopback::start(move |mut end| {
        let bytes = framed(&p);
        end.write_all(&bytes[..1000]).unwrap();
        end.flush().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        end.write_all(&bytes[1000..]).unwrap();
        end.flush().unwrap();
    });
    let mut t = lb.connect();
    assert_eq!(t.recv_framed(None).unwrap(), payload);
    lb.join();
}

#[test]
fn loopback_one_record_two_frames_one_step_completes_both_slots() {
    let lb = Loopback::start(|mut end| {
        read_frame(&mut end);
        read_frame(&mut end);
        let mut both = framed(&reply_ctrl(0, 1));
        both.extend(framed(&reply_ctrl(0, 2)));
        end.write_all(&both).unwrap();
        end.flush().unwrap();
    });
    let mut s = Session::from_transport(lb.connect());
    let req = reply_ctrl(0, 0);
    let a = s.submit(Request::RawFrame(req.clone())).unwrap();
    let b = s.submit(Request::RawFrame(req)).unwrap();
    assert!(s.step(Interest::WRITE).unwrap().is_empty());
    assert!(s.interest().read && !s.interest().write);
    // Park once; the one readable wakeup must complete both.
    let done = loop {
        poll_fd(s.as_raw_fd(), libc::POLLIN, None, true).unwrap();
        let d = s.step(Interest::READ).unwrap();
        if !d.is_empty() {
            break d;
        }
    };
    let ids: Vec<_> = done.iter().map(|(id, _)| *id).collect();
    assert_eq!(ids, vec![a, b]);
    for (id, r) in done {
        let Reply::Ack(m) = r.unwrap() else { panic!("ack") };
        assert_eq!(m.seek_pk, if id == a { 1 } else { 2 });
    }
    assert_eq!(
        s.interest(),
        Interest::NONE,
        "nothing left in carry or in rustls's plaintext"
    );
    lb.join();
}

#[test]
fn loopback_two_back_to_back_records_come_out_of_one_step() {
    let lb = Loopback::start(|mut end| {
        read_frame(&mut end);
        read_frame(&mut end);
        // Two records: one frame each, flushed separately.
        write_frame(&mut end, &reply_ctrl(0, 1));
        write_frame(&mut end, &reply_ctrl(0, 2));
    });
    let mut s = Session::from_transport(lb.connect());
    let req = reply_ctrl(0, 0);
    s.submit(Request::RawFrame(req.clone())).unwrap();
    s.submit(Request::RawFrame(req)).unwrap();
    s.step(Interest::WRITE).unwrap();
    // Let both records land before the one step reads.
    std::thread::sleep(Duration::from_millis(100));
    poll_fd(s.as_raw_fd(), libc::POLLIN, None, true).unwrap();
    let done = s.step(Interest::READ).unwrap();
    assert_eq!(done.len(), 2, "both frames from one step");
    assert_eq!(s.interest(), Interest::NONE);
    lb.join();
}

#[test]
fn loopback_large_reply_spanning_many_records_is_intact() {
    // A 1 MiB reply is 64+ records: the ciphertext buffer is refilled
    // many times, records straddle refills, and the payload lands in one
    // exact allocation.
    let payload: Vec<u8> = (0u8..=255).cycle().take(1024 * 1024).collect();
    let p = payload.clone();
    let lb = Loopback::start(move |mut end| write_frame(&mut end, &p));
    let mut t = lb.connect();
    set_sockopt_int(t.as_raw_fd(), libc::SO_RCVBUF, 256 * 1024);
    assert_eq!(t.recv_framed(None).unwrap(), payload);
    lb.join();
}

#[test]
fn loopback_step_write_can_empty_the_queue_with_ciphertext_still_pending() {
    // 60 KiB fits rustls's send buffer but not the pinned socket buffers: the
    // queue empties while ciphertext is still pending.
    let frame: Vec<u8> = (0u8..=255).cycle().take(60 * 1024).collect();
    let expect = frame.clone();
    let (tx, rx) = mpsc::channel::<()>();
    let lb = Loopback::start_with_rcvbuf(Some(8 * 1024), move |mut end| {
        rx.recv().unwrap();
        assert_eq!(read_frame(&mut end), expect);
        write_frame(&mut end, &reply_ctrl(0, 9));
    });
    let t = lb.connect();
    set_sockopt_int(t.as_raw_fd(), libc::SO_SNDBUF, 8 * 1024);
    let mut s = Session::from_transport(t);
    let slot = s.submit(Request::RawFrame(frame)).unwrap();
    assert!(s.step(Interest::WRITE).unwrap().is_empty());
    assert_eq!(s.queued_bytes(), 0, "rustls took the whole frame");
    assert!(
        s.interest().write,
        "ciphertext still in sendable_tls: the queue alone is not the predicate"
    );
    tx.send(()).unwrap();
    let mut ready = Interest::WRITE;
    let reply = loop {
        let mut d = s.step(ready).unwrap();
        if let Some(i) = d.iter().position(|(id, _)| *id == slot) {
            break d.swap_remove(i).1.unwrap();
        }
        let rev = poll_fd(s.as_raw_fd(), s.interest().poll_events(), None, true).unwrap();
        ready = Interest::from_revents(rev);
    };
    let Reply::Ack(m) = reply else { panic!("ack") };
    assert_eq!(m.seek_pk, 9);
    lb.join();
}

#[test]
fn loopback_deadline_over_tls_never_tears_a_frame() {
    // 4 MiB outgrows rustls's 1 MiB send buffer, so the deadline expires with
    // the frame's tail still in the queue.
    let big: Vec<u8> = (0u8..=255).cycle().take(4 * 1024 * 1024).collect();
    let expect = big.clone();
    let lb = Loopback::start(move |mut end| {
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(read_frame(&mut end), expect);
        assert_eq!(read_frame(&mut end), b"after");
    });
    let mut t = lb.connect();
    set_sockopt_int(t.as_raw_fd(), libc::SO_SNDBUF, 16 * 1024);
    let deadline = || Some(Instant::now() + Duration::from_millis(100));
    // The big frame is sent once; on expiry its remainder is queue state,
    // and the small frame that follows queues behind it.
    let expired = match t.send_framed(&big, deadline()) {
        Ok(()) => false,
        Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => true,
        Err(e) => panic!("{e}"),
    };
    assert!(expired, "the deadline must have fired");
    loop {
        match t.send_framed(b"after", deadline()) {
            Ok(()) => break,
            Err(ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("{e}"),
        }
    }
    lb.join();
}

#[test]
fn loopback_silent_peer_fails_the_hello_at_the_deadline_once() {
    // A peer that completes the TLS handshake and then says nothing:
    // the HELLO read fails at CONNECT_TIMEOUT, not at twice it.
    let (tx, rx) = mpsc::channel::<()>();
    let lb = Loopback::start_silent(Peer::SilentTls, move || rx.recv().unwrap_or(()));
    let t0 = Instant::now();
    let until = Some(t0 + CONNECT_TIMEOUT);
    let mut t = ClientTransport::connect(&lb.target, until).unwrap();
    assert!(matches!(
        hello_handshake(&mut t, until),
        Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock
    ));
    let took = t0.elapsed();
    assert!(
        took >= CONNECT_TIMEOUT && took < CONNECT_TIMEOUT + Duration::from_secs(2),
        "{took:?}"
    );
    tx.send(()).unwrap();
    lb.join();
}

#[test]
fn loopback_silent_tcp_peer_fails_the_hello_at_the_deadline() {
    // A peer that accepts TCP and never answers the ClientHello: the stalled
    // handshake is bounded by the one deadline over the HELLO exchange.
    let (tx, rx) = mpsc::channel::<()>();
    let lb = Loopback::start_silent(Peer::SilentTcp, move || rx.recv().unwrap_or(()));
    let deadline = Duration::from_millis(300);
    let t0 = Instant::now();
    let until = Some(t0 + deadline);
    let mut t = ClientTransport::connect(&lb.target, until).unwrap();
    assert!(matches!(
        hello_handshake(&mut t, until),
        Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock
    ));
    let took = t0.elapsed();
    assert!(took >= deadline && took < Duration::from_secs(2), "{took:?}");
    tx.send(()).unwrap();
    lb.join();
}

#[test]
fn loopback_clean_close_notify_surfaces_as_eof() {
    let lb = Loopback::start(|mut end| {
        write_frame(&mut end, b"last");
        end.conn.send_close_notify();
        end.flush().unwrap();
    });
    let mut t = lb.connect();
    assert_eq!(t.recv_framed(None).unwrap(), b"last");
    assert!(
        matches!(t.recv_framed(None), Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof)
    );
    lb.join();
}

#[test]
fn loopback_bytes_after_close_notify_surface_eof() {
    // Bytes past a close_notify, beyond what rustls takes in one `read_tls`,
    // must never be fed: the read after the last frame reports EOF rather than
    // spinning on a slice rustls refuses.
    let lb = Loopback::start(|mut end| {
        write_frame(&mut end, b"last");
        end.conn.send_close_notify();
        end.flush().unwrap();
        end.sock.write_all(&[0u8; 16 * 1024]).unwrap();
    });
    let t = lb.connect();
    let (tx, rx) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut t = t;
        // Everything lands before the first read, so one socket read takes it all.
        std::thread::sleep(Duration::from_millis(200));
        let frame = t.recv_framed(None).map_err(|e| e.to_string());
        let next = t.recv_framed(None);
        let next_is_eof =
            matches!(next, Err(ProtocolError::IoError(ref e)) if e.kind() == std::io::ErrorKind::UnexpectedEof);
        tx.send((frame, next_is_eof)).unwrap();
    });
    let (frame, next_is_eof) = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("the read after close_notify must not spin");
    assert_eq!(frame.unwrap(), b"last");
    assert!(next_is_eof);
    reader.join().unwrap();
    lb.join();
}

#[test]
fn loopback_reply_and_close_notify_in_one_read_complete_the_slot() {
    // A reply and the close_notify behind it arrive in one socket read: the
    // step must hand out the reply's completion, and the close surfaces later.
    let lb = Loopback::start(|mut end| {
        read_frame(&mut end);
        write_frame(&mut end, &reply_ctrl(0, 7));
        end.conn.send_close_notify();
        end.flush().unwrap();
    });
    let mut s = Session::from_transport(lb.connect());
    let slot = s.submit(Request::RawFrame(reply_ctrl(0, 0))).unwrap();
    assert!(s.step(Interest::WRITE).unwrap().is_empty());
    std::thread::sleep(Duration::from_millis(200));
    poll_fd(s.as_raw_fd(), libc::POLLIN, None, true).unwrap();
    let mut done = s
        .step(Interest::READ)
        .expect("the reply completes before the close surfaces");
    assert_eq!(done.len(), 1);
    let (id, reply) = done.swap_remove(0);
    assert_eq!(id, slot);
    let Reply::Ack(m) = reply.unwrap() else { panic!("ack") };
    assert_eq!(m.seek_pk, 7);
    lb.join();
}

#[test]
fn parse_target_accepts_all_forms() {
    let t = parse_target("db.example.com:5433").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("db.example.com", 5433));
    assert!(t.ca.is_none());
    assert!(t.client_auth.is_none());

    let t = parse_target("[::1]:65535?ca=/some/dir/cert.pem").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("::1", 65535));
    assert_eq!(t.ca.as_deref(), Some("/some/dir/cert.pem"));
    assert!(t.client_auth.is_none());

    // Client auth: cert+key, default verification.
    let t = parse_target("h:5?cert=/c&key=/k").unwrap();
    assert_eq!(
        t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        Some(("/c", "/k"))
    );
    assert!(t.ca.is_none());

    // Client auth + explicit CA.
    let t = parse_target("h:5?ca=/x&cert=/c&key=/k").unwrap();
    assert_eq!(t.ca.as_deref(), Some("/x"));
    assert_eq!(
        t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        Some(("/c", "/k"))
    );

    // A `PATH` may contain `=` (taken literally to the next `&`).
    let t = parse_target("h:5?cert=/p=q&key=/k").unwrap();
    assert_eq!(
        t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        Some(("/p=q", "/k"))
    );
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
        "h:443?insecure",        // removed mode: unknown param
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
            "{bad:?} must be rejected by the target parser",
        );
    }
}
