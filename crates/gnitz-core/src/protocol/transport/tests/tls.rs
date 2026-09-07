use super::*;
use crate::connection::{Interest, Reply, Request, Session};
use crate::protocol::transport::{hello_handshake, poll_fd};
use crate::test_support::{framed, reply_ctrl};
use std::io::Read;
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// A `rustls::ServerConnection` over a loopback `TcpStream` on a helper
/// thread: mints a self-signed cert, answers the HELLO with a real ACK so
/// the client's `mark_established` runs, then hands the connection to
/// `script`. The client reaches it through the fully public
/// `ClientTransport::connect("tls://127.0.0.1:{port}?insecure")`.
struct Loopback {
    target: String,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// The far end as the script sees it: a blocking rustls stream. One
/// `write` with several frames in it puts them in as few records as
/// rustls makes of the run — one, below 16 KiB.
type ServerEnd = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;

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
        Self::spawn(None, true, script)
    }

    /// `rcvbuf` pins the accepted socket's `SO_RCVBUF` (inherited from
    /// the listener) so the peer's window bounds what the client can push
    /// unread.
    fn start_with_rcvbuf(rcvbuf: Option<libc::c_int>, script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
        Self::spawn(rcvbuf, true, script)
    }

    /// Completes the TLS handshake and then never answers the HELLO,
    /// holding the socket open until `hold` returns.
    fn start_silent(hold: impl FnOnce() + Send + 'static) -> Self {
        Self::spawn(None, false, move |_end| hold())
    }

    fn spawn(rcvbuf: Option<libc::c_int>, ack_hello: bool, script: impl FnOnce(ServerEnd) + Send + 'static) -> Self {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = rustls::pki_types::PrivateKeyDer::try_from(cert.signing_key.serialize_der()).unwrap();
        let mut cfg = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
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
            let mut end = rustls::StreamOwned::new(rustls::ServerConnection::new(cfg).unwrap(), sock);
            // `StreamOwned` handshakes lazily on first I/O; a silent
            // script never does any, so finish it here.
            while end.conn.is_handshaking() {
                end.conn.complete_io(&mut end.sock).unwrap();
            }
            if ack_hello {
                let hello = read_frame(&mut end);
                assert_eq!(hello.len(), gnitz_wire::HELLO_PAYLOAD_LEN as usize);
                // `encode_hello_ack` frames the ACK itself.
                let ack = gnitz_wire::encode_hello_ack(gnitz_wire::MAX_FRAME_PAYLOAD_SERVER as u32, 0);
                end.write_all(&ack).unwrap();
                end.flush().unwrap();
            }
            script(end);
        });
        Loopback {
            target: format!("tls://127.0.0.1:{port}?insecure"),
            thread: Some(thread),
        }
    }

    fn connect(&self) -> ClientTransport {
        let mut t = ClientTransport::connect(&self.target).unwrap();
        hello_handshake(&mut t, Some(CONNECT_TIMEOUT)).unwrap();
        assert_eq!(t.max_payload_len(), gnitz_wire::MAX_FRAME_PAYLOAD_SERVER);
        t
    }

    fn join(mut self) {
        self.thread.take().unwrap().join().unwrap();
    }
}

/// 0-RTT lock, client side: early data must stay off, so that once a
/// future auth layer gives a session DML authority, replayable early data
/// cannot become replayable DML. The server-side half is asserted by
/// `zero_rtt_stays_locked_off_server_side` in gnitz-server's `tls::config`.
#[test]
fn client_config_leaves_zero_rtt_off() {
    let cfg = build_client_config(&Verify::Insecure, None).expect("client config");
    assert!(!cfg.enable_early_data, "client 0-RTT early data must be off");
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
        let Reply::Train(t) = r.unwrap() else { panic!("train") };
        assert_eq!(t.terminal.seek_pk, if id == a { 1 } else { 2 });
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
    // A frame under rustls's 64 KiB limit is taken by one `write_vectored`
    // — the queue empties — while the socket, with both buffers pinned
    // below the frame and a peer that is not yet reading, takes only part
    // of the ciphertext. `interest()` must still report WRITE for the
    // rest, and only a later step(WRITE) puts the frame's last bytes on
    // the wire.
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
    let Reply::Train(t) = reply else { panic!("train") };
    assert_eq!(t.terminal.seek_pk, 9);
    lb.join();
}

#[test]
fn loopback_deadline_over_tls_never_tears_a_frame() {
    // A >64 KiB frame (so rustls forces mid-frame flushes) under a short
    // deadline to a peer that drains only after a delay, followed by a
    // small one: the server parses two well-formed frames.
    let big: Vec<u8> = (0u8..=255).cycle().take(400 * 1024).collect();
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
fn loopback_established_connection_idle_past_connect_timeout_still_reads() {
    let lb = Loopback::start(|mut end| {
        std::thread::sleep(CONNECT_TIMEOUT + Duration::from_millis(500));
        write_frame(&mut end, b"still here");
    });
    let mut t = lb.connect();
    assert_eq!(t.recv_framed(None).unwrap(), b"still here");
    lb.join();
}

#[test]
fn loopback_silent_peer_fails_the_hello_at_the_deadline_once() {
    // A peer that completes the TLS handshake and then says nothing:
    // the HELLO read fails at CONNECT_TIMEOUT, not at twice it.
    let (tx, rx) = mpsc::channel::<()>();
    let lb = Loopback::start_silent(move || rx.recv().unwrap_or(()));
    let mut t = ClientTransport::connect(&lb.target).unwrap();
    let t0 = std::time::Instant::now();
    assert!(matches!(
        hello_handshake(&mut t, Some(CONNECT_TIMEOUT)),
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
fn parse_target_accepts_all_forms() {
    let t = parse_target("db.example.com:5433").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("db.example.com", 5433));
    assert!(matches!(t.verify, Verify::Default));
    assert!(t.client_auth.is_none());

    let t = parse_target("127.0.0.1:1?insecure").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("127.0.0.1", 1));
    assert!(matches!(t.verify, Verify::Insecure));
    assert!(t.client_auth.is_none());

    let t = parse_target("[::1]:65535?ca=/some/dir/cert.pem").unwrap();
    assert_eq!((t.host.as_str(), t.port), ("::1", 65535));
    assert!(matches!(t.verify, Verify::Ca(ref p) if p == "/some/dir/cert.pem"));
    assert!(t.client_auth.is_none());

    // Client auth: cert+key, default verification.
    let t = parse_target("h:5?cert=/c&key=/k").unwrap();
    assert_eq!(
        t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        Some(("/c", "/k"))
    );
    assert!(matches!(t.verify, Verify::Default));

    // Client auth + explicit CA.
    let t = parse_target("h:5?ca=/x&cert=/c&key=/k").unwrap();
    assert!(matches!(t.verify, Verify::Ca(ref p) if p == "/x"));
    assert_eq!(
        t.client_auth.as_ref().map(|(c, k)| (c.as_str(), k.as_str())),
        Some(("/c", "/k"))
    );

    // Client auth + insecure.
    let t = parse_target("h:5?insecure&cert=/c&key=/k").unwrap();
    assert!(matches!(t.verify, Verify::Insecure));
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
        "",                        // empty
        "hostonly",                // no port
        ":443",                    // empty host
        "h:0x1f",                  // non-numeric port
        "h:99999",                 // port out of u16 range
        "h:443?",                  // empty query (bare trailing `?`)
        "h:443?ca=",               // empty CA path
        "h:443?insecure=1",        // unknown param
        "h:443?Insecure",          // params are case-sensitive
        "[::1]443",                // missing `:` after bracket
        "[::1:443",                // unterminated bracket
        "h:443?cert=/c",           // cert without key
        "h:443?key=/k",            // key without cert
        "h:443?insecure&ca=/x",    // insecure + ca mutually exclusive
        "h:443?cert=/c&cert=/d",   // duplicate cert
        "h:443?ca=/x&ca=/y",       // duplicate ca
        "h:443?insecure&insecure", // duplicate insecure
        "h:443?insecure&",         // trailing `&` (empty element)
        "h:443?&insecure",         // leading `&` (empty element)
    ] {
        assert!(
            parse_target(bad).is_err(),
            "{bad:?} must be rejected by the target parser",
        );
    }
}

#[test]
fn loopback_detection() {
    assert!(is_loopback_host("localhost"));
    assert!(is_loopback_host("127.0.0.1"));
    assert!(is_loopback_host("::1"));
    assert!(!is_loopback_host("example.com"));
    assert!(!is_loopback_host("10.0.0.7"));
}
