//! The transports against a real server: TLS on a `--tls-listen` listener
//! (self-signed dev cert, ephemeral port) beside the AF_UNIX socket, and what
//! the server does to a connection that stalls, floods or never says HELLO.

use std::io::{Read, Write};
use std::os::unix::io::RawFd;
use std::os::unix::net::UnixStream;
use std::time::{Duration, Instant};

use super::*;
use gnitz_core::IdRun;
use gnitz_foundation::posix_io::set_sockopt_int;

/// `(client, schema_name, table_id, schema)` for a fresh `(pk BIGINT, a
/// BIGINT, b BIGINT)` table reachable via `target`.
fn client_with_table(target: &str) -> (GnitzClient, String, u64, Arc<Schema>) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let (sn, tid, schema) = create_table(
        &mut client,
        schema_of(&[("pk", TypeCode::I64), ("a", TypeCode::I64), ("b", TypeCode::I64)]),
    );
    (client, sn, tid, schema)
}

/// Pin both socket buffers small so backpressure paths engage well below
/// the multi-MB exchange sizes these tests move. NOT smaller: shrinking a
/// connected loopback TCP socket's rcvbuf below the 64 KiB loopback segment
/// size makes the kernel DROP segments, collapsing the connection into
/// exponential RTO backoff (cwnd=1, ~26 s retransmits) — which reads as a
/// deadlock but is a test artifact.
fn set_small_bufs(fd: RawFd) {
    for opt in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        set_sockopt_int(fd, libc::SOL_SOCKET, opt, 128 * 1024);
    }
}

/// Whether the server drops `s` within `ms`. Over TCP a server-side close is
/// not visible to a poll while the client's receive window is closed (the FIN
/// queues behind the stalled data), so probe: a small request every 200 ms,
/// whose write fails once the peer is gone. Never reads, so a stalled reply
/// stays stalled.
fn evicted_within(s: &mut Session, ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(ms);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        if s.submit(Request::Alloc(IdRun::Ids(1))).is_err() || !write_out(s) {
            return true;
        }
    }
    false
}

/// Run `f` on a helper thread and fail the test if it has not finished
/// within `secs` — a deadlock fails instead of hanging the suite.
fn with_watchdog(secs: u64, f: impl FnOnce() + Send + 'static) {
    let handle = std::thread::spawn(f);
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !handle.is_finished() {
        assert!(Instant::now() < deadline, "watchdog: test body deadlocked ({secs}s)");
        std::thread::sleep(Duration::from_millis(50));
    }
    handle.join().unwrap();
}

// ── Verification and the handshake ─────────────────────────────────────────

#[test]
fn ca_pin_connects_and_bad_verifications_fail() {
    let srv = ServerHandle::start_tls(1);

    // ?ca=dev cert: full verification against the minted self-signed cert.
    let mut pinned = GnitzClient::connect(&srv.tls_target()).expect("ca-pinned connect");
    pinned.alloc_id().unwrap();

    // Default webpki roots must REJECT the self-signed dev cert.
    let err = GnitzClient::connect(&format!("tls://{}", srv.tls_endpoint()))
        .err()
        .expect("webpki roots must reject");
    assert!(
        err.to_string().to_lowercase().contains("certificate"),
        "expected a certificate error, got: {err}"
    );

    // Wrong CA: a *different* server's dev cert must not verify this one.
    let other = ServerHandle::start_tls(1);
    let wrong_ca = format!("tls://{}?ca={}", srv.tls_endpoint(), other.tls_ca_path().display());
    assert!(
        GnitzClient::connect(&wrong_ca).is_err(),
        "a foreign CA must fail verification"
    );

    // ALPN mismatch: a raw rustls client offering only http/1.1 must fail
    // the handshake (the server pins a non-empty ALPN list). The client
    // verifies for real against the dev cert — no skip-verifier — so the
    // ALPN mismatch is the only possible failure cause.
    let addr = srv.tls_endpoint();
    let mut roots = rustls::RootCertStore::empty();
    let certs = {
        use rustls::pki_types::pem::PemObject;
        rustls::pki_types::CertificateDer::pem_file_iter(srv.tls_ca_path()).unwrap()
    };
    roots.add_parsable_certificates(certs.filter_map(Result::ok));
    let mut cfg = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    let name = rustls::pki_types::ServerName::try_from("127.0.0.1".to_string()).unwrap();
    let conn = rustls::ClientConnection::new(Arc::new(cfg), name).unwrap();
    let sock = std::net::TcpStream::connect(&addr).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut tls = rustls::StreamOwned::new(conn, sock);
    let mut failed = false;
    while tls.conn.is_handshaking() {
        if tls.conn.complete_io(&mut tls.sock).is_err() {
            failed = true;
            break;
        }
    }
    assert!(failed, "ALPN mismatch must fail the handshake");
}

#[test]
fn ipv6_loopback_connect_and_ca_verify() {
    let srv = ServerHandle::start_tls_v6(1);
    let target = srv.tls_target();
    assert!(target.starts_with("tls://[::1]:"), "v6 endpoint expected, got {target}");
    // Full verification against the dev cert's ::1 IP SAN.
    let mut client = GnitzClient::connect(&target).expect("ca-pinned tls over [::1]");
    client.alloc_id().unwrap();
}

/// A required-mTLS server admits the leaf its own client CA signed, and neither
/// a client with no certificate nor one presenting another CA's leaf.
#[test]
fn an_mtls_server_admits_only_a_leaf_its_client_ca_signed() {
    let srv = ServerHandle::start_mtls(1);
    let (mut client, _, tid, schema) = client_with_table(&srv.mtls_target());
    let pushed = rows(&schema, 0..500);
    client.push(tid, &schema, &pushed, WireConflictMode::Update).unwrap();
    assert_eq!(
        weighted_rows(&scan_all(&mut client, tid, &schema)),
        weighted_rows(&pushed)
    );

    // The handshake completes inside the HELLO exchange, so a full connect fails.
    assert!(
        GnitzClient::connect(&srv.tls_target()).is_err(),
        "a client presenting no certificate"
    );
    let other = ServerHandle::start_mtls(1);
    let (foreign_cert, foreign_key) = other.mtls_client_cert_key();
    let foreign = format!(
        "{}&cert={}&key={}",
        srv.tls_target(),
        foreign_cert.display(),
        foreign_key.display()
    );
    assert!(
        GnitzClient::connect(&foreign).is_err(),
        "a client cert from an untrusted CA"
    );
}

#[test]
fn a_wire_version_mismatch_is_answered_with_the_servers_hello_then_closed() {
    let srv = ServerHandle::start();
    let mut sock = UnixStream::connect(srv.sock_path()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
    let mut hello = gnitz_wire::HELLO;
    hello[4] ^= 1;
    sock.write_all(&gnitz_wire::frame_len_prefix(hello.len())).unwrap();
    sock.write_all(&hello).unwrap();
    let mut reply = Vec::new();
    sock.read_to_end(&mut reply).unwrap();
    let want = [&gnitz_wire::frame_len_prefix(hello.len())[..], &gnitz_wire::HELLO].concat();
    assert_eq!(reply, want);
}

const HELLO_DEADLINE: Duration = Duration::from_millis(500);

/// Assert the server closes `sock`, fresh and silent, around [`HELLO_DEADLINE`]:
/// by EOF, a TLS close alert or a reset, not by the read timing out.
fn assert_reaped(transport: &str, sock: &mut impl Read) {
    use std::io::ErrorKind::{TimedOut, WouldBlock};
    let t0 = Instant::now();
    if let Err(e) = sock.read(&mut [0u8; 1]) {
        assert!(!matches!(e.kind(), WouldBlock | TimedOut), "{transport}: never closed");
    }
    let elapsed = t0.elapsed();
    assert!(
        elapsed >= HELLO_DEADLINE * 3 / 5,
        "{transport}: closed after {elapsed:?}"
    );
}

#[test]
fn first_frame_deadline_reaps_silent_connections() {
    let deadline_ms = HELLO_DEADLINE.as_millis().to_string();
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_HELLO_TIMEOUT_MS", &deadline_ms)]);
    let timeout = Some(HELLO_DEADLINE * 6);
    let mut tcp = std::net::TcpStream::connect(srv.tls_endpoint()).expect("tcp connect");
    tcp.set_read_timeout(timeout).unwrap();
    assert_reaped("tls", &mut tcp);
    let mut unix = UnixStream::connect(srv.sock_path()).expect("unix connect");
    unix.set_read_timeout(timeout).unwrap();
    assert_reaped("unix", &mut unix);

    // A client that sends its HELLO at once is unaffected.
    let mut client = GnitzClient::connect(&srv.tls_target()).expect("a normal client must connect");
    client.alloc_id().unwrap();
}

#[test]
fn global_connection_cap_closes_excess() {
    let srv = ServerHandle::start_tls_with_env(1, &[("GNITZ_MAX_CONNS", "2")]);
    let target = srv.tls_target();

    // Hold two connections, one per transport: the cap counts both. Each
    // counts for its whole lifetime.
    let _c1 = GnitzClient::connect(&target).expect("1st connection under the cap");
    let c2 = GnitzClient::connect(srv.sock_path()).expect("2nd connection under the cap");

    // The 3rd is closed immediately (fd closed before any TLS work): its TCP
    // connect succeeds, and the handshake inside HELLO cannot complete.
    assert!(
        GnitzClient::connect(&target).is_err(),
        "a connection accepted past the cap must be closed"
    );
    assert!(GnitzClient::connect(srv.sock_path()).is_err(), "on either transport");

    // Free a slot; a fresh connect then succeeds. Retry: the server-side
    // decrement lands after the close cascade completes.
    drop(c2);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut reconnected = false;
    while Instant::now() < deadline {
        if GnitzClient::connect(&target).is_ok() {
            reconnected = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(reconnected, "a cap slot must free after a connection closes");
}

// ── Data over both transports ──────────────────────────────────────────────

/// A TLS push spanning many TLS records (~28 MB in one frame) and a scan train
/// spanning many worker frames, then a retraction over AF_UNIX: both transports
/// read back every cell at its net weight.
#[test]
fn both_transports_carry_every_cell_at_its_net_weight() {
    let srv = ServerHandle::start_tls(4);
    let (mut tls, _, tid, schema) = client_with_table(&srv.tls_target());
    let mut unix = GnitzClient::connect(srv.sock_path()).unwrap();

    tls.push(tid, &schema, &rows(&schema, 0..700_000), WireConflictMode::Update)
        .unwrap();
    let mut retract = rows(&schema, [500]);
    retract.weights[0] = -1;
    unix.push(tid, &schema, &retract, WireConflictMode::Update).unwrap();

    let want = weighted_rows(&rows(&schema, (0..700_000).filter(|&pk| pk != 500)));
    for client in [&mut tls, &mut unix] {
        assert_eq!(weighted_rows(&scan_all(client, tid, &schema)), want);
    }
}

/// A restart fails an old connection fast — EOF or reset, no timeout involved —
/// and on every later call, while a fresh connect to the same target succeeds.
#[test]
fn a_restart_fails_the_old_connection_fast_and_admits_a_new_one() {
    let mut srv = ServerHandle::start_tls(1);
    let targets = [srv.sock_path().to_string(), srv.tls_target()];
    let mut clients: Vec<_> = targets.iter().map(|t| client_with_table(t)).collect();

    srv.restart();

    for ((client, _, tid, schema), target) in clients.iter_mut().zip(&targets) {
        let t0 = Instant::now();
        for _ in 0..2 {
            let err = client
                .scan_spec(*tid, &ReadSpec::all_rows(ReadBound::None), schema)
                .unwrap_err();
            assert!(matches!(err, ClientError::ConnectionLost(_)), "{target}: {err:?}");
        }
        assert!(t0.elapsed() < Duration::from_secs(5), "{target}: {:?}", t0.elapsed());
        GnitzClient::connect(target).unwrap().alloc_id().unwrap();
    }
}

// ── A client that writes and never reads ───────────────────────────────────

/// More than 16 MB of pushes and a scan whose reply exceeds the shrunken
/// buffers, all written before any reply is read: the server's ACK sends stall
/// against the unread socket meanwhile, so liveness rests on its always-reading
/// pump.
#[test]
fn pipelined_pushes_ahead_of_scan_do_not_deadlock() {
    let srv = ServerHandle::start_tls(4);
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (_setup, _, tid, schema) = client_with_table(&target);
        let mut s = Session::connect(&target).unwrap();
        set_small_bufs(s.as_raw_fd());

        let (n_pushes, per) = (30u64, 25_000u64);
        let pushes: Vec<SlotId> = (0..n_pushes)
            .map(|i| {
                let batch = rows(&schema, i * per..(i + 1) * per);
                s.submit(push_req(tid, &schema, &batch)).unwrap()
            })
            .collect();
        let all = ReadSpec::all_rows(ReadBound::None);
        let scan = s.submit(scan_req(tid, &all, &schema)).unwrap();
        assert!(write_out(&mut s), "the server keeps reading");

        let (done, _) = drive_all(&mut s, pushes.len() + 1);
        for id in pushes {
            assert!(matches!(done[&id], Ok(Reply::Lsn(_))), "{:?}", done[&id]);
        }
        let Ok(Reply::Scan(data)) = &done[&scan] else {
            panic!("{:?}", done[&scan])
        };
        assert_eq!(
            data.batch.len() as u64,
            n_pushes * per,
            "every pipelined row comes back"
        );
    });
}

/// A client that asks for a big scan and stops reading: only the send deadline
/// can end the stall, and it must evict rather than wedge the send lock, on
/// either transport.
#[test]
fn a_stalled_scan_client_is_evicted_by_the_send_deadline() {
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_CLIENT_SEND_TIMEOUT_MS", "1500")]);
    for target in [srv.sock_path().to_string(), srv.tls_target()] {
        with_watchdog(120, move || {
            let (mut setup, _, tid, schema) = client_with_table(&target);
            setup
                .push(tid, &schema, &rows(&schema, 0..200_000), WireConflictMode::Update)
                .unwrap();

            let mut s = Session::connect(&target).unwrap();
            set_small_bufs(s.as_raw_fd());
            let all = ReadSpec::all_rows(ReadBound::None);
            s.submit(scan_req(tid, &all, &schema)).unwrap();
            assert!(write_out(&mut s));
            // Never read. Allow a few deadlines of slack.
            assert!(evicted_within(&mut s, 8_000), "{target}: not evicted");

            // Everyone else kept going.
            setup
                .push(tid, &schema, &rows(&schema, [200_000]), WireConflictMode::Update)
                .unwrap();
            assert_eq!(scan_all(&mut setup, tid, &schema).len(), 200_001);
        });
    }
}

/// With the inbound cap pinned to its 64 MiB floor, a client stalling its scan
/// reply and then flooding pushes past the cap is closed, and none of the flood
/// is applied. The default send deadline (30 s) stays out of the way.
#[test]
fn inbound_cap_breach_closes_stalled_connection() {
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_INBOUND_MEM_BYTES", "67108864")]);
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (mut setup, _, tid, schema) = client_with_table(&target);
        setup
            .push(tid, &schema, &rows(&schema, 0..200_000), WireConflictMode::Update)
            .unwrap();

        let mut s = Session::connect(&target).unwrap();
        set_small_bufs(s.as_raw_fd());
        let all = ReadSpec::all_rows(ReadBound::None);
        s.submit(scan_req(tid, &all, &schema)).unwrap();

        // ~140 MB of rows the table does not hold, ~1 MB a frame.
        let flood = rows(&schema, 200_000..225_000);
        let mut sent = 0;
        while sent < 140 && s.submit(push_req(tid, &schema, &flood)).is_ok() && write_out(&mut s) {
            sent += 1;
        }
        assert!(
            evicted_within(&mut s, 20_000),
            "server must close the cap-breaching connection (sent {sent} frames)"
        );
        assert_eq!(
            scan_all(&mut setup, tid, &schema).len(),
            200_000,
            "nothing of the flood was applied"
        );
    });
}
