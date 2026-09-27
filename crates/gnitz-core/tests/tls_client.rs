#![cfg(feature = "integration")]

//! End-to-end TLS transport tests against a real `gnitz-server` with a
//! `--tls-listen` listener (self-signed dev cert, ephemeral port).

use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

use gnitz_core::protocol::{encode_frame, hello_handshake, ClientTransport, ClientVerb, WireFlags, WireStatus};
use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_wire::control::{peek_control_block, ControlHeader};

/// One control-only frame, blocking until it is on the wire.
fn send_control(t: &mut ClientTransport, target_id: u64, flags: WireFlags) -> Result<(), gnitz_core::ProtocolError> {
    let hdr = ControlHeader { flags, target_id, ..Default::default() };
    t.send_frame(encode_frame(hdr, &[], None, None), None)
}
use gnitz_core::TableProps;
use gnitz_core::{ColumnDef, GnitzClient, PkColumn, Schema, TypeCode, WireConflictMode, ZSetBatch};
use gnitz_test_harness::{unique_schema, ServerHandle};

/// `(client, schema_name, table_id, schema)` for a fresh `(pk BIGINT, a
/// BIGINT, b BIGINT)` table reachable via `target`.
fn client_with_table(target: &str) -> (GnitzClient, String, u64, std::sync::Arc<Schema>) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema("tls");
    client.create_schema(&sn).unwrap();
    let cols = vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    client
        .create_table(
            &sn,
            "t",
            &Schema { columns: cols, pk_cols: vec![0] },
            &[],
            TableProps::default(),
            &[],
        )
        .unwrap();
    let (tid, schema) = client.resolve_table_or_view_id(&sn, "t").unwrap();
    (client, sn, tid, schema)
}

/// Ship one cold PUSH frame over a raw transport, bypassing `Session`'s schema
/// cache — these tests drive the wire, not the client's fast paths.
fn send_push(
    t: &mut ClientTransport,
    tid: u64,
    flags: WireFlags,
    schema: &Schema,
    batch: &ZSetBatch,
) -> Result<(), gnitz_core::protocol::ProtocolError> {
    let hdr = ControlHeader {
        flags,
        target_id: tid,
        ..Default::default()
    };
    t.send_frame(encode_frame(hdr, &[], Some(schema), Some(batch)), None)
}

/// Rows `(start + i, (start + i) * 3, 7)` for `count` rows.
fn make_batch(schema: &Schema, start: u64, count: usize) -> ZSetBatch {
    let pks: Vec<u64> = (start..start + count as u64).collect();
    let mut a = Vec::with_capacity(count * 8);
    let mut b = Vec::with_capacity(count * 8);
    for &pk in &pks {
        a.extend_from_slice(&((pk as i64) * 3).to_le_bytes());
        b.extend_from_slice(&7i64.to_le_bytes());
    }
    let mut z = ZSetBatch::new(schema);
    z.pks = PkColumn::from_natives(schema, pks.into_iter().map(u128::from));
    z.weights = vec![1i64; count];
    z.nulls = vec![0u64; count];
    for (pi, ci, _) in schema.payload_columns() {
        z.payload[pi].bytes = if ci == 1 { a.clone() } else { b.clone() };
    }
    z
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

/// Observe a server-side eviction from the client side. Over TCP a
/// shutdown+close on the server is NOT client-visible via POLLHUP while the
/// client's receive window is closed (the FIN queues behind the stalled
/// data), so probe actively: periodically send a tiny frame — a probe
/// segment reaching the closed server socket draws an RST, and a subsequent
/// send errors. Probe writes are deadline-bounded so a full send buffer
/// surfaces as WouldBlock (keep probing) instead of hanging.
fn eviction_observed_within(t: &mut ClientTransport, ms: u64) -> bool {
    let deadline = Instant::now() + Duration::from_millis(ms);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        match t.send_frame(vec![0u8; 8], Some(Instant::now() + Duration::from_secs(1))) {
            Err(gnitz_core::ProtocolError::IoError(e)) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(_) => return true,
            Ok(()) => continue,
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

// ── 1. connect + HELLO + alloc roundtrip ───────────────────────────────────

#[test]
fn connect_hello_and_alloc_roundtrip() {
    let srv = ServerHandle::start_tls(4);
    let mut client = GnitzClient::connect(&srv.tls_target()).expect("tls connect");
    let id1 = client.alloc_id().unwrap();
    let id2 = client.alloc_id().unwrap();
    assert!(id2 > id1, "alloc ids must advance over TLS");
}

// ── 2. verification modes ──────────────────────────────────────────────────

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
    let conn = rustls::ClientConnection::new(std::sync::Arc::new(cfg), name).unwrap();
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

// ── 3. push → scan roundtrip (data integrity, weights) ────────────────────

#[test]
fn push_scan_roundtrip_over_tls() {
    let srv = ServerHandle::start_tls(4);
    let (mut client, _sn, tid, schema) = client_with_table(&srv.tls_target());

    client.push(tid, &schema, &make_batch(&schema, 0, 1_000)).unwrap();
    // Retract one row (weight -1) so the scan proves net weights, not just
    // row presence.
    let mut retract = make_batch(&schema, 500, 1);
    retract.weights = vec![-1];
    client.push(tid, &schema, &retract).unwrap();

    let batch = client.scan(tid).unwrap().batch;
    assert_eq!(batch.len(), 999, "1000 inserts − 1 retraction");
    assert!(batch.weights.iter().all(|&w| w == 1), "all net weights must be +1");
    // Spot-check payload integrity via a seek.
    let row = client.seek(tid, &123u64.to_le_bytes()).unwrap().batch;
    assert_eq!(row.len(), 1, "seek must find pk=123");
    {
        let bytes = &row.payload[0].bytes;
        assert_eq!(i64::from_le_bytes(bytes[0..8].try_into().unwrap()), 369);
    }
}

// ── 4. big frames both directions ──────────────────────────────────────────

#[test]
fn big_push_and_multiframe_scan() {
    let srv = ServerHandle::start_tls(4);
    let (mut client, _sn, tid, schema) = client_with_table(&srv.tls_target());

    // ~16 MB in one push frame (700k rows × 24 B payload)...
    let count = 700_000;
    client.push(tid, &schema, &make_batch(&schema, 0, count)).unwrap();
    // ...and a scan whose train spans many worker frames.
    let batch = client.scan(tid).unwrap().batch;
    assert_eq!(batch.len(), count);
}

// ── 5. UNIX + TLS clients concurrently ─────────────────────────────────────

#[test]
fn unix_and_tls_clients_share_a_table() {
    let srv = ServerHandle::start_tls(4);
    let (mut tls_client, sn, tid, schema) = client_with_table(&srv.tls_target());
    let mut unix_client = GnitzClient::connect(srv.sock_path()).unwrap();

    tls_client.push(tid, &schema, &make_batch(&schema, 0, 100)).unwrap();
    let (utid, uschema) = unix_client.resolve_table_or_view_id(&sn, "t").unwrap();
    assert_eq!(utid, tid);
    unix_client
        .push(utid, &uschema, &make_batch(&uschema, 100, 100))
        .unwrap();

    assert_eq!(tls_client.scan(tid).unwrap().batch.len(), 200);
    assert_eq!(unix_client.scan(utid).unwrap().batch.len(), 200);
}

// ── 6. wire-version mismatch HELLO ─────────────────────────────────────────

#[test]
fn wire_version_mismatch_is_refused() {
    let srv = ServerHandle::start_tls(1);
    let mut t = ClientTransport::connect(&srv.tls_target(), None).unwrap();
    let mut hello = gnitz_wire::HELLO;
    hello[4] ^= 1;
    t.send_frame(hello.to_vec(), None).unwrap();
    // The server answers with its own HELLO, then closes.
    assert_eq!(t.recv_framed(None).unwrap(), gnitz_wire::HELLO);
    assert!(t.recv_framed(None).is_err());
}

// ── 7. restart: fail fast, same port, fresh connect works ─────────────────

#[test]
fn restart_same_port_fails_fast_then_reconnects() {
    let mut srv = ServerHandle::start_tls(1);
    let target = srv.tls_target();
    let (mut client, _sn, tid, _schema) = client_with_table(&target);

    srv.restart();

    // The old client's next call must fail fast (EOF/RST-derived), well
    // under 5 s — no timeout machinery involved.
    let t0 = Instant::now();
    assert!(client.scan(tid).is_err(), "stale connection must error after restart");
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "stale-connection error must be immediate, took {:?}",
        t0.elapsed()
    );

    // A fresh connect on the SAME target (port preserved) succeeds.
    let mut fresh = GnitzClient::connect(&target).expect("reconnect after restart");
    fresh.alloc_id().unwrap();
}

// ── 8. pipelining liveness (the four-party deadlock shape) ─────────────────

#[test]
fn pipelined_pushes_ahead_of_scan_do_not_deadlock() {
    let srv = ServerHandle::start_tls(4);
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (_setup, _sn, tid, schema) = client_with_table(&target);

        // Raw pipelining connection with both socket buffers pinned small
        // (default autotuned buffers can absorb the whole exchange and
        // false-green the test).
        let mut t = ClientTransport::connect(&target, None).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t, None).unwrap();

        // >16 MB of pushes pipelined ahead of a scan, before reading ANY
        // response. The server's ACK sends stall against our unread socket
        // while we are still mid-send-batch — liveness requires the
        // server's always-reading pump.
        let push_flags = WireFlags {
            verb: ClientVerb::Push,
            conflict_mode: WireConflictMode::Update,
            ..Default::default()
        };
        let n_pushes = 30usize;
        for i in 0..n_pushes {
            let batch = make_batch(&schema, (i * 25_000) as u64, 25_000);
            send_push(&mut t, tid, push_flags, &schema, &batch).unwrap();
        }
        // The scan whose response (~18 MB) exceeds the shrunken buffers.
        send_control(&mut t, tid, WireFlags::default()).unwrap();

        // Now read everything: n ACKs, then the scan train.
        for _ in 0..n_pushes {
            let buf = t.recv_framed(None).unwrap();
            let ack = peek_control_block(&buf).unwrap();
            assert_eq!(ack.hdr.status, WireStatus::Ok, "push ACK must be OK");
        }
        let mut rows = 0usize;
        loop {
            let buf = t.recv_framed(None).unwrap();
            let ctrl = peek_control_block(&buf).unwrap();
            assert_eq!(ctrl.hdr.status, WireStatus::Ok, "scan frame must be OK");
            if let Some(r) = ctrl.data {
                rows += gnitz_wire::read_u32_le(&buf[r], gnitz_wire::wal::WAL_OFF_ROWS) as usize;
            }
            if !ctrl.hdr.flags.continuation {
                break;
            }
        }
        assert_eq!(rows, n_pushes * 25_000, "every pipelined row must come back");
    });
}

// ── 9. IPv6 loopback ────────────────────────────────────────────────────────

#[test]
fn ipv6_loopback_connect_and_ca_verify() {
    let srv = ServerHandle::start_tls_v6(1);
    let target = srv.tls_target();
    assert!(target.starts_with("tls://[::1]:"), "v6 endpoint expected, got {target}");
    // Full verification against the dev cert's ::1 IP SAN.
    let mut client = GnitzClient::connect(&target).expect("ca-pinned tls over [::1]");
    client.alloc_id().unwrap();
}

// ── 10. teardown under write backpressure (recv-side / inbound-cap path) ──

#[test]
fn inbound_cap_breach_closes_stalled_connection() {
    // Inbound cap pinned to its 64 MiB floor; the client stalls the scan
    // train (never reads) and then pipelines a cap-breaching push burst.
    // Default send deadline (30 s) stays out of the way — the recv-side
    // cap breach is what must close the connection.
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_INBOUND_MEM_BYTES", "67108864")]);
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (mut setup, _sn, tid, schema) = client_with_table(&target);
        // Enough rows that the scan train exceeds the shrunken buffers.
        for block in 0..8u64 {
            setup
                .push(tid, &schema, &make_batch(&schema, block * 25_000, 25_000))
                .unwrap();
        }

        let mut t = ClientTransport::connect(&target, None).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t, None).unwrap();
        // Ask for the scan, then never read: the train stalls, pinning
        // connection_loop in its guarded send.
        send_control(&mut t, tid, WireFlags::default()).unwrap();

        // Pipeline ~80 MB of pushes past the 64 MiB cap; the breach discards
        // them unapplied. Each duplicates block 0, so the scan below holds regardless.
        let push_flags = WireFlags {
            verb: ClientVerb::Push,
            conflict_mode: WireConflictMode::Update,
            ..Default::default()
        };
        let batch = make_batch(&schema, 0, 25_000); // ~1 MB/frame
        let mut sent = 0usize;
        for _ in 0..140 {
            if send_push(&mut t, tid, push_flags, &schema, &batch).is_err() {
                break; // server already shut us down mid-burst — success path
            }
            sent += 1;
        }
        assert!(
            eviction_observed_within(&mut t, 20_000),
            "server must close the cap-breaching connection (sent {sent} frames)"
        );

        // The cluster keeps serving other clients.
        assert_eq!(setup.scan(tid).unwrap().batch.len(), 200_000);
    });
}

// ── 11. per-send eviction deadline (the send lock must never wedge) ───────

#[test]
fn stalled_scan_client_is_evicted_by_send_deadline() {
    // The client asks for a big scan and stops reading: only the send deadline
    // can end the stall, and it must evict rather than wedge the send lock.
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_CLIENT_SEND_TIMEOUT_MS", "1500")]);
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (mut setup, _sn, tid, schema) = client_with_table(&target);
        for block in 0..8u64 {
            setup
                .push(tid, &schema, &make_batch(&schema, block * 25_000, 25_000))
                .unwrap();
        }

        let mut t = ClientTransport::connect(&target, None).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t, None).unwrap();
        send_control(&mut t, tid, WireFlags::default()).unwrap();
        // Never read. Allow a few deadlines of slack.
        assert!(
            eviction_observed_within(&mut t, 8_000),
            "server must evict the stalled scan client within the deadline window"
        );

        // No cluster freeze / no lock wedge: a concurrent client still works.
        assert_eq!(setup.scan(tid).unwrap().batch.len(), 200_000);
    });
}

// ── 12. mTLS roundtrip (CA-signed client cert) ─────────────────────────────

#[test]
fn mtls_roundtrip_push_and_scan() {
    let srv = ServerHandle::start_mtls(4);
    // `mtls_target()` presents the CA-signed client leaf + verifies the dev
    // server cert. A required-mTLS server accepts it, so the full data path
    // works end to end.
    let (mut client, _sn, tid, schema) = client_with_table(&srv.mtls_target());
    client.push(tid, &schema, &make_batch(&schema, 0, 500)).unwrap();
    let batch = client.scan(tid).unwrap().batch;
    assert_eq!(batch.len(), 500, "authenticated client's rows must round-trip");
}

// ── 13. no client cert vs a required-mTLS server ───────────────────────────

#[test]
fn mtls_server_rejects_client_without_cert() {
    let srv = ServerHandle::start_mtls(1);
    // `tls_target()` verifies the server but presents NO client cert. The
    // handshake completes inside the HELLO exchange, so a full connect
    // (handshake + HELLO) must fail.
    assert!(
        GnitzClient::connect(&srv.tls_target()).is_err(),
        "an mTLS-required server must reject a client presenting no certificate"
    );
}

// ── 14. client cert from an untrusted CA ───────────────────────────────────

#[test]
fn mtls_server_rejects_untrusted_client_cert() {
    let srv = ServerHandle::start_mtls(1);
    let other = ServerHandle::start_mtls(1);
    // Present `other`'s leaf (signed by other's CA) against `srv`, which trusts
    // only its OWN client CA: srv's endpoint + srv's dev cert + other's leaf.
    let (foreign_cert, foreign_key) = other.mtls_client_cert_key();
    let target = format!(
        "tls://{}?ca={}&cert={}&key={}",
        srv.tls_endpoint(),
        srv.tls_ca_path().display(),
        foreign_cert.display(),
        foreign_key.display()
    );
    assert!(
        GnitzClient::connect(&target).is_err(),
        "a client cert from an untrusted CA must be rejected"
    );
}

// ── 15. non-loopback bind refusal + escape hatch ───────────────────────────

#[test]
fn non_loopback_bind_refused_without_client_auth() {
    // `0.0.0.0` is non-loopback AND bindable everywhere. With neither a client
    // CA nor the escape hatch, the server must refuse to start.
    let stderr = match ServerHandle::try_start_tls(1, &["--tls-listen=0.0.0.0:0"]) {
        Ok(_) => panic!("a non-loopback bind without client auth must not boot"),
        Err(e) => e,
    };
    assert!(
        stderr.contains("refusing to bind"),
        "boot stderr must carry the bind refusal, got: {stderr}"
    );

    // The escape hatch permits the very same bind (server boots and stays up).
    let booted = ServerHandle::try_start_tls(1, &["--tls-listen=0.0.0.0:0", "--allow-unauthenticated"])
        .expect("--allow-unauthenticated must permit a non-loopback bind");
    drop(booted); // clean shutdown
}

// ── 16. global connection cap ──────────────────────────────────────────────

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

// ── 17. HELLO deadline ─────────────────────────────────────────────────────

const HELLO_DEADLINE: Duration = Duration::from_millis(500);

/// Assert the server closes `sock`, fresh and silent, around [`HELLO_DEADLINE`]:
/// by EOF, a TLS close alert or a reset, not by the read timing out.
fn assert_reaped(transport: &str, sock: &mut impl std::io::Read) {
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
    use std::net::TcpStream;
    use std::os::unix::net::UnixStream;

    let deadline_ms = HELLO_DEADLINE.as_millis().to_string();
    let srv = ServerHandle::start_tls_with_env(4, &[("GNITZ_HELLO_TIMEOUT_MS", &deadline_ms)]);
    let timeout = Some(HELLO_DEADLINE * 6);
    let mut tcp = TcpStream::connect(srv.tls_endpoint()).expect("tcp connect");
    tcp.set_read_timeout(timeout).unwrap();
    assert_reaped("tls", &mut tcp);
    let mut unix = UnixStream::connect(srv.sock_path()).expect("unix connect");
    unix.set_read_timeout(timeout).unwrap();
    assert_reaped("unix", &mut unix);

    // A client that sends its HELLO at once is unaffected.
    let mut client = GnitzClient::connect(&srv.tls_target()).expect("a normal client must connect");
    client.alloc_id().unwrap();
}
