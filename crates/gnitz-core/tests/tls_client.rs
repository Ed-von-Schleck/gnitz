#![cfg(feature = "integration")]

//! End-to-end TLS transport tests against a real `gnitz-server` with a
//! `--tls-listen` listener (self-signed dev cert, ephemeral port).
//!
//! Covers the full committed surface: verification modes (`?insecure`,
//! `?ca=`, default webpki roots, wrong CA, ALPN mismatch), data integrity
//! and weights over TLS, big frames in both directions, UNIX+TLS
//! coexistence, HELLO version rejection, restart fail-fast, pipelining
//! liveness (the four-party deadlock shape), and the per-send eviction
//! deadline that keeps a `send_mutex`-holding send from wedging a
//! connection against a TCP-alive non-reading peer.

use std::os::unix::io::RawFd;
use std::time::{Duration, Instant};

use gnitz_core::{
    hello_handshake, parse_response, send_message, wire_flags_set_conflict_mode, ClientTransport, ColData, ColumnDef,
    GnitzClient, PkColumn, PkTuple, Schema, TypeCode, WireConflictMode, ZSetBatch, FLAG_CONTINUATION, FLAG_PUSH,
    STATUS_ERROR,
};
use gnitz_test_harness::ServerHandle;

/// Per-test unique schema name (parallel tests share nothing — each has its
/// own server — but uniqueness keeps failures unambiguous).
fn unique_schema() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    format!("tls{}", SEQ.fetch_add(1, Ordering::Relaxed))
}

/// `(client, schema_name, table_id, schema)` for a fresh `(pk BIGINT, a
/// BIGINT, b BIGINT)` table reachable via `target`.
fn client_with_table(target: &str) -> (GnitzClient, String, u64, Schema) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema();
    client.create_schema(&sn).unwrap();
    let cols = vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    client.create_table(&sn, "t", &cols, &[0], true, false, 0, &[]).unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    (client, sn, tid, schema)
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
    let columns: Vec<ColData> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(ci, _)| {
            if schema.is_pk_col(ci) {
                ColData::Fixed(vec![])
            } else if ci == 1 {
                ColData::Fixed(a.clone())
            } else {
                ColData::Fixed(b.clone())
            }
        })
        .collect();
    ZSetBatch {
        pks: PkColumn::U64s(pks),
        weights: vec![1i64; count],
        nulls: vec![0u64; count],
        columns,
    }
}

/// Pin both socket buffers small so backpressure paths engage well below
/// the multi-MB exchange sizes these tests move. NOT smaller: shrinking a
/// connected loopback TCP socket's rcvbuf below the 64 KiB loopback segment
/// size makes the kernel DROP segments, collapsing the connection into
/// exponential RTO backoff (cwnd=1, ~26 s retransmits) — which reads as a
/// deadlock but is a test artifact.
fn set_small_bufs(fd: RawFd) {
    let bufsz: libc::c_int = 128 * 1024;
    for opt in [libc::SO_SNDBUF, libc::SO_RCVBUF] {
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &bufsz as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

/// Observe a server-side eviction from the client side. Over TCP a
/// shutdown+close on the server is NOT client-visible via POLLHUP while the
/// client's receive window is closed (the FIN queues behind the stalled
/// data), so probe actively: periodically send a tiny frame — a probe
/// segment reaching the closed server socket draws an RST, and a subsequent
/// send errors. Probe writes are deadline-bounded (SO_SNDTIMEO) so a full
/// send buffer surfaces as WouldBlock (keep probing) instead of hanging.
fn eviction_observed_within(t: &mut ClientTransport, ms: u64) -> bool {
    let tv = libc::timeval { tv_sec: 1, tv_usec: 0 };
    unsafe {
        libc::setsockopt(
            t.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_SNDTIMEO,
            &tv as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::timeval>() as libc::socklen_t,
        );
    }
    let deadline = Instant::now() + Duration::from_millis(ms);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        match t.send_framed(&[0u8; 8]) {
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

// ── 1. insecure connect + HELLO + alloc roundtrip ─────────────────────────

#[test]
fn insecure_connect_hello_and_alloc_roundtrip() {
    let Some(srv) = ServerHandle::start_tls(4) else { return };
    let mut client = GnitzClient::connect(&srv.tls_target()).expect("tls connect");
    let id1 = client.alloc_table_id().unwrap();
    let id2 = client.alloc_table_id().unwrap();
    assert!(id2 > id1, "alloc ids must advance over TLS");
}

// ── 2. verification modes ──────────────────────────────────────────────────

#[test]
fn ca_pin_connects_and_bad_verifications_fail() {
    let Some(srv) = ServerHandle::start_tls(1) else { return };

    // ?ca=dev cert: full verification against the minted self-signed cert.
    let mut pinned = GnitzClient::connect(&srv.tls_ca_target()).expect("ca-pinned connect");
    pinned.alloc_table_id().unwrap();

    // Default webpki roots must REJECT the self-signed dev cert.
    let bare = srv.tls_target().replace("?insecure", "");
    let err = ClientTransport::connect(&bare).err().expect("webpki roots must reject");
    assert!(
        err.to_string().to_lowercase().contains("handshake"),
        "expected a handshake/certificate error, got: {err}"
    );

    // Wrong CA: a *different* server's dev cert must not verify this one.
    let Some(other) = ServerHandle::start_tls(1) else {
        return;
    };
    let endpoint = srv.tls_ca_target();
    let (host_port, _) = endpoint.split_once('?').unwrap();
    let other_ca = other.tls_ca_target();
    let (_, other_param) = other_ca.split_once('?').unwrap();
    let wrong_ca = format!("{host_port}?{other_param}");
    assert!(
        ClientTransport::connect(&wrong_ca).is_err(),
        "a foreign CA must fail verification"
    );

    // ALPN mismatch: a raw rustls client offering only http/1.1 must fail
    // the handshake (the server pins a non-empty ALPN list). The client
    // verifies for real against the dev cert — no skip-verifier — so the
    // ALPN mismatch is the only possible failure cause.
    let addr = {
        let t = srv.tls_target();
        t.strip_prefix("tls://").unwrap().split_once('?').unwrap().0.to_string()
    };
    let mut roots = rustls::RootCertStore::empty();
    let certs = {
        use rustls::pki_types::pem::PemObject;
        rustls::pki_types::CertificateDer::pem_file_iter(srv.tls_ca_path()).unwrap()
    };
    roots.add_parsable_certificates(certs.filter_map(Result::ok));
    let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
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
    let Some(srv) = ServerHandle::start_tls(4) else { return };
    let (mut client, _sn, tid, schema) = client_with_table(&srv.tls_target());

    client.push(tid, &schema, &make_batch(&schema, 0, 1_000)).unwrap();
    // Retract one row (weight -1) so the scan proves net weights, not just
    // row presence.
    let mut retract = make_batch(&schema, 500, 1);
    retract.weights = vec![-1];
    client.push(tid, &schema, &retract).unwrap();

    let (_, batch, _) = client.scan(tid).unwrap();
    let batch = batch.expect("scan must return rows");
    assert_eq!(batch.len(), 999, "1000 inserts − 1 retraction");
    assert!(batch.weights.iter().all(|&w| w == 1), "all net weights must be +1");
    // Spot-check payload integrity via a seek.
    let (_, row, _) = client.seek(tid, &PkTuple::from_u128_narrow(123)).unwrap();
    let row = row.expect("seek must find pk=123");
    match &row.columns[1] {
        ColData::Fixed(bytes) => assert_eq!(i64::from_le_bytes(bytes[0..8].try_into().unwrap()), 369),
        _ => panic!("expected Fixed column"),
    }
}

// ── 4. big frames both directions ──────────────────────────────────────────

#[test]
fn big_push_and_multiframe_scan() {
    let Some(srv) = ServerHandle::start_tls(4) else { return };
    let (mut client, _sn, tid, schema) = client_with_table(&srv.tls_target());

    // ~16 MB in one push frame (700k rows × 24 B payload)...
    let count = 700_000;
    client.push(tid, &schema, &make_batch(&schema, 0, count)).unwrap();
    // ...and a scan whose train spans many worker frames.
    let (_, batch, _) = client.scan(tid).unwrap();
    let batch = batch.expect("scan must return rows");
    assert_eq!(batch.len(), count);
}

// ── 5. UNIX + TLS clients concurrently ─────────────────────────────────────

#[test]
fn unix_and_tls_clients_share_a_table() {
    let Some(srv) = ServerHandle::start_tls(4) else { return };
    let (mut tls_client, sn, tid, schema) = client_with_table(&srv.tls_target());
    let mut unix_client = GnitzClient::connect(&srv.sock_path).unwrap();

    tls_client.push(tid, &schema, &make_batch(&schema, 0, 100)).unwrap();
    let (utid, uschema) = unix_client.resolve_table_id(&sn, "t").unwrap();
    assert_eq!(utid, tid);
    unix_client
        .push(utid, &uschema, &make_batch(&uschema, 100, 100))
        .unwrap();

    let (_, via_tls, _) = tls_client.scan(tid).unwrap();
    let (_, via_unix, _) = unix_client.scan(utid).unwrap();
    assert_eq!(via_tls.map(|b| b.len()), Some(200));
    assert_eq!(via_unix.map(|b| b.len()), Some(200));
}

// ── 6. wire-version mismatch HELLO ─────────────────────────────────────────

#[test]
fn wire_version_mismatch_hello_gets_status_error() {
    let Some(srv) = ServerHandle::start_tls(1) else { return };
    let mut t = ClientTransport::connect(&srv.tls_target()).unwrap();
    let payload = gnitz_wire::encode_hello_payload(gnitz_wire::WAL_FORMAT_VERSION as u16 + 1);
    t.send_framed(&payload).unwrap();
    let buf = t.recv_framed(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
    let msg = parse_response(&buf, None).unwrap();
    assert_eq!(msg.status, STATUS_ERROR);
    let text = msg.error_text.unwrap_or_default();
    assert!(
        text.contains("version"),
        "STATUS_ERROR must name the version mismatch, got: {text}"
    );
    // Clean close follows the error frame.
    assert!(t.recv_framed(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).is_err());
}

// ── 7. restart: fail fast, same port, fresh connect works ─────────────────

#[test]
fn restart_same_port_fails_fast_then_reconnects() {
    let Some(mut srv) = ServerHandle::start_tls(1) else {
        return;
    };
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
    fresh.alloc_table_id().unwrap();
}

// ── 8. pipelining liveness (the four-party deadlock shape) ─────────────────

#[test]
fn pipelined_pushes_ahead_of_scan_do_not_deadlock() {
    let Some(srv) = ServerHandle::start_tls(4) else { return };
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (_setup, _sn, tid, schema) = client_with_table(&target);

        // Raw pipelining connection with both socket buffers pinned small
        // (default autotuned buffers can absorb the whole exchange and
        // false-green the test).
        let mut t = ClientTransport::connect(&target).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t).unwrap();

        // >16 MB of pushes pipelined ahead of a scan, before reading ANY
        // response. The server's ACK sends stall against our unread socket
        // while we are still mid-send-batch — liveness requires the
        // server's always-reading pump.
        let push_flags = wire_flags_set_conflict_mode(FLAG_PUSH, WireConflictMode::Update);
        let n_pushes = 30usize;
        for i in 0..n_pushes {
            let batch = make_batch(&schema, (i * 25_000) as u64, 25_000);
            send_message(
                &mut t,
                tid,
                0xF00D,
                push_flags,
                &PkTuple::EMPTY,
                0,
                Some(&schema),
                Some(&batch),
            )
            .unwrap();
        }
        // The scan whose response (~18 MB) exceeds the shrunken buffers.
        send_message(&mut t, tid, 0xF00D, 0, &PkTuple::EMPTY, 0, None, None).unwrap();

        // Now read everything: n ACKs, then the scan train.
        for _ in 0..n_pushes {
            let buf = t.recv_framed(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
            let ack = parse_response(&buf, None).unwrap();
            assert_eq!(ack.status, 0, "push ACK must be OK");
        }
        let mut rows = 0usize;
        let mut schema_seen: Option<(std::sync::Arc<Schema>, u16)> = None;
        loop {
            let buf = t.recv_framed(gnitz_wire::MAX_FRAME_PAYLOAD_CLIENT).unwrap();
            let hint = schema_seen.as_ref().map(|(s, v)| (s.as_ref(), *v));
            let msg = parse_response(&buf, hint).unwrap();
            assert_eq!(msg.status, 0, "scan frame must be OK");
            let flags = msg.flags;
            if let Some(s) = msg.schema {
                schema_seen = Some((s, gnitz_core::wire_flags_get_schema_version(flags)));
            }
            if let Some(b) = msg.data_batch {
                rows += b.len();
            }
            if flags & FLAG_CONTINUATION == 0 {
                break;
            }
        }
        assert_eq!(rows, n_pushes * 25_000, "every pipelined row must come back");
    });
}

// ── 9. IPv6 loopback ────────────────────────────────────────────────────────

#[test]
fn ipv6_loopback_connect_and_ca_verify() {
    let Some(srv) = ServerHandle::start_tls_v6(1) else {
        return;
    };
    let target = srv.tls_target();
    assert!(target.starts_with("tls://[::1]:"), "v6 endpoint expected, got {target}");
    let mut client = GnitzClient::connect(&target).expect("tls over [::1]");
    client.alloc_table_id().unwrap();

    // Full verification against the dev cert's ::1 IP SAN.
    let mut pinned = GnitzClient::connect(&srv.tls_ca_target()).expect("ca-pinned over [::1]");
    pinned.alloc_table_id().unwrap();
}

// ── 10. teardown under write backpressure (recv-side / inbound-cap path) ──

#[test]
fn inbound_cap_breach_closes_stalled_connection() {
    // Inbound cap pinned to its 64 MiB floor; the client stalls the scan
    // train (never reads) and then pipelines a cap-breaching push burst.
    // Default send deadline (30 s) stays out of the way — the recv-side
    // cap breach is what must close the connection.
    let Some(srv) = ServerHandle::start_tls_with_env(4, &[("GNITZ_INBOUND_MEM_BYTES", "67108864")]) else {
        return;
    };
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (mut setup, _sn, tid, schema) = client_with_table(&target);
        // Enough rows that the scan train exceeds the shrunken buffers.
        for block in 0..8u64 {
            setup
                .push(tid, &schema, &make_batch(&schema, block * 25_000, 25_000))
                .unwrap();
        }

        let mut t = ClientTransport::connect(&target).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t).unwrap();
        // Ask for the scan, then never read: the train stalls, pinning
        // connection_loop in its guarded send.
        send_message(&mut t, tid, 0xB0BA, 0, &PkTuple::EMPTY, 0, None, None).unwrap();

        // Pipeline ~80 MB of pushes; the pump queues them until the 64 MiB
        // cap trips and the recv side tears the connection down. The rows
        // duplicate block 0 exactly (idempotent upserts), so however many
        // queued frames the draining connection_loop applies before hitting
        // the closed queue, the table's net content is unchanged.
        let push_flags = wire_flags_set_conflict_mode(FLAG_PUSH, WireConflictMode::Update);
        let batch = make_batch(&schema, 0, 25_000); // ~1 MB/frame
        let mut sent = 0usize;
        for _ in 0..140 {
            if send_message(
                &mut t,
                tid,
                0xB0BA,
                push_flags,
                &PkTuple::EMPTY,
                0,
                Some(&schema),
                Some(&batch),
            )
            .is_err()
            {
                break; // server already shut us down mid-burst — success path
            }
            sent += 1;
        }
        assert!(
            eviction_observed_within(&mut t, 20_000),
            "server must close the cap-breaching connection (sent {sent} frames)"
        );

        // The cluster keeps serving other clients.
        let (_, batch, _) = setup.scan(tid).unwrap();
        assert_eq!(batch.map(|b| b.len()), Some(200_000));
    });
}

// ── 11. per-send eviction deadline (send_mutex must never wedge) ───────────

#[test]
fn stalled_scan_client_is_evicted_by_send_deadline() {
    // The client completes HELLO, asks for a big scan, then stops reading
    // entirely — no recv-side death, no inbound-cap breach; ONLY the send
    // deadline can fire. Holding send_mutex across an unbounded send would
    // wedge the connection forever; the deadline's shutdown must evict
    // instead. This exercises the one guarded send primitive every TLS
    // send shares (`send_guarded` → `send_bytes` → `send_raw`): the
    // send_slot, send_buffer, and HELLO-ACK paths differ only in the byte
    // source, so the eviction proven here is the eviction for all of them.
    // (A separate send_buffer stall scenario is not constructible
    // deterministically: every send_buffer reply is a bounded control /
    // schema frame that the kernel socket buffer absorbs without parking
    // the send.)
    let Some(srv) = ServerHandle::start_tls_with_env(4, &[("GNITZ_CLIENT_SEND_TIMEOUT_MS", "1500")]) else {
        return;
    };
    let target = srv.tls_target();
    with_watchdog(120, move || {
        let (mut setup, _sn, tid, schema) = client_with_table(&target);
        for block in 0..8u64 {
            setup
                .push(tid, &schema, &make_batch(&schema, block * 25_000, 25_000))
                .unwrap();
        }

        let mut t = ClientTransport::connect(&target).unwrap();
        set_small_bufs(t.as_raw_fd());
        hello_handshake(&mut t).unwrap();
        send_message(&mut t, tid, 0xB0BA, 0, &PkTuple::EMPTY, 0, None, None).unwrap();
        // Never read. Allow a few deadlines of slack.
        assert!(
            eviction_observed_within(&mut t, 8_000),
            "server must evict the stalled scan client within the deadline window"
        );

        // No cluster freeze / no mutex wedge: a concurrent client still works.
        let (_, batch, _) = setup.scan(tid).unwrap();
        assert_eq!(batch.map(|b| b.len()), Some(200_000));
    });
}

// ── 12. mTLS roundtrip (CA-signed client cert) ─────────────────────────────

#[test]
fn mtls_roundtrip_push_and_scan() {
    let Some(srv) = ServerHandle::start_mtls(4) else { return };
    // `mtls_target()` presents the CA-signed client leaf + verifies the dev
    // server cert. A required-mTLS server accepts it, so the full data path
    // works end to end.
    let (mut client, _sn, tid, schema) = client_with_table(&srv.mtls_target());
    client.push(tid, &schema, &make_batch(&schema, 0, 500)).unwrap();
    let (_, batch, _) = client.scan(tid).unwrap();
    assert_eq!(
        batch.map(|b| b.len()),
        Some(500),
        "authenticated client's rows must round-trip"
    );
}

// ── 13. no client cert vs a required-mTLS server ───────────────────────────

#[test]
fn mtls_server_rejects_client_without_cert() {
    let Some(srv) = ServerHandle::start_mtls(1) else { return };
    // `tls_ca_target()` verifies the server but presents NO client cert. The
    // rejection may surface at the TLS handshake or at the first framed
    // exchange (a TLS 1.3 client finishes 0.5-RTT before the server validates
    // its cert), so drive a full connect (handshake + HELLO) and require it
    // to fail.
    assert!(
        GnitzClient::connect(&srv.tls_ca_target()).is_err(),
        "an mTLS-required server must reject a client presenting no certificate"
    );
}

// ── 14. client cert from an untrusted CA ───────────────────────────────────

#[test]
fn mtls_server_rejects_untrusted_client_cert() {
    let Some(srv) = ServerHandle::start_mtls(1) else { return };
    let Some(other) = ServerHandle::start_mtls(1) else {
        return;
    };
    // Present `other`'s leaf (signed by other's CA) against `srv`, which trusts
    // only its OWN client CA. Reuse the existing wrong-CA target-splicing
    // pattern: srv's endpoint + srv's dev cert (?ca=) + other's leaf.
    let (foreign_cert, foreign_key) = other.mtls_client_cert_key();
    let srv_ca_target = srv.tls_ca_target(); // tls://IP:PORT?ca=<srv dev cert>
    let (host_port, srv_ca_param) = srv_ca_target.split_once('?').unwrap();
    let target = format!(
        "{host_port}?{srv_ca_param}&cert={}&key={}",
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
    // CA nor the escape hatch, boot must abort with the refusal message.
    // `boot_expecting_exit` waits for the process to exit (the AF_UNIX socket
    // is already listening when the refusal fires, so a readiness probe would
    // race it).
    let Some((exited_zero, stderr)) = ServerHandle::boot_expecting_exit(1, &["--tls-listen=0.0.0.0:0"]) else {
        return;
    };
    assert!(
        !exited_zero,
        "a non-loopback bind without client auth must exit non-zero"
    );
    assert!(
        stderr.to_lowercase().contains("refusing to bind"),
        "boot stderr must carry the bind refusal, got: {stderr}"
    );

    // The escape hatch permits the very same bind (server boots and stays up).
    let booted = ServerHandle::try_start_tls(1, &["--tls-listen=0.0.0.0:0", "--allow-unauthenticated"])
        .expect("server binary present")
        .expect("--allow-unauthenticated must permit a non-loopback bind");
    drop(booted); // clean shutdown
}

// ── 16. global connection cap ──────────────────────────────────────────────

#[test]
fn global_connection_cap_closes_excess() {
    let Some(result) = ServerHandle::try_start_tls(1, &["--tls-listen=127.0.0.1:0", "--tls-max-conns=2"]) else {
        return;
    };
    let srv = result.expect("a server with --tls-max-conns must boot");
    let target = srv.tls_target();

    // Hold two connections — each completes HELLO, so each counts against the
    // cap for its whole lifetime.
    let _c1 = GnitzClient::connect(&target).expect("1st connection under the cap");
    let c2 = GnitzClient::connect(&target).expect("2nd connection under the cap");

    // The 3rd is closed immediately (fd closed before any TLS work), so its
    // handshake cannot complete.
    assert!(
        ClientTransport::connect(&target).is_err(),
        "a connection accepted past the cap must be closed"
    );

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

// ── 17. pre-auth first-frame deadline ──────────────────────────────────────

#[test]
fn first_frame_deadline_reaps_silent_connections() {
    use std::io::Read;
    use std::net::TcpStream;

    let Some(srv) = ServerHandle::start_tls_with_env(4, &[("GNITZ_TLS_HELLO_TIMEOUT_MS", "500")]) else {
        return;
    };
    // Raw loopback IP:PORT (strip the `tls://` scheme and `?insecure`).
    let addr = {
        let t = srv.tls_target();
        t.strip_prefix("tls://").unwrap().split_once('?').unwrap().0.to_string()
    };

    // A raw TCP connection that sends NO bytes never starts the TLS handshake,
    // so its first frame never deframes. The 500 ms deadline must close it —
    // well before our 3 s read timeout (which, absent the deadline, is the
    // only thing that would ever return).
    let mut sock = TcpStream::connect(&addr).expect("tcp connect");
    sock.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let t0 = Instant::now();
    let mut buf = [0u8; 1];
    // At the deadline the server runs `peer.close()`, which makes rustls emit a
    // (plaintext, pre-handshake) close alert and then shut the socket down.
    // So the read unblocks with EOF (`Ok(0)`), the alert byte (`Ok(n>0)`), or
    // an RST (`Err`) — ALL of which mean the server reaped us at the deadline.
    // Only a WouldBlock/TimedOut means the server never acted (no deadline).
    match sock.read(&mut buf) {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
            panic!("server did not reap the silent connection within 3 s")
        }
        _ => {}
    }
    let elapsed = t0.elapsed();
    assert!(
        (Duration::from_millis(300)..Duration::from_secs(3)).contains(&elapsed),
        "the close must land near the 500 ms deadline, took {elapsed:?}"
    );

    // A normal client (HELLO well within 500 ms) is unaffected.
    let mut client = GnitzClient::connect(&srv.tls_target()).expect("a normal client must connect");
    client.alloc_table_id().unwrap();
}

// ── 18. client `?insecure` fail-closed against a non-loopback host ─────────

#[test]
fn insecure_non_loopback_is_fail_closed() {
    // Pure client-side: the refusal fires before any socket work, so no server
    // is needed. Skip if the override happens to be set in this process env.
    if std::env::var("GNITZ_TLS_INSECURE").as_deref() == Ok("1") {
        return;
    }
    let err = ClientTransport::connect("tls://example.com:9?insecure")
        .err()
        .expect("non-loopback ?insecure must be refused without GNITZ_TLS_INSECURE=1");
    assert!(
        err.to_string().to_lowercase().contains("non-loopback"),
        "expected a fail-closed refusal before any connect, got: {err}"
    );
    // Loopback `?insecure` stays allowed — covered by every tls_target() test.
}
