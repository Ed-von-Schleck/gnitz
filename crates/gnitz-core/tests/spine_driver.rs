#![cfg(feature = "integration")]

//! A second driver over the connection spine: many operations in flight on
//! one connection against a real server, on both transports. Carries no
//! futures, no waker and no executor — submit N, then `step` / drain / park
//! until every slot is done. It lives out of crate on purpose: it proves the
//! spine's public surface is complete for a driver with nothing private in
//! reach, and it exercises what the one-slot blocking client cannot — a deep
//! outbound queue, the head accumulator handing off across slots, a
//! driver-side slot map, both interests armed at once, and the cap.

use std::collections::HashMap;
use std::time::Duration;

use gnitz_core::protocol::set_sockopt_int;
use gnitz_core::{
    ColumnDef, GnitzClient, Interest, PkColumn, Reply, Request, Schema, Session, SlotId, TableProps, TypeCode,
    WireConflictMode, ZSetBatch, MAX_IN_FLIGHT,
};
use gnitz_test_harness::{strace_test, unique_schema, ServerHandle};

/// A `(pk BIGINT, a BIGINT)` table reachable through `target`, and the
/// blocking client that made it.
fn table(target: &str) -> (GnitzClient, u64, std::sync::Arc<Schema>) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let sn = unique_schema("spine");
    client.create_schema(&sn).unwrap();
    client
        .create_table(&sn, "t", &cols(), &[0], TableProps::default(), &[])
        .unwrap();
    let (tid, schema) = client.resolve_table_id(&sn, "t").unwrap();
    (client, tid, schema)
}

fn cols() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
    ]
}

/// The fixture table's schema, built locally — [`rows`] needs it to encode a
/// key, and `table()`'s copy comes back from the server.
fn local_schema() -> Schema {
    Schema { columns: cols(), pk_cols: vec![0] }
}

fn rows(start: i64, count: usize) -> ZSetBatch {
    let mut a = Vec::with_capacity(count * 8);
    for i in 0..count as i64 {
        a.extend_from_slice(&((start + i) * 3).to_le_bytes());
    }
    let schema = local_schema();
    let mut b = ZSetBatch::new(&schema);
    b.pks = PkColumn::from_natives(&schema, (0..count as i64).map(|i| (start + i) as u128));
    b.weights = vec![1; count];
    b.nulls = vec![0; count];
    b.payload[0].bytes = a;
    b
}

/// The driver: park on `interest()`, step with what `poll` reported, drain
/// completions into the map, until every submitted slot is done. Also
/// reports whether both interests were ever armed at once.
fn drive_all(s: &mut Session, want: usize) -> (HashMap<SlotId, Result<Reply, gnitz_core::ClientError>>, bool) {
    let mut done = HashMap::new();
    let mut ready = Interest::WRITE;
    let mut both_armed = false;
    while done.len() < want {
        for (id, r) in s.step(ready).expect("step") {
            assert!(done.insert(id, r).is_none(), "a slot completes exactly once");
        }
        if done.len() == want {
            break;
        }
        let interest = s.interest();
        assert!(!interest.is_empty(), "slots pending but nothing to wait for");
        both_armed |= interest.read && interest.write;
        let mut pfd = libc::pollfd {
            fd: s.as_raw_fd(),
            events: interest.poll_events(),
            revents: 0,
        };
        // SAFETY: one valid pollfd.
        let rc = unsafe { libc::poll(&mut pfd, 1, 30_000) };
        assert!(rc > 0, "poll: {}", std::io::Error::last_os_error());
        ready = Interest::from_revents(pfd.revents);
    }
    (done, both_armed)
}

/// Pushes and scans interleaved, all submitted before any is driven; every
/// slot completes, every scan sees every push submitted ahead of it.
fn concurrent_pushes_and_scans(target: &str) {
    let (mut blocking, tid, schema) = table(target);
    let (mut s, _lsn) = Session::connect(target).unwrap();
    // Small socket buffers so the outbound queue is drained across several
    // steps rather than in one writev.
    set_sockopt_int(s.as_raw_fd(), libc::SO_SNDBUF, 64 * 1024);
    let sent_before = s.requests_sent();
    let n = 40usize;
    let per = 5_000usize;
    let mut slots: Vec<(SlotId, bool)> = Vec::new();
    for i in 0..n {
        let batch = rows((i * per) as i64, per);
        let id = s.submit(push_req(tid, &schema, &batch)).unwrap();
        slots.push((id, true));
        let id = s.submit(Request::scan(tid)).unwrap();
        slots.push((id, false));
    }
    assert_eq!(s.requests_sent() - sent_before, 2 * n as u64, "counted on enqueue");
    let (mut done, both_armed) = drive_all(&mut s, slots.len());
    assert!(
        both_armed,
        "a pending write and a pending read arm both interests at once"
    );
    let mut pushed = 0usize;
    for (id, is_push) in slots {
        let r = done
            .remove(&id)
            .expect("every slot completes")
            .expect("no server error");
        if is_push {
            pushed += per;
            assert!(matches!(r, Reply::Lsn(_)), "a push completes as its ingest LSN");
        } else {
            let Reply::Scan(data) = r else { panic!("scan") };
            let got = data.batch.len();
            assert_eq!(got, pushed, "a scan sees every push submitted ahead of it");
        }
    }
    assert_eq!(s.interest(), Interest::NONE);
    // The blocking client and the driver agree on the table.
    assert_eq!(blocking.scan(tid).unwrap().batch.len(), n * per);
    assert_eq!(blocking.seek(tid, 7, &[]).unwrap().batch.len(), 1);
}

#[test]
fn concurrent_pushes_and_scans_unix() {
    let srv = ServerHandle::start_with_env(4, &[]);
    concurrent_pushes_and_scans(srv.sock_path());
}

#[test]
fn concurrent_pushes_and_scans_tls() {
    let srv = ServerHandle::start_tls(4);
    concurrent_pushes_and_scans(&srv.tls_target());
}

#[test]
fn cap_raises_and_every_slot_below_it_completes() {
    let srv = ServerHandle::start_with_env(4, &[]);
    let (_blocking, tid, _schema) = table(srv.sock_path());
    let (mut s, _) = Session::connect(srv.sock_path()).unwrap();
    let mut ids = Vec::new();
    for _ in 0..MAX_IN_FLIGHT {
        ids.push(s.submit(Request::scan(tid)).unwrap());
    }
    assert!(
        s.submit(Request::scan(tid)).is_err(),
        "the cap raises rather than hanging"
    );
    let (done, _) = drive_all(&mut s, MAX_IN_FLIGHT);
    for id in ids {
        assert!(done[&id].is_ok());
    }
    assert!(s.submit(Request::scan(tid)).is_ok(), "below the cap again");
    let _ = drive_all(&mut s, 1);
}

#[test]
fn abandoned_slot_does_not_desync_and_close_abandons_every_slot() {
    let srv = ServerHandle::start_with_env(4, &[]);
    let (_blocking, tid, schema) = table(srv.sock_path());
    let (mut s, _) = Session::connect(srv.sock_path()).unwrap();
    let batch = rows(0, 10);
    // Submit a push and never wait for it: the driver walks away.
    let abandoned = s.submit(push_req(tid, &schema, &batch)).unwrap();
    s.step(Interest::WRITE).unwrap();
    // The next request's reply arrives behind the abandoned one's; the head
    // accumulator consumes that train first, so this one decodes correctly.
    let scan = s.submit(Request::scan(tid)).unwrap();
    let (done, _) = drive_all(&mut s, 2);
    assert!(done[&abandoned].is_ok());
    let Reply::Scan(data) = done[&scan].as_ref().unwrap() else {
        panic!("scan")
    };
    assert_eq!(data.batch.len(), 10);

    // Now close with work pending.
    s.submit(Request::scan(tid)).unwrap();
    s.submit(Request::scan(tid)).unwrap();
    s.close();
    assert_eq!(s.interest(), Interest::NONE);
    assert!(matches!(
        s.submit(Request::scan(tid)),
        Err(gnitz_core::ClientError::Closed)
    ));
}

fn push_req<'a>(tid: u64, schema: &'a Schema, batch: &'a ZSetBatch) -> Request<'a> {
    Request::Push {
        target_id: tid,
        schema,
        batch,
        mode: WireConflictMode::Update,
    }
}

// ── Syscalls per operation ────────────────────────────────────────────────

/// The child half of `three_syscalls_per_push_*`: pushes `GNITZ_SYSCALL_N`
/// small batches over the blocking client against `GNITZ_SYSCALL_TARGET` and
/// exits. Run under `strace -f -c` by the parent; a no-op otherwise.
#[test]
fn syscall_count_child() {
    let (Ok(target), Ok(tid), Ok(n)) = (
        std::env::var("GNITZ_SYSCALL_TARGET"),
        std::env::var("GNITZ_SYSCALL_TID"),
        std::env::var("GNITZ_SYSCALL_N"),
    ) else {
        return;
    };
    let tid: u64 = tid.parse().unwrap();
    let n: usize = n.parse().unwrap();
    let mut client = GnitzClient::connect(&target).unwrap();
    let schema = Schema {
        columns: vec![
            ColumnDef::new("pk", TypeCode::I64, false),
            ColumnDef::new("a", TypeCode::I64, false),
        ],
        pk_cols: vec![0],
    };
    for i in 0..n {
        client.push(tid, &schema, &rows(i as i64, 1)).unwrap();
    }
}

/// Run the child under `strace -f -c` and count the socket syscalls it made:
/// `writev` (the request), `poll` (the park) and the one read that takes
/// header and payload together, per push, plus connect-time slack.
fn count_syscalls(target: &str, tid: u64) {
    let n = 200usize;
    let Some(counts) = strace_test(
        "syscall_count_child",
        &[
            ("GNITZ_SYSCALL_TARGET", target),
            ("GNITZ_SYSCALL_TID", &tid.to_string()),
            ("GNITZ_SYSCALL_N", &n.to_string()),
        ],
    ) else {
        eprintln!("strace not installed; skipping the syscall count");
        return;
    };
    let io = counts.sum(&[
        "writev", "write", "sendto", "sendmsg", "poll", "ppoll", "recvfrom", "read", "recvmsg",
    ]);
    // Connect, the handshake and the test harness's own I/O are the slack.
    let slack = 400;
    assert!(
        io <= 3 * n + slack,
        "expected ≤ {} socket syscalls for {n} pushes, strace counted {io}:\n{}",
        3 * n + slack,
        counts.report
    );
    assert!(
        counts.get("writev") >= n && counts.get("poll") >= n,
        "each push is one writev and one poll:\n{}",
        counts.report
    );
}

#[test]
fn three_syscalls_per_push_unix() {
    let srv = ServerHandle::start_with_env(1, &[]);
    let (_c, tid, _s) = table(srv.sock_path());
    count_syscalls(srv.sock_path(), tid);
}

#[test]
fn three_syscalls_per_push_tls() {
    let srv = ServerHandle::start_tls(1);
    let (_c, tid, _s) = table(&srv.tls_target());
    count_syscalls(&srv.tls_target(), tid);
}

#[test]
fn blocking_client_survives_server_restart_with_a_closed_verdict() {
    // A dead peer produces readability; the step that follows reads the EOF
    // and the blocking client reports the connection closed thereafter.
    let mut srv = ServerHandle::start_with_env(1, &[]);
    let (mut client, tid, _schema) = table(srv.sock_path());
    srv.restart();
    let t0 = std::time::Instant::now();
    assert!(client.scan(tid).is_err());
    assert!(t0.elapsed() < Duration::from_secs(5));
    assert!(matches!(client.scan(tid), Err(gnitz_core::ClientError::Closed)));
}
