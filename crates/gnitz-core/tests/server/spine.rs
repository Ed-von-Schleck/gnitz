//! A second driver over the connection spine: many operations in flight on
//! one connection against a real server, on both transports. Carries no
//! futures, no waker and no executor — submit N, then `step` / drain / park
//! until every slot is done. It lives out of crate on purpose: it proves the
//! spine's public surface is complete for a driver with nothing private in
//! reach, and it exercises what the one-slot blocking client cannot — a deep
//! outbound queue, the head accumulator handing off across slots, a
//! driver-side slot map, both interests armed at once, and the cap.

use super::*;
use gnitz_core::MAX_IN_FLIGHT;
use gnitz_foundation::posix_io::set_sockopt_int;
use gnitz_test_harness::strace_test;

/// The `(pk, a)` table every test here drives.
fn pk_a() -> Schema {
    schema_of(&[("pk", TypeCode::I64), ("a", TypeCode::I64)])
}

/// A fresh [`pk_a`] table reachable through `target`, and the blocking client
/// that made it.
fn table(target: &str) -> (GnitzClient, u64, Arc<Schema>) {
    let mut client = GnitzClient::connect(target).expect("connect");
    let (_, tid, schema) = create_table(&mut client, pk_a());
    (client, tid, schema)
}

/// Pushes and scans interleaved, all submitted before any is driven; every
/// slot completes, every scan sees every push submitted ahead of it.
fn concurrent_pushes_and_scans(target: &str) {
    // Shown only if the test fails.
    eprintln!("pipelining over {target}");
    let (mut blocking, tid, schema) = table(target);
    let mut s = Session::connect(target).unwrap();
    // Small socket buffers so the outbound queue is drained across several
    // steps rather than in one writev.
    set_sockopt_int(s.as_raw_fd(), libc::SOL_SOCKET, libc::SO_SNDBUF, 64 * 1024).unwrap();
    let all = ReadSpec::all_rows(ReadBound::None);
    let sent_before = s.requests_sent();
    let (n, per) = (40u64, 5_000u64);
    let mut slots: Vec<(SlotId, bool)> = Vec::new();
    for i in 0..n {
        let batch = rows(&schema, i * per..(i + 1) * per);
        slots.push((s.submit(push_req(tid, &schema, &batch)).unwrap(), true));
        slots.push((s.submit(scan_req(tid, &all, &schema)).unwrap(), false));
    }
    assert_eq!(s.requests_sent() - sent_before, 2 * n, "counted on enqueue");
    let (mut done, both_armed) = drive_all(&mut s, slots.len());
    assert!(
        both_armed,
        "a pending write and a pending read arm both interests at once"
    );
    let mut pushed = 0;
    for (id, is_push) in slots {
        let r = done
            .remove(&id)
            .expect("every slot completes")
            .expect("no server error");
        if is_push {
            pushed += per;
            assert!(matches!(r, Reply::Ack(_)), "a push completes as its ingest LSN");
        } else {
            let Reply::Scan(data) = r else { panic!("scan") };
            assert_eq!(
                data.batch.len() as u64,
                pushed,
                "a scan sees every push submitted ahead of it"
            );
        }
    }
    assert_eq!(s.interest(), Interest::NONE);
    // The blocking client and the driver agree on the table.
    assert_eq!(
        weighted_rows(&scan_all(&mut blocking, tid, &schema)),
        weighted_rows(&rows(&schema, 0..n * per))
    );
}

#[test]
fn concurrent_pushes_and_scans_on_each_transport() {
    let srv = ServerHandle::start_tls(4);
    concurrent_pushes_and_scans(srv.sock_path());
    concurrent_pushes_and_scans(&srv.tls_target());
}

#[test]
fn every_slot_up_to_the_cap_completes() {
    let srv = ServerHandle::start_n(4);
    let (_blocking, tid, schema) = table(srv.sock_path());
    let all = ReadSpec::all_rows(ReadBound::None);
    let mut s = Session::connect(srv.sock_path()).unwrap();
    let ids: Vec<SlotId> = (0..MAX_IN_FLIGHT)
        .map(|_| s.submit(scan_req(tid, &all, &schema)).unwrap())
        .collect();
    assert!(s.submit(scan_req(tid, &all, &schema)).is_err(), "at the cap");
    let (done, _) = drive_all(&mut s, MAX_IN_FLIGHT);
    for id in ids {
        assert!(done[&id].is_ok());
    }
    assert!(s.submit(scan_req(tid, &all, &schema)).is_ok(), "below the cap again");
    let _ = drive_all(&mut s, 1);
}

// ── Syscalls per operation ────────────────────────────────────────────────

/// The child half of [`three_syscalls_per_push_on_each_transport`]: pushes `GNITZ_SYSCALL_N`
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
    let n: u64 = n.parse().unwrap();
    let mut client = GnitzClient::connect(&target).unwrap();
    let schema = Arc::new(pk_a());
    for i in 0..n {
        client
            .push(tid, &schema, rows(&schema, [i]), WireConflictMode::Update)
            .unwrap();
    }
}

/// Run the child under `strace -f -c` and count the socket syscalls it made:
/// `writev` (the request), `poll` (the park) and the one read that takes
/// header and payload together, per push, plus connect-time slack.
fn count_syscalls(target: &str) {
    let (_client, tid, _schema) = table(target);
    let n = 1000usize;
    let Some(counts) = strace_test(
        "spine::syscall_count_child",
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
    // Connect, the handshake and the test harness's own I/O are the slack,
    // well under one extra syscall per push.
    let slack = 400;
    assert!(
        io <= 3 * n + slack,
        "expected ≤ {} socket syscalls for {n} pushes to {target}, strace counted {io}:\n{}",
        3 * n + slack,
        counts.report
    );
    assert!(
        counts.get("writev") >= n && counts.get("poll") >= n,
        "each push to {target} is one writev and one poll:\n{}",
        counts.report
    );
}

#[test]
fn three_syscalls_per_push_on_each_transport() {
    let srv = ServerHandle::start_tls(1);
    count_syscalls(srv.sock_path());
    count_syscalls(&srv.tls_target());
}
