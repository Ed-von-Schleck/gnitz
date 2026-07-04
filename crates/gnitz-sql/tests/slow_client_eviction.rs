#![cfg(feature = "integration")]

//! End-to-end guard for the slow-client W2M egress deadline.
//!
//! A client that issues a SCAN and then stops draining its socket pins the
//! zero-copy W2M ring slot the master is forwarding out of a worker's ring.
//! Without a deadline the master's `send_slot` parks forever, the client is
//! never evicted, and — once enough stalled frames fill the worker's ring — the
//! single-threaded worker futex-blocks in `send_encoded` and freezes SAL
//! progress cluster-wide. The deadline built into the master's ring-slot sends
//! bounds that: a scan frame that makes no progress within
//! `GNITZ_CLIENT_SEND_TIMEOUT_MS` gets the client `shutdown()` + evicted,
//! freeing the ring.
//!
//! These run against a real multi-worker `gnitz-server`. The 1 GiB W2M ring is
//! not test-configurable, so filling it to reproduce the worker futex-block
//! itself is impractical; instead we assert the directly observable,
//! deterministic fix: a stalled scan client IS evicted after the deadline
//! (pre-fix the un-timed send parks forever, so it never is), and a healthy
//! client draining a multi-frame train at a normal pace under the same short
//! deadline is never penalised.

use std::os::fd::RawFd;

use gnitz_core::{connect, hello_handshake, send_message, ColData, GnitzClient, PkColumn, PkTuple, Schema, ZSetBatch};
use gnitz_test_harness::ServerHandle;

mod common;
use common::{exec, make_planner};

/// Short per-frame egress deadline (ms) so the eviction fires quickly. Still
/// enormously generous for a single frame to a *draining* client, so the
/// healthy-scan test never trips it.
const TIMEOUT_MS: &str = "1500";

/// Bulk-`push` `count` rows `(start + i, 0, 0)` into a `(pk, a, b)` BIGINT table
/// via the fast binary path. Payload values are inert — only the row count (and
/// hence the scan frame size) matters here.
fn push_block(client: &mut GnitzClient, table_id: u64, schema: &Schema, start: u64, count: usize) {
    let pks: Vec<u64> = (start..start + count as u64).collect();
    let columns: Vec<ColData> = schema
        .columns
        .iter()
        .enumerate()
        .map(|(ci, col)| {
            if schema.is_pk_col(ci) {
                ColData::Fixed(vec![])
            } else {
                ColData::Fixed(vec![0u8; count * col.type_code.wire_stride()])
            }
        })
        .collect();
    let batch = ZSetBatch {
        pks: PkColumn::U64s(pks),
        weights: vec![1i64; count],
        nulls: vec![0u64; count],
        columns,
    };
    client.push(table_id, schema, &batch).unwrap();
}

/// Shrink the slow client's receive buffer — belt-and-braces with the large
/// per-worker frame so the first scan frame's server-side `OP_SEND` stalls.
fn set_tiny_rcvbuf(fd: RawFd) {
    let bufsz: libc::c_int = 4096;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &bufsz as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

/// Wait up to `deadline_ms` for the server to close/shut-down its end of `fd`,
/// detected via POLLHUP/POLLRDHUP WITHOUT reading — reading would drain the
/// receive buffer and let the stalled server send resume, undoing the stall we
/// are probing. `events` is POLLRDHUP only; POLLHUP/POLLERR are output-only and
/// always reported, so buffered-but-unread data (POLLIN) does not wake the poll.
fn peer_hung_up_within(fd: RawFd, deadline_ms: i32) -> bool {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLRDHUP,
        revents: 0,
    };
    let rc = unsafe { libc::poll(&mut pfd, 1, deadline_ms) };
    rc > 0 && (pfd.revents & (libc::POLLHUP | libc::POLLRDHUP)) != 0
}

#[test]
fn slow_scan_client_is_evicted_after_deadline() {
    let srv = match ServerHandle::start_with_env(4, &[("GNITZ_CLIENT_SEND_TIMEOUT_MS", TIMEOUT_MS)]) {
        Some(s) => s,
        None => return,
    };

    // Client B: create + populate a table large enough that each worker's single
    // scan frame is multiple MB — far past the server's socket send buffer, so
    // the first frame's send is guaranteed to stall against a non-draining peer.
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    let (table_id, schema) = client.resolve_table_id(&sn, "t").unwrap();
    // 200k rows / 4 workers ≈ 2 MB per worker frame.
    for block in 0..8 {
        push_block(&mut client, table_id, &schema, block * 25_000, 25_000);
    }

    // Slow client A: a raw connection that issues the scan then never reads.
    let fd = connect(&srv.sock_path).expect("connect");
    set_tiny_rcvbuf(fd);
    hello_handshake(fd).expect("hello");
    // The scan request is just an empty control frame addressed to the table id.
    send_message(
        fd,
        table_id,
        /*client_id*/ 0xB0BA,
        /*flags*/ 0,
        &PkTuple::EMPTY,
        0,
        None,
        None,
    )
    .expect("send scan");

    // The server must evict A once the per-frame deadline elapses; allow a few
    // deadlines of slack. Pre-fix the master's send parks forever, A is never
    // evicted, and this poll times out with no HUP → the assert fails.
    let evicted = peer_hung_up_within(fd, 8000);
    unsafe { libc::close(fd) };
    assert!(
        evicted,
        "server did not evict the stalled scan client within the deadline window"
    );

    // The cluster stays healthy for other clients: B can still run DDL + DML +
    // scan after the eviction.
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
    );
    exec(&mut client, &sn, "INSERT INTO t2 VALUES (1, 10), (2, 20), (3, 30)");
    let (t2_id, _) = client.resolve_table_id(&sn, "t2").unwrap();
    let (_, batch, _) = client.scan(t2_id).expect("client B scan after eviction");
    assert_eq!(
        batch.map(|b| b.len()).unwrap_or(0),
        3,
        "cluster must keep serving other clients after the eviction"
    );
}

#[test]
fn healthy_scan_is_not_evicted_under_short_deadline() {
    // Same short deadline, plus a tiny reply-frame budget so a modest table
    // already streams a multi-frame train per worker: proves the PER-FRAME
    // deadline never fires for a client that keeps draining, however many frames
    // the train spans.
    let srv = match ServerHandle::start_with_env(
        4,
        &[
            ("GNITZ_CLIENT_SEND_TIMEOUT_MS", TIMEOUT_MS),
            ("GNITZ_REPLY_FRAME_BUDGET", "16384"),
        ],
    ) {
        Some(s) => s,
        None => return,
    };
    let (mut client, sn) = make_planner(&srv);
    exec(
        &mut client,
        &sn,
        "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
    );
    let (table_id, schema) = client.resolve_table_id(&sn, "t").unwrap();
    // Keep each worker's train under the 64-frame in-flight bound (64 × 16 KiB =
    // 1 MiB): 8000 rows across 4 workers ≈ 100 KB/worker ≈ a handful of frames.
    push_block(&mut client, table_id, &schema, 0, 8000);

    // The high-level client drains the whole multi-frame train at a normal pace.
    // Every row must return and no frame may trip the deadline.
    let (_, batch, _) = client.scan(table_id).expect("healthy multi-frame scan");
    let n = batch.map(|b| b.len()).unwrap_or(0);
    assert_eq!(n, 8000, "healthy client must read every row of the multi-frame scan");
}
