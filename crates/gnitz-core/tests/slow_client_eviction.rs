#![cfg(feature = "integration")]

//! The master's per-send egress deadline: a client that issues a SCAN and
//! then stops draining its socket is `shutdown()` and evicted once a send
//! makes no progress for `GNITZ_CLIENT_SEND_TIMEOUT_MS`, freeing the W2M ring
//! slot it pinned; every other client keeps progressing throughout.

use std::os::fd::RawFd;

use gnitz_core::protocol::{encode_message_parts, hello_handshake, ClientTransport};
use gnitz_core::{BatchAppender, ColumnDef, GnitzClient, Schema, TableProps, TypeCode, ZSetBatch};
use gnitz_test_harness::{unique_schema, ServerHandle};

/// Shrink a socket's receive buffer so the first scan frame's server-side send
/// stalls against a non-draining peer.
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

/// Wait up to `deadline_ms` for the server to shut down its end of `fd`,
/// detected via POLLHUP/POLLRDHUP without reading — reading would drain the
/// receive buffer and let the stalled send resume. `events` is POLLRDHUP only;
/// POLLHUP/POLLERR are output-only and always reported, so buffered-but-unread
/// data does not wake the poll.
fn peer_hung_up_within(fd: RawFd, deadline_ms: i32) -> bool {
    let mut pfd = libc::pollfd { fd, events: libc::POLLRDHUP, revents: 0 };
    let rc = unsafe { libc::poll(&mut pfd, 1, deadline_ms) };
    rc > 0 && (pfd.revents & (libc::POLLHUP | libc::POLLRDHUP)) != 0
}

#[test]
fn slow_scan_client_is_evicted_after_deadline() {
    let srv = ServerHandle::start_with_env(4, &[("GNITZ_CLIENT_SEND_TIMEOUT_MS", "1500")]);

    // A table whose per-worker scan frame exceeds the server's socket send
    // buffer, so the first frame to a non-draining peer stalls.
    let mut client = GnitzClient::connect(srv.sock_path()).unwrap();
    let sn = unique_schema("evict");
    client.create_schema(&sn).unwrap();
    let cols = vec![
        ColumnDef::new("pk", TypeCode::I64, false),
        ColumnDef::new("a", TypeCode::I64, false),
        ColumnDef::new("b", TypeCode::I64, false),
    ];
    let table_id = client
        .create_table(&sn, "t", &cols, &[0], TableProps::default(), &[])
        .unwrap();
    let schema = Schema { columns: cols, pk_cols: vec![0] };
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch, &schema);
    for pk in 0..40_000u128 {
        app.add_row(pk, 1).i64_val(0).i64_val(0);
    }
    client.push(table_id, &schema, &batch).unwrap();

    // A raw connection that issues the scan and never reads.
    let mut slow = ClientTransport::connect(srv.sock_path()).expect("connect");
    set_tiny_rcvbuf(slow.as_raw_fd());
    hello_handshake(&mut slow, None).expect("hello");
    let scan = encode_message_parts(table_id, 0xB0BA, 0, 0, &[], 0, None);
    slow.send_parts(scan, None).expect("send scan");

    let evicted = peer_hung_up_within(slow.as_raw_fd(), 8000);
    drop(slow);
    assert!(
        evicted,
        "the stalled scan client was not evicted within the deadline window"
    );

    // Everyone else kept going: a push and a scan on the same table succeed.
    let mut more = ZSetBatch::new(&schema);
    BatchAppender::new(&mut more, &schema)
        .add_row(40_000, 1)
        .i64_val(0)
        .i64_val(0);
    client.push(table_id, &schema, &more).unwrap();
    let batch = client.scan(table_id).expect("scan after the eviction").batch;
    assert_eq!(batch.len(), 40_001);
}
