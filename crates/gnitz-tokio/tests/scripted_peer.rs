//! `Connection` against a scripted peer on a Unix socket: no server, no
//! feature. The peer answers the HELLO and then does exactly what each test
//! scripts, which is what makes "the frame reached the wire" and "the peer
//! died" observable at the byte level.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::time::Duration;

use gnitz_tokio::{AsyncClient, Connection};
use tokio::runtime::Runtime;

/// A client connected to a peer that has answered its HELLO, and the peer's end.
fn connect_to_peer(rt: &Runtime) -> (AsyncClient, Connection, UnixStream) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer.sock");
    let listener = UnixListener::bind(&path).expect("bind");
    // `connect` blocks until the HELLO is answered.
    let peer = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().expect("accept");
        read_frame(&mut s);
        s.write_all(&gnitz_wire::encode_hello_ack(u32::MAX, 0))
            .expect("hello ack");
        s
    });
    let (client, conn) = rt
        .block_on(gnitz_tokio::connect(path.to_str().expect("utf-8 path")))
        .expect("connect");
    (client, conn, peer.join().unwrap())
}

/// One length-prefixed frame the client wrote.
fn read_frame(s: &mut UnixStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).expect("frame prefix");
    let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut payload).expect("frame payload");
    payload
}

/// A submit issued while a reply is outstanding leaves before that reply
/// arrives, rather than waiting for the next read to re-arm write readiness.
#[test]
fn a_submit_flushes_while_a_reply_is_outstanding() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    // A stall shows up as a failed read rather than a hung test.
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    rt.spawn(conn);
    let first = rt.spawn({
        let c = client.clone();
        async move { c.scan(1).await }
    });
    assert!(!read_frame(&mut peer).is_empty(), "the first request is on the wire");

    // Nothing has been answered. The second submit must still leave.
    let second = rt.spawn({
        let c = client.clone();
        async move { c.scan(2).await }
    });
    assert!(
        !read_frame(&mut peer).is_empty(),
        "a submit behind an unanswered reply must flush anyway"
    );

    // Tear down: the peer's close fails both.
    drop(peer);
    assert!(rt.block_on(first).unwrap().is_err());
    assert!(rt.block_on(second).unwrap().is_err());
    drop(client);
}

/// A dead peer fails every outstanding operation rather than hanging one.
#[test]
fn a_connection_error_fails_every_outstanding_operation() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    peer.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

    let driver = rt.spawn(conn);
    let ops: Vec<_> = (1..=8u64)
        .map(|tid| {
            let c = client.clone();
            rt.spawn(async move { c.scan(tid).await })
        })
        .collect();
    // At least one frame is on the wire before the peer goes.
    read_frame(&mut peer);
    drop(peer);

    for op in ops {
        assert!(
            rt.block_on(op).unwrap().is_err(),
            "every operation resolves, none is left awaiting"
        );
    }
    assert!(rt.block_on(driver).unwrap().is_err(), "the driver reports the cause");
    // Further work is refused rather than queued onto a dead connection.
    assert!(rt.block_on(client.scan(1)).is_err());
}

/// Dropping every handle closes the channel; the driver quiesces and completes.
#[test]
fn dropping_every_handle_ends_the_connection() {
    let rt = Runtime::new().unwrap();
    let (client, conn, _peer) = connect_to_peer(&rt);

    let driver = rt.spawn(conn);
    drop(client);
    let done = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), driver).await });
    assert!(
        done.expect("the driver completes once no handle is left")
            .unwrap()
            .is_ok(),
        "a quiesced connection with no handle left completes cleanly"
    );
}

/// `abort` must resolve a waiter whose request never reached the wire. The
/// `Connection` is kept alive past its own completion — what a `select!` on
/// `&mut conn` leaves — so nothing drops the channel receiver on its behalf.
#[test]
fn a_verb_submitted_after_the_connection_died_still_resolves() {
    let rt = Runtime::new().unwrap();
    let (client, mut conn, peer) = connect_to_peer(&rt);
    // The driver's first step reads EOF and aborts.
    drop(peer);

    let outstanding = rt.spawn({
        let c = client.clone();
        async move { c.scan(1).await }
    });
    let driven = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), &mut conn).await });
    assert!(
        driven.expect("the driver reaches its failure").is_err(),
        "the driver reports the peer's death"
    );
    assert!(
        rt.block_on(outstanding).unwrap().is_err(),
        "the outstanding verb resolves"
    );

    let late = rt.block_on(async { tokio::time::timeout(Duration::from_secs(5), client.scan(2)).await });
    match late {
        Ok(r) => assert!(r.is_err(), "a verb submitted onto a dead connection must fail"),
        Err(_) => panic!("a verb submitted after the driver aborted must resolve, not hang"),
    }
}
