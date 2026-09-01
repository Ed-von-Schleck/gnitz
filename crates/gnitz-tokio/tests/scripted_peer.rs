//! `Connection` against a scripted peer on a Unix socket: no server, no
//! feature. The peer answers the HELLO and then does exactly what each test
//! scripts, which is what makes "the frame reached the wire" and "the peer
//! died" observable at the byte level.

use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use tokio::runtime::Runtime;

/// A socket path nothing else in this process will claim.
fn unique_path() -> PathBuf {
    static SEQ: AtomicU32 = AtomicU32::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("gnitz-tokio-{}-{n}.sock", std::process::id()))
}

/// Removes its socket path when the test ends.
struct SockPath(PathBuf);

impl Drop for SockPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Bind, then answer the client's HELLO on a thread of its own — `connect` is
/// blocking, so somebody has to be servicing it — and hand the stream back.
fn scripted_peer() -> (SockPath, std::thread::JoinHandle<UnixStream>) {
    let path = SockPath(unique_path());
    let listener = UnixListener::bind(&path.0).expect("bind");
    let handle = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().expect("accept");
        // The HELLO: a 4-byte LE length prefix and its payload.
        let mut len = [0u8; 4];
        s.read_exact(&mut len).expect("hello prefix");
        let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
        s.read_exact(&mut payload).expect("hello payload");
        // `encode_hello_ack` frames the ACK itself. `u32::MAX` for the payload
        // ceiling: the client clamps it to its own hard maximum.
        s.write_all(&gnitz_wire::encode_hello_ack(u32::MAX, 0))
            .expect("hello ack");
        s
    });
    (path, handle)
}

/// One length-prefixed request frame the client wrote.
fn read_frame(s: &mut UnixStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    s.read_exact(&mut len).expect("frame prefix");
    let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
    s.read_exact(&mut payload).expect("frame payload");
    payload
}

/// What `give_back`'s write rule exists for: a submit issued while a reply is
/// outstanding must flush before that reply arrives. Giving back write
/// readiness the fd never refused leaves it to be re-set only by the next read,
/// so the second frame would wait for the first reply — pipelining lost rather
/// than a hang, and invisible to a test that submits before the first flush.
#[test]
fn a_submit_flushes_while_a_reply_is_outstanding() {
    let (path, accepted) = scripted_peer();
    let rt = Runtime::new().unwrap();
    let (client, conn) = rt
        .block_on(gnitz_tokio::connect(path.0.to_str().unwrap()))
        .expect("connect");
    let mut peer = accepted.join().unwrap();
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
    let (path, accepted) = scripted_peer();
    let rt = Runtime::new().unwrap();
    let (client, conn) = rt
        .block_on(gnitz_tokio::connect(path.0.to_str().unwrap()))
        .expect("connect");
    let mut peer = accepted.join().unwrap();
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
    let (path, accepted) = scripted_peer();
    let rt = Runtime::new().unwrap();
    let (client, conn) = rt
        .block_on(gnitz_tokio::connect(path.0.to_str().unwrap()))
        .expect("connect");
    let _peer = accepted.join().unwrap();

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

/// The four mirror accessors are total: a handle that never attached answers as
/// a store-less `GnitzClient` does rather than refusing.
#[test]
fn an_unattached_handle_answers_every_mirror_accessor() {
    let (path, accepted) = scripted_peer();
    let rt = Runtime::new().unwrap();
    let (client, _conn) = rt
        .block_on(gnitz_tokio::connect(path.0.to_str().unwrap()))
        .expect("connect");
    let _peer = accepted.join().unwrap();

    rt.block_on(async {
        assert!(
            !client.mirrors(7).await.unwrap(),
            "a handle with no store mirrors nothing"
        );
        assert!(
            client.cursor_of(7).await.unwrap().is_none(),
            "no copy means no round to report"
        );
        assert!(
            client.mirrored_ids().await.unwrap().is_empty(),
            "no copy means no registrations"
        );
        assert!(
            client.mirror_poisoned().await.unwrap().is_none(),
            "no copy means nothing poisoned"
        );
    });
}

/// `abort` must resolve a waiter whose request never reached the wire. The
/// `Connection` is kept alive past its own completion — what a `select!` on
/// `&mut conn` leaves — so nothing drops the channel receiver on its behalf.
#[test]
fn a_verb_submitted_after_the_connection_died_still_resolves() {
    let (path, accepted) = scripted_peer();
    let rt = Runtime::new().unwrap();
    let (client, mut conn) = rt
        .block_on(gnitz_tokio::connect(path.0.to_str().unwrap()))
        .expect("connect");
    // The peer's half closes, so the driver's first step reads EOF and aborts.
    drop(accepted.join().unwrap());

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
