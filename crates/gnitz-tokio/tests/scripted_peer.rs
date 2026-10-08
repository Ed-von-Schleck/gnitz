//! `Connection` against a scripted peer on a Unix socket: no server, no
//! feature. The peer answers the HELLO and then does exactly what each test
//! scripts — which requests it has read, which it answers, and when it dies —
//! so every state the driver can wait in is one a test can hold it in.

mod support;

use std::future::Future;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::{UnixListener, UnixStream};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};

use gnitz_core::{
    BatchAppender, ClientError, GnitzClient, ScanReply, Schema, ZSetBatch, MAX_IN_FLIGHT, MAX_QUEUED_BYTES,
};
use gnitz_tokio::{AsyncClient, Connection};
use gnitz_wire::control::{append_frame, peek_control_block, ControlHeader};
use gnitz_wire::{
    frame_len_prefix, ColumnDef, ReadBound, ReadSpec, TypeCode, WireConflictMode, FRAME_LEN_PREFIX_BYTES,
};
use support::{settled, PATIENCE};
use tokio::runtime::Runtime;

/// A key column and nothing else; the peer never checks a layout.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U64, false)], vec![0]).unwrap())
}

/// `rows` keys at weight 1. A row is its key, its weight and its null word.
fn batch(schema: &Schema, rows: usize) -> ZSetBatch {
    let mut batch = ZSetBatch::new(schema);
    let mut app = BatchAppender::new(&mut batch);
    for pk in 0..rows {
        app.add_row(pk as u128, 1);
    }
    batch
}
const ROW_BYTES: usize = 24;
/// Rows whose push no socket buffer holds.
const BIG: usize = 200_000;

/// A read of every row of `tid`, submitted when called.
fn scan(c: &AsyncClient, tid: u64) -> impl Future<Output = Result<ScanReply, ClientError>> {
    c.send(move |c| c.scan_spec(tid, &ReadSpec::all_rows(ReadBound::None), &schema()))
}

/// A push of `batch` to `tid`, submitted when called.
fn push(c: &AsyncClient, tid: u64, batch: ZSetBatch) -> impl Future<Output = Result<u64, ClientError>> {
    c.send(move |c| c.push(tid, &schema(), batch, WireConflictMode::Update))
}

/// A listener on a socket of its own, and the path to it.
fn listen() -> (UnixListener, String, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer.sock");
    let listener = UnixListener::bind(&path).expect("bind");
    (listener, path.to_str().expect("utf-8 path").to_owned(), dir)
}

/// The next connection to `listener`, its HELLO answered.
fn accept(listener: &UnixListener) -> UnixStream {
    let (mut s, _) = listener.accept().expect("accept");
    s.set_read_timeout(Some(PATIENCE)).unwrap();
    read_frame(&mut s);
    write_frame(&mut s, &gnitz_wire::HELLO);
    s
}

/// A client connected to `listener`'s socket at `path`, and the peer's end.
fn connect_to(rt: &Runtime, listener: UnixListener, path: &str) -> (GnitzClient, UnixStream) {
    // `connect` blocks until the HELLO is answered.
    let peer = std::thread::spawn(move || accept(&listener));
    let client = rt.block_on(gnitz_tokio::connect(path)).expect("connect");
    (client, peer.join().unwrap())
}

/// A shared client connected to a scripted peer, its driver, and the peer's end.
fn connect_to_peer(rt: &Runtime) -> (AsyncClient, Connection, UnixStream) {
    let (listener, path, _dir) = listen();
    let (client, peer) = connect_to(rt, listener, &path);
    let (client, conn) = gnitz_tokio::share(client);
    (client, conn, peer)
}

/// The length the next frame's prefix states.
fn read_len(s: &mut UnixStream) -> usize {
    let mut len = [0u8; FRAME_LEN_PREFIX_BYTES];
    s.read_exact(&mut len).expect("frame prefix");
    u32::from_le_bytes(len) as usize
}

fn read_frame(s: &mut UnixStream) -> Vec<u8> {
    let mut payload = vec![0u8; read_len(s)];
    s.read_exact(&mut payload).expect("frame payload");
    payload
}

fn write_frame(s: &mut UnixStream, payload: &[u8]) {
    s.write_all(&frame_len_prefix(payload.len())).expect("frame prefix");
    s.write_all(payload).expect("frame payload");
}

/// How many of the next `len` bytes have arrived, consuming none. Reading a
/// request out frees socket buffer, which signals writability to the driver
/// afresh — the very readiness a test may need it to have kept on its own.
fn unread(s: &UnixStream, len: usize) -> usize {
    let mut buf = vec![0u8; len];
    // SAFETY: an open socket, and a buffer of `len` bytes.
    let n = unsafe {
        libc::recv(
            s.as_raw_fd(),
            buf.as_mut_ptr().cast(),
            len,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    n.max(0) as usize
}

fn await_unread(s: &UnixStream, len: usize) {
    let deadline = Instant::now() + PATIENCE;
    while unread(s, len) < len {
        assert!(Instant::now() < deadline, "{len} bytes never arrived");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// The relation the next request on the wire names.
fn request(s: &mut UnixStream) -> u64 {
    peek_control_block(&read_frame(s))
        .expect("a control header")
        .hdr
        .target_id
}

/// Answer the oldest unanswered request, `tid`'s, with `lsn` and no rows.
fn reply(s: &mut UnixStream, tid: u64, lsn: u64) {
    let hdr = ControlHeader {
        target_id: tid,
        arg0: lsn,
        ..Default::default()
    };
    let mut frame = Vec::new();
    append_frame(&mut frame, &hdr, &[], None, None);
    write_frame(s, &frame);
}

/// A submit issued while a reply is outstanding leaves before that reply
/// arrives, rather than waiting for a write readiness the last flush cleared;
/// and each reply resolves the verb that asked.
#[test]
fn a_submit_flushes_while_a_reply_is_outstanding() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    rt.spawn(conn);

    let first = scan(&client, 1);
    let len = read_len(&mut peer);
    // Nothing has been answered, and the first request still sits in the
    // socket. The second must leave anyway.
    let second = scan(&client, 2);
    await_unread(&peer, len + 1);
    peer.read_exact(&mut vec![0u8; len]).expect("the first request");
    assert_eq!(request(&mut peer), 2);

    reply(&mut peer, 1, 11);
    reply(&mut peer, 2, 22);
    assert_eq!(settled(&rt, first).unwrap().lsn, Some(11));
    assert_eq!(settled(&rt, second).unwrap().lsn, Some(22));
}

/// Past either cap a verb waits for room; the client's own refusal at the cap
/// never reaches one.
#[test]
fn past_a_cap_a_verb_waits() {
    // Every push is submitted before the peer reads a byte: one-row pushes past
    // the in-flight cap, and big ones past the queued-bytes cap by half again.
    let past_queued_bytes = MAX_QUEUED_BYTES * 3 / 2 / (BIG * ROW_BYTES);
    for (rows, pushes) in [(1, MAX_IN_FLIGHT + 100), (BIG, past_queued_bytes)] {
        let rt = Runtime::new().unwrap();
        let (client, conn, mut peer) = connect_to_peer(&rt);
        rt.spawn(conn);

        let batch = batch(&schema(), rows);
        let pushes = pushes as u64;
        let verbs: Vec<_> = (1..=pushes).map(|tid| push(&client, tid, batch.clone())).collect();
        // As many as may be in flight arrive and go unanswered, which is what
        // holds the rest back; they follow as the answers free slots.
        let held = pushes.min(MAX_IN_FLIGHT as u64);
        for tid in 1..=held {
            assert_eq!(request(&mut peer), tid);
        }
        for tid in 1..=pushes {
            if tid > held {
                assert_eq!(request(&mut peer), tid);
            }
            reply(&mut peer, tid, tid);
        }
        for (tid, verb) in (1..).zip(verbs) {
            assert_eq!(settled(&rt, verb).unwrap(), tid);
        }
    }
}

/// A request the client refuses fails that verb alone: it reaches no wire, and
/// the verb behind it still gets its own reply.
#[test]
fn a_refused_verb_reaches_no_wire() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    rt.spawn(conn);

    let mut keyless = ZSetBatch::new(&schema());
    keyless.weights.push(1);
    let refused = settled(&rt, push(&client, 1, keyless));
    assert!(matches!(refused, Err(ClientError::Refused(_))), "{refused:?}");

    let next = scan(&client, 2);
    assert_eq!(request(&mut peer), 2, "the refused push was never written");
    reply(&mut peer, 2, 22);
    assert_eq!(settled(&rt, next).unwrap().lsn, Some(22));
}

/// A lost connection fails every verb — one holding a slot, one the loss is
/// discovered on, one submitted after it — and the driver still ends once its
/// handles are gone.
#[test]
fn a_lost_connection_fails_every_verb_and_the_driver_still_ends() {
    // The peer dies with replies owed, or before the first request.
    for outstanding in [8u64, 0] {
        let rt = Runtime::new().unwrap();
        let (client, conn, mut peer) = connect_to_peer(&rt);
        let driver = rt.spawn(conn);

        let verbs: Vec<_> = (1..=outstanding).map(|tid| scan(&client, tid)).collect();
        // Every one of them is on the wire, so each holds a slot when the peer goes.
        for _ in &verbs {
            request(&mut peer);
        }
        drop(peer);

        let mut results: Vec<_> = verbs.into_iter().map(|v| settled(&rt, v)).collect();
        results.push(settled(&rt, scan(&client, 9)));
        results.push(settled(&rt, scan(&client, 10)));
        for r in results {
            assert!(matches!(r, Err(ClientError::ConnectionLost(_))), "{r:?}");
        }
        drop(client);
        settled(&rt, driver).unwrap();
    }
}

/// Dropping a verb cancels nothing, and a driver whose handles are all gone
/// still writes what it was handed and waits for the reply.
#[test]
fn a_dropped_verb_is_still_written() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    drop(scan(&client, 1));
    drop(client);

    let driver = rt.spawn(conn);
    assert_eq!(request(&mut peer), 1);
    // Its result has nobody to go to, which is not an error.
    reply(&mut peer, 1, 11);
    settled(&rt, driver).unwrap();
    // A clean end of stream: the driver took the reply before it closed, where
    // closing over an unread one resets the connection.
    assert_eq!(peer.read(&mut [0]).map_err(|e| e.kind()), Ok(0));
}

/// A driver dropped before its handles fails their verbs rather than leaving
/// them pending.
#[test]
fn a_dropped_driver_closes_every_verb() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    let driver = rt.spawn(conn);

    let outstanding = scan(&client, 1);
    request(&mut peer);
    driver.abort();

    for r in [settled(&rt, outstanding), settled(&rt, scan(&client, 2))] {
        assert!(matches!(r, Err(ClientError::Closed)), "{r:?}");
    }
}

/// A driver with nothing to do is not polled. Each state it can wait in holds a
/// readiness it must have cleared, or it spins: awaiting a reply, owing
/// nothing, and holding bytes the socket refused.
#[test]
fn a_waiting_driver_is_not_polled() {
    let rt = Runtime::new().unwrap();
    let (client, mut conn, mut peer) = connect_to_peer(&rt);
    // Odd while a poll runs, so a spin inside one poll never reads as waiting.
    let polls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&polls);
    rt.spawn(std::future::poll_fn(move |cx| {
        counter.fetch_add(1, Relaxed);
        let polled = Pin::new(&mut conn).poll(cx);
        counter.fetch_add(1, Relaxed);
        polled
    }));
    let assert_waiting = |state: &str| {
        settled(&rt, async {
            let seen = loop {
                match polls.load(Relaxed) {
                    n if n % 2 == 0 => break n,
                    _ => tokio::task::yield_now().await,
                }
            };
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(polls.load(Relaxed), seen, "the driver was polled with {state}");
        })
    };

    let first = scan(&client, 1);
    assert_eq!(request(&mut peer), 1);
    assert_waiting("a reply outstanding");

    reply(&mut peer, 1, 11);
    settled(&rt, first).unwrap();
    assert_waiting("nothing in flight");

    // To a peer that reads no further than the length prefix: the socket takes
    // what it buffers and refuses the rest.
    let stuck = push(&client, 3, batch(&schema(), BIG));
    let len = read_len(&mut peer);
    assert_waiting("bytes the socket refused");
    assert!(unread(&peer, len) < len, "the socket took the whole push");

    // The other half of clearing write readiness: the wakeup is not lost, so
    // the rest follows once the peer reads.
    peer.read_exact(&mut vec![0u8; len]).expect("the rest of the push");
    reply(&mut peer, 3, 33);
    assert_eq!(settled(&rt, stuck).unwrap(), 33);
}

/// A client its task owns awaits its verbs itself, with no driver; and a
/// reconnect moves its registration to the new socket.
#[test]
fn an_owned_client_waits_on_the_runtime_and_reconnects() {
    let rt = Runtime::new().unwrap();
    let (listener, path, _dir) = listen();
    let (mut client, peer) = connect_to(&rt, listener, &path);
    let answering = |mut peer: UnixStream, tids: std::ops::RangeInclusive<u64>| {
        std::thread::spawn(move || {
            for tid in tids {
                assert_eq!(request(&mut peer), tid);
                reply(&mut peer, tid, tid * 11);
            }
            peer
        })
    };
    let spec = ReadSpec::all_rows(ReadBound::None);

    let answered = answering(peer, 1..=3);
    settled(&rt, async {
        assert_eq!(client.scan_spec(1, &spec, &schema()).await.unwrap().lsn, Some(11));
        let first = client.scan_spec(2, &spec, &schema()).detach();
        assert_eq!(client.scan_spec(3, &spec, &schema()).await.unwrap().lsn, Some(33));
        assert_eq!(first.await.unwrap().lsn, Some(22));
    });
    let mut old = answered.join().unwrap();

    let (listener, path, _dir) = listen();
    let accepted = std::thread::spawn(move || accept(&listener));
    settled(&rt, client.reconnect(&path)).unwrap();
    assert_eq!(old.read(&mut [0]).map_err(|e| e.kind()), Ok(0), "the old socket closed");
    let answered = answering(accepted.join().unwrap(), 4..=4);
    settled(&rt, async {
        assert_eq!(client.scan_spec(4, &spec, &schema()).await.unwrap().lsn, Some(44));
    });
    answered.join().unwrap();
}
