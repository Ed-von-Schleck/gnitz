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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::task::Poll;
use std::time::{Duration, Instant};

use gnitz_core::{
    BatchAppender, ClientError, DeltaCursor, GnitzClient, MAX_IN_FLIGHT, MAX_QUEUED_BYTES, ScanReply, Schema, Sent,
    Target, ZSetBatch,
};
use gnitz_tokio::{AsyncClient, Connection};
use gnitz_wire::{
    ClientVerb, ColumnDef, ControlHeader, FRAME_LEN_PREFIX_BYTES, ReadBound, ReadSpec, TypeCode, WireConflictMode,
    WireFlags, WireLane, append_frame, frame_len_prefix, peek_control_block,
};
use support::{PATIENCE, settled};
use tokio::runtime::Runtime;

/// A key column and nothing else; the peer never checks a layout.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U64, false)], &[0]).unwrap())
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
fn scan(c: &AsyncClient, tid: u64) -> Sent<ScanReply> {
    c.send(move |c| c.scan_spec(tid, &ReadSpec::all_rows(ReadBound::None), &schema()))
}

/// A push of `batch` to `tid`, submitted when called.
fn push(c: &AsyncClient, tid: u64, batch: ZSetBatch) -> Sent<u64> {
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
    write_frame(&mut s, &gnitz_wire::server_hello(gnitz_wire::AuthStatus::Accepted));
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

/// The relation the next request on the wire names: a scan's in its one item.
fn request(s: &mut UnixStream) -> u64 {
    let frame = read_frame(s);
    let ctrl = peek_control_block(&frame).expect("a control header");
    match ctrl.hdr.flags.verb {
        ClientVerb::ScanMulti => match gnitz_wire::txn_frame::decode_scan_multi(&frame[ctrl.body]).as_deref() {
            Ok([item]) => item.target.tid,
            other => panic!("a scan names one relation: {other:?}"),
        },
        _ => ctrl.hdr.target_id,
    }
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

/// The feed tag every cursor of the sync test carries.
const TAG: u64 = 0xFEED;

/// A frame of `hdr`, carrying `rows` — `(key, weight)` of [`schema`] — if any.
fn frame(hdr: ControlHeader, rows: &[(u64, i64)]) -> Vec<u8> {
    let pks: Vec<u8> = rows.iter().flat_map(|&(k, _)| k.to_be_bytes()).collect();
    let weights: Vec<u8> = rows.iter().flat_map(|&(_, w)| w.to_le_bytes()).collect();
    let nulls = vec![0u8; rows.len() * 8];
    let regions: [&[u8]; 4] = [&pks, &weights, &nulls, &[]];
    let mut out = Vec::new();
    append_frame(&mut out, &hdr, &[], None, (!rows.is_empty()).then_some(&regions[..]));
    out
}

/// One pushed train of subscription `sub` to view 40: `rows`, ending at round
/// `tick`, then the sync's answer.
fn push_train_and_answer(s: &mut UnixStream, sub: u64, rows: &[(u64, i64)], tick: u64) {
    let lane = |lane| WireFlags { lane, ..Default::default() };
    let opener = ControlHeader {
        target_id: 40,
        arg0: sub,
        flags: lane(WireLane::PushedTrain),
        ..Default::default()
    };
    write_frame(s, &frame(opener, &[]));
    let data = ControlHeader {
        target_id: 40,
        arg1: TAG,
        flags: WireFlags { continuation: true, ..Default::default() },
        ..Default::default()
    };
    write_frame(s, &frame(data, rows));
    let terminal = ControlHeader {
        target_id: 40,
        arg0: tick,
        arg1: TAG,
        ..Default::default()
    };
    write_frame(s, &frame(terminal, &[]));
    let answer = ControlHeader {
        flags: lane(WireLane::SyncAnswer),
        ..Default::default()
    };
    write_frame(s, &frame(answer, &[]));
}

/// The header of the next request, which must be a SYNC_PUSHED.
fn expect_sync(s: &mut UnixStream) -> ControlHeader {
    let hdr = peek_control_block(&read_frame(s)).expect("a control header").hdr;
    assert_eq!(hdr.flags.verb, ClientVerb::SyncPushed);
    hdr
}

/// `f`'s output, polling `conn` alongside it on this thread and nothing else.
fn drive<F: Future>(rt: &Runtime, conn: &mut Connection, f: F) -> F::Output {
    let mut f = std::pin::pin!(f);
    settled(
        rt,
        std::future::poll_fn(move |cx| {
            let _ = conn.as_mut().poll(cx);
            f.as_mut().poll(cx)
        }),
    )
}

/// A sync dropped while the client finishes it loses nothing: the next sync
/// reports the rows it took in front of its own, weights and all.
#[test]
fn a_sync_cancelled_after_its_answer_hands_its_rows_to_the_next() {
    cancel_after_the_answer(false);
}

/// So does one dropped once the client finished it, its delivery arrived and
/// never read.
#[test]
fn a_sync_cancelled_after_its_delivery_hands_its_rows_to_the_next() {
    cancel_after_the_answer(true);
}

/// Drop a sync once its answer is in — and, if `delivered`, once the client
/// finished it — then check the next sync reports its rows.
fn cancel_after_the_answer(delivered: bool) {
    let rt = Runtime::new().unwrap();
    let (client, mut conn, mut peer) = connect_to_peer(&rt);

    let script = std::thread::spawn(move || {
        let poll = peek_control_block(&read_frame(&mut peer))
            .expect("a control header")
            .hdr;
        assert_eq!(poll.flags.verb, ClientVerb::DeltaPoll);
        let sub = poll.arg0;
        let terminal = ControlHeader {
            target_id: 40,
            arg0: 9,
            arg1: TAG,
            ..Default::default()
        };
        write_frame(&mut peer, &frame(terminal, &[]));

        expect_sync(&mut peer);
        assert_eq!(request(&mut peer), 5, "the probe");
        push_train_and_answer(&mut peer, sub, &[(1, 1), (2, 2)], 11);
        reply(&mut peer, 5, 55);

        let next = expect_sync(&mut peer);
        assert_eq!(next.arg0, 0, "a held delivery is no reason to wait");
        push_train_and_answer(&mut peer, sub, &[(1, -1)], 12);
        peer
    });

    let from = DeltaCursor::from_pair(TAG, 9).unwrap();
    let spec = ReadSpec::all_rows(ReadBound::None).encode();
    let subscribed =
        client.run(move |c| Box::pin(async move { c.subscribe(Target::from(40), Some(from), &schema(), &spec).await }));
    let (sub, _, _) = drive(&rt, &mut conn, subscribed).unwrap().expect("subscribed");

    // The sync's request goes first, then the probe's; the probe's reply
    // follows the sync's answer, so once it is in the answer is too.
    let mut sync = Box::pin(client.sync(Duration::from_secs(30)));
    let noop = std::task::Waker::noop();
    assert!(
        sync.as_mut()
            .poll(&mut std::task::Context::from_waker(noop))
            .is_pending()
    );
    assert_eq!(drive(&rt, &mut conn, scan(&client, 5)).unwrap().lsn, Some(55));
    // Its answer in, the sync queues the client's half and awaits it.
    let polled = rt.block_on(std::future::poll_fn(|cx| Poll::Ready(sync.as_mut().poll(cx))));
    assert!(polled.is_pending());
    if delivered {
        // Calls run in order, so the client's half has run once this has.
        drive(&rt, &mut conn, client.run(|_| Box::pin(std::future::ready(())))).unwrap();
    }
    drop(sync);

    rt.spawn(conn);
    let synced = settled(&rt, client.sync(Duration::from_secs(30))).expect("the next sync");
    drop(script.join().unwrap());
    let [pushed] = &synced.pushed[..] else {
        panic!("one entry per subscription")
    };
    let (rows, at) = pushed.result.as_ref().expect("the subscription is held");
    assert_eq!((pushed.sub, at.tick.get()), (sub, 12), "under the later cursor");
    let rows: Vec<(u128, i64)> = (0..rows.batch.len())
        .map(|i| (rows.batch.pks.get(i), rows.batch.weights[i]))
        .collect();
    assert_eq!(rows, [(1, 1), (2, 2), (1, -1)], "the cancelled sync's rows first");
}
