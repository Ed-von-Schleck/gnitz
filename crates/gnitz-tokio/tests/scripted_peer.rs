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

use gnitz_core::{BatchAppender, ClientError, ScanReply, Schema, ZSetBatch, MAX_IN_FLIGHT};
use gnitz_tokio::{AsyncClient, Connection};
use gnitz_wire::control::{append_frame, peek_control_block, ControlHeader};
use gnitz_wire::{frame_len_prefix, ColumnDef, ReadBound, ReadSpec, TypeCode, FRAME_LEN_PREFIX_BYTES};
use support::submitted;
use tokio::runtime::Runtime;

/// How long anything that is on its way may take. Paid in full only by a
/// regression, which then fails rather than hangs.
const PATIENCE: Duration = Duration::from_secs(5);

/// A key column and nothing else; the peer never checks a layout.
fn schema() -> Arc<Schema> {
    Arc::new(Schema::from_parts(vec![ColumnDef::new("k", TypeCode::U64, false)], vec![0]).unwrap())
}

/// A read of every row of `tid`.
async fn scan(c: AsyncClient, tid: u64) -> Result<ScanReply, ClientError> {
    c.scan_spec(tid, ReadSpec::all_rows(ReadBound::None), schema()).await
}

/// `f`'s output. A verb or a driver left pending is the failure most of these
/// tests look for.
fn settled<F: Future>(rt: &Runtime, f: F) -> F::Output {
    rt.block_on(async { tokio::time::timeout(PATIENCE, f).await })
        .expect("resolves rather than hangs")
}

/// A client connected to a peer that has answered its HELLO, and the peer's end.
fn connect_to_peer(rt: &Runtime) -> (AsyncClient, Connection, UnixStream) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("peer.sock");
    let listener = UnixListener::bind(&path).expect("bind");
    // `connect` blocks until the HELLO is answered.
    let peer = std::thread::spawn(move || {
        let (mut s, _) = listener.accept().expect("accept");
        s.set_read_timeout(Some(PATIENCE)).unwrap();
        read_frame(&mut s);
        write_frame(&mut s, &gnitz_wire::HELLO);
        s
    });
    let (client, conn) = rt
        .block_on(gnitz_tokio::connect(path.to_str().expect("utf-8 path")))
        .expect("connect");
    (client, conn, peer.join().unwrap())
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

/// Wait until `len` bytes are readable, consuming none. Reading a request out
/// frees socket buffer, which signals writability to the driver afresh — the
/// very readiness a test may need it to have kept on its own.
fn await_unread(s: &UnixStream, len: usize) {
    let mut buf = vec![0u8; len];
    let deadline = Instant::now() + PATIENCE;
    loop {
        // SAFETY: an open socket, and a buffer of `len` bytes.
        let n = unsafe {
            libc::recv(
                s.as_raw_fd(),
                buf.as_mut_ptr().cast(),
                len,
                libc::MSG_PEEK | libc::MSG_DONTWAIT,
            )
        };
        if n == len as isize {
            return;
        }
        assert!(Instant::now() < deadline, "{n} of {len} bytes arrived");
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

    let first = rt.spawn(scan(client.clone(), 1));
    let len = read_len(&mut peer);
    // Nothing has been answered, and the first request still sits in the
    // socket. The second must leave anyway.
    let second = rt.spawn(scan(client, 2));
    await_unread(&peer, len + 1);
    peer.read_exact(&mut vec![0u8; len]).expect("the first request");
    assert_eq!(request(&mut peer), 2);

    reply(&mut peer, 1, 11);
    reply(&mut peer, 2, 22);
    assert_eq!(settled(&rt, first).unwrap().unwrap().lsn, Some(11));
    assert_eq!(settled(&rt, second).unwrap().unwrap().lsn, Some(22));
}

/// Past the in-flight cap a verb waits for a slot; the spine's own refusal at
/// the cap never reaches one.
#[test]
fn past_the_in_flight_cap_a_verb_waits() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    rt.spawn(conn);

    let total = MAX_IN_FLIGHT as u64 + 100;
    let verbs: Vec<_> = (1..=total).map(|tid| rt.spawn(scan(client.clone(), tid))).collect();
    // The cap's worth arrives and goes unanswered, which is what holds the rest
    // back; they follow as the answers free slots.
    let held: Vec<u64> = (0..MAX_IN_FLIGHT).map(|_| request(&mut peer)).collect();
    for tid in held {
        reply(&mut peer, tid, tid);
    }
    for _ in MAX_IN_FLIGHT as u64..total {
        let tid = request(&mut peer);
        reply(&mut peer, tid, tid);
    }
    for (tid, verb) in (1..=total).zip(verbs) {
        assert_eq!(settled(&rt, verb).unwrap().unwrap().lsn, Some(tid));
    }
}

/// A request the spine refuses fails that verb alone: it reaches no wire, and
/// the verb behind it still gets its own reply.
#[test]
fn a_refused_verb_reaches_no_wire() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    rt.spawn(conn);

    let schema = schema();
    let mut keyless = ZSetBatch::new(&schema);
    keyless.weights.push(1);
    let refused = settled(&rt, client.push(1, schema, keyless));
    assert!(matches!(refused, Err(ClientError::Refused(_))), "{refused:?}");

    let next = rt.spawn(scan(client, 2));
    assert_eq!(request(&mut peer), 2, "the refused push was never written");
    reply(&mut peer, 2, 22);
    assert_eq!(settled(&rt, next).unwrap().unwrap().lsn, Some(22));
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

        let verbs: Vec<_> = (1..=outstanding)
            .map(|tid| rt.spawn(scan(client.clone(), tid)))
            .collect();
        // Every one of them is on the wire, so each holds a slot when the peer goes.
        for _ in &verbs {
            request(&mut peer);
        }
        drop(peer);

        let mut results: Vec<_> = verbs.into_iter().map(|v| settled(&rt, v).unwrap()).collect();
        results.push(settled(&rt, scan(client.clone(), 9)));
        results.push(settled(&rt, scan(client, 10)));
        for r in results {
            assert!(matches!(r, Err(ClientError::ConnectionLost(_))), "{r:?}");
        }
        settled(&rt, driver).unwrap();
    }
}

/// Dropping a submitted verb cancels nothing, and a driver whose handles are
/// all gone still writes what it was handed and waits for the reply.
#[test]
fn a_dropped_verb_is_still_written() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    // The verb owns the only handle, so both go here.
    rt.block_on(async { drop(submitted(scan(client, 1)).await) });

    let driver = rt.spawn(conn);
    assert_eq!(request(&mut peer), 1);
    assert!(!driver.is_finished(), "a reply is owed");
    // Its result has nobody to go to, which is not an error.
    reply(&mut peer, 1, 11);
    settled(&rt, driver).unwrap();
}

/// A driver dropped before its handles fails their verbs rather than leaving
/// them pending.
#[test]
fn a_dropped_driver_closes_every_verb() {
    let rt = Runtime::new().unwrap();
    let (client, conn, mut peer) = connect_to_peer(&rt);
    let driver = rt.spawn(conn);

    let outstanding = rt.spawn(scan(client.clone(), 1));
    request(&mut peer);
    driver.abort();
    assert!(settled(&rt, driver).unwrap_err().is_cancelled());

    for r in [settled(&rt, outstanding).unwrap(), settled(&rt, scan(client, 2))] {
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

    let first = rt.spawn(scan(client.clone(), 1));
    assert_eq!(request(&mut peer), 1);
    assert_waiting("a reply outstanding");

    reply(&mut peer, 1, 11);
    settled(&rt, first).unwrap().unwrap();
    assert_waiting("nothing in flight");

    // Megabytes, to a peer that reads no further than the length prefix: the
    // socket takes what it buffers and refuses the rest.
    let schema = schema();
    let mut big = ZSetBatch::new(&schema);
    let mut rows = BatchAppender::new(&mut big, &schema);
    for pk in 0..200_000 {
        rows.add_row(pk, 1);
    }
    let stuck = rt.spawn(async move { client.push(3, schema, big).await });
    let len = read_len(&mut peer);
    assert!(len > 1 << 20, "{len} bytes may fit the socket buffer");
    assert_waiting("bytes the socket refused");

    // The other half of clearing write readiness: the wakeup is not lost, so
    // the rest follows once the peer reads.
    peer.read_exact(&mut vec![0u8; len]).expect("the rest of the push");
    reply(&mut peer, 3, 33);
    assert_eq!(settled(&rt, stuck).unwrap().unwrap(), 33);
}
