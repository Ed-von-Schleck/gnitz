//! Client connections: accept dispatch, the send loop, its CQE and its eviction
//! deadline, and how a connection's recv side ends and its socket closes.

use std::io::Read;
use std::os::fd::{AsRawFd, OwnedFd};
use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::orchestration::peer::{Peer, COALESCE_MAX_BYTES};
use crate::runtime::test_support::try_poll_once;
use gnitz_store::storage::batch_pool::PooledSendBuf;

/// One whole-payload client send, with nothing racing it.
async fn owned_send(r: &Reactor, conn: &ClientConn, payload: Vec<u8>) -> i32 {
    r.send_owned(conn, SendBody::Pooled(PooledSendBuf(payload))).await.0
}

/// A pooled send buffer holding `bytes`.
fn pooled(bytes: &[u8]) -> PooledSendBuf {
    let mut b = gnitz_store::storage::batch_pool::acquire_buf();
    b.extend_from_slice(bytes);
    PooledSendBuf(b)
}

/// Everything readable on `fd` right now, without blocking.
fn read_available(fd: &OwnedFd, cap: usize) -> Vec<u8> {
    let mut buf = vec![0u8; cap];
    unsafe {
        libc::fcntl(fd.as_raw_fd(), libc::F_SETFL, libc::O_NONBLOCK);
        let n = libc::read(fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, cap);
        buf.truncate(n.max(0) as usize);
    }
    buf
}

/// A fresh ring holding one frame whose error text is `pad` bytes, read back as a
/// slot. The ring is leaked, so the slot outlives any test scope.
fn ring_slot(pad: usize) -> (W2mReceiver, W2mSlot) {
    let ptr = unsafe { crate::runtime::w2m::fixtures::test_ring(256 * 1024) }.leak();
    let error_msg = vec![0x42u8; pad];
    let msg = crate::runtime::wire::WireMsg {
        request_id: 100,
        error_msg: &error_msg,
        ..Default::default()
    };
    crate::runtime::w2m::W2mWriter::new(ptr).send_msg(1, &msg);
    let receiver = W2mReceiver::new(vec![ptr]);
    let slot = receiver.try_read_slot(0).expect("a frame");
    (receiver, slot)
}

// ─────────────────────────────────────────────────────────────────
// KIND_OP CQE dispatch + a send op's lifecycle.
//
// Regression guards for:
//   (a) partial-send handling in `send_owned` (OP_SEND on a stream
//       socket can return rc < len); treating one CQE as "done"
//       truncated the scan response at ~208 KB and hung the client;
//   (b) the send's park slot holding its body so the kernel's
//       in-flight pointer stays valid after cancellation.
// ─────────────────────────────────────────────────────────────────

#[test]
fn send_cqe_wakes_its_waker_and_returns_the_body() {
    let r = make_reactor();
    r.inner.ops.open(77, Some(SendBody::Cipher(vec![0xAB; 16])));
    let mut fut = std::pin::pin!(OpFuture { id: 77, inner: Rc::clone(&r.inner) });
    let waker = make_waker(11);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    cqe(&r, KIND_OP, 77, 16);
    assert!(
        r.inner.run_queue.borrow().is_queued(11),
        "KIND_OP must wake the op future"
    );
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready((rc, body)) => {
            assert_eq!(rc, 16, "KIND_OP must deliver the CQE rc verbatim");
            assert_eq!(
                body.expect("a send carries its body").bytes(),
                &[0xAB; 16],
                "and hand the body back"
            );
        }
        Poll::Pending => panic!("a completed send must resolve"),
    }
    assert_eq!(r.inner.ops.len(), 0, "a resolved send must retire its slot");
}

/// A dropped send keeps its body until the late CQE — a ring slot here, whose
/// `release_cursor` shows when it drops.
#[test]
fn dropped_send_future_keeps_its_body_until_the_cqe() {
    let (receiver, slot) = ring_slot(0);
    let held = receiver.release_cursor(0);

    let r = make_reactor();
    r.inner.ops.open(88, Some(SendBody::Slot(slot)));
    {
        let mut fut = Box::pin(OpFuture { id: 88, inner: Rc::clone(&r.inner) });
        assert!(fut.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
    }
    assert!(r.inner.ops.is_abandoned(88), "drop must abandon the slot");
    assert_eq!(
        receiver.release_cursor(0),
        held,
        "the kernel may still read the body — it must outlive the future"
    );

    cqe(&r, KIND_OP, 88, 64);
    assert_eq!(r.inner.ops.len(), 0, "the late CQE must retire the abandoned slot");
    assert!(receiver.release_cursor(0) > held, "and free the body");
}

// ─────────────────────────────────────────────────────────────────
// The recv side and the socket's lifetime.
// ─────────────────────────────────────────────────────────────────

/// A recv that completes with EOF removes the reactor's entry, but the socket
/// stays open for as long as any holder keeps the connection, and closes with
/// the last. Observed from the partner end, so no fd number is probed.
#[test]
fn a_closed_connection_drops_its_entry_and_closes_with_its_last_holder() {
    let (local, mut partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let r = make_reactor();
    let conn = r.client_conn(OwnedFd::from(local));
    let fd = conn.fd();
    r.register_conn(&conn, None);

    partner.shutdown(std::net::Shutdown::Write).expect("half-close");
    assert!(
        poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&fd)),
        "EOF must end the recv side and drop the reactor's entry"
    );
    assert!(conn.recv_closed());
    assert!(matches!(try_poll_once(conn.recv()), Some(None)), "recv reports the end");

    partner.set_nonblocking(true).expect("nonblocking");
    let mut buf = [0u8; 1];
    assert_eq!(
        partner.read(&mut buf).map_err(|e| e.kind()),
        Err(std::io::ErrorKind::WouldBlock),
        "the socket stays open while the connection is held"
    );
    drop(conn);
    assert_eq!(partner.read(&mut buf).ok(), Some(0), "and closes with its last holder");
}

/// `close_conn` ends an armed recv, even one not yet submitted, so a silent peer
/// cannot pin the connection.
#[test]
fn close_conn_ends_an_armed_recv() {
    let (local, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let r = make_reactor();
    let conn = r.client_conn(OwnedFd::from(local));
    let fd = conn.fd();
    r.register_conn(&conn, None);
    r.close_conn(&conn);

    // The peer neither writes nor closes: only the shutdown can complete the
    // recv.
    assert!(
        poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&fd)),
        "close_conn must end the armed recv; without it the entry lives until the peer acts"
    );
    drop(partner);
}

/// A payload far larger than the socket buffers completes across many short sends.
#[test]
fn send_owned_loops_until_full_payload_sent_over_socketpair() {
    let (r, sender, receiver) = egress_pair(Limits::TEST, Some(8 * 1024));
    let conn = r.client_conn(sender);
    let payload = vec![0x5Au8; 200 * 1024];
    let payload_len = payload.len();
    let drain_t = spawn_drain(receiver, payload_len);

    let r2 = Rc::clone(&r);
    // `conn` drops with the task, closing the sender before the join: on the
    // truncated send this test exists to catch, the drain's blocking `read`
    // would otherwise never return and the whole binary would hang.
    let sent = r.block_on(async move { owned_send(&r2, &conn, payload).await });
    let received = drain_t.join().expect("drain thread");
    assert_eq!(
        sent as usize, payload_len,
        "send_owned must loop on partial CQEs until the full \
         payload is sent (got rc={sent}, expected {payload_len})"
    );
    assert_eq!(
        received, payload_len,
        "receiver must observe every byte — a truncated send would leave the \
         client blocked waiting for bytes that never arrive"
    );
}

/// A peer that never reads is evicted once a send makes no progress past the deadline.
#[test]
fn send_owned_evicts_a_client_that_never_drains() {
    const TIMEOUT: Duration = Duration::from_millis(10);
    let limits = Limits {
        client_send_timeout: TIMEOUT,
        ..Limits::TEST
    };
    let (r, sender, _receiver) = egress_pair(limits, Some(4 * 1024));
    let conn = Rc::new(r.client_conn(sender));

    // Far larger than both buffers, and nothing ever reads the other
    // end — the send stalls partway and only the deadline can end it.
    let payload = vec![0x7Au8; 1024 * 1024];
    let (r2, c2) = (Rc::clone(&r), Rc::clone(&conn));
    let start = Instant::now();
    let rc = r.block_on(async move { owned_send(&r2, &c2, payload).await });
    let elapsed = start.elapsed();

    assert!(rc < 0, "a client that never drains must be evicted, got rc={rc}");
    assert!(
        elapsed >= TIMEOUT,
        "eviction must wait out the full deadline ({TIMEOUT:?}), took {elapsed:?}"
    );
    // The eviction path shuts the socket down, so nothing more can be
    // written to it — that is what releases the send's held resources.
    let probe = unsafe {
        libc::send(
            conn.fd(),
            [0u8; 1].as_ptr() as *const libc::c_void,
            1,
            libc::MSG_NOSIGNAL,
        )
    };
    assert_eq!(probe, -1, "evicted fd must be shut down for send");
}

/// W frames as W sends versus one send of their concatenation, the trade
/// `COALESCE_MAX_BYTES` is set from. Only the ratio is meaningful.
///
/// `cd crates && cargo test -p gnitz-server --release fanout_coalesced_egress_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn fanout_coalesced_egress_bench() {
    use gnitz_store::storage::batch_pool::acquire_buf;
    use std::hint::black_box;

    const ITERS: usize = 3000;

    for w in [2usize, 4, 8] {
        for total in [4 * 1024usize, 32 * 1024, 64 * 1024, 128 * 1024] {
            let per_frame = total / w;
            let (r, sender, receiver) = egress_pair(Limits::TEST, None);
            let conn = r.client_conn(sender);
            // Both arms push `total` bytes per sample; the reader keeps
            // the socket buffers from ever stalling a send, so the timed
            // region is kernel-op cost, not backpressure.
            let expect = 2 * ITERS * total;
            let drain_t = spawn_drain(receiver, expect);

            let frame = vec![0xA5u8; per_frame];
            let r2 = Rc::clone(&r);
            let (per_frame_dur, coalesced_dur) = r.block_on(async move {
                let (mut a, mut b) = (Duration::ZERO, Duration::ZERO);
                for i in 0..ITERS {
                    // Source buffers are filled outside the timed region
                    // on both arms except the concatenation itself,
                    // which is the copy under test.
                    let bufs: Vec<PooledSendBuf> = (0..w).map(|_| pooled(&frame)).collect();
                    let run_per_frame = async |bufs: Vec<PooledSendBuf>| {
                        let t = Instant::now();
                        for buf in bufs {
                            black_box(r2.send_owned(&conn, SendBody::Pooled(buf)).await.0);
                        }
                        t.elapsed()
                    };
                    let run_coalesced = async || {
                        let t = Instant::now();
                        let mut buf = acquire_buf();
                        buf.reserve(total);
                        for _ in 0..w {
                            buf.extend_from_slice(&frame);
                        }
                        black_box(r2.send_owned(&conn, SendBody::Pooled(PooledSendBuf(buf))).await.0);
                        t.elapsed()
                    };
                    if i % 2 == 0 {
                        a += run_per_frame(bufs).await;
                        b += run_coalesced().await;
                    } else {
                        b += run_coalesced().await;
                        a += run_per_frame(bufs).await;
                    }
                }
                (a, b)
            });

            let seen = drain_t.join().expect("drain thread");
            assert_eq!(seen, expect, "reader must observe every byte both arms sent");

            let delta = coalesced_dur.as_secs_f64() / per_frame_dur.as_secs_f64() - 1.0;
            println!(
                "coalesced egress W={w} total={total}B: coalesced vs per-frame {:+.1}% \
                 (per-frame {:?}/batch, coalesced {:?}/batch)",
                delta * 100.0,
                per_frame_dur / ITERS as u32,
                coalesced_dur / ITERS as u32,
            );
        }
    }
}

/// The accept dispatch arm: a failed accept queues nothing, a successful one
/// queues `(conn_fd, listener_fd)` — the listener rides the udata id and is the
/// accept loop's unix-vs-tls routing key — and wakes the parked awaiter.
#[test]
fn dispatch_accept_queues_successes_and_wakes_the_awaiter() {
    let r = make_reactor();
    let listener = fake_listener();
    let waker = make_waker(42);
    let mut fut = std::pin::pin!(r.accept());
    assert!(fut.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    cqe(&r, KIND_ACCEPT, listener as u64, -libc::ECONNABORTED);
    assert_eq!(r.inner.accepts.borrow().len(), 0, "res<0 must queue nothing");
    assert!(!r.inner.run_queue.borrow().is_queued(42), "nor wake the awaiter");

    cqe(&r, KIND_ACCEPT, listener as u64, 9);
    assert!(
        r.inner.run_queue.borrow().is_queued(42),
        "res>=0 must wake the parked accept future"
    );
    assert_eq!(r.inner.accepts.borrow_mut().pop(), Some((9, listener)));

    unsafe { libc::close(listener) };
}

/// fd exhaustion cancels a listener's multishot accept, and the re-arm is
/// deferred behind a backoff so closing connections get a window. Both
/// listeners can cancel in the same window — exhaustion is global — and each
/// must come back.
#[test]
fn both_listeners_rearm_after_an_fd_exhaustion_backoff() {
    let r = make_reactor();
    let (a, b) = (fake_listener(), fake_listener());

    // `CQE_F_MORE` clear (`flags = 0`) is the kernel saying the multishot SQE
    // is gone; -EMFILE is why.
    r.handle_accept_cqe(a, -libc::EMFILE, 0);
    r.handle_accept_cqe(b, -libc::EMFILE, 0);
    assert_eq!(
        r.inner.tasks.borrow().len(),
        2,
        "each cancelled listener gets its own backoff task"
    );

    let deadline = Instant::now() + Limits::TEST.accept_rearm_backoff + Duration::from_secs(5);
    while !r.inner.tasks.borrow().is_empty() && Instant::now() < deadline {
        r.tick(true);
    }
    assert!(
        r.inner.tasks.borrow().is_empty(),
        "both backoff tasks must fire and re-arm their listener"
    );
    unsafe {
        libc::close(a);
        libc::close(b);
    }
}

// ─────────────────────────────────────────────────────────────────
// Corked egress: what waits in `Peer`'s accumulator, and what it leaves behind.
// ─────────────────────────────────────────────────────────────────

/// Corked replies put nothing on the wire until something flushes, and then
/// leave as one send: the far end reads the whole concatenation in one `read`.
#[test]
fn corked_replies_leave_as_one_send() {
    let (r, sender, receiver) = egress_pair(Limits::TEST, None);
    let receiver = Rc::new(receiver);
    let peer = Peer::unix(sender, Rc::clone(&r));
    let frames: Vec<Vec<u8>> = (0..8u8).map(|i| vec![0xD0 | i; 200]).collect();
    let expected: Vec<u8> = frames.iter().flatten().copied().collect();

    let rx = Rc::clone(&receiver);
    r.block_on(async move {
        for frame in &frames {
            peer.cork(frame);
        }
        assert!(
            read_available(&rx, 4096).is_empty(),
            "corking must put nothing on the wire before a flush",
        );
        assert!(peer.flush_egress().await > 0, "the flush must send");
    });

    assert_eq!(
        read_available(&receiver, 4096),
        expected,
        "one flush is one send carrying every corked frame in order",
    );
    drop(receiver);
}

/// A slot too large to cork, sent with bytes corked, puts the corked bytes on the
/// wire first: a zero-copy forward may not overtake a reply already written.
#[test]
fn a_slot_forward_cannot_overtake_a_corked_reply() {
    let (_ring, slot) = ring_slot(COALESCE_MAX_BYTES);
    let slot_bytes = slot.frame_bytes().to_vec();
    assert!(slot_bytes.len() > COALESCE_MAX_BYTES, "the slot goes out alone");

    let (r, sender, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::unix(sender, Rc::clone(&r));
    let corked = vec![0x5Au8; 128];

    let c = corked.clone();
    r.block_on(async move {
        peer.cork(&c);
        assert!(peer.send(slot).await > 0, "the slot forward must send");
    });

    let seen = read_available(&receiver, 256 * 1024);
    let mut expected = corked;
    expected.extend_from_slice(&slot_bytes);
    assert_eq!(seen, expected, "the corked bytes precede the forwarded slot");
    drop(receiver);
}

/// A small slot sent with bytes corked joins them: nothing reaches the wire until
/// a flush, and that one flush carries both.
#[test]
fn a_small_slot_is_corked_behind_corked_bytes() {
    let (_ring, slot) = ring_slot(64);
    let slot_bytes = slot.frame_bytes().to_vec();

    let (r, sender, receiver) = egress_pair(Limits::TEST, None);
    let receiver = Rc::new(receiver);
    let peer = Peer::unix(sender, Rc::clone(&r));
    let corked = vec![0x5Au8; 128];

    let (c, rx) = (corked.clone(), Rc::clone(&receiver));
    r.block_on(async move {
        peer.cork(&c);
        assert_eq!(peer.send(slot).await, 0, "a small slot is corked, not sent");
        assert!(read_available(&rx, 4096).is_empty(), "nothing is on the wire yet");
        assert!(peer.flush_egress().await > 0, "the flush must send");
    });

    let mut expected = corked;
    expected.extend_from_slice(&slot_bytes);
    assert_eq!(
        read_available(&receiver, 4096),
        expected,
        "one flush carries both, in order"
    );
    drop(receiver);
}

/// N frames written in one `write` are all queued off a single recv completion:
/// a pipelined run costs one read, not one per frame.
#[test]
fn one_recv_completion_queues_a_whole_pipelined_run() {
    let (local, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
    let r = make_reactor();
    let conn = r.client_conn(OwnedFd::from(local));
    r.register_conn(&conn, None);
    conn.set_max_payload_len(1 << 20);

    const N: usize = 12;
    let wire: Vec<u8> = (0..N).flat_map(|i| framed(&vec![i as u8; 600])).collect();
    gnitz_foundation::posix_io::write_all_fd(partner.as_raw_fd(), &wire).expect("write");

    let queued = || conn.q.borrow().queued();
    assert!(poll_until(&r, 10_000, || queued() > 0), "the run must be deframed");
    assert_eq!(
        queued(),
        N,
        "the first completion must queue every frame the read carried, not one",
    );
    assert_eq!(conn.try_recv().map(|b| b.as_slice()[0]), Some(0), "in order");
    drop(partner);
}

/// Corking is unbounded on its own; `flush_if_full` is what ships a long run,
/// and it leaves nothing behind once it does.
#[test]
fn a_full_accumulator_ships_between_messages() {
    let (r, sender, receiver) = egress_pair(Limits::TEST, None);
    let peer = Peer::unix(sender, Rc::clone(&r));
    let drain = spawn_drain(receiver, COALESCE_MAX_BYTES);
    let frame = vec![0x3Cu8; 4096];

    r.block_on(async move {
        let mut shipped = 0;
        while shipped == 0 {
            peer.cork(&frame);
            shipped = peer.flush_if_full().await;
        }
        assert!(
            shipped as usize >= COALESCE_MAX_BYTES,
            "the flush ships everything corked, got {shipped}"
        );
        assert_eq!(peer.flush_if_full().await, 0, "and leaves nothing pending");
    });

    assert!(drain.join().expect("drain") >= COALESCE_MAX_BYTES);
}
