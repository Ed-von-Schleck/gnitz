//! Client connections: accept dispatch, the send loop and its CQE, the egress
//! deadline, and the reap that retires an fd.

use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::orchestration::peer::Peer;
use crate::runtime::wire::COALESCE_MAX_BYTES;

/// One whole-payload client send, with nothing racing it.
async fn owned_send(r: &Reactor, fd: i32, payload: Vec<u8>) -> i32 {
    r.send_owned(fd, Rc::new(gnitz_store::storage::batch_pool::PooledSendBuf(payload)))
        .await
}

/// A pooled send buffer holding `bytes`.
fn pooled(bytes: &[u8]) -> gnitz_store::storage::batch_pool::PooledSendBuf {
    let mut b = gnitz_store::storage::batch_pool::acquire_buf();
    b.extend_from_slice(bytes);
    gnitz_store::storage::batch_pool::PooledSendBuf(b)
}

/// Everything readable on `fd` right now, without blocking.
fn read_available(fd: i32, cap: usize) -> Vec<u8> {
    let mut buf = vec![0u8; cap];
    unsafe {
        libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
        let n = libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, cap);
        buf.truncate(n.max(0) as usize);
    }
    buf
}

/// The same send the way `Peer::send` runs it: under the egress deadline. Only
/// for the tests whose subject *is* that deadline — `Limits::TEST` sets it to
/// 10 ms, so any send racing it is a send racing the machine's scheduler.
async fn guarded_send(r: &Reactor, fd: i32, payload: Vec<u8>) -> i32 {
    let buf = Rc::new(gnitz_store::storage::batch_pool::PooledSendBuf(payload));
    let what = buf.what();
    guard_egress_deadline(r, fd, what, r.send_owned(fd, buf)).await
}

// ─────────────────────────────────────────────────────────────────
// KIND_SEND CQE dispatch + SendFuture lifecycle.
//
// Regression guards for:
//   (a) partial-send handling in `send_buffer` (OP_SEND on a stream
//       socket can return rc < len); treating one CQE as "done"
//       truncated the scan response at ~208 KB and hung the client;
//   (b) SendFuture::Drop parking its buffer Rc so the kernel's
//       in-flight pointer stays valid after cancellation.
// ─────────────────────────────────────────────────────────────────

#[test]
fn send_cqe_wakes_its_waker_and_settles_the_conn() {
    let r = make_reactor();
    let alive: SendAlive = Rc::new(gnitz_store::storage::batch_pool::PooledSendBuf(vec![0u8; 16]));
    let (fd, write_end) = unsafe { pipe_pair() };
    r.inner.sends.open(77, Some((fd, Rc::clone(&alive))));
    r.inner
        .conns
        .borrow_mut()
        .insert(fd, Box::new(io::Conn::new(Rc::clone(&r.inner.inbound))));
    r.inner.conns.borrow_mut().get_mut(&fd).unwrap().send_inflight = 1;

    let mut fut = std::pin::pin!(SendFuture { send_id: 77, inner: Rc::clone(&r.inner) });
    let waker = make_waker(11);
    let mut cx = Context::from_waker(&waker);
    assert!(fut.as_mut().poll(&mut cx).is_pending());

    cqe(&r, KIND_SEND, 77, 16);

    assert!(
        r.inner.run_queue.borrow().is_queued(11),
        "KIND_SEND must wake the send future"
    );
    assert_eq!(
        r.inner.conns.borrow().get(&fd).unwrap().send_inflight,
        0,
        "KIND_SEND must decrement conn.send_inflight (gates close_fd)"
    );
    // The keep-alive rides the slot until the awaiter collects the result;
    // once it does, the last reference goes with it.
    assert_eq!(Rc::strong_count(&alive), 2, "slot still holds the buffer");
    assert_eq!(
        fut.as_mut().poll(&mut cx),
        Poll::Ready(16),
        "KIND_SEND must deliver the CQE rc verbatim"
    );
    assert_eq!(Rc::strong_count(&alive), 1, "collecting the result frees the buffer");
    assert_eq!(r.inner.sends.len(), 0, "a resolved send must retire its slot");
    unsafe {
        libc::close(fd);
        libc::close(write_end);
    }
}

/// A dropped SendFuture with an in-flight SQE must leave the buffer alive
/// for the kernel — the park slot holds it — and let the late CQE free it.
/// This is `II.2 io_uring SQE buffer lifetime` made concrete.
#[test]
fn dropped_send_future_keeps_buffer_alive_until_its_cqe() {
    let r = make_reactor();
    let alive: SendAlive = Rc::new(gnitz_store::storage::batch_pool::PooledSendBuf(vec![0xAB_u8; 64]));
    r.inner.sends.open(88, Some((i32::MAX, Rc::clone(&alive))));
    drop(SendFuture { send_id: 88, inner: Rc::clone(&r.inner) });

    assert!(r.inner.sends.is_abandoned(88), "drop must abandon the slot");
    assert_eq!(
        Rc::strong_count(&alive),
        2,
        "the kernel may still read the buffer — it must outlive the future"
    );

    cqe(&r, KIND_SEND, 88, 64);
    assert_eq!(r.inner.sends.len(), 0, "the late CQE must retire the abandoned slot");
    assert_eq!(Rc::strong_count(&alive), 1, "and free the buffer");
}

// ─────────────────────────────────────────────────────────────────
// Per-fd state lifecycle. Every bit of it lives in the `Conn`, so
// retiring a connection retires all of it at once.
//
// Regression guard: a kernel-reused fd number carrying the previous
// incarnation's closed flag made the new connection see immediate EOF.
// ─────────────────────────────────────────────────────────────────

/// A reaped connection leaves nothing behind for the next incarnation of
/// the same fd number: registering again starts open, with no backlog.
/// Otherwise `recv().await` on the new connection returns `None` at once
/// and the connection is dead on arrival.
#[test]
fn reaped_conn_leaves_no_state_for_the_next_connection() {
    let r = make_reactor();
    let (read_end, write_end) = unsafe { pipe_pair() };
    r.register_conn(read_end);
    // Peer EOF: closes the connection and queues it for reaping.
    r.handle_recv_cqe(read_end, 0);
    assert!(r.inner.conns.borrow().get(&read_end).unwrap().q.recv_closed());
    r.reap_closing_conns();
    assert!(
        r.inner.conns.borrow().is_empty(),
        "reaping must retire the whole Conn, closing its fd"
    );
    assert!(
        r.inner.closing_fds.borrow().is_empty(),
        "and forget the fd, so a later reap cannot close the number twice"
    );

    // The kernel is now free to hand that number back out.
    let (next_read, next_write) = unsafe { pipe_pair() };
    r.register_conn(next_read);
    let conns = r.inner.conns.borrow();
    let conn = conns.get(&next_read).expect("registered");
    assert!(!conn.q.recv_closed(), "a fresh connection must not inherit phantom EOF");
    assert!(conn.q.queued() == 0, "nor a stale delivery backlog");
    drop(conns);
    unsafe {
        libc::close(write_end);
        libc::close(next_read);
        libc::close(next_write);
    }
}

/// Concrete test of the partial-send contract: io_uring's OP_SEND on a
/// stream socket can return `rc < len`. A 200 KB payload over ~8 KB socket
/// buffers forces `send_buffer`'s loop to resubmit the remaining slice until
/// the full buffer drains. Deliberately unguarded: ~25 socket-buffer round
/// trips do not fit `Limits::TEST`'s 10 ms egress deadline on a loaded box,
/// and the eviction it would trigger is the next test's subject, not this one's.
#[test]
fn send_buffer_loops_until_full_payload_sent_over_socketpair() {
    unsafe {
        let (r, sender, receiver) = egress_pair(Some(8 * 1024));
        let payload = vec![0x5Au8; 200 * 1024];
        let payload_len = payload.len();
        let drain_t = spawn_drain(receiver, payload_len);

        let r2 = Rc::clone(&r);
        let sent = r.block_on(async move { owned_send(&r2, sender, payload).await });
        // Close before joining: on the truncated send this test exists to
        // catch, the drain's blocking `read` would otherwise never return and
        // the whole binary would hang instead of failing.
        libc::close(sender);
        let received = drain_t.join().expect("drain thread");
        assert_eq!(
            sent as usize, payload_len,
            "send_buffer must loop on partial CQEs until the full \
             payload is sent (got rc={sent}, expected {payload_len})"
        );
        assert_eq!(
            received, payload_len,
            "receiver must observe every byte — a truncated send_buffer \
             would leave the client blocked waiting for bytes that never \
             arrive"
        );
    }
}

/// `send_buffer` carries the client-egress deadline: a peer that never reads
/// must be evicted, not park the connection task forever on a
/// master-authored frame. No reader thread here, so once both socket buffers
/// fill the send makes zero progress and only the timer can end it — by
/// shutting the fd down and surfacing a negative rc.
#[test]
fn send_buffer_evicts_a_client_that_never_drains() {
    const TIMEOUT: Duration = Limits::TEST.client_send_timeout;
    unsafe {
        let (r, sender, receiver) = egress_pair(Some(4 * 1024));

        // Far larger than both buffers, and nothing ever reads the other
        // end — the send stalls partway and only the deadline can end it.
        let payload = vec![0x7Au8; 1024 * 1024];
        let r2 = Rc::clone(&r);
        let start = Instant::now();
        let rc = r.block_on(async move { guarded_send(&r2, sender, payload).await });
        let elapsed = start.elapsed();

        assert!(rc < 0, "a client that never drains must be evicted, got rc={rc}");
        assert!(
            elapsed >= TIMEOUT,
            "eviction must wait out the full deadline ({TIMEOUT:?}), took {elapsed:?}"
        );
        // The eviction path shuts the socket down, so nothing more can be
        // written to it — that is what releases the send's held resources.
        let probe = libc::send(sender, [0u8; 1].as_ptr() as *const libc::c_void, 1, libc::MSG_NOSIGNAL);
        assert_eq!(probe, -1, "evicted fd must be shut down for send");

        libc::close(sender);
        libc::close(receiver);
    }
}

/// Where coalescing stops paying, which is what `COALESCE_MAX_BYTES` is set
/// from: W head frames leaving as W guarded sends versus one guarded send of
/// their concatenation. Each send costs an `OP_SEND` + `OP_TIMEOUT` +
/// `OP_ASYNC_CANCEL` triple across two `io_uring_enter` calls, so the win is
/// a fixed cost per elided frame and the loss is the concatenation copy —
/// the grid brackets the crossover on both axes.
///
/// Only the RATIO is meaningful: absolute per-batch times swing up to 2.5x
/// with machine load. Both arms send through `send_buffer`, so this isolates
/// the per-frame kernel cost; the real per-worker drain uses `send_slot`,
/// which additionally pins a ring slot. Arm order alternates per sample
/// because the second send of a pair is systematically the cheaper one.
///
/// `cd crates && cargo test -p gnitz-server --release fanout_coalesced_egress_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn fanout_coalesced_egress_bench() {
    use gnitz_store::storage::batch_pool::{acquire_buf, PooledSendBuf};
    use std::hint::black_box;

    const ITERS: usize = 3000;

    for w in [2usize, 4, 8] {
        for total in [4 * 1024usize, 32 * 1024, 64 * 1024, 128 * 1024] {
            let per_frame = total / w;
            unsafe {
                let (r, sender, receiver) = egress_pair(None);
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
                                black_box(
                                    guard_egress_deadline(&r2, sender, "bench", r2.send_owned(sender, Rc::new(buf)))
                                        .await,
                                );
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
                            black_box(
                                guard_egress_deadline(
                                    &r2,
                                    sender,
                                    "bench",
                                    r2.send_owned(sender, Rc::new(PooledSendBuf(buf))),
                                )
                                .await,
                            );
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
                libc::close(sender);

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
}

/// A live `PeerToken` — what `Peer::unix` holds for its whole life — must
/// defer the reap. Without it the fd is closed while its `Peer` can still
/// name that number, and the kernel may hand it to a freshly-accepted
/// client, so the old peer's next send goes into the new client's socket.
#[test]
fn peer_token_defers_reap_until_it_drops() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        let token = PeerToken::new(&r, read_fd);

        // Peer FIN while the token lives: the recv completes with 0, so
        // nothing is outstanding but the token.
        libc::close(write_fd);
        let closing = poll_until(&r, 10_000, || r.inner.closing_fds.borrow().contains(&read_fd));
        assert!(closing, "peer FIN must mark the connection closing");
        r.tick(false);
        assert!(
            r.inner.conns.borrow().contains_key(&read_fd),
            "a live PeerToken must defer the reap"
        );

        drop(token);
        let reaped = poll_until(&r, 10, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(reaped, "the first reap after the token drops must retire the fd");
    }
}

/// `close_fd` on a connection whose recv is armed must cancel it, so a
/// rejected client that then goes silent does not pin its fd forever.
/// The recv SQE is queued but unflushed when `close_fd` runs, which is the
/// same-tick ordering the reachable path produces (the HELLO rejection
/// closes right after `handle_recv_cqe` re-armed).
#[test]
fn close_fd_cancels_an_armed_recv_so_a_silent_peer_is_reaped() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        assert!(
            r.inner.conns.borrow().get(&read_fd).unwrap().recv_armed,
            "register_conn arms the recv"
        );
        r.close_fd(read_fd);

        // The peer neither writes nor closes: only the cancellation can
        // complete the recv.
        let reaped = poll_until(&r, 10_000, || !r.inner.conns.borrow().contains_key(&read_fd));
        assert!(
            reaped,
            "close_fd must cancel the armed recv; without it the fd leaks until the peer acts"
        );

        libc::close(write_fd); // read_fd was closed by reap
    }
}

// ─────────────────────────────────────────────────────────────────
// `closing_fds` is the one record that a connection is closing, and what
// `reap_closing_conns` iterates.
// ─────────────────────────────────────────────────────────────────

#[test]
fn reap_closing_conns_defers_conn_with_outstanding_send() {
    let r = make_reactor();
    let (fd, write_end) = unsafe { pipe_pair() };
    let mut conn = Box::new(io::Conn::new(Rc::clone(&r.inner.inbound)));
    conn.send_inflight = 1; // outstanding send SQE
    r.inner.conns.borrow_mut().insert(fd, conn);
    r.inner.closing_fds.borrow_mut().insert(fd);

    r.reap_closing_conns();

    assert!(
        r.inner.conns.borrow().contains_key(&fd),
        "conn with outstanding send must NOT be reaped yet"
    );
    assert!(
        r.inner.closing_fds.borrow().contains(&fd),
        "conn deferred by outstanding send must stay in closing_fds"
    );
    unsafe {
        libc::close(fd);
        libc::close(write_end);
    }
}

/// The accept dispatch arm: a failed accept queues nothing, a successful one
/// queues `(conn_fd, listener_fd)` — the listener rides the udata id and is the
/// accept loop's unix-vs-tls routing key — and wakes the parked awaiter once.
#[test]
fn dispatch_accept_queues_successes_and_wakes_the_awaiter() {
    let r = make_reactor();
    let listener = fake_listener();
    let waker = make_waker(42);
    let mut fut = std::pin::pin!(r.accept());
    assert!(fut.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    cqe(&r, KIND_ACCEPT, listener as u64, -libc::ECONNABORTED);
    assert_eq!(r.inner.accepts.borrow().len(), 0, "res<0 must queue nothing");
    assert!(r.inner.accepts.borrow().has_waiter(), "and leave the awaiter parked");

    cqe(&r, KIND_ACCEPT, listener as u64, 9);
    assert!(
        r.inner.run_queue.borrow().is_queued(42),
        "res>=0 must wake the parked accept future"
    );
    assert!(!r.inner.accepts.borrow().has_waiter(), "and consume the waker it woke");
    assert_eq!(r.inner.accepts.borrow_mut().pop(), Some((9, listener)));

    unsafe { libc::close(listener) };
}

/// fd exhaustion cancels a listener's multishot accept, and the re-arm is
/// deferred behind a backoff so `reap_closing_conns` gets a window. Both
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
    unsafe {
        let (r, sender, receiver) = egress_pair(None);
        let peer = Peer::unix(sender, Rc::clone(&r));
        let frames: Vec<Vec<u8>> = (0..8u8).map(|i| vec![0xD0 | i; 200]).collect();
        let expected: Vec<u8> = frames.iter().flatten().copied().collect();

        let f = frames.clone();
        r.block_on(async move {
            for frame in &f {
                peer.cork(frame);
            }
            assert!(
                read_available(receiver, 4096).is_empty(),
                "corking must put nothing on the wire before a flush",
            );
            assert!(peer.flush_egress().await > 0, "the flush must send");
        });

        assert_eq!(
            read_available(receiver, 4096),
            expected,
            "one flush is one send carrying every corked frame in order",
        );
        libc::close(sender);
        libc::close(receiver);
    }
}

/// A `send_slot` issued with bytes corked puts the corked bytes on the wire
/// first: a ring-slot forward may not overtake a reply already written.
#[test]
fn a_slot_forward_cannot_overtake_a_corked_reply() {
    unsafe {
        let (receiver_w2m, _region) = make_scan_ring(1, 1);
        let slot = receiver_w2m.try_read_slot(0).expect("a frame");
        let slot_bytes = slot.frame_bytes().to_vec();

        let (r, sender, receiver) = egress_pair(None);
        let peer = Peer::unix(sender, Rc::clone(&r));
        let corked = vec![0x5Au8; 128];

        let c = corked.clone();
        r.block_on(async move {
            peer.cork(&c);
            assert!(peer.send_slot(slot).await > 0, "the slot forward must send");
        });

        let seen = read_available(receiver, 64 * 1024);
        let mut expected = corked;
        expected.extend_from_slice(&slot_bytes);
        assert_eq!(seen, expected, "the corked bytes precede the forwarded slot");
        libc::close(sender);
        libc::close(receiver);
    }
}

/// N frames written in one `write` are all queued off a single recv completion:
/// a pipelined run costs one read, not one per frame.
#[test]
fn one_recv_completion_queues_a_whole_pipelined_run() {
    unsafe {
        let (read_fd, write_fd) = stream_pair();
        let r = make_reactor();
        r.register_conn(read_fd);
        r.set_max_payload_len(read_fd, 1 << 20);

        const N: usize = 12;
        let wire: Vec<u8> = (0..N).flat_map(|i| framed(&vec![i as u8; 600])).collect();
        gnitz_store::foundation::posix_io::write_all_fd(write_fd, &wire).expect("write");

        let queued = |r: &Reactor| r.inner.conns.borrow().get(&read_fd).map_or(0, |c| c.q.queued());
        assert!(poll_until(&r, 10_000, || queued(&r) > 0), "the run must be deframed",);
        assert_eq!(
            queued(&r),
            N,
            "the first completion must queue every frame the read carried, not one",
        );
        assert_eq!(r.try_recv(read_fd).map(|b| b.as_slice()[0]), Some(0), "in order");

        libc::close(read_fd);
        libc::close(write_fd);
    }
}

/// Corking is unbounded on its own; `flush_if_full` is what ships a long run,
/// and it leaves nothing behind once it does.
#[test]
fn a_full_accumulator_ships_between_messages() {
    unsafe {
        let (r, sender, receiver) = egress_pair(None);
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
        libc::close(sender);
    }
}
