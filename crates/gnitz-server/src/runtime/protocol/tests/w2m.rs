use super::fixtures::{make_ring, master_parked};
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within, SharedRegion};
use gnitz_wire::control::CTRL_HEADER_SIZE;
use gnitz_wire::WireStatus;
use proptest::prelude::*;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// Publish one message directly, without `W2mWriter`'s park loop; `false` when
/// the ring is full.
///
/// # Safety
/// `base` must be a live ring from [`test_ring`], and the caller must be its
/// sole producer.
unsafe fn publish(base: *mut u8, sz: usize, internal_req_id: u32, encode: impl FnOnce(&mut [u8])) -> bool {
    let mut wc = RingCursor::producer(base);
    let Some(mut reservation) = try_reserve(&wc, sz, internal_req_id) else {
        return false;
    };
    encode(reservation.slot());
    commit(&mut wc, reservation);
    true
}

// -- ring layout ---------------------------------------------------------

/// Consume one message and release its space, as the master does when it
/// drops the slot.
unsafe fn consume_one(recv: &W2mReceiver) -> Option<&'static [u8]> {
    let slot = recv.try_read_slot(0)?;
    let bytes = &slot.frame[gnitz_wire::FRAME_LEN_PREFIX_BYTES..];
    drop(slot);
    Some(bytes)
}

/// A published message is read in place, and its slot's frame bytes are the
/// client framing as they stand: the `u32` LE length, then the payload.
#[test]
fn a_published_message_is_read_in_place_as_a_client_frame() {
    unsafe {
        let ptr = make_ring(128, 4, 8);
        let recv = W2mReceiver::new(vec![ptr]);
        let payload: Vec<u8> = (0..128).collect();
        assert!(publish(ptr, payload.len(), 5, |slot| slot.copy_from_slice(&payload)));

        let slot = recv.try_read_slot(0).expect("message must be visible");
        assert_eq!(slot.internal_req_id, 5);
        assert_eq!(slot.frame_bytes(), [128u32.to_le_bytes().as_slice(), &payload].concat());
        assert_eq!(
            slot.bytes().as_ptr(),
            ptr.add(W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize) as *const u8,
            "the payload must be mmap-resident — a train frame's rows are read in place"
        );
        drop(slot);
        assert!(consume_one(&recv).is_none(), "ring must now read empty");
    }
}

/// The ring's message size: over zero slack, a run of them ends exactly on
/// the ring's physical end.
const MSG: usize = 256;

#[derive(Clone, Debug)]
enum RingOp {
    Publish(usize),
    Consume,
}

proptest! {
    /// Every consumed message is the oldest unread one, intact, and only a ring
    /// holding unread messages refuses a publish.
    #[test]
    fn a_ring_delivers_every_message_in_order_across_wraps(
        slack in prop::sample::select(vec![0u64, 8, 16, 24]),
        ops in prop::collection::vec(
            prop_oneof![
                2 => prop_oneof![Just(MSG), 1..=MSG + 44].prop_map(RingOp::Publish),
                1 => Just(RingOp::Consume),
            ],
            1..80,
        ),
    ) {
        let ptr = make_ring(MSG, 4, slack);
        let recv = W2mReceiver::new(vec![ptr]);
        let mut unread: VecDeque<(u8, usize)> = VecDeque::new();
        for (tag, op) in ops.into_iter().enumerate() {
            let tag = tag as u8;
            match op {
                RingOp::Publish(len) => {
                    if unsafe { publish(ptr, len, 0, |slot| slot.fill(tag)) } {
                        unread.push_back((tag, len));
                    } else {
                        prop_assert!(!unread.is_empty(), "an empty ring refused a {len}-byte publish");
                    }
                }
                RingOp::Consume => {
                    let got = unsafe { consume_one(&recv) };
                    match unread.pop_front() {
                        None => prop_assert!(got.is_none()),
                        Some((tag, len)) => {
                            let got = got.expect("an unread message is visible");
                            prop_assert_eq!(got.len(), len);
                            prop_assert!(got.iter().all(|&b| b == tag), "message {} overwritten", tag);
                        }
                    }
                }
            }
        }
    }
}

/// A message this ring cannot hold is a caller bug, and the ring says so
/// rather than reporting a full ring the caller would park on forever.
#[test]
#[should_panic(expected = "exceeds this ring's")]
fn an_oversized_publish_panics() {
    unsafe {
        let ring = make_ring(64, 4, 8);
        let dcap = RingCursor::producer(ring).dcap();
        publish(ring, dcap as usize + 1, 0, |_| {});
    }
}

// -- slot retirement -----------------------------------------------------

/// `release_cursor` is the end of the longest released prefix of the slots
/// taken, whatever order they are released in.
#[test]
fn release_follows_the_front_consecutive_prefix() {
    const N: usize = 3 * INFLIGHT_INITIAL_CAP;
    let evens_back_then_odds = (0..N).filter(|i| i % 2 == 0).rev().chain((0..N).filter(|i| i % 2 == 1));
    for order in [
        (0..N).collect(),
        (0..N).rev().collect(),
        evens_back_then_odds.collect::<Vec<_>>(),
    ] {
        let ptr = make_ring(8, N, 8);
        let receiver = W2mReceiver::new(vec![ptr]);
        for i in 0..N {
            assert!(unsafe { publish(ptr, 8, 0, |s| s[0] = i as u8) }, "publish #{i}");
        }
        let mut slots: Vec<Option<W2mSlot>> = Vec::with_capacity(N);
        let mut vrcs = Vec::with_capacity(N);
        for _ in 0..N {
            slots.push(Some(receiver.try_read_slot(0).expect("slot")));
            vrcs.push(receiver.read_cursor(0));
        }
        let mut released = [false; N];
        for idx in order {
            slots[idx] = None;
            released[idx] = true;
            let p = released.iter().position(|&r| !r).unwrap_or(N);
            let expected = if p == 0 { W2M_HEADER_SIZE as u64 } else { vrcs[p - 1] };
            assert_eq!(receiver.release_cursor(0), expected, "front-consecutive prefix of {p}");
        }
    }
}

/// Dropping a slot advances release_cursor and unparks a blocked writer.
#[test]
fn a_retired_slot_unparks_the_writer() {
    let ptr = make_ring(CTRL_HEADER_SIZE, 1, 8);
    assert!(unsafe { publish(ptr, CTRL_HEADER_SIZE, 0, |s| s[0] = 1) });
    let ring = ptr as usize;
    within(Duration::from_secs(30), move || {
        let ptr = ring as *mut u8;
        let receiver = W2mReceiver::new(vec![ptr]);
        let writer = std::thread::spawn(move || {
            W2mWriter::new(ring as *mut u8).send_status(1, WireStatus::Ok, &[]);
        });
        while receiver.header(0).writer_park.flags.load(Ordering::Acquire) & FLAG_WRITER_PARKED == 0 {
            std::thread::yield_now();
        }
        drop(receiver.try_read_slot(0).expect("slot"));
        writer.join().expect("writer thread panicked");
    });
}

// -- park / wake ---------------------------------------------------------

/// The rings arm only while every one is quiet, and a publish takes its own
/// ring's arm.
#[test]
fn arm_waitv_arms_only_while_every_ring_is_quiet() {
    let (a, b) = (make_ring(64, 4, 8), make_ring(64, 4, 8));
    let receiver = W2mReceiver::new(vec![a, b]);
    let parked = || unsafe { [master_parked(a), master_parked(b)] };
    let mut out = [FutexWaitV::new(); 2];

    assert!(receiver.arm_waitv(&mut out).is_some(), "quiet rings arm");
    assert_eq!(parked(), [true, true]);
    assert!(unsafe { publish(b, 64, 0, |s| s[0] = 1) });
    assert_eq!(parked(), [true, false], "a publish takes its ring's arm");
    assert!(receiver.arm_waitv(&mut out).is_none(), "unread data refuses the arm");
    assert_eq!(parked(), [false, false], "and every ring is disarmed again");
}

/// An arm taken after a publish sees it, and survives it: only a later publish
/// takes the arm.
#[test]
fn a_publish_before_an_arm_leaves_it_armed() {
    let park = MasterPark { word: AtomicU64::new(0), _pad: 0 };
    park.publish(8);
    assert_eq!(park.arm(), 8);
    assert!(park.armed());
    park.publish(16);
    assert!(!park.armed());
}

/// Publish/drain throughput over one ring, and how many publishes spend a
/// `FUTEX_WAKE` — the quantity every change to the park protocol moves.
///
/// A forked child publishes `N` control frames as fast as it can while the
/// parent drains them, parking on the ring whenever it runs dry. Only the
/// ratios carry meaning: absolute rates swing with machine load. The master's
/// armed bit is sampled just before each publish's swap, so the wake count
/// approximates the syscalls without `strace`.
///
/// `cd crates && cargo test -p gnitz-server --release w2m_publish_drain_bench -- --ignored --nocapture --test-threads=1`
#[test]
#[ignore]
fn w2m_publish_drain_bench() {
    use std::hint::black_box;

    const N: u64 = 200_000;
    const RING_FRAMES: usize = 64;

    let ptr = make_ring(CTRL_HEADER_SIZE, RING_FRAMES, 8);
    // Two u64s the child fills before `_exit`: publishes that found a master
    // park armed, and the child's own elapsed nanos.
    let counters = SharedRegion::new(4096);
    let cptr = counters.ptr() as *mut u64;

    let child = || {
        let writer = W2mWriter::new(ptr);
        let hdr = unsafe { W2mRingHeader::from_raw(ptr) };
        let t = Instant::now();
        let mut woke_master = 0u64;
        for req in 1..=N {
            if hdr.master_park.armed() {
                woke_master += 1;
            }
            writer.send_status(req as u32, gnitz_wire::WireStatus::Ok, &[]);
        }
        unsafe {
            cptr.write(woke_master);
            cptr.add(1).write(t.elapsed().as_nanos() as u64);
        }
    };

    let pid = unsafe { fork_child(child) };

    let receiver = W2mReceiver::new(vec![ptr]);
    let t = Instant::now();
    let (mut drained, mut parks) = (0u64, 0u64);
    while drained < N {
        match receiver.try_read_slot(0) {
            Some(slot) => {
                black_box(slot.bytes());
                drained += 1;
            }
            None => {
                parks += 1;
                let mut waitv = [FutexWaitV::new(); 1];
                // A successful arm means the armed value equals the read cursor.
                if receiver.arm_waitv(&mut waitv).is_some() {
                    futex_wait_u32(
                        receiver.header(0).master_park.futex_word(),
                        receiver.read_cursor(0) as u32,
                        "bench",
                    );
                }
                receiver.clear_waitv();
            }
        }
    }
    let drain_ns = t.elapsed().as_nanos() as u64;

    unsafe { assert_child_exited_ok(pid) };
    let (woke_master, publish_ns) = unsafe { (cptr.read(), cptr.add(1).read()) };

    println!(
        "w2m publish/drain N={N} ring={RING_FRAMES} frames: \
         drain {:.2} Mmsg/s, publish {:.2} Mmsg/s, \
         {:.1}% of publishes spent a FUTEX_WAKE, {parks} drain parks ({:.1} per 1k msgs)",
        N as f64 * 1000.0 / drain_ns as f64,
        N as f64 * 1000.0 / publish_ns as f64,
        woke_master as f64 * 100.0 / N as f64,
        parks as f64 * 1000.0 / N as f64,
    );
}

// -- concurrent publish/drain --------------------------------------------

/// A writer thread's frames arrive once each, in publish order, blob intact,
/// across many wraps of a ring a few frames long.
#[test]
fn concurrent_publishes_drain_in_order() {
    for (ring_frames, n, pad) in [(8, 2_000u32, &[][..]), (4, 500, &[b'x'; 4000][..])] {
        let ptr = make_ring(CTRL_HEADER_SIZE + pad.len(), ring_frames, 8);
        let ring = ptr as usize;
        let pad_w = pad.to_vec();
        let writer = std::thread::spawn(move || {
            let writer = W2mWriter::new(ring as *mut u8);
            for req_id in 1..=n {
                writer.send_status(req_id, WireStatus::Ok, &pad_w);
            }
        });

        let receiver = W2mReceiver::new(vec![ptr]);
        let started = Instant::now();
        let mut next = 1;
        while next <= n {
            match receiver.try_read_slot(0) {
                Some(s) => {
                    assert_eq!(s.internal_req_id, next, "{n} frames: out of order");
                    assert_eq!(&s.bytes()[s.control().blob], pad, "{n} frames: blob of {next}");
                    next += 1;
                }
                None => {
                    assert!(
                        started.elapsed() < Duration::from_secs(30),
                        "{n} frames: stalled at {next}"
                    );
                    std::thread::yield_now();
                }
            }
        }
        writer.join().expect("writer thread");
    }
}

/// A slot names the ring it was read from, which is what attributes a reply to
/// its worker.
#[test]
fn a_slot_names_the_worker_whose_ring_it_was_read_from() {
    let (a, b) = (make_ring(CTRL_HEADER_SIZE, 2, 8), make_ring(CTRL_HEADER_SIZE, 2, 8));
    W2mWriter::new(b).send_status(7, WireStatus::Ok, b"");
    let receiver = W2mReceiver::new(vec![a, b]);
    assert!(receiver.try_read_slot(0).is_none());
    assert_eq!(receiver.try_read_slot(1).expect("worker 1's frame").worker, 1);
}

/// A park whose re-test behind the armed flag finds a group returns without
/// sleeping, and leaves the flag clear.
#[test]
fn a_sal_park_returns_at_once_when_its_retest_finds_a_group() {
    let ring = make_ring(CTRL_HEADER_SIZE, 2, 8);
    let hdr = unsafe { W2mRingHeader::from_raw(ring) };
    let mut armed_when_tested = false;
    W2mWriter::new(ring).sal_park().park(|| {
        armed_when_tested = hdr.sal_park.flags.load(Ordering::Acquire) & FLAG_SAL_PARKED != 0;
        false
    });
    assert!(armed_when_tested, "the re-test runs behind the armed flag");
    assert_eq!(hdr.sal_park.flags.load(Ordering::Acquire), 0, "and the park disarms");
}

/// A worker parked on an empty SAL wakes on another process's `SalWake`.
#[test]
fn a_parked_worker_wakes_on_a_forked_masters_sal_wake() {
    let ptr = make_ring(CTRL_HEADER_SIZE, 2, 8);
    let child = || {
        let hdr = unsafe { W2mRingHeader::from_raw(ptr) };
        while hdr.sal_park.flags.load(Ordering::Acquire) & FLAG_SAL_PARKED == 0 {
            std::thread::yield_now();
        }
        unsafe { SalWake::new(ptr) }.wake();
    };
    let pid = unsafe { fork_child(child) };
    let ring = ptr as usize;
    within(Duration::from_secs(30), move || {
        let ptr = ring as *mut u8;
        let seq = || unsafe { super::fixtures::sal_wake_seq(ptr) };
        // The wake sequence stands in for the SAL the worker re-tests.
        W2mWriter::new(ptr).sal_park().park(|| seq() == 0);
        assert_eq!(seq(), 1, "the park ended on the wake");
    });
    unsafe { assert_child_exited_ok(pid) };
}
