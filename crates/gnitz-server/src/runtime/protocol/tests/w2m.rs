use super::super::park::futex_wait_u32;
use super::fixtures::{make_ring, ring_capacity, test_rings};
use super::*;
use crate::runtime::test_support::{assert_child_exited_ok, fork_child, within};
use gnitz_wire::control::CTRL_HEADER_SIZE;
use proptest::prelude::*;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

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
        let (mut writer, recv) = make_ring(128, 4, 8);
        let payload: Vec<u8> = (0..128).collect();
        assert!(writer.try_publish(5, payload.len(), |slot| slot.copy_from_slice(&payload)));

        let slot = recv.try_read_slot(0).expect("message must be visible");
        assert_eq!(slot.internal_req_id, 5);
        assert_eq!(slot.frame_bytes(), [128u32.to_le_bytes().as_slice(), &payload].concat());
        assert_eq!(
            slot.bytes().as_ptr(),
            writer.wc.base.add(W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize) as *const u8,
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
        let (mut writer, recv) = make_ring(MSG, 4, slack);
        let mut unread: VecDeque<(u8, usize)> = VecDeque::new();
        for (tag, op) in ops.into_iter().enumerate() {
            let tag = tag as u8;
            match op {
                RingOp::Publish(len) => {
                    if writer.try_publish(0, len, |slot| slot.fill(tag)) {
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
    let (mut writer, _receiver) = make_ring(64, 4, 8);
    let dcap = writer.wc.dcap();
    writer.try_publish(0, dcap as usize + 1, |_| {});
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
        let (mut writer, receiver) = make_ring(8, N, 8);
        for i in 0..N {
            assert!(writer.try_publish(0, 8, |s| s[0] = i as u8), "publish #{i}");
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
            let expected = if p == 0 { 0 } else { vrcs[p - 1] };
            assert_eq!(receiver.release_cursor(0), expected, "front-consecutive prefix of {p}");
        }
    }
}

/// Dropping a slot advances release_cursor and unparks a blocked writer.
#[test]
fn a_retired_slot_unparks_the_writer() {
    within(|| {
        let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, 1, 8);
        assert!(writer.try_publish(0, CTRL_HEADER_SIZE, |s| s[0] = 1));
        let writer = std::thread::spawn(move || writer.send_ack(1));
        while !receiver.header(0).release.armed() {
            std::thread::yield_now();
        }
        drop(receiver.try_read_slot(0).expect("slot"));
        writer.join().expect("writer thread panicked");
    });
}

/// A release that frees too little leaves the writer waiting, and the release
/// that frees enough still wakes it: the writer re-arms after every wake.
#[test]
fn a_writer_short_of_room_after_a_release_waits_for_the_next() {
    within(|| {
        // Three small slots fill the ring; the big message needs all three back.
        let small = CTRL_HEADER_SIZE;
        let (mut writer, receiver) = make_ring(small, 3, 0);
        for id in 1..=3 {
            assert!(writer.try_publish(id, small, |s| s[0] = id as u8));
        }
        let big = (3 * slot_stride(small) - RING_PREFIX_BYTES) as usize;
        let writer = std::thread::spawn(move || {
            writer.send_msg(
                9,
                &WireMsg {
                    blob: &vec![7u8; big - CTRL_HEADER_SIZE],
                    ..Default::default()
                },
            )
        });
        let armed = || receiver.header(0).release.armed();
        for _ in 0..2 {
            while !armed() {
                std::thread::yield_now();
            }
            // Takes the arm; too little room, so the writer arms again.
            drop(receiver.try_read_slot(0).expect("slot"));
        }
        while !armed() {
            std::thread::yield_now();
        }
        drop(receiver.try_read_slot(0).expect("slot"));
        writer.join().expect("writer thread panicked");
        assert_eq!(receiver.try_read_slot(0).expect("the big message").internal_req_id, 9);
    });
}

// -- park / wake ---------------------------------------------------------

/// The rings arm only while every one is quiet, and a publish takes its own
/// ring's arm.
#[test]
fn arm_waitv_arms_only_while_every_ring_is_quiet() {
    let (mut writers, receiver) = test_rings(&[ring_capacity(64, 4, 8); 2]);
    let parked = |writers: &[W2mWriter]| [writers[0].master_parked(), writers[1].master_parked()];
    let mut out = [FutexWaitV::new(); 2];

    assert!(receiver.arm_waitv(&mut out).is_some(), "quiet rings arm");
    assert_eq!(parked(&writers), [true, true]);
    assert!(writers[1].try_publish(0, 64, |s| s[0] = 1));
    assert_eq!(parked(&writers), [true, false], "a publish takes its ring's arm");
    assert!(receiver.arm_waitv(&mut out).is_none(), "unread data refuses the arm");
    assert_eq!(parked(&writers), [false, false], "and every ring is disarmed again");
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

    let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, RING_FRAMES, 8);
    // Two u64s the child fills before `_exit`: publishes that found a master
    // park armed, and the child's own elapsed nanos.
    let cptr = map_anon_shared(4096).unwrap().cast::<u64>();

    let child = || {
        let t = Instant::now();
        let mut woke_master = 0u64;
        for req in 1..=N {
            if writer.master_parked() {
                woke_master += 1;
            }
            writer.send_ack(req as u32);
        }
        unsafe {
            cptr.write(woke_master);
            cptr.add(1).write(t.elapsed().as_nanos() as u64);
        }
    };

    let pid = unsafe { fork_child(child) };

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
                        receiver.header(0).write.futex_word(),
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
        let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE + pad.len(), ring_frames, 8);
        let pad_w = pad.to_vec();
        let writer = std::thread::spawn(move || {
            for req_id in 1..=n {
                writer.send_msg(req_id, &WireMsg { blob: &pad_w, ..Default::default() });
            }
        });

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
    let (mut writers, receiver) = test_rings(&[ring_capacity(CTRL_HEADER_SIZE, 2, 8); 2]);
    writers[1].send_ack(7);
    assert!(receiver.try_read_slot(0).is_none());
    assert_eq!(receiver.try_read_slot(1).expect("worker 1's frame").worker, 1);
}

/// `try_send_msg` on a full ring writes nothing and answers false; with room
/// again it sends.
#[test]
fn try_send_msg_on_a_full_ring_writes_nothing() {
    let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE, 2, 0);
    let msg = WireMsg { arg0: 7, ..Default::default() };
    assert!(
        writer.try_send_msg(1, &msg) && writer.try_send_msg(2, &msg),
        "two frames fit"
    );
    let full = receiver.write_cursor(0);
    assert!(!writer.try_send_msg(3, &msg), "the ring has no room for a third");
    assert_eq!(receiver.write_cursor(0), full, "a refused frame publishes nothing");

    let ids = |n: usize| -> Vec<u32> {
        (0..n)
            .map(|_| receiver.try_read_slot(0).expect("a frame").internal_req_id)
            .collect()
    };
    assert_eq!(ids(2), [1, 2]);
    assert!(receiver.try_read_slot(0).is_none(), "nothing of the refused frame");
    assert!(writer.try_send_msg(3, &msg), "the released slots make room");
    assert_eq!(ids(1), [3]);
}
