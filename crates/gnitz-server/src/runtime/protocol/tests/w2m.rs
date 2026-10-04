use super::fixtures::{make_ring, ring_capacity, test_rings};
use super::*;
use crate::runtime::test_support::within;
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
