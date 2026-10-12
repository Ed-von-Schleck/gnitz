use super::fixtures::{make_ring, ring_capacity, test_rings};
use super::*;
use crate::runtime::test_support::within;
use gnitz_wire::control::CTRL_HEADER_SIZE;
use proptest::prelude::*;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

// -- ring layout ---------------------------------------------------------

/// The address a message published into an empty ring is read at.
fn data_head(writer: &W2mWriter) -> *const u8 {
    unsafe { writer.wc.base.add(W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize) }
}

/// A published message is read in place, and its slot's frame bytes are the
/// client framing as they stand: the `u32` LE length, then the payload.
#[test]
fn a_published_message_is_read_in_place_as_a_client_frame() {
    let (mut writer, recv) = make_ring(128, 4, 8);
    let payload: Vec<u8> = (0..128).collect();
    assert!(writer.try_publish(5, payload.len(), |slot| slot.copy_from_slice(&payload)));

    let slot = recv.try_read_slot(0).expect("message must be visible");
    assert_eq!(slot.internal_req_id, 5);
    assert_eq!(slot.frame_bytes(), [128u32.to_le_bytes().as_slice(), &payload].concat());
    assert_eq!(
        slot.bytes().as_ptr(),
        data_head(&writer),
        "the payload must be mmap-resident — a train frame's rows are read in place"
    );
    drop(slot);
    assert!(recv.try_read_slot(0).is_none(), "ring must now read empty");
}

/// The ring's message size: over zero slack, a run of them ends exactly on
/// the ring's physical end.
const MSG: usize = 256;

/// A ring its reader has emptied starts over at its head, wherever the write
/// position had got to, and at no cost in virtual bytes.
#[test]
fn an_empty_ring_starts_over_at_its_head() {
    let (mut writer, recv) = make_ring(MSG, 8, 8);
    for round in 0..3u8 {
        // Two a round: the second one is published into a ring that holds the first.
        for i in 0..2 {
            assert!(writer.try_publish(0, MSG, |slot| slot.fill(round * 2 + i)));
        }
        let first = recv.try_read_slot(0).expect("the first message");
        assert_eq!(
            first.bytes().as_ptr(),
            data_head(&writer),
            "round {round} starts at the head"
        );
        let second = recv.try_read_slot(0).expect("the second message");
        assert_eq!(second.bytes().as_ptr(), unsafe {
            data_head(&writer).add(slot_stride(MSG) as usize)
        });
        assert!(first.bytes().iter().all(|&b| b == round * 2));
        assert!(second.bytes().iter().all(|&b| b == round * 2 + 1));
    }
    assert_eq!(recv.write_cursor(0), 6 * slot_stride(MSG), "a rewind publishes no byte");
    assert_eq!(recv.release_cursor(0), recv.write_cursor(0));
}

/// The origin names a rewound message's offset before the write cursor shows
/// the message: a reader that sees the one has the other.
#[test]
fn a_rewind_moves_the_origin_before_it_publishes() {
    let (mut writer, recv) = make_ring(MSG, 8, 8);
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(1)));
    assert!(recv.try_read_slot(0).is_some());
    let at = recv.write_cursor(0);
    assert_eq!(recv.header(0).origin.load(Ordering::Acquire), 0);
    assert!(writer.try_publish(0, MSG, |slot| {
        assert_eq!(
            recv.header(0).origin.load(Ordering::Acquire),
            at,
            "the origin moves first"
        );
        assert_eq!(recv.write_cursor(0), at, "and the message is not yet published");
        slot.fill(2);
    }));
    assert_eq!(
        recv.try_read_slot(0).expect("slot").bytes().as_ptr(),
        data_head(&writer)
    );
}

/// A ring that still holds a slot does not start over: the slot's bytes are the
/// master's until it is released, however far behind the reader it is.
#[test]
fn a_held_slot_keeps_the_ring_from_starting_over() {
    let (mut writer, recv) = make_ring(MSG, 8, 8);
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(1)));
    let held = recv.try_read_slot(0).expect("the held message");
    for tag in 2..=4u8 {
        assert!(writer.try_publish(0, MSG, |slot| slot.fill(tag)));
        let next = recv.try_read_slot(0).expect("the next message");
        assert_ne!(
            next.bytes().as_ptr(),
            data_head(&writer),
            "message {tag} overwrote a held slot"
        );
        assert!(next.bytes().iter().all(|&b| b == tag));
    }
    assert!(held.bytes().iter().all(|&b| b == 1));
    drop(held);
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(5)));
    assert_eq!(
        recv.try_read_slot(0).expect("slot").bytes().as_ptr(),
        data_head(&writer)
    );
}

/// A message as large as the ring is refused while the ring holds anything, and
/// placed once it is empty, wherever the write position stands.
#[test]
fn a_message_filling_the_ring_is_placed_once_the_ring_is_empty() {
    let (mut writer, recv) = make_ring(MSG, 4, 0);
    let whole = (writer.wc.dcap() - RING_PREFIX_BYTES) as usize;
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(1)));
    assert!(
        !writer.try_publish(0, whole, |slot| slot.fill(2)),
        "the ring holds a message"
    );
    assert!(recv.try_read_slot(0).is_some());
    assert!(
        writer.try_publish(0, whole, |slot| slot.fill(2)),
        "an empty ring holds any message it can"
    );
    let got = recv.try_read_slot(0).expect("the whole-ring message");
    assert_eq!(got.bytes().len(), whole);
    assert!(got.bytes().iter().all(|&b| b == 2));
}

/// A message the tail is too short for is read at the head, and the tail it
/// skipped counts as published.
#[test]
fn a_message_short_of_the_tail_is_read_at_the_head_past_it() {
    let (mut writer, recv) = make_ring(MSG, 4, 8);
    let read = || recv.try_read_slot(0).expect("the next message");
    for tag in 1..=3u8 {
        assert!(writer.try_publish(0, MSG, |slot| slot.fill(tag)));
    }
    // Never empty: each publish finds the message before it unread.
    drop((read(), read()));
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(4)));
    drop(read());
    assert!(writer.try_publish(0, MSG, |slot| slot.fill(5)));
    assert_eq!(recv.write_cursor(0), 5 * slot_stride(MSG) + 8);

    assert!(read().bytes().iter().all(|&b| b == 4));
    let wrapped = read();
    assert_eq!(wrapped.bytes().as_ptr(), data_head(&writer));
    assert!(wrapped.bytes().iter().all(|&b| b == 5));
    assert_eq!(recv.read_cursor(0), recv.write_cursor(0));
}

#[derive(Clone, Debug)]
enum SlotOp {
    Publish(usize),
    Read,
    Release(usize),
}

proptest! {
    /// Slots are read in publish order and released in any order. A message
    /// stays intact for as long as its slot is held; an empty ring refuses
    /// nothing, and its next message is at the head.
    #[test]
    fn a_ring_keeps_every_unreleased_message_intact(
        slack in prop::sample::select(vec![0u64, 8, 16, 24]),
        ops in prop::collection::vec(
            prop_oneof![
                3 => prop_oneof![Just(MSG), 1..=3 * MSG].prop_map(SlotOp::Publish),
                3 => Just(SlotOp::Read),
                2 => (0usize..8).prop_map(SlotOp::Release),
            ],
            1..160,
        ),
    ) {
        let (mut writer, recv) = make_ring(MSG, 6, slack);
        let mut unread: VecDeque<(u8, usize, bool)> = VecDeque::new();
        let mut held: Vec<(W2mSlot, u8, usize)> = Vec::new();
        for (tag, op) in ops.into_iter().enumerate() {
            let tag = tag as u8;
            match op {
                SlotOp::Publish(len) => {
                    let empty = unread.is_empty() && held.is_empty();
                    if writer.try_publish(0, len, |slot| slot.fill(tag)) {
                        unread.push_back((tag, len, empty));
                    } else {
                        prop_assert!(!empty, "an empty ring refused a {}-byte publish", len);
                    }
                }
                SlotOp::Read => match (recv.try_read_slot(0), unread.pop_front()) {
                    (None, None) => {}
                    (Some(slot), Some((tag, len, at_head))) => {
                        if at_head {
                            prop_assert_eq!(slot.bytes().as_ptr(), data_head(&writer));
                        }
                        held.push((slot, tag, len));
                    }
                    (got, want) => prop_assert!(false, "read {:?}, expected {:?}", got.is_some(), want),
                },
                SlotOp::Release(i) => {
                    if !held.is_empty() {
                        held.swap_remove(i % held.len());
                    }
                }
            }
            for (slot, tag, len) in &held {
                prop_assert_eq!(slot.bytes().len(), *len);
                prop_assert!(slot.bytes().iter().all(|b| b == tag), "held message {} overwritten", tag);
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
/// from a ring a few frames long that the reader races.
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
