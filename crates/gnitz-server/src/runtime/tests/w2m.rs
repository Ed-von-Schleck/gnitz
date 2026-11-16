use super::fixtures::{id, make_ring, ring_capacity, test_rings};
use super::*;
use crate::runtime::test_support::within;
use gnitz_wire::CTRL_HEADER_SIZE;
use proptest::prelude::*;
use std::collections::VecDeque;

// -- ring layout ---------------------------------------------------------

/// The address a message published into an empty ring is read at.
fn data_head(writer: &W2mWriter) -> *const u8 {
    unsafe { writer.wc.base.add(W2M_HEADER_SIZE + RING_PREFIX_BYTES as usize) }
}

/// The ring's message size: over zero slack, a run of them ends exactly on
/// the ring's physical end.
const MSG: usize = 256;

/// A ring its reader has emptied starts over at its head, wherever the write
/// position had got to, and at no cost in virtual bytes. The origin names the
/// rewound message's offset before the write cursor shows the message, and a
/// message is read in place, its frame bytes the client framing as they stand.
#[test]
fn an_empty_ring_starts_over_at_its_head() {
    let (mut writer, recv) = make_ring(MSG, 8, 8);
    for round in 0..3u8 {
        let at = recv.write_cursor(0);
        assert!(
            writer
                .try_publish(id(round as u32 + 1), false, MSG, |slot| {
                    let origin = recv.header(0).origin.load(Ordering::Acquire);
                    assert_eq!(origin, at, "round {round}: the origin moves first");
                    assert_eq!(recv.write_cursor(0), at, "and the message is not yet published");
                    slot.fill(round * 2);
                })
                .is_ok()
        );
        // The second one is published into a ring that holds the first.
        assert!(
            writer
                .try_publish(id(1), false, MSG, |slot| slot.fill(round * 2 + 1))
                .is_ok()
        );
        let first = recv.try_read_slot(0).expect("the first message");
        assert_eq!(first.request_id.get(), round as u32 + 1);
        assert_eq!(first.frame_bytes(), gnitz_wire::framed(&[round * 2; MSG]));
        assert_eq!(
            first.bytes().as_ptr(),
            data_head(&writer),
            "round {round} starts at the head"
        );
        let second = recv.try_read_slot(0).expect("the second message");
        assert_eq!(second.bytes().as_ptr(), unsafe {
            data_head(&writer).add(slot_stride(MSG) as usize)
        });
        assert!(second.bytes().iter().all(|&b| b == round * 2 + 1));
    }
    assert_eq!(recv.write_cursor(0), 6 * slot_stride(MSG), "a rewind publishes no byte");
    assert_eq!(recv.release_cursor(0), recv.write_cursor(0));
}

/// A message the tail is too short for is read at the head, and the tail it
/// skipped counts as published.
#[test]
fn a_message_short_of_the_tail_is_read_at_the_head_past_it() {
    let (mut writer, recv) = make_ring(MSG, 4, 8);
    let read = || recv.try_read_slot(0).expect("the next message");
    for tag in 1..=3u8 {
        assert!(writer.try_publish(id(1), false, MSG, |slot| slot.fill(tag)).is_ok());
    }
    // Never empty: each publish finds the message before it unread.
    drop((read(), read()));
    assert!(writer.try_publish(id(1), false, MSG, |slot| slot.fill(4)).is_ok());
    drop(read());
    assert!(writer.try_publish(id(1), false, MSG, |slot| slot.fill(5)).is_ok());
    assert_eq!(recv.write_cursor(0), 5 * slot_stride(MSG) + 8);

    assert!(read().bytes().iter().all(|&b| b == 4));
    let wrapped = read();
    assert_eq!(wrapped.bytes().as_ptr(), data_head(&writer));
    assert!(wrapped.bytes().iter().all(|&b| b == 5));
    assert!(recv.try_read_slot(0).is_none());
}

/// The oldest publish the master has not released names the lease it answers
/// on, wherever it was placed — at the head of an emptied ring, behind the
/// publish before it, or at the head past a tail it skipped — and whether or
/// not it was published as passing. A slot released behind a held one frees
/// nothing, and a ring released whole names no lease.
#[test]
fn the_oldest_unreleased_publish_names_its_lease() {
    let (mut writer, recv) = make_ring(MSG, 4, 8);
    let read = || recv.try_read_slot(0).expect("the next message");
    let publish = |writer: &mut W2mWriter, n: u32| {
        assert!(writer.try_publish(id(n), n % 2 == 1, MSG, |slot| slot.fill(0)).is_ok());
    };
    assert_eq!(writer.oldest_unreleased(), None, "nothing was published");

    for n in 1..=3 {
        publish(&mut writer, n);
    }
    assert_eq!(writer.oldest_unreleased(), Some(id(1)), "at the head of the ring");
    let (first, second, third) = (read(), read(), read());
    drop(second);
    assert_eq!(writer.oldest_unreleased(), Some(id(1)), "the first is still held");
    drop(first);
    assert_eq!(writer.oldest_unreleased(), Some(id(3)), "behind the two released");

    // The fourth ends 8 bytes short of the ring's end, so the fifth skips them.
    publish(&mut writer, 4);
    publish(&mut writer, 5);
    assert_eq!(recv.write_cursor(0), 5 * slot_stride(MSG) + 8);
    drop(third);
    assert_eq!(writer.oldest_unreleased(), Some(id(4)));
    drop(read());
    assert_eq!(
        writer.oldest_unreleased(),
        Some(id(5)),
        "at the head, past the tail skipped"
    );
    drop(read());
    assert_eq!(writer.oldest_unreleased(), None, "a ring released whole");

    publish(&mut writer, 6);
    assert_eq!(recv.header(0).origin.load(Ordering::Acquire), 5 * slot_stride(MSG) + 8);
    assert_eq!(
        writer.oldest_unreleased(),
        Some(id(6)),
        "at the head of the ring started over"
    );
}

#[derive(Clone, Debug)]
enum SlotOp {
    Publish(usize),
    Read,
    Release(usize),
}

proptest! {
    /// Slots are read in publish order and released in any order. A message
    /// stays intact for as long as its slot is held, so a ring holding one never
    /// starts over; an empty ring refuses nothing, a message as large as the ring
    /// included, and its next message is at the head.
    #[test]
    fn a_ring_keeps_every_unreleased_message_intact(
        slack in prop::sample::select(vec![0u64, 8, 16, 24]),
        ops in prop::collection::vec(
            prop_oneof![
                3 => prop_oneof![Just(MSG), 1..=3 * MSG, Just(usize::MAX)].prop_map(SlotOp::Publish),
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
                    let len = len.min((writer.wc.dcap() - RING_PREFIX_BYTES) as usize);
                    let empty = unread.is_empty() && held.is_empty();
                    if writer.try_publish(id(1), false, len, |slot| slot.fill(tag)).is_ok() {
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
    let _ = writer.try_publish(id(1), false, dcap as usize + 1, |_| {});
}

// -- slot retirement -----------------------------------------------------

/// `release_cursor` is the end of the longest released prefix of the slots
/// taken, whatever order they are released in.
#[test]
fn release_follows_the_front_consecutive_prefix() {
    const N: usize = 192;
    let evens_back_then_odds = (0..N).filter(|i| i % 2 == 0).rev().chain((0..N).filter(|i| i % 2 == 1));
    for order in [
        (0..N).collect(),
        (0..N).rev().collect(),
        evens_back_then_odds.collect::<Vec<_>>(),
    ] {
        let (mut writer, receiver) = make_ring(8, N, 8);
        for i in 0..N {
            assert!(
                writer.try_publish(id(1), false, 8, |s| s[0] = i as u8).is_ok(),
                "publish #{i}"
            );
        }
        // The ring never wraps, so slot `i` ends at `(i + 1)` strides.
        let mut slots: Vec<Option<W2mSlot>> = (0..N).map(|_| receiver.try_read_slot(0)).collect();
        for idx in order {
            slots[idx] = None;
            let p = slots.iter().position(Option::is_some).unwrap_or(N);
            assert_eq!(
                receiver.release_cursor(0),
                p as u64 * slot_stride(8),
                "front-consecutive prefix of {p}"
            );
        }
    }
}

/// A release that frees too little leaves the writer waiting, and the release
/// that frees enough still wakes it: the writer re-arms after every wake.
#[test]
fn a_writer_short_of_room_after_a_release_waits_for_the_next() {
    within(|| {
        // Three small slots fill the ring; the big message needs all three back.
        let small = CTRL_HEADER_SIZE;
        let (mut writer, receiver) = make_ring(small, 3, 0);
        for n in 1..=3 {
            assert!(writer.try_publish(id(n), false, small, |s| s[0] = n as u8).is_ok());
        }
        let big = (3 * slot_stride(small) - RING_PREFIX_BYTES) as usize;
        let writer = std::thread::spawn(move || {
            writer.send_msg(
                id(9),
                &WireMsg {
                    blob: &vec![7u8; big - CTRL_HEADER_SIZE],
                    ..Default::default()
                },
            )
        });
        // Each release takes the arm; the first two free too little, so the
        // writer arms again.
        for _ in 0..3 {
            while !receiver.header(0).release.armed() {
                std::thread::yield_now();
            }
            drop(receiver.try_read_slot(0).expect("slot"));
        }
        writer.join().expect("writer thread panicked");
        assert_eq!(receiver.try_read_slot(0).expect("the big message").request_id.get(), 9);
    });
}

// -- park / wake ---------------------------------------------------------

/// The master's park arms only while every ring is quiet, and the arm it waits
/// on moves with the one publish, on either ring, that takes it. Its slot names
/// the ring it was read from.
#[test]
fn the_master_park_arms_only_while_every_ring_is_quiet() {
    let (mut writers, receiver) = test_rings(2, ring_capacity(64, 4, 8));

    for w in 0..2 {
        receiver.arm().expect("quiet rings arm");
        assert!(writers[w].master_parked());
        let wakes = receiver.wakes();
        assert!(writers[w].try_publish(id(1), false, 64, |s| s[0] = 1).is_ok());
        assert!(!writers[1 - w].master_parked(), "ring {w}'s publish took the one arm");
        assert_eq!(receiver.wakes(), wakes + 1, "the take moved the waited value");
        assert!(writers[1 - w].try_publish(id(1), false, 64, |s| s[0] = 2).is_ok());
        assert_eq!(receiver.wakes(), wakes + 1, "a publish behind the take wakes nobody");

        assert!(receiver.arm().is_none(), "unread data refuses the arm");
        assert!(!writers[0].master_parked(), "and leaves the park disarmed");
        assert_eq!(receiver.try_read_slot(0).expect("ring 0's publish").worker, 0);
        assert!(receiver.arm().is_none(), "ring 1's unread data refuses it too");
        assert_eq!(receiver.try_read_slot(1).expect("ring 1's publish").worker, 1);
    }
}

// -- concurrent publish/drain --------------------------------------------

/// A writer thread's frames arrive once each, in publish order, blob intact,
/// from a ring a few frames long that the reader races.
#[test]
fn concurrent_publishes_drain_in_order() {
    for (ring_frames, n, pad) in [(8, 2_000u32, &[][..]), (4, 500, &[b'x'; 4000][..])] {
        within(move || {
            let (mut writer, receiver) = make_ring(CTRL_HEADER_SIZE + pad.len(), ring_frames, 8);
            let writer = std::thread::spawn(move || {
                for req_id in 1..=n {
                    writer.send_msg(id(req_id), &WireMsg { blob: pad, ..Default::default() });
                }
            });
            let mut next = 1;
            while next <= n {
                match receiver.try_read_slot(0) {
                    Some(s) => {
                        assert_eq!(s.request_id.get(), next, "{n} frames: out of order");
                        assert_eq!(&s.bytes()[s.control().blob], pad, "{n} frames: blob of {next}");
                        next += 1;
                    }
                    None => std::thread::yield_now(),
                }
            }
            writer.join().expect("writer thread");
        });
    }
}
