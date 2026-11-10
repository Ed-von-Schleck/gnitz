//! The leases W2M frames are routed to: their id space, an ACK lease's answers
//! and a train lease's frame stream.

use super::super::test_support::*;
use super::*;
use crate::runtime::frame::WireMsg;
use crate::runtime::w2m::fixtures::id;

/// Lease ids wrap past 0, which no lease takes; a lease never takes an id a live
/// lease still routes, nor a dropped lease's straight back.
#[test]
fn lease_ids_wrap_past_zero_and_skip_live_ids() {
    let (r, _writers) = reactor_with_rings(1);
    let dropped = r.lease_acks().id();
    assert_ne!(r.lease_acks().id(), dropped);

    r.routes.borrow_mut().next_id = RequestId::MAX;
    let top = r.lease_acks();
    assert_eq!(top.id(), RequestId::MAX);
    let wrapped = r.lease_acks();
    assert_eq!(wrapped.id(), id(1), "the id after the largest is 1, not 0");

    r.routes.borrow_mut().next_id = id(1);
    let skipped = r.lease_train(WorkerSet::ALL, "ScanSpec");
    assert_eq!(skipped.id(), id(2), "a live lease's id is skipped");
}

/// An ACK lease waits on every launched worker, and keeps the ACKs drained
/// before anything awaits it.
#[test]
fn an_ack_lease_keeps_early_acks_and_resolves_on_every_workers() {
    let (r, mut writers) = reactor_with_rings(4);
    let lease = r.lease_acks();
    let id = lease.id();
    for w in [0, 1, 3] {
        writers[w].ack(id);
    }
    r.tick(); // drained before any awaiter
    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    r.spawn(async move {
        lease.acks().await;
        d.set(true);
    });
    r.tick();
    assert!(!done.get(), "three of the four have answered");
    writers[2].ack(id);
    r.tick(); // the drain wakes the awaiter, and the same pass polls it
    assert!(done.get());
}

/// A frame no lease routes is released at its ring undecoded: the frames a train
/// lease holds when it drops, and a late answer on a dropped lease's id, as an
/// abandoned request's late reply is. A late ACK on it is read off and dropped.
#[test]
fn a_frame_no_lease_holds_is_released() {
    let (r, lease, mut writers) = train_lease(1, "ScanSpec");
    let id = lease.id();
    for _ in 0..3 {
        writers[0].send_msg(id, &Default::default());
    }
    r.drain_all_w2m();
    assert!(r.w2m.release_cursor(0) < r.w2m.write_cursor(0), "the route holds them");
    drop(lease);
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));

    writers[0].send_msg(id, &Default::default());
    writers[0].ack(id);
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));
    assert!(r.w2m.acks(0).is_empty(), "the drain read the ACK");
}

/// ACKs resolve the lease each names whatever order they come in, as a group
/// applied inside a drive is ACKed ahead of the drive's own; and they take no
/// room a held reply frame pins.
#[test]
fn acks_resolve_their_own_leases_in_any_order_past_a_held_frame() {
    let (r, train, mut writers) = train_lease(1, "ScanSpec");
    let tick = r.lease_acks();
    let push = r.lease_acks();
    writers[0].send_train_frame(train.id(), false, None);
    r.drain_all_w2m();
    assert!(
        r.w2m.release_cursor(0) < r.w2m.write_cursor(0),
        "the train frame is held"
    );

    writers[0].ack(push.id());
    r.drain_all_w2m();
    assert!(try_poll_once(push.acks()).is_some(), "the later group's ACK came first");
    assert!(try_poll_once(tick.acks()).is_none(), "and resolved only its own lease");
    writers[0].ack(tick.id());
    r.drain_all_w2m();
    assert!(try_poll_once(tick.acks()).is_some());
}

/// A passing frame is copied out of its ring as it is routed, releasing its
/// space, unless its reader is parked on its worker; a frame sent in order
/// stays in place either way.
#[test]
fn a_passing_frame_is_copied_out_unless_its_reader_waits_on_its_worker() {
    let (r, lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    let last = WireMsg::train_frame(0, true, None);

    writers[1].try_send_msg(id, true, &last);
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(1), r.w2m.write_cursor(1), "no reader: copied out");

    let other = r.lease_train(WorkerSet::ALL, "ScanSpec");
    writers[1].send_msg(other.id(), &last);
    r.drain_all_w2m();
    assert!(
        r.w2m.release_cursor(1) < r.w2m.write_cursor(1),
        "an in-order frame stays in place"
    );
    drop(other);

    // The reader parks on worker 0, then takes the passing frame from it.
    let mut wait = Box::pin(lease.frame_of(0));
    assert!(try_poll_once(wait.as_mut()).is_none(), "nothing from worker 0 yet");
    writers[0].try_send_msg(id, true, &last);
    r.drain_all_w2m();
    assert!(
        r.w2m.release_cursor(0) < r.w2m.write_cursor(0),
        "its reader takes it in place"
    );
    let (slot, _) = try_poll_once(wait)
        .expect("routed")
        .expect("no fault")
        .expect("a frame");
    assert!(slot.in_ring());
    drop(slot);
    let (slot, _) = try_poll_once(lease.frame_of(1))
        .expect("routed")
        .expect("no fault")
        .expect("a frame");
    assert!(!slot.in_ring(), "worker 1's frame was copied");
    assert_eq!(slot.bytes(), last.encode_to_vec());
}

/// A fault frame is its worker's last: the lease reports it once, then reads
/// the remaining workers' trains without waiting on the faulted one again.
#[test]
fn a_fault_frame_ends_its_workers_train() {
    let (r, lease, mut writers) = train_lease(2, "ScanSpec");
    let fault = gnitz_wire::WireFault::from("no such store");
    writers[0].send_msg(lease.id(), &WireMsg::fault(&fault));
    writers[1].send_msg(lease.id(), &WireMsg::train_frame(0, true, None));
    r.drain_all_w2m();

    let err = try_poll_once(lease.next())
        .expect("routed")
        .map(drop)
        .expect_err("worker 0 faulted");
    assert!(err.text.starts_with("worker 0: ScanSpec"), "{err}");
    assert!(
        matches!(try_poll_once(lease.frame_of(0)), Some(Ok(None))),
        "worker 0 owes no further frame"
    );
    try_poll_once(lease.finish())
        .expect("only worker 1 is waited on")
        .expect("no second fault");
}

/// A wait dropped while parked on one worker no longer counts as the reader
/// parked there once the reader parks on another.
#[test]
fn a_dropped_wait_stops_counting_once_the_reader_parks_elsewhere() {
    let (r, lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    let mut wait = Box::pin(lease.frame_of(0));
    assert!(try_poll_once(wait.as_mut()).is_none());
    drop(wait);
    let mut elsewhere = Box::pin(lease.frame_of(1));
    assert!(try_poll_once(elsewhere.as_mut()).is_none());
    writers[0].try_send_msg(id, true, &WireMsg::train_frame(0, true, None));
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0), "copied out");
}

/// A ring given back has every frame a lease holds queued copied out of it,
/// releasing its space, and holds the lease's train on that worker until its
/// reader asks for a frame past the copies; a dropped lease ends its holds.
#[test]
fn a_ring_given_back_copies_queued_frames_and_holds_their_train() {
    let (r, lease, mut writers) = train_lease(1, "ScanSpec");
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(7, 1, 0)]);
    writers[0].send_train_frame(id, false, Some(&rows));
    writers[0].send_train_frame(id, false, Some(&rows));
    r.drain_all_w2m();
    assert!(r.w2m.release_cursor(0) < r.w2m.write_cursor(0), "queued in place");

    writers[0].name_shortage();
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0), "copied out");
    assert!(writers[0].held(id));
    for _ in 0..2 {
        let f = try_poll_once(lease.next())
            .expect("routed")
            .expect("no fault")
            .expect("a row frame");
        assert!(!f.slot.in_ring());
        assert_eq!(f.rows(&schema).get_pk_bytes(0), 7u64.to_be_bytes());
    }
    assert!(writers[0].held(id), "taking the copies keeps the hold");
    assert!(try_poll_once(lease.next()).is_none());
    assert!(!writers[0].held(id), "asking past them ends it");

    writers[0].send_train_frame(id, false, Some(&rows));
    writers[0].name_shortage();
    r.drain_all_w2m();
    assert!(writers[0].held(id));
    drop(lease);
    assert!(!writers[0].held(id), "a dropped lease holds nothing");
}

/// A ring given back while its worker holds as many leases as it can still has
/// a lease's queued frames copied out, without a hold: the worker sends that
/// lease on, and the next shortage its frames cause copies them again, holding
/// the train once the table has room.
#[test]
fn a_give_back_past_the_hold_table_copies_without_a_hold() {
    let (r, lease, mut writers) = train_lease(1, "ScanSpec");
    let train = lease.id();
    let fillers: Vec<RequestId> = (0..)
        .map(|n| id(10_000 + n))
        .take_while(|&filler| r.w2m.hold(0, filler))
        .collect();
    assert!(!fillers.is_empty(), "the table takes holds until it is full");
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(7, 1, 0)]);

    writers[0].send_train_frame(train, false, Some(&rows));
    writers[0].name_shortage();
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0), "copied out");
    assert!(!writers[0].held(train), "with the table full, nothing holds the lease");

    r.w2m.unhold(0, fillers[0]);
    writers[0].send_train_frame(train, false, Some(&rows));
    writers[0].name_shortage();
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0), "copied out again");
    assert!(writers[0].held(train), "a freed entry takes the hold");
    for filler in &fillers[1..] {
        r.w2m.unhold(0, *filler);
    }
}

/// A dropped train lease's queue box is emptied and taken by the next train
/// lease: a lease read to its end, then one dropped with frames still queued,
/// leave the lease after them only its own frames, and the dropped frames'
/// ring space released.
#[test]
fn a_reused_queue_box_holds_only_its_own_leases_frames() {
    let (r, mut writers) = reactor_with_rings(3);
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = |pk| crate::test_support::make_batch(&schema, &[(pk, 1, 0)]);
    let read_all = |lease: &TrainLease| {
        let mut pks = Vec::new();
        while let Some(f) = try_poll_once(lease.next()).expect("routed").expect("no fault") {
            pks.push(u64::from_be_bytes(f.rows(&schema).get_pk_bytes(0).try_into().unwrap()));
        }
        pks
    };

    let read = r.lease_train(WorkerSet::one(1), "ScanSpec");
    writers[1].send_train_frame(read.id(), true, Some(&rows(1)));
    r.drain_all_w2m();
    assert_eq!(read_all(&read), [1]);
    drop(read);
    assert_eq!(r.routes.borrow().spare.len(), 1, "the box is kept");

    let unread = r.lease_train(WorkerSet::one(0).with(2), "ScanSpec");
    assert!(r.routes.borrow().spare.is_empty(), "the kept box is taken");
    for w in [0, 2] {
        writers[w].send_train_frame(unread.id(), false, Some(&rows(2)));
    }
    r.drain_all_w2m();
    assert!(r.w2m.release_cursor(0) < r.w2m.write_cursor(0), "queued in place");
    drop(unread);
    for w in [0, 2] {
        assert_eq!(
            r.w2m.release_cursor(w),
            r.w2m.write_cursor(w),
            "worker {w}'s frames released"
        );
    }

    let next = r.lease_train(WorkerSet::ALL, "ScanSpec");
    assert!(r.routes.borrow().spare.is_empty(), "the kept box is taken");
    for (w, writer) in writers.iter_mut().enumerate() {
        writer.send_train_frame(next.id(), true, (w == 2).then(|| rows(3)).as_ref());
    }
    r.drain_all_w2m();
    assert_eq!(read_all(&next), [3], "only its own frames");
}
