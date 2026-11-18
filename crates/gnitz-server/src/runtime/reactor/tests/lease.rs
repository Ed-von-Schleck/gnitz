//! What a W2M drain feeds: the applied level an ACK wait parks on, and the
//! train leases frames are routed to, their id space and their frame stream.

use super::super::test_support::*;
use super::*;
use crate::runtime::frame::WireMsg;
use crate::runtime::w2m::fixtures::id;

/// A lease on `r` of one read per set of `reads`, each written to its set.
fn lease_of(r: &Reactor, reads: &[WorkerSet]) -> TrainLease {
    let mut lease = r.lease_train();
    for &set in reads {
        lease.add(set, "ScanSpec");
    }
    lease
}

/// Lease ids wrap past 0, which no lease takes; a lease never takes an id a live
/// lease still routes, nor a dropped lease's straight back.
#[test]
fn lease_ids_wrap_past_zero_and_skip_live_ids() {
    let (r, _writers) = reactor_with_rings(1);
    let lease = || r.lease_train();
    let dropped = lease().id();
    assert_eq!(dropped, id(1), "the first id is 1, not 0");
    assert_ne!(lease().id(), dropped);

    r.routes.borrow_mut().next_id = RequestId::MAX;
    let top = lease();
    assert_eq!(top.id(), RequestId::MAX);
    let wrapped = lease();
    assert_eq!(wrapped.id(), id(1), "the id after the largest is 1, not 0");

    r.routes.borrow_mut().next_id = id(1);
    let skipped = r.lease_train();
    assert_eq!(skipped.id(), id(2), "a live lease's id is skipped");
}

/// The applied level is the lowest position any launched worker has published,
/// and keeps what was drained before anything awaits it.
#[test]
fn the_applied_level_is_the_lowest_of_every_workers_and_keeps_early_positions() {
    let (r, mut writers) = reactor_with_rings(4);
    for w in [0, 1, 3] {
        writers[w].applied(7);
    }
    r.tick(); // drained before any awaiter
    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    let wait = r.applied_through(7);
    r.spawn(async move {
        wait.acks().await;
        d.set(true);
    });
    r.tick();
    assert!(!done.get(), "three of the four have published");
    writers[2].applied(5);
    r.tick();
    assert!(!done.get(), "the fourth is short of the position");
    writers[2].applied(9);
    r.tick(); // the drain wakes the awaiter, and the same pass polls it
    assert!(done.get());
    assert_eq!(r.applied.get(), 7, "the three at 7 hold the level there");
}

/// A reactor over no worker has every position applied, with no drain run.
#[test]
fn a_reactor_over_no_worker_awaits_no_position() {
    let (r, _) = reactor_with_rings(0);
    assert!(try_poll_once(r.applied_through(u64::MAX).acks()).is_some());
}

/// A frame no lease routes is released at its ring undecoded: the frames a train
/// lease holds when it drops, and a late answer on a dropped lease's id, as an
/// abandoned request's late reply is.
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
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));
}

/// A position takes no room a held reply frame pins: the drain reads it past
/// the frame.
#[test]
fn a_position_passes_a_held_frame() {
    let (r, lease, mut writers) = train_lease(1, "ScanSpec");
    writers[0].send_train_frame(lease.id(), false, None);
    r.drain_all_w2m();
    assert!(
        r.w2m.release_cursor(0) < r.w2m.write_cursor(0),
        "the train frame is held"
    );

    writers[0].applied(3);
    r.drain_all_w2m();
    assert!(try_poll_once(r.applied_through(3).acks()).is_some());
    assert!(try_poll_once(r.applied_through(4).acks()).is_none());
}

/// A passing frame is copied out of its ring as it is routed, releasing its
/// space, unless its reader is parked on its worker; a frame sent in order
/// stays in place either way.
#[test]
fn a_passing_frame_is_copied_out_unless_its_reader_waits_on_its_worker() {
    let (r, mut lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    let train = train_of(&mut lease);
    let last = WireMsg::train_frame(0, true, None);

    writers[1].try_send_msg(id, true, &last).expect("room");
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(1), r.w2m.write_cursor(1), "no reader: copied out");

    let other = lease_of(&r, &[WorkerSet::ALL]);
    writers[1].send_msg(other.id(), &last);
    r.drain_all_w2m();
    assert!(
        r.w2m.release_cursor(1) < r.w2m.write_cursor(1),
        "an in-order frame stays in place"
    );
    drop(other);

    // The reader parks on worker 0, then takes the passing frame from it.
    let mut wait = Box::pin(train.frame_of(0));
    assert!(try_poll_once(wait.as_mut()).is_none(), "nothing from worker 0 yet");
    writers[0].try_send_msg(id, true, &last).expect("room");
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
    let (slot, _) = try_poll_once(train.frame_of(1))
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
    let (r, mut lease, mut writers) = train_lease(2, "ScanSpec");
    let fault = gnitz_wire::WireFault::from("no such store");
    writers[0].send_msg(lease.id(), &WireMsg::fault(&fault));
    writers[1].send_msg(lease.id(), &WireMsg::train_frame(0, true, None));
    r.drain_all_w2m();

    let train = train_of(&mut lease);
    let err = try_poll_once(train.next())
        .expect("routed")
        .map(drop)
        .expect_err("worker 0 faulted");
    assert!(err.text.starts_with("worker 0: ScanSpec"), "{err}");
    assert!(
        matches!(try_poll_once(train.frame_of(0)), Some(Ok(None))),
        "worker 0 owes no further frame"
    );
    try_poll_once(train.finish())
        .expect("only worker 1 is waited on")
        .expect("no second fault");
}

/// A wait dropped while parked on one worker no longer counts as the reader
/// parked there once the reader parks on another.
#[test]
fn a_dropped_wait_stops_counting_once_the_reader_parks_elsewhere() {
    let (r, mut lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    let train = train_of(&mut lease);
    let mut wait = Box::pin(train.frame_of(0));
    assert!(try_poll_once(wait.as_mut()).is_none());
    drop(wait);
    let mut elsewhere = Box::pin(train.frame_of(1));
    assert!(try_poll_once(elsewhere.as_mut()).is_none());
    writers[0]
        .try_send_msg(id, true, &WireMsg::train_frame(0, true, None))
        .expect("room");
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0), "copied out");
}

/// A ring given back has every frame a lease holds queued copied out of it,
/// releasing its space, and holds the lease's train on that worker until its
/// reader asks for a frame past the copies; a dropped lease ends its holds.
#[test]
fn a_ring_given_back_copies_queued_frames_and_holds_their_train() {
    let (r, mut lease, mut writers) = train_lease(1, "ScanSpec");
    let id = lease.id();
    let train = train_of(&mut lease);
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
        let f = try_poll_once(train.next())
            .expect("routed")
            .expect("no fault")
            .expect("a row frame");
        assert!(!f.slot.in_ring());
        assert_eq!(f.rows(&schema).get_pk_bytes(0), 7u64.to_be_bytes());
    }
    assert!(writers[0].held(id), "taking the copies keeps the hold");
    assert!(try_poll_once(train.next()).is_none());
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
    let read_all = |lease: &mut TrainLease| {
        let train = train_of(lease);
        let mut pks = Vec::new();
        while let Some(f) = try_poll_once(train.next()).expect("routed").expect("no fault") {
            pks.push(u64::from_be_bytes(f.rows(&schema).get_pk_bytes(0).try_into().unwrap()));
        }
        pks
    };

    let mut read = lease_of(&r, &[WorkerSet::one(1)]);
    writers[1].send_train_frame(read.id(), true, Some(&rows(1)));
    r.drain_all_w2m();
    assert_eq!(read_all(&mut read), [1]);
    drop(read);
    assert_eq!(r.routes.borrow().spare.len(), 1, "the box is kept");

    let unread = lease_of(&r, &[WorkerSet::one(0).with(2)]);
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

    let mut next = lease_of(&r, &[WorkerSet::ALL]);
    assert!(r.routes.borrow().spare.is_empty(), "the kept box is taken");
    for (w, writer) in writers.iter_mut().enumerate() {
        writer.send_train_frame(next.id(), true, (w == 2).then(|| rows(3)).as_ref());
    }
    r.drain_all_w2m();
    assert_eq!(read_all(&mut next), [3], "only its own frames");
}

/// One lease's trains on a worker are split at their terminals: each read's
/// train hands out the frames its own workers sent for it, rows and weights,
/// and the frames of the read behind it stay queued until that read is taken.
#[test]
fn a_leases_trains_split_at_their_terminals() {
    let (r, mut writers) = reactor_with_rings(3);
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = |pk: u64, weight: i64| crate::test_support::make_batch(&schema, &[(pk, weight, 0)]);
    let read_all = |train: Train<'_>| {
        let mut got = Vec::new();
        while let Some(f) = try_poll_once(train.next()).expect("routed").expect("no fault") {
            let rows = f.rows(&schema);
            let pk = u64::from_be_bytes(rows.get_pk_bytes(0).try_into().unwrap());
            got.push((f.slot.worker, pk, rows.get_weight(0)));
        }
        got
    };

    let (first, second) = (WorkerSet::one(0).with(1), WorkerSet::one(1).with(2));
    let mut lease = lease_of(&r, &[first, second]);
    let id = lease.id();
    // Worker 1 answers both reads, and has sent the second's train before
    // worker 0 has sent anything of the first's.
    writers[1].send_train_frame(id, true, Some(&rows(3, -2)));
    writers[1].send_train_frame(id, false, Some(&rows(4, 5)));
    writers[2].send_train_frame(id, true, Some(&rows(6, 1)));
    writers[1].send_train_frame(id, true, Some(&rows(5, -7)));
    r.drain_all_w2m();
    writers[0].send_train_frame(id, false, Some(&rows(1, 2)));
    writers[0].send_train_frame(id, true, Some(&rows(2, 3)));
    r.drain_all_w2m();

    let train = train_of(&mut lease);
    assert_eq!(train.workers(), first);
    assert_eq!(read_all(train), [(0, 1, 2), (0, 2, 3), (1, 3, -2)]);
    let train = train_of(&mut lease);
    assert_eq!(train.workers(), second);
    assert_eq!(read_all(train), [(1, 4, 5), (1, 5, -7), (2, 6, 1)]);
}

/// A read given up mid-train is read off before the next one is taken: every
/// worker's train of it to its terminal, a fault frame included. The next
/// read's train hands out its own frames alone, and the frames read off hold
/// no ring space.
#[test]
fn a_given_up_trains_remainder_is_read_off_before_the_next() {
    let (r, mut writers) = reactor_with_rings(2);
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = |pk: u64| crate::test_support::make_batch(&schema, &[(pk, 1, 0)]);
    let mut lease = lease_of(&r, &[WorkerSet::ALL, WorkerSet::ALL]);
    let id = lease.id();

    let fault = gnitz_wire::WireFault::from("no such store");
    writers[0].send_msg(id, &WireMsg::fault(&fault));
    writers[0].send_train_frame(id, true, Some(&rows(10)));
    writers[1].send_train_frame(id, false, Some(&rows(1)));
    writers[1].send_train_frame(id, false, Some(&rows(2)));
    r.drain_all_w2m();

    let train = train_of(&mut lease);
    let err = try_poll_once(train.next())
        .expect("routed")
        .map(drop)
        .expect_err("worker 0 faulted");
    assert!(err.text.starts_with("worker 0: ScanSpec"), "{err}");

    // Worker 1 has not ended the read given up: the next one waits for it.
    let mut taking = Box::pin(lease.next_train());
    assert!(try_poll_once(taking.as_mut()).is_none());
    writers[1].send_train_frame(id, true, Some(&rows(3)));
    writers[1].send_train_frame(id, true, Some(&rows(11)));
    r.drain_all_w2m();
    let train = try_poll_once(taking).expect("the read given up has ended on every worker");

    let mut pks = Vec::new();
    while let Some(f) = try_poll_once(train.next()).expect("routed").expect("no fault") {
        pks.push(u64::from_be_bytes(f.rows(&schema).get_pk_bytes(0).try_into().unwrap()));
    }
    assert_eq!(pks, [10, 11], "only its own frames");
    for w in 0..2 {
        assert_eq!(
            r.w2m.release_cursor(w),
            r.w2m.write_cursor(w),
            "worker {w}'s frames released"
        );
    }
}

/// A hold is the lease's on its worker, whichever of its reads the copied
/// frames answer: it ends when the read being taken waits on that worker past
/// the copies.
#[test]
fn a_hold_ends_when_the_read_being_taken_waits_on_its_worker() {
    let (r, mut writers) = reactor_with_rings(1);
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(7, 1, 0)]);
    let mut lease = lease_of(&r, &[WorkerSet::ALL, WorkerSet::ALL]);
    let id = lease.id();
    // All of the first read's train, and the start of the second's.
    writers[0].send_train_frame(id, true, Some(&rows));
    writers[0].send_train_frame(id, false, Some(&rows));
    r.drain_all_w2m();
    writers[0].name_shortage();
    r.drain_all_w2m();
    assert!(writers[0].held(id));

    let train = train_of(&mut lease);
    assert!(matches!(try_poll_once(train.next()), Some(Ok(Some(_)))));
    assert!(matches!(try_poll_once(train.next()), Some(Ok(None))));
    assert!(writers[0].held(id), "a train that has ended waits on no worker");

    let train = train_of(&mut lease);
    assert!(matches!(try_poll_once(train.next()), Some(Ok(Some(_)))));
    assert!(writers[0].held(id), "taking the copies keeps the hold");
    assert!(try_poll_once(train.next()).is_none());
    assert!(!writers[0].held(id), "asking past them ends it");
}

/// A frame from a worker none of the lease's reads was written to is a worker
/// bug, reported when the lease drops.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "worker 1 answered a read it was not sent")]
fn a_frame_from_a_worker_the_lease_never_wrote_to_is_reported() {
    let (r, mut writers) = reactor_with_rings(2);
    let lease = lease_of(&r, &[WorkerSet::one(0)]);
    writers[1].send_train_frame(lease.id(), true, None);
    r.drain_all_w2m();
    drop(lease);
}
