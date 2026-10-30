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
    let dropped = r.lease_acks(WorkerSet::ALL).id();
    assert_ne!(r.lease_acks(WorkerSet::ALL).id(), dropped);

    r.routes.next_id.set(id(u32::MAX));
    let top = r.lease_acks(WorkerSet::ALL);
    assert_eq!(top.id(), id(u32::MAX));
    let wrapped = r.lease_acks(WorkerSet::ALL);
    assert_eq!(wrapped.id(), id(1), "the id after u32::MAX is 1, not 0");

    r.routes.next_id.set(id(1));
    let skipped = r.lease_train(WorkerSet::ALL, "ScanSpec");
    assert_eq!(skipped.id(), id(2), "a live lease's id is skipped");
}

/// An ACK lease is complete on its own set's ACKs — the other workers are never
/// waited on — and keeps an ACK drained before anything awaits it; one over the
/// empty set is complete at once, and one over every worker is clipped to the
/// launched.
#[test]
fn an_ack_lease_resolves_on_its_own_sets_acks() {
    let (r, mut writers) = reactor_with_rings(4);
    assert_eq!(r.lease_acks(WorkerSet::ALL).targets().set.len(), 4);
    assert!(try_poll_once(r.lease_acks(WorkerSet::EMPTY).acks()).is_some());

    let lease = r.lease_acks(WorkerSet::one(1).with(3));
    let id = lease.id();
    writers[3].send_ack(id);
    r.tick(false); // drained before any awaiter
    let done = Rc::new(Cell::new(false));
    let d = Rc::clone(&done);
    r.spawn(async move {
        lease.acks().await;
        d.set(true);
    });
    r.tick(false);
    assert!(!done.get(), "one of the two has answered");
    writers[1].send_ack(id);
    r.tick(false); // the drain wakes the awaiter, and the same pass polls it
    assert!(done.get());
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

    writers[0].send_ack(id);
    r.drain_all_w2m();
    assert_eq!(r.w2m.release_cursor(0), r.w2m.write_cursor(0));
}

/// `next` yields exactly the frames that carry rows, workers in ascending order
/// and each worker's in arrival order, and ends once every worker's terminal
/// frame is read — a row-less one included. Every slot it passes over is
/// released at its ring.
#[test]
fn next_yields_the_row_frames_and_releases_the_rest() {
    let (r, lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let mut send = |w: usize, last, pk: Option<u64>| {
        let rows = pk.map(|pk| crate::test_support::make_batch(&schema, &[(pk, 1, 0)]));
        writers[w].send_train_frame(id, last, rows.as_ref());
    };
    send(0, false, None);
    for pk in 1..=3 {
        send(0, false, Some(pk));
    }
    send(0, true, None);
    send(1, true, Some(4));
    r.drain_all_w2m();

    for (w, pk) in [(0u32, 1u64), (0, 2), (0, 3), (1, 4)] {
        let f = try_poll_once(lease.next())
            .expect("routed")
            .expect("no fault")
            .expect("a row frame");
        assert_eq!(f.slot.worker, w);
        assert_eq!(f.rows(&schema).get_pk_bytes(0), pk.to_be_bytes());
    }
    assert!(try_poll_once(lease.next())
        .expect("routed")
        .expect("no fault")
        .is_none());
    for w in 0..2 {
        assert_eq!(r.w2m.release_cursor(w), r.w2m.write_cursor(w), "worker {w}");
    }
}

/// `frame_of` hands out every frame of a worker's train, a row-less one
/// included, and its row-less terminal; it hands out nothing once that
/// terminal is read.
#[test]
fn frame_of_hands_out_a_row_less_terminal() {
    let (r, lease, mut writers) = train_lease(2, "SnapshotPage");
    let id = lease.id();
    let schema = crate::test_support::make_schema_u64_i64();
    let rows = crate::test_support::make_batch(&schema, &[(7, 1, 0)]);
    writers[0].send_train_frame(id, false, None);
    writers[0].send_train_frame(id, false, Some(&rows));
    writers[0].send_train_frame(id, true, None);
    writers[1].send_train_frame(id, true, None);
    r.drain_all_w2m();

    let frame_of = |w: usize| try_poll_once(lease.frame_of(w)).expect("routed").expect("no fault");
    for (w, carries_rows, last) in [(0, false, false), (0, true, false), (0, false, true), (1, false, true)] {
        let (slot, data) = frame_of(w).expect("a frame");
        assert_eq!(slot.worker as usize, w);
        assert_eq!(data.is_some(), carries_rows);
        assert_eq!(slot.control().hdr.flags.scan_last, last);
    }
    for w in 0..2 {
        assert!(frame_of(w).is_none(), "worker {w}'s train has ended");
        assert_eq!(r.w2m.release_cursor(w), r.w2m.write_cursor(w), "worker {w}");
    }
}

/// A fault frame ends a train's read with the fault, its text naming the
/// worker that sent it and the request it answered.
#[test]
fn a_fault_frame_names_its_worker() {
    let (r, lease, mut writers) = train_lease(2, "ScanSpec");
    let id = lease.id();
    writers[0].send_train_frame(id, true, None);
    let fault = WireFault::from("boom");
    writers[1].send_msg(id, &WireMsg::fault(&fault));
    r.drain_all_w2m();

    let err = try_poll_once(lease.finish()).expect("routed").expect_err("the fault");
    assert!(err.text.contains("worker 1"), "{err}");
    assert!(err.text.contains("ScanSpec"), "{err}");
    assert!(err.text.contains("boom"), "{err}");
}
