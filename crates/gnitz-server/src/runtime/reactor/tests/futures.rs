//! The reactor's futures: timers, and the leases W2M frames are routed to — their
//! id space, an ACK lease's answers and a train lease's frame stream.

use std::time::Duration;

use super::super::test_support::*;
use super::*;
use crate::runtime::wire::WireMsg;

/// A sleep of nothing resolves on its first poll and registers no deadline; a
/// pending one holds its deadline until it drops, so a dropped timer neither
/// wakes its waker nor bounds a later sleep.
#[test]
fn a_timer_holds_a_deadline_only_while_pending() {
    let r = make_reactor();
    assert!(try_poll_once(r.sleep(Duration::ZERO)).is_some());
    assert!(r.deadlines.borrow().is_empty());

    let mut tf = Box::pin(r.sleep(Duration::from_secs(3600)));
    assert!(try_poll_once(tf.as_mut()).is_none());
    assert_eq!(r.deadlines.borrow().len(), 1, "the poll registers the deadline");
    drop(tf);
    assert!(r.deadlines.borrow().is_empty(), "the drop removes it");
}

/// Lease ids wrap past 0, which no lease takes; a lease never takes an id a live
/// lease still routes, nor a dropped lease's straight back.
#[test]
fn lease_ids_wrap_past_zero_and_skip_live_ids() {
    let (r, _writers) = reactor_with_rings(1);
    let dropped = r.lease_acks(WorkerSet::ALL).targets().request_id;
    assert_ne!(r.lease_acks(WorkerSet::ALL).targets().request_id, dropped);

    r.next_request_id.set(u32::MAX);
    let top = r.lease_acks(WorkerSet::ALL);
    assert_eq!(top.targets().request_id, u32::MAX);
    let wrapped = r.lease_acks(WorkerSet::ALL);
    assert_eq!(wrapped.targets().request_id, 1, "the id after u32::MAX is 1, not 0");

    r.next_request_id.set(1);
    let skipped = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    assert_eq!(skipped.targets().request_id, 2, "a live lease's id is skipped");
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
    let id = lease.targets().request_id;
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
    let (r, mut writers) = reactor_with_rings(1);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    let id = lease.targets().request_id;
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
    let (r, mut writers) = reactor_with_rings(2);
    let lease = r.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    let id = lease.targets().request_id;
    let schema = crate::test_support::make_schema_u64_i64();
    let mut send = |w: usize, last, pk: Option<u64>| {
        let rows = pk.map(|pk| crate::test_support::make_batch(&schema, &[(pk, 1, 0)]));
        writers[w].send_msg(
            id,
            &WireMsg::train_frame(0, last, rows.as_ref().and_then(|b| b.wire_whole())),
        );
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
