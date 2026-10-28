use super::*;
use crate::runtime::frame::WireMsg;
use crate::runtime::park::WorkerSet;
use crate::runtime::reactor::{client_pair, reactor_with_rings, read_nonblocking, Reactor};
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::W2mWriter;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_wire::framed;

/// A scan lease over `n` workers' fresh rings, with each ring's writer.
fn scan_lease(n: usize) -> (Reactor, TrainLease, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(n);
    let lease = reactor.lease_train(WorkerSet::ALL, "ScanSpec");
    (reactor, lease, writers)
}

/// Publish one frame of a scan train, `last` ending the worker's train, and
/// return the bytes a client forward of it carries.
fn frame(writer: &mut W2mWriter, req: u32, last: bool, batch: Option<&Batch>) -> Vec<u8> {
    let msg = WireMsg::train_frame(1, last, batch.and_then(Batch::wire_whole));
    writer.send_msg(req, &msg);
    framed(&msg.encode_to_vec())
}

/// Worker `w`'s next frame is still routed: a drain that returned early left it
/// unread.
fn assert_frame_still_parked(lease: &TrainLease, w: usize) {
    assert!(
        matches!(try_poll_once(lease.next_of(w)), Some(Ok(Some(_)))),
        "worker {w}'s frame was consumed past the early return"
    );
}

/// Every frame that carries rows reaches the client verbatim, workers in
/// ascending order; a row-less one is not forwarded.
#[test]
fn forward_scan_sends_every_row_frame_in_worker_order() {
    let schema = make_schema_u64_i64();
    let (reactor, lease, mut writers) = scan_lease(3);
    let req = lease.targets().request_id;
    let mut want = frame(&mut writers[0], req, false, Some(&make_batch(&schema, &[(1, 1, 10)])));
    frame(&mut writers[0], req, true, None);
    frame(&mut writers[1], req, true, None);
    want.extend(frame(
        &mut writers[2],
        req,
        true,
        Some(&make_batch(&schema, &[(2, 1, 20)])),
    ));

    let (conn, partner) = client_pair(&reactor);
    let peer = Peer::new(&reactor, conn, None);
    reactor.block_on(async {
        forward_scan(&peer, &lease).await.expect("healthy trains");
        peer.flush_egress().await.expect("the partner is open");
    });
    assert_eq!(read_nonblocking(&partner, 64 * 1024), Some(want));
}

/// A failed send ends the forward cleanly: the peer is finished, so the rest of
/// the train is left for the lease drop rather than read.
#[test]
fn forward_scan_stops_at_a_failed_send() {
    let schema = make_schema_u64_i64();
    let rows = make_batch(&schema, &[(1, 1, 10)]);
    let (reactor, lease, mut writers) = scan_lease(2);
    let req = lease.targets().request_id;
    frame(&mut writers[0], req, true, Some(&rows));
    frame(&mut writers[1], req, true, Some(&rows));

    let (conn, _partner) = client_pair(&reactor);
    let peer = Peer::new(&reactor, conn, None);
    peer.close();
    reactor
        .block_on(forward_scan(&peer, &lease))
        .expect("a gone peer is not a fault");
    assert_frame_still_parked(&lease, 1);
}
