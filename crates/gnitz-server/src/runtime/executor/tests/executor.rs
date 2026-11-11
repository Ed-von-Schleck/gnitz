use super::*;
use crate::runtime::reactor::test_support::train_lease;
use crate::runtime::test_support::read_nonblocking;
use crate::runtime::test_support::try_poll_once;
use crate::test_support::{make_batch, make_schema_u64_i64};

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
    let (reactor, lease, mut writers) = train_lease(3, "ScanSpec");
    let req = lease.id();
    let mut want = writers[0].send_train_frame(req, false, Some(&make_batch(&schema, &[(1, 1, 10)])));
    writers[0].send_train_frame(req, true, None);
    writers[1].send_train_frame(req, true, None);
    want.extend(writers[2].send_train_frame(req, true, Some(&make_batch(&schema, &[(2, 1, 20)]))));

    let (mut peer, partner) = Peer::over_socketpair(&reactor);
    reactor.block_on(async {
        forward_scan(&mut peer, &lease).await.expect("healthy trains");
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
    let (reactor, lease, mut writers) = train_lease(2, "ScanSpec");
    let req = lease.id();
    writers[0].send_train_frame(req, true, Some(&rows));
    writers[1].send_train_frame(req, true, Some(&rows));

    let (mut peer, _partner) = Peer::over_socketpair(&reactor);
    peer.abort();
    reactor
        .block_on(forward_scan(&mut peer, &lease))
        .expect("a gone peer is not a fault");
    assert_frame_still_parked(&lease, 1);
}
