use super::*;
use crate::runtime::reactor::{client_pair, framed, reactor_with_rings, read_nonblocking};
use crate::runtime::sal::SalMessageKind;
use crate::runtime::test_support::try_poll_once;
use crate::runtime::w2m::W2mWriter;
use crate::runtime::wire::WireMsg;
use crate::test_support::{make_batch, make_schema_u64_i64, weighted_rows};
use gnitz_zset::repr::Batch;

/// A scan lease over `n` workers' fresh rings, with each ring's writer.
fn scan_lease(n: usize) -> (Reactor, TrainLease, Vec<W2mWriter>) {
    let (reactor, writers) = reactor_with_rings(n);
    let lease = reactor.lease_train(WorkerSet::ALL, SalMessageKind::ScanSpec);
    (reactor, lease, writers)
}

/// Publish one frame of a scan train, `last` ending the worker's train, and
/// return the bytes a client forward of it carries.
fn frame(writer: &mut W2mWriter, req: u32, last: bool, batch: Option<&Batch>) -> Vec<u8> {
    let msg = WireMsg {
        data: batch.and_then(Batch::wire_whole),
        ..WireMsg::train_frame(1, last)
    };
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

/// The sink sees every row of every frame, workers in ascending order, each
/// decoded whole against the schema — row-less frames mid-train and at its end
/// contributing nothing.
#[test]
fn drain_rows_hands_the_sink_every_row_in_worker_order() {
    let schema = make_schema_u64_i64();
    let chunks = [
        make_batch(&schema, &[(1, 1, 10), (2, 3, 20)]),
        make_batch(&schema, &[(3, 1, 30)]),
        make_batch(&schema, &[(4, 1, 40), (5, -1, 50)]),
    ];
    let (reactor, lease, mut writers) = scan_lease(2);
    let req = lease.id();
    frame(&mut writers[0], req, false, Some(&chunks[0]));
    frame(&mut writers[0], req, false, None);
    frame(&mut writers[0], req, false, Some(&chunks[1]));
    frame(&mut writers[0], req, true, None);
    frame(&mut writers[1], req, true, Some(&chunks[2]));

    let got = reactor.block_on(async move {
        let mut got = Vec::new();
        drain_rows(&lease, &schema, |mb| {
            got.extend(weighted_rows(&Batch::concat(&schema, std::iter::once(mb.clone()))));
            Ok(())
        })
        .await
        .expect("healthy trains drain cleanly");
        got
    });
    let want: Vec<_> = chunks.iter().flat_map(weighted_rows).collect();
    assert_eq!(got, want);
}

/// A worker fault and a sink error each end the drain at once, leaving the rest
/// of the train parked for the lease drop to discard.
#[test]
fn drain_rows_stops_at_the_first_error() {
    let schema = make_schema_u64_i64();
    let rows = make_batch(&schema, &[(1, 1, 10)]);
    for sink_fails in [false, true] {
        let (reactor, lease, mut writers) = scan_lease(2);
        let req = lease.id();
        if sink_fails {
            frame(&mut writers[0], req, true, Some(&rows));
        } else {
            writers[0].send_msg(req, &WireMsg::fault(&"boom".into()));
        }
        frame(&mut writers[1], req, true, Some(&rows));

        let (err, lease) = reactor.block_on(async move {
            let r = drain_rows(&lease, &schema, |_| {
                if sink_fails {
                    Err("the sink refuses".into())
                } else {
                    Ok(())
                }
            })
            .await;
            (r.expect_err("the drain fails"), lease)
        });
        let want = if sink_fails { "the sink refuses" } else { "boom" };
        assert!(err.text.contains(want), "{err}");
        assert_frame_still_parked(&lease, 1);
    }
}

/// Every frame that carries rows reaches the client verbatim, workers in
/// ascending order; a row-less one is not forwarded.
#[test]
fn forward_scan_sends_every_row_frame_in_worker_order() {
    let schema = make_schema_u64_i64();
    let (reactor, lease, mut writers) = scan_lease(3);
    let req = lease.id();
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
    reactor.block_on(async move {
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
    frame(&mut writers[0], lease.id(), true, Some(&rows));
    frame(&mut writers[1], lease.id(), true, Some(&rows));

    let (conn, _partner) = client_pair(&reactor);
    let peer = Peer::new(&reactor, conn, None);
    peer.close();
    let lease = reactor.block_on(async move {
        forward_scan(&peer, &lease).await.expect("a gone peer is not a fault");
        lease
    });
    assert_frame_still_parked(&lease, 1);
}
