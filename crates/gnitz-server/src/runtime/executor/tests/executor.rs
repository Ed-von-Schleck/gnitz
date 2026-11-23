use super::*;
use crate::runtime::master::fixtures::test_dispatcher;
use crate::runtime::reactor::test_support::{train_lease, train_of};
use crate::runtime::test_support::read_nonblocking;
use crate::runtime::test_support::{try_poll_once, within};
use crate::test_support::{certified, col_def, empty_catalog, make_batch, make_schema_u64_i64};
use gnitz_wire::multi_frame::{BLIND, FrameItem, encode_items};
use gnitz_wire::{TableDistribution, TableProps, TypeCode, WireConflictMode, append_frame};
use std::io::Write;

/// The committer serving `engine` with no worker launched: every round's ACKs
/// are in at its write. Neither the tick loop nor the feeds' pump is running;
/// the pump's requests are returned to stay open.
pub(super) fn workerless_committer(engine: CatalogEngine) -> (&'static Shared, impl Sized) {
    let (dispatcher, _) = test_dispatcher(0, engine);
    let reactor = dispatcher.reactor();
    let (shared, requests, asks) = Shared::new(dispatcher, Duration::ZERO, None);
    reactor.spawn(committer::run(requests, shared));
    (shared, asks)
}

/// Worker `w`'s next frame is still routed: a drain that returned early left it
/// unread.
fn assert_frame_still_parked(train: Train<'_>, w: usize) {
    assert!(
        matches!(try_poll_once(train.next_of(w)), Some(Ok(Some(_)))),
        "worker {w}'s frame was consumed past the early return"
    );
}

/// Every frame that carries rows reaches the client verbatim, workers in
/// ascending order; a row-less one is not forwarded.
#[test]
fn forward_scan_sends_every_row_frame_in_worker_order() {
    let schema = make_schema_u64_i64();
    let (reactor, mut lease, mut writers) = train_lease(3, "ScanSpec");
    let req = lease.id();
    let train = train_of(&mut lease);
    let mut want = writers[0].send_train_frame(req, false, Some(&certified(make_batch(&schema, &[(1, 1, 10)]))));
    writers[0].send_train_frame(req, true, None);
    writers[1].send_train_frame(req, true, None);
    want.extend(writers[2].send_train_frame(req, true, Some(&certified(make_batch(&schema, &[(2, 1, 20)])))));

    let (mut peer, partner) = Peer::over_socketpair(reactor);
    reactor.block_on(async {
        forward_scan(&mut peer, train).await.expect("healthy trains");
        peer.flush_egress().await.expect("the partner is open");
    });
    assert_eq!(read_nonblocking(&partner, 64 * 1024), Some(want));
}

/// A failed send ends the forward cleanly: the peer is finished, so the rest of
/// the train is left for the lease drop rather than read.
#[test]
fn forward_scan_stops_at_a_failed_send() {
    let schema = make_schema_u64_i64();
    let rows = certified(make_batch(&schema, &[(1, 1, 10)]));
    let (reactor, mut lease, mut writers) = train_lease(2, "ScanSpec");
    let req = lease.id();
    let train = train_of(&mut lease);
    writers[0].send_train_frame(req, true, Some(&rows));
    writers[1].send_train_frame(req, true, Some(&rows));

    let (mut peer, _partner) = Peer::over_socketpair(reactor);
    peer.abort();
    reactor
        .block_on(forward_scan(&mut peer, train))
        .expect("a gone peer is not a fault");
    assert_frame_still_parked(train, 1);
}

/// A PUSH's ACK and a PUSH_TXN's leave their connection only once the
/// fdatasync that covers the write has completed, though the handler returns
/// as soon as the write is laid out.
#[test]
fn a_push_is_acked_only_once_it_is_durable() {
    within(|| {
        let (_tmp, mut engine) = empty_catalog(1);
        let replicated = TableProps {
            distribution: TableDistribution::Replicated,
            ..Default::default()
        };
        let cols = [col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)];
        let tid = engine.create_table_with("public.t", &cols, &[0], replicated).unwrap();
        let (shared, _asks) = workerless_committer(engine);
        let reactor = shared.dispatcher.reactor();

        let (push, push_txn) = {
            let cat = shared.cat();
            let (rel, record) = cat.shape_target(tid).unwrap();
            let rows = make_batch(&rel.schema(), &[(1, 1, 10)]);
            let mut push = Vec::new();
            let hdr = ControlHeader::naming(ClientVerb::Push, tid.into(), 0);
            append_frame(&mut push, &hdr, record, Some(&rows.regions()));
            let mut push_txn = Vec::new();
            let item = FrameItem {
                target: tid.into(),
                conflict_mode: WireConflictMode::Update,
                basis: BLIND,
                blob: record,
                data: rows.regions(),
            };
            encode_items(&mut push_txn, ClientVerb::PushTxn, &[item]);
            (push, push_txn)
        };

        let sal = shared.dispatcher.sal();
        // One peer per verb, so each raises the fence its own ACK waits on.
        let mut acked = Vec::new();
        // A PUSH_TXN names no single relation, so its ACK echoes target 0.
        for (frame, target) in [(push, tid), (push_txn, 0)] {
            let (mut peer, partner) = Peer::over_socketpair(reactor);
            peer.admit_full_frames();
            (&partner).write_all(&gnitz_wire::framed(&frame)).expect("write");
            let (mut subs, mut snapshots) = Default::default();
            reactor.block_on(async {
                let buf = peer.next_request().await.expect("the push");
                handle_message(&mut peer, &mut subs, &mut snapshots, buf, shared).await
            });
            let lsn = sal.watermark();
            assert!(sal.durable_level().get() < lsn, "laid out, not yet durable");
            let mut want = Vec::new();
            WireMsg {
                hdr: ControlHeader::ack(target, lsn),
                ..Default::default()
            }
            .encode_framed(&mut want);

            let mut flush = Box::pin(async move {
                peer.flush_egress().await.expect("an open peer");
                peer
            });
            let timer = reactor.sleep(Duration::from_millis(20));
            assert!(
                matches!(reactor.block_on(select2(flush.as_mut(), timer)), Either::B(())),
                "the flush waits on the fdatasync"
            );
            assert!(read_nonblocking(&partner, 64).is_none(), "no ACK ahead of durability");
            acked.push((flush, partner, want));
        }

        reactor.spawn(shared.dispatcher.sal().run_syncs(reactor));
        for (flush, partner, want) in acked {
            let _peer = reactor.block_on(flush);
            assert_eq!(read_nonblocking(&partner, 64), Some(want), "the ACK, once durable");
        }
    });
}
