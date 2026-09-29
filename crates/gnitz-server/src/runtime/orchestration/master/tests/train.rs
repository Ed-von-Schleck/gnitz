use super::super::fixtures::{make_row_batch, two_col_schema};
use super::*;
use crate::runtime::reactor::{reactor_with_rings, Reactor};
use crate::runtime::test_support::try_poll_once;
use gnitz_wire::WireStatus;

struct DrainFixture {
    reactor: Rc<Reactor>,
    /// A peer whose partner end is closed; never ticked, so its recv never completes.
    peer: Peer,
    /// The scan lease the drains under test read.
    lease: TrainLease,
}

impl DrainFixture {
    fn new(n_workers: usize) -> (Self, Vec<crate::runtime::w2m::W2mWriter>) {
        let (reactor, writers) = reactor_with_rings(n_workers);
        let reactor = Rc::new(reactor);
        let lease = reactor.lease_train(crate::runtime::sal::WorkerSet::ALL, SalMessageKind::ScanSpec);
        let (peer_sock, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        drop(partner);
        let conn = reactor
            .client_conn(std::os::fd::OwnedFd::from(peer_sock))
            .expect("under the cap");
        let peer = Peer::new(&reactor, conn, None);
        (DrainFixture { reactor, peer, lease }, writers)
    }
}

/// Publish one reply frame, returning the bytes a client send of it carries.
fn frame(writer: &crate::runtime::w2m::W2mWriter, req: u32, flags: WireFlags, batch: Option<&Batch>) -> usize {
    send_frame(writer, req, flags, WireStatus::Ok, b"", batch)
}

/// A worker fault: an `Error` frame ending its train.
fn fault_frame(writer: &crate::runtime::w2m::W2mWriter, req: u32, msg: &[u8]) {
    send_frame(writer, req, WireFlags::train_frame(true), WireStatus::Error, msg, None);
}

fn send_frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: WireFlags,
    status: WireStatus,
    text: &[u8],
    batch: Option<&Batch>,
) -> usize {
    use crate::runtime::wire::{self as ipc};
    let msg = ipc::WireMsg {
        target_id: 1,
        flags,
        status,
        blob: text,
        data: batch.map_or(ipc::WireData::None, ipc::WireData::Whole),
        ..Default::default()
    };
    writer.send_msg(req, &msg);
    msg.size() + gnitz_wire::FRAME_LEN_PREFIX_BYTES
}

/// A frame carrying rows must still be routed for worker `i` — the drain
/// returned without consuming it. Consumes the frame itself, so it is a terminal
/// check.
fn assert_frame_still_parked(lease: &TrainLease, i: usize) {
    assert!(
        matches!(try_poll_once(lease.next_of(i)), Some(Ok(Some(_)))),
        "expected an undrained parked frame — the drain consumed frames \
         past its early-return point"
    );
}

/// A worker fault surfaces as an `Err` on the first poll, labelled by the
/// lease's kind, leaving the other worker's train undrained.
#[test]
fn drain_rows_errs_immediately_on_fault_frame() {
    let schema = two_col_schema();
    let rows = make_row_batch(schema, &[(1, 1, Some(10))]);
    let (fx, writers) = DrainFixture::new(2);
    let req = fx.lease.id();

    fault_frame(&writers[0], req, b"boom");
    frame(&writers[1], req, WireFlags::train_frame(false), Some(&rows));
    frame(&writers[1], req, WireFlags::train_frame(true), Some(&rows));

    fx.reactor.route_w2m_for_test();

    let result = try_poll_once(drain_rows(&fx.lease, &schema, |_| Ok(()))).expect("completes in one poll");
    let err = result.expect_err("worker fault must surface as Err");
    assert!(
        err.text.contains("worker 0: ScanSpec:"),
        "error names the worker and the kind: {err}"
    );
    assert!(err.text.contains("boom"), "error carries the worker message: {err}");

    // Early return: worker 1's train must still be parked.
    assert_frame_still_parked(&fx.lease, 1);
}

/// Multi-frame trains from one worker merge with a one-frame train from another:
/// the sink sees every row, each frame decoded against the expected schema.
#[test]
fn drain_rows_merges_chunked_and_single_frame_trains() {
    let schema = two_col_schema();
    let chunk_a = make_row_batch(schema, &[(1, 1, Some(10)), (2, 1, Some(20))]);
    let chunk_b = make_row_batch(schema, &[(3, 1, Some(30))]);
    let single = make_row_batch(schema, &[(4, 1, Some(40)), (5, -1, Some(50))]);

    let (fx, writers) = DrainFixture::new(2);
    let req = fx.lease.id();

    // Worker 0: a chunked train.
    frame(&writers[0], req, WireFlags::train_frame(false), Some(&chunk_a));
    frame(&writers[0], req, WireFlags::train_frame(true), Some(&chunk_b));
    // Worker 1: a one-frame train.
    frame(&writers[1], req, WireFlags::train_frame(true), Some(&single));

    fx.reactor.route_w2m_for_test();

    let mut rows: Vec<(u128, i64)> = Vec::new();
    let result = try_poll_once(drain_rows(&fx.lease, &schema, |mb| {
        for i in 0..mb.len() {
            rows.push((gnitz_wire::widen_pk_be(mb.get_pk_bytes(i)), mb.get_weight(i)));
        }
        Ok(())
    }))
    .expect("completes in one poll");
    result.expect("healthy trains must drain cleanly");
    assert_eq!(
        rows,
        vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, -1)],
        "every frame's rows reach the sink in worker order, weights verbatim",
    );
}

/// A sink `Err` aborts the drain immediately; the train's parked continuation
/// stays parked for the lease drop to discard.
#[test]
fn drain_rows_sink_error_aborts_drain() {
    let schema = two_col_schema();
    let chunk = make_row_batch(schema, &[(1, 1, Some(10))]);

    let (fx, writers) = DrainFixture::new(1);
    let req = fx.lease.id();
    frame(&writers[0], req, WireFlags::train_frame(false), Some(&chunk));
    frame(&writers[0], req, WireFlags::train_frame(true), Some(&chunk));

    fx.reactor.route_w2m_for_test();

    let result = try_poll_once(drain_rows(&fx.lease, &schema, |_| {
        Err("gather: result exceeds the reply cap".into())
    }))
    .expect("completes in one poll");
    let err = result.expect_err("sink error must abort the drain");
    assert!(err.text.contains("reply cap"), "sink error surfaces verbatim: {err}");

    // The terminal continuation was never consumed.
    assert_frame_still_parked(&fx.lease, 0);
}

/// `next` yields exactly the frames that carry rows, in worker order, and ends
/// once every worker's terminal frame is read — a row-less terminal included.
#[test]
fn next_yields_only_row_frames_and_ends_at_each_terminal() {
    let schema = two_col_schema();
    let rows_a = make_row_batch(schema, &[(1, 1, Some(10))]);
    let rows_b = make_row_batch(schema, &[(2, 1, Some(20))]);

    let (fx, writers) = DrainFixture::new(2);
    let req = fx.lease.id();
    frame(&writers[0], req, WireFlags::train_frame(false), None);
    frame(&writers[0], req, WireFlags::train_frame(false), Some(&rows_a));
    frame(&writers[0], req, WireFlags::train_frame(true), None);
    frame(&writers[1], req, WireFlags::train_frame(true), Some(&rows_b));

    fx.reactor.route_w2m_for_test();

    let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
    for (w, pk) in [(0u32, 1u128), (1, 2)] {
        let f = try_poll_once(fx.lease.next())
            .expect("completes in one poll")
            .expect("no fault")
            .expect("a row frame");
        assert_eq!(f.slot.worker, w, "frames arrive in worker order");
        let mb = f.rows(&schema, &mut offsets);
        assert_eq!(gnitz_wire::widen_pk_be(mb.get_pk_bytes(0)), pk);
    }
    assert!(
        try_poll_once(fx.lease.next())
            .expect("completes in one poll")
            .expect("no fault")
            .is_none(),
        "every train has ended"
    );
}

/// A scan frame carrying no rows — every worker that matched nothing on a
/// selective broadcast read — is dropped rather than forwarded: its slot is
/// released at the ring, not leaked, with the peer holding nothing corked.
#[test]
fn forward_scan_drops_a_row_less_frame() {
    let (fx, writers) = DrainFixture::new(1);
    let req = fx.lease.id();
    frame(&writers[0], req, WireFlags::train_frame(true), None);

    fx.reactor.route_w2m_for_test();

    let before = fx.reactor.release_cursor_for_test(0);
    try_poll_once(forward_scan(&fx.peer, &fx.lease))
        .expect("completes in one poll")
        .expect("healthy train");
    assert!(
        fx.reactor.release_cursor_for_test(0) > before,
        "the dropped slot was released at the ring"
    );
    assert_eq!(fx.peer.corked_len(), 0, "and nothing of it was corked");
}

/// Small single-frame heads are corked: each is copied out and its ring slot
/// released without parking, so the forward finishes in one poll. A zero-copy
/// send would instead pin worker 0's slot awaiting a CQE this fixture never
/// drives.
#[test]
fn forward_scan_coalesces_single_frame_heads() {
    let schema = two_col_schema();
    let rows_a = make_row_batch(schema, &[(1, 1, Some(10))]);
    let rows_c = make_row_batch(schema, &[(2, 1, Some(20))]);

    let (fx, writers) = DrainFixture::new(3);
    let req = fx.lease.id();
    let observable_bytes = frame(&writers[0], req, WireFlags::train_frame(true), Some(&rows_a))
        + frame(&writers[2], req, WireFlags::train_frame(true), Some(&rows_c));
    frame(&writers[1], req, WireFlags::train_frame(true), None);

    fx.reactor.route_w2m_for_test();
    let before: Vec<u64> = (0..3).map(|w| fx.reactor.release_cursor_for_test(w)).collect();

    let done = try_poll_once(forward_scan(&fx.peer, &fx.lease));
    assert!(
        matches!(done, Some(Ok(()))),
        "corking sends nothing, so the forward finishes in one poll"
    );
    for (w, &was) in before.iter().enumerate() {
        assert!(
            fx.reactor.release_cursor_for_test(w) > was,
            "worker {w}'s slot must be released by the copy",
        );
    }
    assert_eq!(
        fx.peer.corked_len(),
        observable_bytes,
        "the two observable heads are corked; worker 1's row-less frame is not"
    );
}
