use super::super::fixtures::{make_row_batch, two_col_schema};
use super::*;
use crate::runtime::test_support::try_poll_once;
use gnitz_wire::WireStatus;

// Synthetic-train pattern: anonymous-mmap W2M rings (no fork), frames
// pre-written via W2mWriter, the drain driven by a single manual poll with
// a noop waker. Every fixture parks all continuation frames up front, so a
// healthy drain never returns `Pending` — a `Pending` poll IS the failure
// signal for a phantom-continuation regression.

/// The fixture's rings hold a handful of small reply frames each — every message
/// it publishes is a bare control block.
const DRAIN_MSG_SZ: usize = 1024;
const DRAIN_RING_MSGS: usize = 60;

struct DrainFixture {
    reactor: Rc<crate::runtime::reactor::Reactor>,
    receiver: Rc<crate::runtime::w2m::W2mReceiver>,
    /// A `Peer` over a socketpair end whose partner is already closed, for the
    /// drains that take one. Its armed recv never completes: the fixture never
    /// ticks.
    peer: Peer,
    /// The scan lease the drains under test read, one reply id per worker.
    lease: Lease,
}

impl DrainFixture {
    fn new(n_workers: usize) -> (Self, Vec<crate::runtime::w2m::W2mWriter>) {
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        let mut ring_ptrs = Vec::with_capacity(n_workers);
        let mut writers = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            // Leaked: a `W2mSlot` parked in the reactor writes `release_cursor`
            // through the ring on drop, and a still-parked slot is exactly what a
            // failing assert leaves behind. Never unmapping is what makes the
            // fixture's teardown order irrelevant instead of load-bearing.
            let ptr = unsafe { crate::runtime::w2m::fixtures::make_ring(DRAIN_MSG_SZ, DRAIN_RING_MSGS, 16) }.leak();
            writers.push(W2mWriter::new(ptr));
            ring_ptrs.push(ptr);
        }
        let receiver = Rc::new(W2mReceiver::new(ring_ptrs));
        // Pinned for the same reason: a parked slot points into it.
        std::mem::forget(Rc::clone(&receiver));

        let reactor = Rc::new(crate::runtime::test_support::make_reactor_over(Rc::clone(&receiver)));
        let lease = reactor.lease_train(crate::runtime::sal::WorkerSet::ALL);
        let (peer_sock, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        drop(partner);
        let peer = Peer::unix(std::os::fd::OwnedFd::from(peer_sock), Rc::clone(&reactor));
        (DrainFixture { reactor, receiver, peer, lease }, writers)
    }

    /// Route every published frame, so `next_frame` resolves without a live
    /// worker.
    fn route(&self) {
        self.reactor.route_w2m_for_test();
    }

    /// The request id every worker's train answers on.
    fn req(&self) -> u32 {
        self.lease.id(0)
    }
}

/// Encode one healthy reply frame onto a test ring; `flags` carries the train
/// flags (`WireFlags::train_frame`). Returns the framed bytes a client send of it
/// carries.
fn frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: WireFlags,
    schema: Option<&SchemaDescriptor>,
    batch: Option<&Batch>,
) -> usize {
    send_frame(writer, req, flags, WireStatus::Ok, b"", schema, batch)
}

/// A worker fault: an `Error` frame ending its train.
fn fault_frame(writer: &crate::runtime::w2m::W2mWriter, req: u32, msg: &[u8]) {
    send_frame(
        writer,
        req,
        WireFlags::train_frame(0, true),
        WireStatus::Error,
        msg,
        None,
        None,
    );
}

fn send_frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: WireFlags,
    status: WireStatus,
    text: &[u8],
    schema: Option<&SchemaDescriptor>,
    batch: Option<&Batch>,
) -> usize {
    use crate::runtime::wire::{self as ipc};
    let block = schema.map(|s| crate::catalog::encode_schema_block(s, 1));
    let msg = ipc::WireMsg {
        target_id: 1,
        flags,
        status,
        blob: text,
        schema_block: block.as_deref(),
        data: batch.map_or(ipc::WireData::None, ipc::WireData::Whole),
        ..Default::default()
    };
    writer.send_msg(req, &msg);
    msg.size() + gnitz_wire::FRAME_LEN_PREFIX_BYTES
}

/// Drive a future to completion in exactly one poll, panicking on Pending.
fn poll_once<T>(fut: impl std::future::Future<Output = T>) -> T {
    try_poll_once(fut).unwrap_or_else(|| {
        panic!(
            "future did not complete in one poll: the drain awaited a \
         frame that is not (and will never be) parked"
        )
    })
}

/// A frame must still be routed for worker `i` — the drain returned without
/// consuming it. Consumes the frame itself, so it is a terminal check.
fn assert_frame_still_parked(lease: &Lease, i: usize) {
    assert!(
        try_poll_once(lease.next_frame(i)).is_some(),
        "expected an undrained parked frame — the drain consumed frames \
         past its early-return point"
    );
}

/// A worker fault surfaces as an `Err` on the first poll, leaving the other
/// worker's train undrained.
#[test]
fn drain_index_scan_errs_immediately_on_fault_frame() {
    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req();
    let w1_req = fx.req();

    fault_frame(&writers[0], w0_req, b"boom");
    frame(&writers[1], w1_req, WireFlags::train_frame(0, false), None, None);
    frame(&writers[1], w1_req, WireFlags::train_frame(0, true), None, None);

    fx.route();

    // The frames carry no schema block, so `expected` is never consulted.
    let result = poll_once(drain_index_scan(&fx.lease, "scan", &two_col_schema(), |_| Ok(())));
    let err = result.expect_err("worker fault must surface as Err");
    assert!(err.text.contains("worker 0"), "error names the faulted worker: {err}");
    assert!(err.text.contains("boom"), "error carries the worker message: {err}");

    // Early return: worker 1's continuation must still be parked.
    assert_frame_still_parked(&fx.lease, 1);
}

/// Multi-frame trains from one worker merge with a one-frame train from another:
/// the sink sees every row, and the continuation decodes against the expected
/// schema.
#[test]
fn drain_index_scan_merges_chunked_and_single_frame_trains() {
    let schema = two_col_schema();
    let chunk_a = make_row_batch(schema, &[(1, 1, Some(10)), (2, 1, Some(20))]);
    let chunk_b = make_row_batch(schema, &[(3, 1, Some(30))]);
    let single = make_row_batch(schema, &[(4, 1, Some(40)), (5, -1, Some(50))]);

    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req();
    let w1_req = fx.req();

    // Worker 0: chunked train — schema on the first frame only.
    frame(
        &writers[0],
        w0_req,
        WireFlags::train_frame(0, false),
        Some(&schema),
        Some(&chunk_a),
    );
    frame(
        &writers[0],
        w0_req,
        WireFlags::train_frame(0, true),
        None,
        Some(&chunk_b),
    );
    // Worker 1: a one-frame train.
    frame(
        &writers[1],
        w1_req,
        WireFlags::train_frame(0, true),
        Some(&schema),
        Some(&single),
    );

    fx.route();

    let mut rows: Vec<(u128, i64)> = Vec::new();
    let result = poll_once(drain_index_scan(&fx.lease, "gather", &schema, |mb| {
        for i in 0..mb.len() {
            rows.push((gnitz_wire::widen_pk_be(mb.get_pk_bytes(i)), mb.get_weight(i)));
        }
        Ok(())
    }));
    result.expect("healthy trains must drain cleanly");
    assert_eq!(
        rows,
        vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, -1)],
        "every frame's rows reach the sink in worker order, weights verbatim",
    );
}

/// A first-frame schema that does not match `expected` must error before
/// any row reaches the sink: the batch append helpers do not validate
/// shape, so a DDL-lagged worker reply would otherwise be mis-decoded.
#[test]
fn drain_index_scan_rejects_first_frame_schema_mismatch() {
    let wire_schema = two_col_schema();
    let expected = crate::test_support::pk_only_schema(&[gnitz_wire::type_code::U64]); // different column count
    let batch = make_row_batch(wire_schema, &[(1, 1, Some(10))]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req();
    frame(
        &writers[0],
        w0_req,
        WireFlags::train_frame(0, true),
        Some(&wire_schema),
        Some(&batch),
    );

    fx.route();

    let mut sink_calls = 0usize;
    let result = poll_once(drain_index_scan(&fx.lease, "gather", &expected, |_| {
        sink_calls += 1;
        Ok(())
    }));
    let err = result.expect_err("schema mismatch must surface as Err");
    assert!(err.text.contains("Schema mismatch"), "error names the mismatch: {err}");
    assert!(err.text.contains("worker 0"), "error names the worker: {err}");
    assert_eq!(sink_calls, 0, "no row may reach the sink under a wrong schema");
}

/// A sink `Err` aborts the drain immediately; the train's parked continuation
/// stays parked for the lease drop to discard.
#[test]
fn drain_index_scan_sink_error_aborts_drain() {
    let schema = two_col_schema();
    let chunk = make_row_batch(schema, &[(1, 1, Some(10))]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req();
    frame(
        &writers[0],
        w0_req,
        WireFlags::train_frame(0, false),
        Some(&schema),
        Some(&chunk),
    );
    frame(&writers[0], w0_req, WireFlags::train_frame(0, true), None, Some(&chunk));

    fx.route();

    let result = poll_once(drain_index_scan(&fx.lease, "gather", &schema, |_| {
        Err("gather: result exceeds the reply cap".into())
    }));
    let err = result.expect_err("sink error must abort the drain");
    assert!(err.text.contains("reply cap"), "sink error surfaces verbatim: {err}");

    // The terminal continuation was never consumed.
    assert_frame_still_parked(&fx.lease, 0);
}

/// A scan frame carrying neither rows nor a schema block — every worker that
/// matched nothing on a selective broadcast read — is dropped rather than
/// forwarded. Without the drop branch `peer.send` would take it; the cursor
/// assertion is that the slot was *released* at the ring, not leaked, with the
/// peer holding nothing corked.
#[test]
fn drain_scan_train_drops_a_frame_with_neither_data_nor_schema() {
    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req();
    frame(&writers[0], w0_req, WireFlags::train_frame(0, true), None, None);

    fx.route();

    let before = fx.receiver.release_cursor(0);
    let drained = poll_once(forward_scan(&fx.peer, &fx.lease)).expect("healthy train");
    assert!(drained, "the train drained without a client disconnect");
    assert!(
        fx.receiver.release_cursor(0) > before,
        "the dropped slot was released at the ring"
    );
    assert_eq!(fx.peer.corked_len(), 0, "and nothing of it was corked");
}

/// Small single-frame heads are corked: each is copied out and its ring slot
/// released without parking, so the forward finishes in one poll. A zero-copy
/// send would instead pin worker 0's slot awaiting a CQE this fixture never
/// drives, which is what makes the one poll decisive.
#[test]
fn forward_scan_coalesces_single_frame_heads() {
    let schema = two_col_schema();
    let rows_a = make_row_batch(schema, &[(1, 1, Some(10))]);
    let rows_c = make_row_batch(schema, &[(2, 1, Some(20))]);

    let (fx, writers) = DrainFixture::new(3);
    let reqs: Vec<u32> = (0..3).map(|_| fx.req()).collect();
    let observable_bytes = frame(
        &writers[0],
        reqs[0],
        WireFlags::train_frame(0, true),
        Some(&schema),
        Some(&rows_a),
    ) + frame(
        &writers[2],
        reqs[2],
        WireFlags::train_frame(0, true),
        Some(&schema),
        Some(&rows_c),
    );
    frame(&writers[1], reqs[1], WireFlags::train_frame(0, true), None, None);

    fx.route();
    let before: Vec<u64> = (0..3).map(|w| fx.receiver.release_cursor(w)).collect();

    let done = try_poll_once(forward_scan(&fx.peer, &fx.lease));
    assert!(
        matches!(done, Some(Ok(true))),
        "corking sends nothing, so the forward finishes in one poll"
    );
    for (w, &was) in before.iter().enumerate() {
        assert!(
            fx.receiver.release_cursor(w) > was,
            "worker {w}'s slot must be released by the copy",
        );
    }
    assert_eq!(
        fx.peer.corked_len(),
        observable_bytes,
        "the two observable heads are corked; worker 1's unobservable frame is not"
    );
}
