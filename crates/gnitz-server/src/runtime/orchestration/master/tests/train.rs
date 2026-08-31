use super::super::fixtures::{make_row_batch, two_col_schema, u64_schema};
use super::*;

// Synthetic-train pattern: anonymous-mmap W2M rings (no fork), frames
// pre-written via W2mWriter, the drain driven by a single manual poll with
// a noop waker. Every fixture parks all continuation frames up front, so a
// healthy drain never returns `Pending` — a `Pending` poll IS the failure
// signal for a phantom-continuation regression.

struct DrainFixture {
    rings: Vec<crate::runtime::test_support::SharedRegion>,
    reactor: Rc<crate::runtime::reactor::Reactor>,
    /// A `Peer` over a socketpair end whose partner is already closed, for
    /// the drains that take one. It holds an owning `Rc<Reactor>`, so the
    /// fixture owns it: a `Peer` local to a test body would outlive
    /// `teardown` and keep the Reactor — and the `W2mSlot`s parked in it —
    /// alive past the ring unmap.
    peer: Peer,
    /// Owns the fd `peer` borrows; closes it on drop.
    peer_sock: std::os::unix::net::UnixStream,
    receiver: Rc<crate::runtime::w2m::W2mReceiver>,
    /// The broadcast dispatch the drains under test are handed, holding the
    /// scan lease. Built the same way production builds one, so the tests
    /// cannot pair ids with a routing decision the drains disagree with.
    scan: ScanDispatch,
}

/// The fixture's rings hold a handful of small reply frames each — every
/// message it publishes is a bare control block.
const DRAIN_MSG_SZ: usize = 1024;
const DRAIN_RING_MSGS: usize = 60;

impl DrainFixture {
    fn new(n_workers: usize) -> (Self, Vec<crate::runtime::w2m::W2mWriter>) {
        use crate::runtime::w2m::{W2mReceiver, W2mWriter};
        let mut rings = Vec::with_capacity(n_workers);
        let mut writers = Vec::with_capacity(n_workers);
        for _ in 0..n_workers {
            let region = unsafe { crate::runtime::w2m::make_ring(DRAIN_MSG_SZ, DRAIN_RING_MSGS, 16) };
            writers.push(W2mWriter::new(region.ptr()));
            rings.push(region);
        }
        let reactor = Rc::new(crate::runtime::reactor::Reactor::new(16).expect("reactor"));
        let scan = ScanDispatch::alloc(&reactor, n_workers, Fanout::Broadcast);
        let receiver = W2mReceiver::new(rings.iter().map(|r| r.ptr()).collect());
        let (peer_sock, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        drop(partner);
        let peer = Peer::unix(std::os::fd::AsRawFd::as_raw_fd(&peer_sock), Rc::clone(&reactor));
        (
            DrainFixture {
                rings,
                reactor,
                peer,
                peer_sock,
                receiver: Rc::new(receiver),
                scan,
            },
            writers,
        )
    }

    /// Hand the first frame of every worker to the caller (what
    /// `dispatch_scan_fanout` returns) and park every later frame so
    /// `await_scan_slot` resolves without a live worker.
    fn initial_slots(&self) -> Vec<crate::runtime::w2m::W2mSlot> {
        let n = self.rings.len();
        let mut slots = Vec::with_capacity(n);
        for w in 0..n {
            slots.push(self.receiver.try_read_slot(w).expect("first frame"));
            while let Some(cont) = self.receiver.try_read_slot(w) {
                self.reactor.route_scan_slot(cont);
            }
        }
        slots
    }

    /// Reply `i`'s request id, as the reactor keys it.
    fn req(&self, i: usize) -> u32 {
        self.scan.reply(i).1 as u32
    }

    fn teardown(self) {
        // Drop the dispatch (hence the scan lease) before the rings: its
        // Drop purges the scan's queue, which would drop any still-queued
        // W2mSlot borrowing the soon-to-be-unmapped region. The Reactor goes
        // next, for the same reason — it owns the queued frames, and a slot
        // dropped after the unmap writes `release_cursor` into freed memory.
        // The Peer holds an owning `Rc<Reactor>`, so it must go before the
        // Reactor.
        drop(self.scan);
        drop(self.peer);
        drop(self.peer_sock);
        drop(self.reactor);
        drop(self.receiver);
        // `self.rings` drops last, unmapping the regions.
    }
}

/// Encode one reply frame onto a test ring, mirroring the worker's reply
/// shapes: `flags` carries the train flags (0 = single-frame reply).
fn write_test_frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    batch: Option<&Batch>,
) {
    use crate::runtime::wire::{self as ipc};
    let block = schema.map(|s| gnitz_engine::catalog::encode_schema_block_ipc(s, 1));
    let msg = ipc::WireMsg {
        target_id: 1,
        flags,
        status,
        error_msg,
        schema_block: block.as_deref(),
        data: ipc::WireData::Whole(batch),
        ..Default::default()
    };
    writer.send_msg(req as u64, &msg);
}

/// Poll a future exactly once with a noop waker; `None` on `Pending`.
fn try_poll_once<T>(fut: impl std::future::Future<Output = T>) -> Option<T> {
    use std::task::{Context, Poll, Waker};
    let mut cx = Context::from_waker(Waker::noop());
    let mut fut = Box::pin(fut);
    match fut.as_mut().poll(&mut cx) {
        Poll::Ready(r) => Some(r),
        Poll::Pending => None,
    }
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

/// A frame must still be parked for `req`: the drain returned without
/// consuming it (the lease drop, not the drain, owns its disposal).
fn assert_frame_still_parked(reactor: &crate::runtime::reactor::Reactor, req: u32) {
    assert!(
        try_poll_once(reactor.await_scan_slot(req)).is_some(),
        "expected an undrained parked frame — the drain consumed frames \
         past its early-return point"
    );
}

/// A worker fault must surface as an IMMEDIATE `Err` in a single poll —
/// without draining the survivor's train (the caller's `ScanLease` drop
/// owns the discard of undrained frames).
///
/// Layout: worker 0 emits one `STATUS_ERROR` frame (flags 0, like
/// `send_error`); worker 1 is healthy with a two-frame train whose
/// `FLAG_SCAN_LAST` continuation is parked.
///  - If `has_more` were keyed on `FLAG_SCAN_LAST` alone (not
///    status-gated), worker 0's flags-0 error frame would read as "more
///    coming" and the first poll would return `Pending` — caught as a
///    failed assert instead of an infinite hang.
///  - A drain that deferred the error instead of returning on it would
///    consume worker 1's parked continuation — caught by the still-parked
///    assert.
#[test]
fn drain_index_scan_errs_immediately_on_fault_frame() {
    use gnitz_wire::{STATUS_ERROR, STATUS_OK};

    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req(0);
    let w1_req = fx.req(1);

    write_test_frame(&writers[0], w0_req, 0, STATUS_ERROR, b"boom", None, None);
    write_test_frame(&writers[1], w1_req, FLAG_CONTINUATION, STATUS_OK, b"", None, None);
    write_test_frame(
        &writers[1],
        w1_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
        STATUS_OK,
        b"",
        None,
        None,
    );

    let slots = fx.initial_slots();

    // The frames carry no schema block, so `expected` is never consulted.
    let result = poll_once(drain_index_scan(
        slots,
        &fx.scan,
        &fx.reactor,
        "scan",
        &two_col_schema(),
        |_, _| Ok(()),
    ));
    let err = result.expect_err("worker fault must surface as Err");
    assert!(err.text.contains("worker 0"), "error names the faulted worker: {err}");
    assert!(err.text.contains("boom"), "error carries the worker message: {err}");

    // Early return: worker 1's continuation must still be parked.
    assert_frame_still_parked(&fx.reactor, w1_req);

    fx.teardown();
}

/// Multi-frame trains from one worker merge with a single-frame (flag-free,
/// length-1 train) reply from another: the sink sees every row, the
/// continuation decodes against the schema hint saved from the first
/// frame, and the flag-free frame terminates its train. Reading "no
/// FLAG_SCAN_LAST ⇒ more" instead would hang this test at `Pending`.
#[test]
fn drain_index_scan_merges_chunked_and_single_frame_trains() {
    use gnitz_wire::STATUS_OK;

    let schema = two_col_schema();
    let chunk_a = make_row_batch(schema, &[(1, 1, 0, 10), (2, 1, 0, 20)]);
    let chunk_b = make_row_batch(schema, &[(3, 1, 0, 30)]);
    let single = make_row_batch(schema, &[(4, 1, 0, 40), (5, -1, 0, 50)]);

    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req(0);
    let w1_req = fx.req(1);

    // Worker 0: chunked train — schema on the first frame only.
    write_test_frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION,
        STATUS_OK,
        b"",
        Some(&schema),
        Some(&chunk_a),
    );
    write_test_frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
        STATUS_OK,
        b"",
        None,
        Some(&chunk_b),
    );
    // Worker 1: single-frame reply, no train flags (send_response shape).
    write_test_frame(&writers[1], w1_req, 0, STATUS_OK, b"", Some(&schema), Some(&single));

    let slots = fx.initial_slots();

    let mut rows: Vec<(u128, i64)> = Vec::new();
    let result = poll_once(drain_index_scan(
        slots,
        &fx.scan,
        &fx.reactor,
        "seek_by_index",
        &schema,
        |mb, frame_len| {
            assert!(frame_len > 0, "sink receives the raw frame byte length");
            for i in 0..mb.count {
                rows.push((
                    gnitz_wire::widen_pk_be(mb.get_pk_bytes(i), mb.pk_stride as usize),
                    mb.get_weight(i),
                ));
            }
            Ok(())
        },
    ));
    result.expect("healthy trains must drain cleanly");
    assert_eq!(
        rows,
        vec![(1, 1), (2, 1), (3, 1), (4, 1), (5, -1)],
        "every frame's rows reach the sink in worker order, weights verbatim",
    );

    fx.teardown();
}

/// A first-frame schema that does not match `expected` must error before
/// any row reaches the sink: the batch append helpers do not validate
/// shape, so a DDL-lagged worker reply would otherwise be mis-decoded.
#[test]
fn drain_index_scan_rejects_first_frame_schema_mismatch() {
    use gnitz_wire::STATUS_OK;

    let wire_schema = two_col_schema();
    let expected = u64_schema(); // different column count
    let batch = make_row_batch(wire_schema, &[(1, 1, 0, 10)]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    write_test_frame(&writers[0], w0_req, 0, STATUS_OK, b"", Some(&wire_schema), Some(&batch));

    let slots = fx.initial_slots();

    let mut sink_calls = 0usize;
    let result = poll_once(drain_index_scan(
        slots,
        &fx.scan,
        &fx.reactor,
        "gather",
        &expected,
        |_, _| {
            sink_calls += 1;
            Ok(())
        },
    ));
    let err = result.expect_err("schema mismatch must surface as Err");
    assert!(err.text.contains("Schema mismatch"), "error names the mismatch: {err}");
    assert!(err.text.contains("worker 0"), "error names the worker: {err}");
    assert_eq!(sink_calls, 0, "no row may reach the sink under a wrong schema");

    fx.teardown();
}

/// A sink `Err` (the reply-cap path in `fan_out_index_collect_common`)
/// aborts the drain immediately; the train's parked continuation stays
/// parked for the lease drop to discard.
#[test]
fn drain_index_scan_sink_error_aborts_drain() {
    use gnitz_wire::STATUS_OK;

    let schema = two_col_schema();
    let chunk = make_row_batch(schema, &[(1, 1, 0, 10)]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    write_test_frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION,
        STATUS_OK,
        b"",
        Some(&schema),
        Some(&chunk),
    );
    write_test_frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
        STATUS_OK,
        b"",
        None,
        Some(&chunk),
    );

    let slots = fx.initial_slots();

    let result = poll_once(drain_index_scan(
        slots,
        &fx.scan,
        &fx.reactor,
        "seek_by_index",
        &schema,
        |_, _| Err("seek_by_index: result exceeds the reply cap".into()),
    ));
    let err = result.expect_err("sink error must abort the drain");
    assert!(err.text.contains("reply cap"), "sink error surfaces verbatim: {err}");

    // The terminal continuation was never consumed.
    assert_frame_still_parked(&fx.reactor, w0_req);

    fx.teardown();
}

/// A worker scan frame carrying neither rows nor a schema block — every
/// worker that matched nothing on a selective broadcast read — is dropped
/// rather than forwarded to the client.
///
/// `poll_once` is the primary detector: without the drop branch,
/// `peer.send_slot` parks forever on an fd the reactor never registered and
/// the single poll returns `Pending`, which `poll_once` panics on. The
/// cursor assertion corroborates that the slot was *released* — the frame is
/// retired at the ring, not leaked.
#[test]
fn drain_scan_train_drops_a_frame_with_neither_data_nor_schema() {
    use gnitz_wire::STATUS_OK;

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    write_test_frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
        STATUS_OK,
        b"",
        None,
        None,
    );

    let mut slots = fx.initial_slots();
    let slot = slots.pop().expect("first frame");

    let before = fx.receiver.release_cursor(0);
    let head = classify_head(&slot, 0).expect("healthy header");
    assert!(!head.observable, "a frame with neither rows nor a schema block");
    let drained = poll_once(drain_scan_train(&fx.reactor, &fx.peer, slot, head, w0_req, 0)).expect("healthy train");
    assert!(drained, "the train drained without a client disconnect");
    assert!(
        fx.receiver.release_cursor(0) > before,
        "the dropped slot was released at the ring"
    );

    fx.teardown();
}

/// When every train is one frame, `forward_scan_slots` concatenates the
/// observable heads into one pooled buffer and issues a single send.
///
/// The discriminator is that the copy is synchronous, so EVERY ring slot is
/// released before the send is even submitted: the per-worker fallback would
/// instead hand worker 0's slot to `send_slot`, which pins it (and every
/// later worker's) until a CQE that never comes here. So all three cursors
/// advancing on a poll that has not yet completed is only possible on the
/// coalesced arm. Worker 1 contributes nothing observable and must be
/// dropped rather than concatenated, exactly as the per-worker drain does.
#[test]
fn forward_scan_slots_coalesces_single_frame_heads() {
    use gnitz_wire::STATUS_OK;

    let schema = two_col_schema();
    let rows_a = make_row_batch(schema, &[(1, 1, 0, 10)]);
    let rows_c = make_row_batch(schema, &[(2, 1, 0, 20)]);

    let (fx, writers) = DrainFixture::new(3);
    let reqs: Vec<u32> = (0..3).map(|i| fx.req(i)).collect();
    write_test_frame(&writers[0], reqs[0], 0, STATUS_OK, b"", Some(&schema), Some(&rows_a));
    write_test_frame(&writers[1], reqs[1], 0, STATUS_OK, b"", None, None);
    write_test_frame(&writers[2], reqs[2], 0, STATUS_OK, b"", Some(&schema), Some(&rows_c));

    let slots = fx.initial_slots();
    let before: Vec<u64> = (0..3).map(|w| fx.receiver.release_cursor(w)).collect();

    // The fixture's peer has no reader, so the send parks and the forward
    // cannot finish in one poll — the cursors are what carry the verdict.
    let done = try_poll_once(forward_scan_slots(&fx.reactor, &fx.peer, slots, &fx.scan));
    assert!(done.is_none(), "the coalesced send parks on a peer nobody reads");
    for (w, &was) in before.iter().enumerate() {
        assert!(
            fx.receiver.release_cursor(w) > was,
            "worker {w}'s slot must be released before the send is submitted",
        );
    }

    fx.teardown();
}
