use super::super::fixtures::{make_row_batch, two_col_schema};
use super::*;
use crate::runtime::test_support::try_poll_once;

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
    n_workers: usize,
    reactor: Rc<crate::runtime::reactor::Reactor>,
    receiver: Rc<crate::runtime::w2m::W2mReceiver>,
    /// A `Peer` over a socketpair end whose partner is already closed, for the
    /// drains that take one.
    peer: Peer,
    /// Owns the fd `peer` borrows.
    _peer_sock: std::os::unix::net::UnixStream,
    /// The broadcast dispatch the drains under test are handed, holding the scan
    /// lease. Built the way production builds one, so the tests cannot pair ids
    /// with a routing decision the drains disagree with.
    scan: ScanDispatch,
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

        let reactor =
            Rc::new(crate::runtime::reactor::Reactor::new(16, crate::runtime::reactor::Limits::TEST).expect("reactor"));
        let scan = ScanDispatch::alloc(&reactor, n_workers, Fanout::Broadcast);
        let (peer_sock, partner) = std::os::unix::net::UnixStream::pair().expect("socketpair");
        drop(partner);
        let peer = Peer::unix(std::os::fd::AsRawFd::as_raw_fd(&peer_sock), Rc::clone(&reactor));
        (
            DrainFixture {
                n_workers,
                reactor,
                receiver,
                peer,
                _peer_sock: peer_sock,
                scan,
            },
            writers,
        )
    }

    /// Hand the first frame of every worker to the caller (what
    /// `dispatch_scan_fanout` returns) and park every later frame so
    /// `await_scan_slot` resolves without a live worker.
    fn initial_slots(&self) -> Vec<crate::runtime::w2m::W2mSlot> {
        let mut slots = Vec::with_capacity(self.n_workers);
        for w in 0..self.n_workers {
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
}

/// Encode one healthy reply frame onto a test ring; `flags` carries the train
/// flags (0 = single-frame reply, the `send_response` shape).
fn frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: u64,
    schema: Option<&SchemaDescriptor>,
    batch: Option<&Batch>,
) {
    send_frame(writer, req, flags, gnitz_wire::STATUS_OK, b"", schema, batch);
}

/// The `send_error` shape: one `STATUS_ERROR` frame carrying a message and no
/// train flags.
fn fault_frame(writer: &crate::runtime::w2m::W2mWriter, req: u32, msg: &[u8]) {
    send_frame(writer, req, 0, gnitz_wire::STATUS_ERROR, msg, None, None);
}

fn send_frame(
    writer: &crate::runtime::w2m::W2mWriter,
    req: u32,
    flags: u64,
    status: u32,
    error_msg: &[u8],
    schema: Option<&SchemaDescriptor>,
    batch: Option<&Batch>,
) {
    use crate::runtime::wire::{self as ipc};
    let block = schema.map(|s| crate::catalog::encode_schema_block_ipc(s, 1));
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

/// Drive a future to completion in exactly one poll, panicking on Pending.
fn poll_once<T>(fut: impl std::future::Future<Output = T>) -> T {
    try_poll_once(fut).unwrap_or_else(|| {
        panic!(
            "future did not complete in one poll: the drain awaited a \
         frame that is not (and will never be) parked"
        )
    })
}

/// A frame must still be parked for `req` — the drain returned without
/// consuming it. Consumes the frame itself, so it is a terminal check.
fn assert_frame_still_parked(reactor: &crate::runtime::reactor::Reactor, req: u32) {
    assert!(
        try_poll_once(reactor.await_scan_slot(req)).is_some(),
        "expected an undrained parked frame — the drain consumed frames \
         past its early-return point"
    );
}

/// A worker fault surfaces as an `Err` on the first poll, without draining the
/// survivor's train. Worker 0's flags-0 error frame must read as terminal —
/// keyed on `FLAG_SCAN_LAST` alone it would read as "more coming" and the poll
/// would return `Pending`, which `poll_once` panics on — and worker 1's
/// still-parked continuation proves the drain returned rather than deferring.
#[test]
fn drain_index_scan_errs_immediately_on_fault_frame() {
    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req(0);
    let w1_req = fx.req(1);

    fault_frame(&writers[0], w0_req, b"boom");
    frame(&writers[1], w1_req, FLAG_CONTINUATION, None, None);
    frame(&writers[1], w1_req, FLAG_CONTINUATION | FLAG_SCAN_LAST, None, None);

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
}

/// Multi-frame trains from one worker merge with a single-frame (flag-free,
/// length-1 train) reply from another: the sink sees every row, the
/// continuation decodes against the schema hint saved from the first
/// frame, and the flag-free frame terminates its train. Reading "no
/// FLAG_SCAN_LAST ⇒ more" instead would hang this test at `Pending`.
#[test]
fn drain_index_scan_merges_chunked_and_single_frame_trains() {
    let schema = two_col_schema();
    let chunk_a = make_row_batch(schema, &[(1, 1, Some(10)), (2, 1, Some(20))]);
    let chunk_b = make_row_batch(schema, &[(3, 1, Some(30))]);
    let single = make_row_batch(schema, &[(4, 1, Some(40)), (5, -1, Some(50))]);

    let (fx, writers) = DrainFixture::new(2);
    let w0_req = fx.req(0);
    let w1_req = fx.req(1);

    // Worker 0: chunked train — schema on the first frame only.
    frame(&writers[0], w0_req, FLAG_CONTINUATION, Some(&schema), Some(&chunk_a));
    frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
        None,
        Some(&chunk_b),
    );
    // Worker 1: single-frame reply, no train flags (send_response shape).
    frame(&writers[1], w1_req, 0, Some(&schema), Some(&single));

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
            for i in 0..mb.len() {
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
}

/// A first-frame schema that does not match `expected` must error before
/// any row reaches the sink: the batch append helpers do not validate
/// shape, so a DDL-lagged worker reply would otherwise be mis-decoded.
#[test]
fn drain_index_scan_rejects_first_frame_schema_mismatch() {
    let wire_schema = two_col_schema();
    let expected = SchemaDescriptor::minimal_u64(); // different column count
    let batch = make_row_batch(wire_schema, &[(1, 1, Some(10))]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    frame(&writers[0], w0_req, 0, Some(&wire_schema), Some(&batch));

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
}

/// A sink `Err` (the reply-cap path in `fan_out_index_collect_common`)
/// aborts the drain immediately; the train's parked continuation stays
/// parked for the lease drop to discard.
#[test]
fn drain_index_scan_sink_error_aborts_drain() {
    let schema = two_col_schema();
    let chunk = make_row_batch(schema, &[(1, 1, Some(10))]);

    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    frame(&writers[0], w0_req, FLAG_CONTINUATION, Some(&schema), Some(&chunk));
    frame(
        &writers[0],
        w0_req,
        FLAG_CONTINUATION | FLAG_SCAN_LAST,
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
}

/// A scan frame carrying neither rows nor a schema block — every worker that
/// matched nothing on a selective broadcast read — is dropped rather than
/// forwarded. Without the drop branch `peer.send_slot` parks on an fd the
/// reactor never registered and the poll returns `Pending`; the cursor
/// assertion adds that the slot was *released* at the ring, not leaked.
#[test]
fn drain_scan_train_drops_a_frame_with_neither_data_nor_schema() {
    let (fx, writers) = DrainFixture::new(1);
    let w0_req = fx.req(0);
    frame(&writers[0], w0_req, FLAG_CONTINUATION | FLAG_SCAN_LAST, None, None);

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
}

/// When every train is one frame, `forward_scan_slots` concatenates the
/// observable heads into one pooled buffer and issues a single send.
///
/// The copy is synchronous, so every ring slot is released before the send is
/// submitted — the per-worker fallback would instead pin worker 0's slot in
/// `send_slot` until a CQE that never comes here. All three cursors advancing
/// on an unfinished poll is therefore only possible on the coalesced arm.
/// Worker 1 contributes nothing observable and must be dropped, not
/// concatenated.
#[test]
fn forward_scan_slots_coalesces_single_frame_heads() {
    let schema = two_col_schema();
    let rows_a = make_row_batch(schema, &[(1, 1, Some(10))]);
    let rows_c = make_row_batch(schema, &[(2, 1, Some(20))]);

    let (fx, writers) = DrainFixture::new(3);
    let reqs: Vec<u32> = (0..3).map(|i| fx.req(i)).collect();
    frame(&writers[0], reqs[0], 0, Some(&schema), Some(&rows_a));
    frame(&writers[1], reqs[1], 0, None, None);
    frame(&writers[2], reqs[2], 0, Some(&schema), Some(&rows_c));

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
}
