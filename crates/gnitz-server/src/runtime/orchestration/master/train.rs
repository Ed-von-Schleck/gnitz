//! The worker reply-train contract and its consumers.
//!
//! A worker answers a scan-shaped request with a *train*: one or more frames on
//! the same request id. `parse_train_header` is the single definition of where a
//! train ends; everything else here is a consumer of it —
//! [`drain_index_scan`] (decode each frame in worker order),
//! [`forward_scan_slots`] / [`drain_scan_train`] (pass frames to a client
//! without decoding), and [`expect_single_frame`] (reject a train outright).
//! The k-way merge in `preflight` is a fourth consumer and parses through
//! `parse_train_header` too.

use super::*;
use crate::runtime::sal::MAX_WORKERS;

pub(super) fn scan_decode_err(w: usize, e: &'static str) -> WorkerFault {
    format!("scan: worker {w}: decode error: {e}").into()
}

/// Parse one frame header of worker `w`'s train. Returns the control block plus
/// whether more frames follow, or `Err` (prefixed with `what`) on a fault frame
/// or a corrupt header. Every caller holds the `ScanLease` from
/// `dispatch_scan_fanout`, so propagating the `Err` is the whole disposal story:
/// the lease drop discards the undrained remainder at the ring boundary.
///
/// A corrupt header yields no frame structure to follow, and a fault frame is
/// terminal by contract — `send_error` reports it as a single frame with
/// `status = STATUS_ERROR` and flags `0` (no `FLAG_SCAN_LAST`), and the worker
/// emits nothing more for the request. The error therefore MUST stop the drain:
/// keying `has_more` off `FLAG_SCAN_LAST` alone would read the fault frame's `0`
/// flags as "more coming" and block forever in `await_scan_slot`.
///
/// A healthy frame WITHOUT `FLAG_CONTINUATION` is a length-1 train: the
/// single-frame `send_response` reply shape carries no train flags at all, so
/// "no continuation flag" must read as terminal. Every multi-frame producer
/// (worker scan chunks, chunked seek/gather replies, unique pre-flight frames)
/// sets `FLAG_CONTINUATION` on every frame and `FLAG_SCAN_LAST` on the last.
pub(super) fn parse_train_header(
    slot: &W2mSlot,
    w: usize,
    what: &str,
) -> Result<(wire::DecodedControl, bool), WorkerFault> {
    let ctrl = peek_control_block_ipc(slot.bytes()).map_err(|e| scan_decode_err(w, e))?;
    if let Some(e) = super::worker_error(w, what, &ctrl) {
        return Err(e);
    }
    let has_more = ctrl.flags & FLAG_SCAN_LAST == 0 && ctrl.flags & FLAG_CONTINUATION != 0;
    Ok((ctrl, has_more))
}

/// Enforce the single-frame reply contract on `slot`: a fault frame or corrupt
/// header errors like `parse_train_header`, and ANY `FLAG_CONTINUATION` frame —
/// including a length-1 train's terminal `FLAG_SCAN_LAST` frame — is rejected.
/// The slot is forwarded verbatim as a complete reply, so a chunked train would
/// be truncated to its first frame with the remainder silently discarded by the
/// lease drop. Callers only route requests whose replies fit one frame (e.g. a
/// unique point seek); a train means that invariant broke (e.g. a shrunken
/// GNITZ_REPLY_FRAME_BUDGET) — fail loudly instead.
pub(super) fn expect_single_frame(slot: &W2mSlot, w: usize, what: &str) -> Result<(), WorkerFault> {
    let (ctrl, _) = parse_train_header(slot, w, what)?;
    if ctrl.flags & FLAG_CONTINUATION != 0 {
        return Err(format!("worker {w}: {what}: unexpected chunked reply on a single-frame path").into());
    }
    Ok(())
}

/// Drain every worker's train in worker order, invoking `on_batch` with the
/// zero-copy `MemBatch` and the raw frame byte length of each non-empty frame.
/// Shared by the unique-filter warmup and the collect paths (index seek, gather).
///
/// Returns on the FIRST error — worker fault, corrupt frame, schema mismatch, or
/// an `Err` from `on_batch` — without draining the rest. All callers hold the
/// `ScanLease` from `dispatch_scan_fanout`; when the early `Err` unwinds it, the
/// lease drop frees every parked slot and `route_scan_slot` discards later
/// frames at the ring boundary, advancing `consume_cursor`, so a still-streaming
/// worker cannot wedge in `send_encoded`.
///
/// `expected` guards each train's first schema-bearing frame. Worker reply
/// schemas can lag the master's during DDL races (`run_tick` releases the
/// catalog read lock before awaiting ACKs; seek handlers take no catalog lock at
/// all), and the batch append helpers do not validate shape — an unguarded
/// mismatch hands garbage onward under the master's schema block.
///
/// Continuation frames carry no schema; the one saved from the first frame is
/// reused as a decode hint. The `MemBatch` borrows from `slot.bytes()`, so the
/// slot is dropped only after `on_batch` returns.
pub(super) async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
    reactor: &crate::runtime::reactor::Reactor,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&gnitz_engine::storage::MemBatch<'_>, usize) -> Result<(), WorkerFault>,
) -> Result<(), WorkerFault> {
    for (i, mut slot) in slots.into_iter().enumerate() {
        let (w, req_id) = scan.reply(i);
        let mut saved_schema: Option<(SchemaDescriptor, u16)> = None;
        loop {
            let (ctrl, has_more) = parse_train_header(&slot, w, what)?;
            let server_version = gnitz_wire::wire_flags_get_schema_version(ctrl.flags);
            let frame_len = slot.bytes().len();
            let schema_hint = saved_schema.as_ref().map(|(s, v)| SchemaWithVersion {
                descriptor: s,
                version: *v,
            });
            let mut offsets = [0usize; gnitz_engine::storage::MAX_BATCH_REGIONS];
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl, schema_hint, &mut offsets)
                .map_err(|e| scan_decode_err(w, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    gnitz_engine::schema::validate_schema_match(s, expected)
                        .map_err(|e| WorkerFault::from(format!("worker {w}: {what}: {e}")))?;
                    saved_schema = Some((*s, server_version));
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if mb.count > 0 {
                    on_batch(mb, frame_len)?;
                }
            }
            drop(zc); // borrows slot
            drop(slot);
            if !has_more {
                break;
            }
            slot = reactor.await_scan_slot(req_id as u32).await;
        }
    }
    Ok(())
}

/// Ceiling on the coalesced head buffer, bounding the copy a scan pays to save
/// its per-frame `OP_SEND`/`OP_TIMEOUT`/`OP_ASYNC_CANCEL` triples.
///
/// `fanout_coalesced_egress_bench` (reactor tests) sweeps the tradeoff: the win
/// grows with worker count and shrinks with total size, reaching break-even
/// around 128 KiB at two workers but still large there at eight. 32 KiB sits
/// well inside the winning region at every worker count.
const COALESCE_MAX_BYTES: usize = 32 * 1024;

/// One awaited scan-train head, classified.
#[derive(Clone, Copy, Default)]
struct TrainHead {
    /// The frame carries rows or a schema block. A frame with neither holds
    /// nothing the client can observe and nothing a later frame could decode
    /// against, so it is dropped rather than forwarded — on a selective
    /// broadcast read that is W−1 of the W trains.
    observable: bool,
    /// The worker's train continues past this frame.
    has_more: bool,
}

/// Parse and classify one scan-train frame. The single definition of which
/// frames reach the client, shared by the fan-out's coalescing decision and by
/// the per-worker drain.
fn classify_head(slot: &W2mSlot, worker: usize) -> Result<TrainHead, WorkerFault> {
    let (ctrl, has_more) = parse_train_header(slot, worker, "scan")?;
    Ok(TrainHead {
        observable: ctrl.flags & (FLAG_HAS_DATA | FLAG_HAS_SCHEMA) != 0,
        has_more,
    })
}

/// Forward each already-awaited worker scan train to the client in reply order.
/// `Ok(false)` on client disconnect, `Err` on a worker fault / malformed train.
///
/// When every train is a single frame and the heads together stay under
/// [`COALESCE_MAX_BYTES`], they are concatenated into one pooled buffer and
/// leave in one egress operation instead of W. Each frame carries its own
/// `[len | payload]` prefix inside the ring mapping, so the concatenation is a
/// valid wire stream with nothing synthesized between frames. Anything else
/// falls back to [`drain_scan_train`] per worker.
pub(super) async fn forward_scan_slots(
    reactor: &crate::runtime::reactor::Reactor,
    peer: &Peer,
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
) -> Result<bool, WorkerFault> {
    // Classify every head before sending anything: whether they can leave as one
    // buffer is not known until every train has been seen. Both arms then run
    // off these, so no head is parsed twice. `slots.len()` is the worker count,
    // which `await_scan_slots` caps at MAX_WORKERS.
    let mut heads = [TrainHead::default(); MAX_WORKERS];
    let mut observable = 0usize;
    let mut total = 0usize;
    let mut any_tail = false;
    for (i, slot) in slots.iter().enumerate() {
        heads[i] = classify_head(slot, scan.reply(i).0)?;
        any_tail |= heads[i].has_more;
        if heads[i].observable {
            observable += 1;
            total += slot.frame_bytes().len();
        }
    }
    if any_tail || observable < 2 || total > COALESCE_MAX_BYTES {
        // Coalescing a multi-frame train would land its continuations behind the
        // next worker's head, reordering the client's rows. Below two frames
        // there is no kernel op to save.
        for (i, slot) in slots.into_iter().enumerate() {
            let (w, req_id) = scan.reply(i);
            if !drain_scan_train(reactor, peer, slot, heads[i], req_id as u32, w).await? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    // Every train is one frame, so the heads in worker order ARE the whole
    // reply, in exactly the order the per-worker drain would have produced.
    let mut buf = gnitz_engine::storage::batch_pool::acquire_buf();
    buf.reserve(total);
    for (i, slot) in slots.iter().enumerate() {
        if heads[i].observable {
            buf.extend_from_slice(slot.frame_bytes());
        }
    }
    // The copy is synchronous, so every ring slot is released before the SQE
    // exists — a client that stalls this send cannot pin a slot and fill the
    // worker's W2M ring.
    drop(slots);
    Ok(peer
        .send_buffer(gnitz_engine::storage::batch_pool::PooledSendBuf(buf))
        .await
        >= 0)
}

/// Forward one worker's train to the client: send each frame to `peer` (dropping
/// it before awaiting the next, per the W2M ring contract) and loop until the
/// header reports no more frames. `slot` is the first, already-awaited frame and
/// `head` its classification. `Ok(false)` if the client disconnects mid-stream,
/// `Err` on a malformed train header.
///
/// An unobservable frame is dropped here rather than at the next reassignment:
/// that releases the slot at the ring now, so a worker parked on a full ring is
/// not held across the await below. A fault frame never gets here —
/// `parse_train_header` returns `Err` on a non-zero status — and the train's
/// terminal frame is master-authored, so the client still sees the train end.
///
/// The send is deadline-guarded inside `send_slot`: a client that stops draining
/// this zero-copy slot is evicted, rc goes negative, and the caller drops the
/// `ScanLease`, discarding the rest of the train and advancing consume_cursor so
/// the worker unblocks.
async fn drain_scan_train(
    reactor: &crate::runtime::reactor::Reactor,
    peer: &Peer,
    mut slot: W2mSlot,
    mut head: TrainHead,
    req_id: u32,
    worker: usize,
) -> Result<bool, WorkerFault> {
    loop {
        if !head.observable {
            drop(slot);
        } else if peer.send_slot(slot).await < 0 {
            return Ok(false);
        }
        if !head.has_more {
            break;
        }
        slot = reactor.await_scan_slot(req_id).await;
        head = classify_head(&slot, worker)?;
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{make_row_batch, two_col_schema, u64_schema};
    use super::*;

    // Synthetic-train pattern: anonymous-mmap W2M rings (no fork), frames
    // pre-written via W2mWriter, the drain driven by a single manual poll with
    // a noop waker. Every fixture parks all continuation frames up front, so a
    // healthy drain never returns `Pending` — a `Pending` poll IS the failure
    // signal for a phantom-continuation regression.

    struct DrainFixture {
        rings: Vec<gnitz_engine_testkit::SharedRegion>,
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
            use crate::runtime::w2m_ring;
            let mut rings = Vec::with_capacity(n_workers);
            let mut writers = Vec::with_capacity(n_workers);
            for _ in 0..n_workers {
                let region = unsafe { w2m_ring::make_ring(DRAIN_MSG_SZ, DRAIN_RING_MSGS, 16) };
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
                    self.reactor.test_route_scan_slot(cont);
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
            // dropped after the unmap writes `consume_cursor` into freed memory.
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
        let msg = ipc::WireMsg {
            target_id: 1,
            flags,
            status,
            error_msg,
            schema,
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
        use crate::runtime::wire::{STATUS_ERROR, STATUS_OK};

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
        use crate::runtime::wire::STATUS_OK;

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
        use crate::runtime::wire::STATUS_OK;

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
        use crate::runtime::wire::STATUS_OK;

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
        use crate::runtime::wire::STATUS_OK;
        use std::sync::atomic::Ordering;

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

        let before = unsafe { fx.receiver.header(0) }
            .consume_cursor()
            .load(Ordering::Acquire);
        let head = classify_head(&slot, 0).expect("healthy header");
        assert!(!head.observable, "a frame with neither rows nor a schema block");
        let drained = poll_once(drain_scan_train(&fx.reactor, &fx.peer, slot, head, w0_req, 0)).expect("healthy train");
        assert!(drained, "the train drained without a client disconnect");
        assert!(
            unsafe { fx.receiver.header(0) }
                .consume_cursor()
                .load(Ordering::Acquire)
                > before,
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
        use crate::runtime::wire::STATUS_OK;
        use std::sync::atomic::Ordering;

        let schema = two_col_schema();
        let rows_a = make_row_batch(schema, &[(1, 1, 0, 10)]);
        let rows_c = make_row_batch(schema, &[(2, 1, 0, 20)]);

        let (fx, writers) = DrainFixture::new(3);
        let reqs: Vec<u32> = (0..3).map(|i| fx.req(i)).collect();
        write_test_frame(&writers[0], reqs[0], 0, STATUS_OK, b"", Some(&schema), Some(&rows_a));
        write_test_frame(&writers[1], reqs[1], 0, STATUS_OK, b"", None, None);
        write_test_frame(&writers[2], reqs[2], 0, STATUS_OK, b"", Some(&schema), Some(&rows_c));

        let slots = fx.initial_slots();
        let before: Vec<u64> = (0..3)
            .map(|w| {
                unsafe { fx.receiver.header(w) }
                    .consume_cursor()
                    .load(Ordering::Acquire)
            })
            .collect();

        // The fixture's peer has no reader, so the send parks and the forward
        // cannot finish in one poll — the cursors are what carry the verdict.
        let done = try_poll_once(forward_scan_slots(&fx.reactor, &fx.peer, slots, &fx.scan));
        assert!(done.is_none(), "the coalesced send parks on a peer nobody reads");
        for (w, &was) in before.iter().enumerate() {
            assert!(
                unsafe { fx.receiver.header(w) }
                    .consume_cursor()
                    .load(Ordering::Acquire)
                    > was,
                "worker {w}'s slot must be released before the send is submitted",
            );
        }

        fx.teardown();
    }
}
