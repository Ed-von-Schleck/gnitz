//! The worker reply-train contract and its consumers.
//!
//! A worker answers a scan-shaped request with a *train*: one or more frames on
//! the same request id. [`train_has_more`] is the single definition of where a
//! train ends — `ExchangeAccumulator` reads its own frames through it too, from
//! an owned decode rather than a slot. Everything else here is a consumer of
//! `parse_train_header` —
//! [`drain_index_scan`] (decode each frame in worker order),
//! [`forward_scan_slots`] / [`drain_scan_train`] (pass frames to a client
//! without decoding), and [`expect_single_frame`] (reject a train outright).
//! The k-way merge in `preflight` is a fourth consumer and parses through
//! `parse_train_header` too.

use super::*;
use crate::runtime::wire::COALESCE_MAX_BYTES;
use gnitz_wire::MAX_WORKERS;

/// A decode failure on one frame of a reply train, named after the verb `what`.
pub(super) fn scan_decode_err(w: usize, what: &str, e: &'static str) -> WorkerFault {
    format!("{what}: worker {w}: decode error: {e}").into()
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
pub(super) fn parse_train_header(
    slot: &W2mSlot,
    w: usize,
    what: &str,
) -> Result<(gnitz_wire::control::DecodedControl, bool), WorkerFault> {
    let ctrl = peek_control_block_ipc(slot.bytes()).map_err(|e| scan_decode_err(w, what, e))?;
    if let Some(e) = super::worker_error(w, what, &ctrl) {
        return Err(e);
    }
    let has_more = train_has_more(ctrl.flags);
    Ok((ctrl, has_more))
}

/// Whether more frames follow the one carrying `flags` — the one definition of
/// where a train ends.
///
/// Every producer sets `FLAG_CONTINUATION` on every frame and `FLAG_SCAN_LAST`
/// on the last, a one-frame train included: worker scan/probe/index-seek
/// replies, unique pre-flight frames, and a worker's exchange partition. So a
/// frame with neither flag is terminal — the single-frame `send_response` shape,
/// and `send_error`'s fault frame, whose `0` flags would otherwise read as
/// "more coming" and block the drain forever.
pub(crate) fn train_has_more(flags: u64) -> bool {
    flags & FLAG_SCAN_LAST == 0 && flags & FLAG_CONTINUATION != 0
}

/// Enforce the single-frame reply contract on `slot`: a fault frame or corrupt
/// header errors like `parse_train_header`, and ANY `FLAG_CONTINUATION` frame —
/// including a length-1 train's terminal `FLAG_SCAN_LAST` frame — is rejected.
/// The slot is forwarded verbatim as a complete reply, so a chunked train would
/// be truncated to its first frame with the remainder silently discarded by the
/// lease drop. The one caller routes a `Seek`, which the worker answers through
/// `send_response` — a shape that carries no train flags at any reply budget. A
/// train here means that arm was rerouted through the chunking path; fail loudly
/// instead.
pub(super) fn expect_single_frame(slot: &W2mSlot, w: usize, what: &str) -> Result<(), WorkerFault> {
    let (ctrl, _) = parse_train_header(slot, w, what)?;
    if ctrl.flags & FLAG_CONTINUATION != 0 {
        return Err(format!("worker {w}: {what}: unexpected chunked reply on a single-frame path").into());
    }
    Ok(())
}

/// Drain every worker's train in worker order, invoking `on_batch` with the
/// zero-copy `MemBatch` and the raw frame byte length of each non-empty frame.
/// Shared by the unique-filter warmup and the collect paths (index seek,
/// constraint probe).
///
/// Returns on the FIRST error — worker fault, corrupt frame, schema mismatch, or
/// an `Err` from `on_batch` — without draining the rest. All callers hold the
/// `ScanLease` from `dispatch_scan_fanout`; when the early `Err` unwinds it, the
/// lease drop frees every parked slot and `route_scan_slot` discards later
/// frames at the ring boundary, advancing `release_cursor`, so a still-streaming
/// worker cannot wedge in `W2mWriter::send_msg`.
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
    mut on_batch: impl FnMut(&gnitz_store::storage::MemBatch<'_>, usize) -> Result<(), WorkerFault>,
) -> Result<(), WorkerFault> {
    for (i, mut slot) in slots.into_iter().enumerate() {
        let (w, req_id) = scan.reply(i);
        let mut saved_schema: Option<SchemaDescriptor> = None;
        loop {
            let (ctrl, has_more) = parse_train_header(&slot, w, what)?;
            let frame_len = slot.bytes().len();
            let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
            let zc = wire::decode_wire_ipc_zero_copy_with_ctrl(slot.bytes(), ctrl, saved_schema.as_ref(), &mut offsets)
                .map_err(|e| scan_decode_err(w, what, e))?;
            if saved_schema.is_none() {
                if let Some(ref s) = zc.schema {
                    wire::validate_schema_match(s, expected)
                        .map_err(|e| WorkerFault::from(format!("worker {w}: {what}: {e}")))?;
                    saved_schema = Some(*s);
                }
            }
            if let Some(ref mb) = zc.data_batch {
                if !mb.is_empty() {
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
/// [`COALESCE_MAX_BYTES`], they are corked rather than sent, so they and the
/// terminal frame behind them leave in one egress operation instead of W+1. Each
/// frame carries its own `[len | payload]` prefix inside the ring mapping, so
/// the concatenation needs nothing synthesized between frames. Anything else
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
        // next worker's head, reordering the client's rows. A lone frame goes
        // zero-copy from the ring instead of being copied to join the terminal.
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
    // `cork` copies synchronously, so every ring slot is released before any SQE
    // exists — a client that stalls the send cannot pin a slot and fill the
    // worker's W2M ring. A disconnect surfaces at whichever flush ships them.
    for (i, slot) in slots.iter().enumerate() {
        if heads[i].observable {
            peer.cork(slot.frame_bytes());
        }
    }
    drop(slots);
    Ok(true)
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
/// `ScanLease`, discarding the rest of the train and advancing release_cursor so
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
#[path = "tests/train.rs"]
mod tests;
