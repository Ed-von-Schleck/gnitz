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

/// A decode failure on one frame of a reply train, named after the verb `what`.
pub(super) fn scan_decode_err(w: usize, what: &str, e: &'static str) -> WorkerFault {
    format!("{what}: worker {w}: decode error: {e}").into()
}

/// Parse one frame header of worker `w`'s train. Returns the control block plus
/// whether more frames follow, or `Err` (prefixed with `what`) on a fault frame
/// or a corrupt header. Every caller holds the scan's lease from
/// `dispatch_scan_fanout`, so propagating the `Err` is the whole disposal story:
/// the lease drop discards the undrained remainder at the ring boundary.
///
/// A corrupt header yields no frame structure to follow, and a fault frame is
/// terminal by contract — `send_error` reports it as a single frame with
/// `status = STATUS_ERROR` and flags `0` (no `FLAG_SCAN_LAST`), and the worker
/// emits nothing more for the request. The error therefore MUST stop the drain:
/// keying `has_more` off `FLAG_SCAN_LAST` alone would read the fault frame's `0`
/// flags as "more coming" and block forever in `next_frame`.
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
/// Returns on the first error; dropping the caller's scan lease discards the rest.
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
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&gnitz_store::storage::MemBatch<'_>, usize) -> Result<(), WorkerFault>,
) -> Result<(), WorkerFault> {
    for (i, mut slot) in slots.into_iter().enumerate() {
        let w = scan.worker(i);
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
            slot = scan.next_frame(i).await;
        }
    }
    Ok(())
}

/// Forward each already-awaited worker scan train to the client in reply order.
/// `Ok(false)` on client disconnect, `Err` on a worker fault / malformed train.
/// Whether a frame is corked beside its neighbours or sent alone is
/// `Peer::send`'s decision, so a run of small single-frame replies leaves in one
/// egress operation.
pub(super) async fn forward_scan_slots(
    peer: &Peer,
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
) -> Result<bool, WorkerFault> {
    for (i, slot) in slots.into_iter().enumerate() {
        if !drain_scan_train(peer, scan, i, slot).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Forward reply `i`'s train to the client: send each frame to `peer` (dropping
/// it before awaiting the next, per the W2M ring contract) and loop until the
/// header reports no more frames. `slot` is the first, already-awaited frame.
/// `Ok(false)` if the client disconnects mid-stream, `Err` on a malformed train
/// header.
///
/// A frame carrying neither rows nor a schema block holds nothing the client can
/// observe and nothing a later frame could decode against — on a selective
/// broadcast read that is W−1 of the W trains — so it is dropped before the next
/// await, releasing the slot at the ring rather than holding a worker parked on
/// a full ring. A fault frame never gets here — `parse_train_header` returns
/// `Err` on a non-zero status — and the train's terminal frame is
/// master-authored, so the client still sees the train end.
///
/// A send carries the eviction deadline: a client that stops draining a
/// zero-copy slot is evicted, rc goes negative, and the caller drops the scan's
/// lease, discarding the rest of the train and advancing release_cursor so the
/// worker unblocks.
async fn drain_scan_train(peer: &Peer, scan: &ScanDispatch, i: usize, mut slot: W2mSlot) -> Result<bool, WorkerFault> {
    loop {
        let (ctrl, has_more) = parse_train_header(&slot, scan.worker(i), "scan")?;
        if ctrl.flags & (FLAG_HAS_DATA | FLAG_HAS_SCHEMA) == 0 {
            drop(slot);
        } else if peer.send(slot).await < 0 {
            return Ok(false);
        }
        if !has_more {
            break;
        }
        slot = scan.next_frame(i).await;
    }
    Ok(true)
}

#[cfg(test)]
#[path = "tests/train.rs"]
mod tests;
