//! Worker reply trains: the frames a worker answers one scan-shaped request with,
//! ending at the one flagged `scan_last`.

use super::*;

/// A decode failure on one frame of a reply train, named after the verb `what`.
fn scan_decode_err(w: usize, what: &str, e: &str) -> WireFault {
    format!("{what}: worker {w}: decode error: {e}").into()
}

/// One frame header of worker `w`'s train and whether more frames follow, or `Err`
/// (prefixed with `what`) on a fault frame or a corrupt header. Dropping the
/// caller's scan lease discards the rest of the train.
fn parse_train_header(
    slot: &W2mSlot,
    w: usize,
    what: &str,
) -> Result<(gnitz_wire::control::DecodedControl, bool), WireFault> {
    let ctrl = peek_control_block(slot.bytes(), false).map_err(|e| scan_decode_err(w, what, e))?;
    if let Some(e) = super::worker_error(w, what, &ctrl) {
        return Err(e);
    }
    let has_more = !ctrl.flags.scan_last;
    Ok((ctrl, has_more))
}

/// Worker `w`'s train frame in `slot`: whether more frames follow, and its rows
/// decoded against `expected`.
pub(super) fn decode_train_slot<'a>(
    slot: &'a W2mSlot,
    w: usize,
    what: &str,
    expected: &SchemaDescriptor,
    offsets: &'a mut [usize; gnitz_store::storage::MAX_BATCH_REGIONS],
) -> Result<(bool, Option<gnitz_store::storage::MemBatch<'a>>), WireFault> {
    let (ctrl, has_more) = parse_train_header(slot, w, what)?;
    let batch =
        wire::decode_train_frame(slot.bytes(), &ctrl, expected, offsets).map_err(|e| scan_decode_err(w, what, &e))?;
    Ok((has_more, batch))
}

/// Drain every worker's train in worker order, handing `on_batch` each non-empty
/// frame's rows and byte length. A block-less frame decodes against `expected`;
/// a frame whose own block disagrees with it — a worker lagging a DDL — is an error.
pub(super) async fn drain_index_scan(
    slots: Vec<W2mSlot>,
    scan: &ScanDispatch,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&gnitz_store::storage::MemBatch<'_>, usize) -> Result<(), WireFault>,
) -> Result<(), WireFault> {
    for (i, mut slot) in slots.into_iter().enumerate() {
        let w = scan.worker(i);
        loop {
            let frame_len = slot.bytes().len();
            let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
            let (has_more, batch) = decode_train_slot(&slot, w, what, expected, &mut offsets)?;
            if let Some(mb) = batch.filter(|mb| !mb.is_empty()) {
                on_batch(&mb, frame_len)?;
            }
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
) -> Result<bool, WireFault> {
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
/// A frame carrying neither rows nor a schema block is dropped, releasing its ring
/// slot at once; the client's train end is the master's terminal frame.
///
/// A send carries the eviction deadline: a client that stops draining a
/// zero-copy slot is evicted, rc goes negative, and the caller drops the scan's
/// lease, discarding the rest of the train and advancing release_cursor so the
/// worker unblocks.
async fn drain_scan_train(peer: &Peer, scan: &ScanDispatch, i: usize, mut slot: W2mSlot) -> Result<bool, WireFault> {
    loop {
        let (ctrl, has_more) = parse_train_header(&slot, scan.worker(i), "scan")?;
        if !ctrl.flags.has_data && !ctrl.flags.has_schema {
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
