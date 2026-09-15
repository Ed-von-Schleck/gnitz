//! Worker reply trains: the frames a worker answers one scan-shaped request with,
//! ending at the one flagged `scan_last`.

use super::*;

/// A decode failure on one frame of a reply train, named after the verb `what`.
fn scan_decode_err(slot: &W2mSlot, what: &str, e: &str) -> WireFault {
    format!("{what}: worker {}: decode error: {e}", slot.worker).into()
}

/// One frame header of a train and whether more frames follow, or `Err`
/// (prefixed with `what`) on a fault frame or a corrupt header. Dropping the
/// caller's scan lease discards the rest of the train.
fn parse_train_header(slot: &W2mSlot, what: &str) -> Result<(gnitz_wire::control::DecodedControl, bool), WireFault> {
    let ctrl = peek_control_block(slot.bytes()).map_err(|e| scan_decode_err(slot, what, e))?;
    if let Some(e) = super::worker_error(slot.worker as usize, what, &ctrl) {
        return Err(e);
    }
    let has_more = !ctrl.hdr.flags.scan_last;
    Ok((ctrl, has_more))
}

/// The train frame in `slot`: whether more frames follow, and its rows decoded
/// against `expected`.
pub(super) fn decode_train_slot<'a>(
    slot: &'a W2mSlot,
    what: &str,
    expected: &SchemaDescriptor,
    offsets: &'a mut [usize; gnitz_store::storage::MAX_BATCH_REGIONS],
) -> Result<(bool, Option<gnitz_store::storage::MemBatch<'a>>), WireFault> {
    let (ctrl, has_more) = parse_train_header(slot, what)?;
    let batch = wire::decode_train_frame(slot.bytes(), &ctrl, expected, offsets)
        .map_err(|e| scan_decode_err(slot, what, &e))?;
    Ok((has_more, batch))
}

/// Drain every reply train of `lease` in reply order, handing `on_batch` each
/// non-empty frame's rows. A block-less frame decodes against `expected`; a frame
/// whose own block disagrees with it — a worker lagging a DDL — is an error.
pub(super) async fn drain_index_scan(
    lease: &Lease,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&gnitz_store::storage::MemBatch<'_>) -> Result<(), WireFault>,
) -> Result<(), WireFault> {
    for w in lease.workers() {
        loop {
            let slot = lease.next_frame(w).await;
            let mut offsets = [0usize; gnitz_store::storage::MAX_BATCH_REGIONS];
            let (has_more, batch) = decode_train_slot(&slot, what, expected, &mut offsets)?;
            if let Some(mb) = batch.filter(|mb| !mb.is_empty()) {
                on_batch(&mb)?;
            }
            if !has_more {
                break;
            }
        }
    }
    Ok(())
}

/// Forward every reply train of `lease` to `peer` in reply order. `Ok(false)` on
/// client disconnect; `Err` on the first worker fault, leaving the rest undrained.
pub(crate) async fn forward_scan(peer: &Peer, lease: &Lease) -> Result<bool, WireFault> {
    for w in lease.workers() {
        if !drain_scan_train(peer, lease, w).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Forward worker `w`'s train to `peer`, skipping frames that carry nothing the
/// client reads. `Ok(false)` if the client is gone.
async fn drain_scan_train(peer: &Peer, lease: &Lease, w: usize) -> Result<bool, WireFault> {
    loop {
        let slot = lease.next_frame(w).await;
        let (ctrl, has_more) = parse_train_header(&slot, "scan")?;
        if !ctrl.hdr.flags.has_data && !ctrl.hdr.flags.has_schema {
            drop(slot);
        } else if peer.send(slot).await < 0 {
            return Ok(false);
        }
        if !has_more {
            break;
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "tests/train.rs"]
mod tests;
