//! Worker reply trains: the frames a worker answers one scan-shaped request with,
//! ending at the one flagged `scan_last`.

use super::*;

use crate::runtime::peer::Peer;
use crate::runtime::reactor::worker_error;
use crate::runtime::w2m::W2mSlot;
use gnitz_store::storage::{MemBatch, MAX_BATCH_REGIONS};
use gnitz_wire::control::{peek_control_block, DecodedControl};

/// A decode failure on one frame of a reply train, named after the verb `what`.
fn decode_err(slot: &W2mSlot, what: &str, e: &str) -> WireFault {
    format!("{what}: worker {}: decode error: {e}", slot.worker).into()
}

/// One worker's frames on a lease, in order, ending at the one flagged `scan_last`.
pub(super) struct Train<'l> {
    lease: &'l Lease,
    worker: usize,
    what: &'l str,
    done: bool,
}

impl<'l> Train<'l> {
    pub(super) fn new(lease: &'l Lease, worker: usize, what: &'l str) -> Self {
        Train { lease, worker, what, done: false }
    }

    /// The next frame and its header, `None` past the terminal one; `Err` on a
    /// fault frame or a corrupt header. Dropping the lease discards the rest.
    pub(super) async fn next(&mut self) -> Result<Option<(W2mSlot, DecodedControl)>, WireFault> {
        if self.done {
            return Ok(None);
        }
        let slot = self.lease.next_frame(self.worker).await;
        let ctrl = peek_control_block(slot.bytes()).map_err(|e| decode_err(&slot, self.what, e))?;
        if let Some(e) = worker_error(slot.worker as usize, self.what, &ctrl) {
            return Err(e);
        }
        self.done = ctrl.hdr.flags.scan_last;
        Ok(Some((slot, ctrl)))
    }

    /// `slot`'s rows decoded against `expected`.
    pub(super) fn rows<'a>(
        &self,
        slot: &'a W2mSlot,
        ctrl: &DecodedControl,
        expected: &SchemaDescriptor,
        offsets: &'a mut [usize; MAX_BATCH_REGIONS],
    ) -> Result<Option<MemBatch<'a>>, WireFault> {
        wire::decode_train_frame(slot.bytes(), ctrl, expected, offsets).map_err(|e| decode_err(slot, self.what, &e))
    }
}

/// Drain every reply train of `lease` in reply order, handing `on_batch` each
/// non-empty frame's rows. A block-less frame decodes against `expected`; a frame
/// whose own block disagrees with it — a worker lagging a DDL — is an error.
pub(super) async fn drain_index_scan(
    lease: &Lease,
    what: &str,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&MemBatch<'_>) -> Result<(), WireFault>,
) -> Result<(), WireFault> {
    for w in lease.workers() {
        let mut train = Train::new(lease, w, what);
        while let Some((slot, ctrl)) = train.next().await? {
            let mut offsets = [0usize; MAX_BATCH_REGIONS];
            if let Some(mb) = train
                .rows(&slot, &ctrl, expected, &mut offsets)?
                .filter(|mb| !mb.is_empty())
            {
                on_batch(&mb)?;
            }
        }
    }
    Ok(())
}

/// Forward every reply train of `lease` to `peer` in reply order, skipping frames
/// that carry nothing the client reads. `Ok(false)` on client disconnect; `Err` on
/// the first worker fault, leaving the rest undrained.
pub(crate) async fn forward_scan(peer: &Peer, lease: &Lease) -> Result<bool, WireFault> {
    for w in lease.workers() {
        let mut train = Train::new(lease, w, "scan");
        while let Some((slot, ctrl)) = train.next().await? {
            if (ctrl.hdr.flags.has_data || ctrl.hdr.flags.has_schema) && peer.send(slot).await.is_err() {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

#[cfg(test)]
#[path = "tests/train.rs"]
mod tests;
