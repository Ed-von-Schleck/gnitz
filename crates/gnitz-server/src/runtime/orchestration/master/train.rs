//! Worker reply trains: the frames a worker answers one scan-shaped request with,
//! ending at the one flagged `scan_last`.

use super::*;

use crate::runtime::peer::Peer;
use gnitz_zset::repr::MemBatch;

/// Hand `on_batch` the rows of every frame of `lease`, workers in ascending order,
/// each decoded against `expected`.
pub(super) async fn drain_rows(
    lease: &TrainLease,
    expected: &SchemaDescriptor,
    mut on_batch: impl FnMut(&MemBatch<'_>) -> Result<(), WireFault>,
) -> Result<(), WireFault> {
    while let Some(f) = lease.next().await? {
        on_batch(&f.rows(expected))?;
    }
    Ok(())
}

/// Forward every frame of `lease` that carries rows to `peer`, workers in ascending
/// order. Stops at the first failed send, which closes the peer; `Err` on the first
/// worker fault, leaving the rest undrained.
pub(crate) async fn forward_scan(peer: &Peer, lease: &TrainLease) -> Result<(), WireFault> {
    while let Some(f) = lease.next().await? {
        if peer.send(f.slot).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests/train.rs"]
mod tests;
