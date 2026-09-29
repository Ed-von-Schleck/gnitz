//! One exchange round's two ends: [`op_exchange_route`] on the sending worker,
//! [`op_exchange_gather`] on the receiving one.

use crate::schema::{OpBuildErr, SchemaDescriptor};
use crate::storage::{merge_consolidated, reset_slots, Batch, Layout, MemBatch};

use super::router::{ScatterKey, ScatterSpec};

/// Reset `out` to one ascending list per worker of the live rows of `batch`
/// that worker owns under `spec`.
///
/// Refused when `spec` does not route against `batch`'s schema — see
/// `ScatterKey::new`, which owns the reason.
pub fn op_exchange_route<'a>(
    batch: &Batch,
    spec: ScatterSpec<'_>,
    out: &'a mut Vec<Vec<u32>>,
    num_workers: usize,
) -> Result<&'a [Vec<u32>], OpBuildErr> {
    let key = ScatterKey::new(spec, batch.schema())?;
    let slots = reset_slots(out, num_workers);
    key.route(&batch.as_mem_batch(), slots);
    Ok(slots)
}

/// Z-set `+` over one receiver's slices of an exchange round, one per sender:
/// merged when every slice is `consolidated`, else concatenated in sender order.
pub fn op_exchange_gather(slices: &[MemBatch], schema: &SchemaDescriptor, consolidated: bool) -> Batch {
    if consolidated && slices.iter().filter(|mb| mb.count > 0).count() >= 2 {
        return merge_consolidated(slices, schema);
    }
    let mut out = Batch::concat(schema, slices.iter().cloned());
    if consolidated {
        out.certify_layout(Layout::Consolidated);
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/round.rs"]
mod tests;
