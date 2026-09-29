//! One exchange round's two ends: [`op_exchange_route`] on the sending worker,
//! [`op_exchange_gather`] on the receiving one.

use crate::schema::SchemaDescriptor;
use crate::storage::{merge_consolidated, reset_slots, Batch, Layout, MemBatch};

use super::router::ScatterPlan;

/// Reset `out` to one ascending list per worker of the live rows of `batch`
/// that worker owns under `plan`.
pub fn op_exchange_route<'a>(
    batch: &Batch,
    plan: &ScatterPlan,
    out: &'a mut Vec<Vec<u32>>,
    num_workers: usize,
) -> &'a [Vec<u32>] {
    let slots = reset_slots(out, num_workers);
    plan.route(&batch.as_mem_batch(), slots);
    slots
}

/// Z-set `+` over one receiver's slices of an exchange round, none of them
/// empty: merged when every slice is `consolidated`, else concatenated in order.
pub fn op_exchange_gather(slices: &[MemBatch], schema: &SchemaDescriptor, consolidated: bool) -> Batch {
    debug_assert!(
        slices.iter().all(|mb| mb.count > 0),
        "an exchange gather over an empty slice"
    );
    if consolidated && slices.len() >= 2 {
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
