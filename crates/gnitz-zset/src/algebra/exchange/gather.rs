//! The receiving end of an exchange round: [`op_exchange_gather`].

use crate::repr::{merge_consolidated, Batch, MemBatch};
use crate::schema::SchemaDescriptor;

/// Z-set `+` over one receiver's slices of an exchange round: merged when
/// every slice is `consolidated`, else concatenated in order.
pub fn op_exchange_gather(slices: &[MemBatch], schema: &SchemaDescriptor, consolidated: bool) -> Batch {
    if consolidated && slices.len() >= 2 {
        return merge_consolidated(slices, schema);
    }
    let mut out = Batch::concat(schema, slices.iter().cloned());
    if consolidated {
        out.certify_consolidated();
    }
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/gather.rs"]
mod tests;
