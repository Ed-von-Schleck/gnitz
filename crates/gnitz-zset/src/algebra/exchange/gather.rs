//! The receiving end of an exchange round: [`op_exchange_gather`].

use crate::repr::{merge_consolidated, Batch, MemBatch};
use crate::schema::SchemaDescriptor;

/// Z-set `+` over one receiver's slices of an exchange round: merged when
/// every slice is `consolidated`, else concatenated in order.
pub fn op_exchange_gather(slices: &[MemBatch], schema: &SchemaDescriptor, consolidated: bool) -> Batch {
    match consolidated {
        true => merge_consolidated(slices, schema),
        false => Batch::concat(schema, slices.iter().cloned()),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/gather.rs"]
mod tests;
