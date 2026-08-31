//! Linear operators: filter, negate, union.
//!
//! The other two live with the batch mechanics they are: MAP is
//! `crate::expr::MapPlan::evaluate_map_batch`, null-extend
//! `Batch::widened_with_null_tail`.

use gnitz_expr::Evaluator;

use crate::schema::SchemaDescriptor;
use crate::storage::{Batch, Layout};

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub fn op_filter(batch: &Batch, pred: &Evaluator, schema: &SchemaDescriptor) -> Batch {
    // The DAG pushes an empty placeholder every epoch, and `filter_ranges` takes
    // its scratch borrow and sizes it before the morsel loop.
    if batch.count == 0 {
        return Batch::empty_with_schema(schema);
    }

    // A per-call `Vec`: measured against a reused one it is a wash. `filter_ranges`
    // lends `out` so a *chunked* scan can carry one list; this caller has one batch.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    pred.filter_ranges(&batch.as_mem_batch(), &mut ranges);
    Batch::from_ranges(batch, &ranges, schema)
}

/// Negate: flip the sign of every weight. `wrapping_neg` because `i64::MIN` must
/// not panic; element identity is untouched, so the layout claim carries over.
pub fn op_negate(mut batch: Batch) -> Batch {
    batch.map_weights(i64::wrapping_neg);
    batch
}

/// Union: algebraic addition of two Z-Set streams; sorted inputs take an O(N)
/// merge. `out_schema` is the UNION's own, not either input's — the merge
/// certifies `Sorted` under it, so a narrower comparator would leave a false
/// order claim for `into_consolidated` to trust.
pub fn op_union(batch_a: Batch, batch_b: &Batch, out_schema: &SchemaDescriptor) -> Batch {
    if batch_b.count == 0 {
        // O(1) pass-through: no allocation, sorted/consolidated preserved.
        gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
        return batch_a;
    }
    if batch_a.count == 0 {
        return batch_b.clone_batch();
    }

    if batch_a.sorted_verified(out_schema) && batch_b.sorted_verified(out_schema) {
        let mut output = batch_a.merged_sorted(batch_b, out_schema);
        // A payload-aware merge of two sorted inputs is genuinely
        // (PK, payload)-sorted, but unfolded (Z-Set `+` does not sum weights).
        output.certify_layout(Layout::Sorted, out_schema);
        gnitz_debug!(
            "op_union: a={} b={} out={} sorted_merge",
            batch_a.count,
            batch_b.count,
            output.count
        );
        return output;
    }

    // Unsorted: concatenate (the appends leave `output` `Raw`).
    let output = batch_a.concatenated(batch_b, out_schema);
    gnitz_debug!(
        "op_union: a={} b={} out={} concat",
        batch_a.count,
        batch_b.count,
        output.count
    );
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/linear.rs"]
mod tests;
