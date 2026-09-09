//! Linear operators: filter, negate, union.
//!
//! The other two live with the batch mechanics they are: MAP is
//! `crate::expr::MapPlan::evaluate_map_batch`, null-extend
//! `Batch::widened_with_null_tail`.

use gnitz_expr::Evaluator;

use crate::schema::{DerivedSchema, OpBuildErr, SchemaColumn, SchemaDescriptor};
use crate::storage::{Batch, Layout};

// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true, by contiguous-range bulk
/// copy. `None` when every row passed, so the caller can hand its own input
/// through instead of paying `from_ranges` a whole-batch copy, blob heap
/// included. An index-bounded backfill takes that path on every chunk: the
/// access path already satisfies the predicate the circuit still carries.
pub fn op_filter(batch: &Batch, pred: &Evaluator, schema: &SchemaDescriptor) -> Option<Batch> {
    // The DAG pushes an empty placeholder every epoch, and `filter_ranges` takes
    // its scratch borrow and sizes it before the morsel loop.
    if batch.count == 0 {
        return Some(Batch::empty_with_schema(schema));
    }

    // A per-call `Vec`: measured against a reused one it is a wash. `filter_ranges`
    // lends `out` so a *chunked* scan can carry one list; this caller has one batch.
    let mut ranges: Vec<(usize, usize)> = Vec::new();
    pred.filter_ranges(&batch.as_mem_batch(), &mut ranges);
    if ranges == [(0, batch.count)] {
        return None;
    }
    Some(Batch::from_ranges(batch, &ranges, schema))
}

/// `a`'s schema with each column's nullability OR-ed with `b`'s, so a
/// null-carrying side forces the null-aware `Generic` row comparator instead of
/// the null-blind `FixedIntNonnull` one, which would fail to coalesce two
/// logically-NULL rows carrying different bytes under the null bit.
///
/// Not [`DerivedSchema`], which forces `pk_indices = 0..pk_len`: a `Union`
/// input's PK need not be a column prefix. `SchemaDescriptor::new`'s asserts fire
/// in release but cannot here — matched layouts leave every PK column untouched.
pub fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Result<SchemaDescriptor, OpBuildErr> {
    if !a.same_physical_layout(b) {
        return Err(OpBuildErr::shape("union: inputs do not share a physical layout"));
    }
    let cols: Vec<SchemaColumn> = (0..a.num_columns())
        .map(|c| {
            let (ac, bc) = (a.columns[c], b.columns[c]);
            SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable)
        })
        .collect();
    Ok(SchemaDescriptor::new(&cols, a.pk_indices()))
}

/// Output schema of an outer-join NULL_EXTEND ([`Batch::widened_with_null_tail`]):
/// the input schema verbatim (PK region unchanged), then one nullable column per
/// null-fill `type_codes` entry. `decode_op_node` rejects an undecodable type
/// code, so every entry is a real column type.
pub fn null_extend_output_schema(
    in_schema: &SchemaDescriptor,
    type_codes: &[u8],
) -> Result<SchemaDescriptor, OpBuildErr> {
    const OVERFLOW: &str = "null-extend: merged schema exceeds MAX_COLUMNS";
    let mut b = DerivedSchema::new();
    let over = || OpBuildErr::shape(OVERFLOW);
    b.push_pk_of(in_schema).ok_or_else(over)?;
    for (_, c) in in_schema.payload_columns() {
        b.push(*c).ok_or_else(over)?;
    }
    for &tc in type_codes {
        b.push(SchemaColumn::new(tc, 1)).ok_or_else(over)?;
    }
    Ok(b.finish())
}

/// Negate: flip the sign of every weight. `wrapping_neg` because `i64::MIN` must
/// not panic; element identity is untouched, so the layout claim carries over.
pub fn op_negate(mut batch: Batch) -> Batch {
    batch.map_weights(i64::wrapping_neg);
    batch
}

/// Union: algebraic addition of two Z-Set streams. Two consolidated inputs take an
/// O(N) merge that sums equal elements' weights, so the output is consolidated and
/// may hold *fewer* rows than the two inputs together. Anything else concatenates
/// and stays `Raw`: the outer- and band-join lowering chains `op_union` over `Raw`
/// operands, and sorting each link would pay for a fold the consumer runs once.
///
/// `out_schema` is the UNION's own, not either input's — it is the comparator the
/// merge folds and certifies under.
pub fn op_union(batch_a: Batch, batch_b: &Batch, out_schema: &SchemaDescriptor) -> Batch {
    if batch_b.count == 0 {
        // O(1) pass-through: no allocation, the layout claim preserved.
        gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
        return batch_a;
    }
    if batch_a.count == 0 {
        return batch_b.clone_batch();
    }
    let (n_a, n_b) = (batch_a.count, batch_b.count);

    if batch_a.consolidated_verified(out_schema) && batch_b.consolidated_verified(out_schema) {
        let mut output = batch_a.merged_consolidated(batch_b, out_schema);
        output.certify_layout(Layout::Consolidated);
        gnitz_debug!("op_union: a={n_a} b={n_b} out={} sorted_merge", output.count);
        return output;
    }

    // Not both consolidated: concatenate (the appends leave `output` `Raw`).
    let output = batch_a.concatenated(batch_b, out_schema);
    gnitz_debug!("op_union: a={n_a} b={n_b} out={} concat", output.count);
    output
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/linear.rs"]
mod tests;
