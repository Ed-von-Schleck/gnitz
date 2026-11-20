//! Linear operators: filter, union and null-extend, with the union's merged
//! output schema and an outer join's null-extend output schema.
//!
//! Negate's rows are a batch mechanic, `Batch::negated`; MAP is `MapPlan`, in
//! the sibling `map` module.

use std::borrow::Cow;

use gnitz_expr::RowFilter;

use super::copy::copy_rows_into;
use crate::repr::{Batch, merge_consolidated};
use crate::schema::{DerivedSchema, SchemaColumn, SchemaDescriptor};
use gnitz_wire::TypeCode;

/// Filter: retain rows where the predicate returns true, by contiguous-range
/// bulk copy. When every row passes, the input is the output, moved when it is
/// owned. An index-bounded backfill takes that path on every chunk: the access
/// path already satisfies the predicate the circuit still carries.
pub fn op_filter(batch: Cow<'_, Batch>, pred: &mut RowFilter) -> Batch {
    let ranges = pred.ranges(&batch.as_mem_batch());
    if *ranges == [(0, batch.len())] {
        return batch.into_owned();
    }
    Batch::from_ranges(&batch, ranges, 0)
}

/// `a`'s schema with each column's nullability OR-ed with `b`'s: a NULL and a
/// zero carry the same bytes, so only a nullable column's comparator parts them.
pub fn union_nullability_merge(a: &SchemaDescriptor, b: &SchemaDescriptor) -> Result<SchemaDescriptor, String> {
    if !a.same_layout(b) {
        return Err("union: inputs do not share a physical layout".to_string());
    }
    let cols: Vec<SchemaColumn> = (0..a.num_columns())
        .map(|c| {
            let (ac, bc) = (a.columns()[c], b.columns()[c]);
            SchemaColumn::new(ac.type_code, ac.nullable | bc.nullable)
        })
        .collect();
    Ok(SchemaDescriptor::new(&cols, a.pk_cols()))
}

/// Output schema of an outer-join NULL_EXTEND ([`null_extend`]):
/// the input's PK region unchanged, then its payload and one nullable column per
/// `type_codes` entry, in the order `nulls_first` selects.
pub fn null_extend_output_schema(
    in_schema: &SchemaDescriptor,
    type_codes: &[TypeCode],
    nulls_first: bool,
) -> Result<SchemaDescriptor, String> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(in_schema);
    let fill = |b: &mut DerivedSchema| type_codes.iter().for_each(|&tc| b.push(SchemaColumn::new(tc, true)));
    if nulls_first {
        fill(&mut b);
        b.push_payload_of(in_schema);
    } else {
        b.push_payload_of(in_schema);
        fill(&mut b);
    }
    b.finish().map_err(|e| format!("null-extend: merged schema {e}"))
}

/// Every row of `b` copied into `out_schema`, which extends `b`'s schema with
/// NULL-filled payload columns — ahead of `b`'s own under `nulls_first`, behind
/// them otherwise. It must share `b`'s PK stride.
///
/// The consolidated claim carries over: an all-NULL column compares equal on every
/// row, so (PK, payload) order and distinctness stay the original columns'.
pub fn null_extend(b: &Batch, out_schema: &SchemaDescriptor, nulls_first: bool) -> Batch {
    let (in_npc, out_npc) = (b.schema().num_payload_cols(), out_schema.num_payload_cols());
    debug_assert!(out_npc >= in_npc);
    let n_new = out_npc - in_npc;
    let (first_slot, new_slots) = match nulls_first {
        true => (n_new, 0..n_new),
        false => (0, in_npc..out_npc),
    };
    let new_null_bits = gnitz_wire::null_word_at(gnitz_wire::low_bits_mask(n_new), new_slots.start);
    let mut out = copy_rows_into(b, out_schema, first_slot, new_null_bits, |w| {
        // The new columns are NULL, and a NULL cell is zeroed.
        for pi in new_slots {
            w.col_mut(pi).fill(0);
        }
    });
    out.inherit_consolidated(b);
    out
}

/// Union: algebraic addition of two Z-Set streams. Two consolidated inputs take an
/// O(N) merge that sums equal elements' weights, so the output is consolidated and
/// may hold *fewer* rows than the two inputs together. An operand counts as
/// consolidated where its rows stand so without the claim. Anything else concatenates
/// and stays unconsolidated: the outer- and band-join lowering chains `op_union` over unconsolidated
/// operands, and sorting each link would pay for a fold the consumer runs once.
///
/// An empty operand passes the other through — moved when it is owned, with its
/// consolidated claim kept.
///
/// `out_schema` is the UNION's own, not either input's — it is the comparator the
/// merge folds and certifies under.
pub fn op_union(batch_a: Cow<'_, Batch>, batch_b: Cow<'_, Batch>, out_schema: &SchemaDescriptor) -> Batch {
    // Addition commutes, so the one pass-through below serves an empty operand
    // on either side.
    let (batch_a, batch_b) = match batch_a.is_empty() {
        true => (batch_b, batch_a),
        false => (batch_a, batch_b),
    };
    if batch_b.is_empty() {
        // `out_schema` only ORs in nullability, which reorders nothing the
        // operand holds, so its claim stands.
        let mut out = batch_a.into_owned();
        out.set_schema(out_schema);
        return out;
    }
    if batch_a.stands_consolidated() && batch_b.stands_consolidated() {
        return merge_consolidated(&[batch_a.as_mem_batch(), batch_b.as_mem_batch()], out_schema);
    }
    match batch_a {
        // Both operands sized once, where a clone would be grown by the append.
        Cow::Borrowed(a) => Batch::concat(out_schema, [a.as_mem_batch(), batch_b.as_mem_batch()].into_iter()),
        Cow::Owned(mut a) => {
            a.set_schema(out_schema);
            a.append_batch(&batch_b);
            a
        }
    }
}

#[cfg(test)]
#[path = "tests/linear.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/linear.rs"]
mod bench;
