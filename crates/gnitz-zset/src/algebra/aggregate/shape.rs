//! `ReduceShape`: what a reduce's rows look like, shared by the circuit reduce,
//! the ad-hoc fold and the window.
use crate::repr::{Batch, MemBatch};
use crate::schema::{ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor};

use super::agg::{Agg, AggValues};
use crate::algebra::group_key::GroupOutKey;
use gnitz_wire::AggDescriptor;

/// What the rows of an operator emitting aggregates behind a group key look
/// like: the output layout, the group key, and the aggregates.
pub(crate) struct ReduceShape {
    pub(crate) output_schema: SchemaDescriptor,
    pub(crate) key: GroupOutKey,
    pub(crate) aggs: Vec<Agg>,
}

impl ReduceShape {
    /// `aggs` over `input` grouped by `group_cols`, behind the group key region
    /// and the group columns it does not spell — the leading columns, whose
    /// schema comes second.
    pub(crate) fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
    ) -> Result<(Self, SchemaDescriptor), String> {
        let (key, prefix) = GroupOutKey::new(input, group_cols, group_cols.iter().copied())?;
        // A prefix of the output's columns: it finishes wherever the output does,
        // whose refusal comes first.
        let leading = prefix.finish();
        // A global reduce's one group has a value over no rows.
        let (output_schema, aggs) = resolve_aggs(input, prefix, aggs, key.is_global())?;
        Ok((ReduceShape { output_schema, key, aggs }, leading?))
    }
}

/// Emit one `[key…, group columns…, aggregates…]` row at weight +1, copying the
/// group columns from `group`'s row at its locators — the ground row has none —
/// and each of `aggs` from group `g` of `vals`.
pub(crate) fn emit_reduce_row(
    output: &mut Batch,
    group: Option<(&MemBatch, usize, &[ColumnLocator])>,
    out_pk_bytes: &[u8],
    aggs: &[Agg],
    vals: &AggValues,
    g: usize,
) {
    output.begin_row(out_pk_bytes, 1);
    if let Some((src, row, locs)) = group {
        output.append_cells_from(0, locs, src, row);
    }
    for (k, agg) in aggs.iter().enumerate() {
        vals.emit(k, agg, g, output);
    }
    output.commit_row();
}

/// `aggs` over `input`, emitted behind the `prefix` columns: the output layout,
/// and each aggregate resolved against both. `empty` is whether a value is ever
/// taken over no rows, as a global reduce's is. `Err` for an aggregate column out
/// of range or of a type its aggregate does not admit, and for columns
/// overflowing a schema bound.
pub(crate) fn resolve_aggs(
    input: &SchemaDescriptor,
    mut prefix: DerivedSchema,
    aggs: &[AggDescriptor],
    empty: bool,
) -> Result<(SchemaDescriptor, Vec<Agg>), String> {
    for d in aggs {
        let (src, _) = input.wire_col("aggregate column", d.col_idx)?;
        let tc = gnitz_wire::agg_output_type(d.agg_op, src.type_code)
            .ok_or_else(|| format!("{:?} is not defined over type code {}", d.agg_op, src.type_code))?;
        prefix.push(SchemaColumn::new(tc, d.agg_op.raw_output_nullable(src.nullable, empty)));
    }
    let output_schema = prefix.finish().map_err(|e| format!("aggregate output {e}"))?;
    // The aggregates are the trailing output columns.
    let cbase = output_schema.num_columns() - aggs.len();
    let aggs = aggs
        .iter()
        .zip(cbase..)
        .map(|(d, c)| Agg::new(d.agg_op, input.locate(d.col_idx as usize), output_schema.locate(c)))
        .collect();
    Ok((output_schema, aggs))
}
