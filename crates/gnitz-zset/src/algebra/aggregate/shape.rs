//! `ReduceShape`: what a reduce's rows look like, shared by the circuit reduce
//! and the ad-hoc fold; and `resolve_aggs`, which the window lays its
//! aggregates out with too.
use std::rc::Rc;

use crate::schema::{DerivedSchema, SchemaColumn, SchemaDescriptor};

use super::agg::Agg;
use crate::algebra::group_key::GroupOutKey;
use gnitz_wire::AggDescriptor;

/// What the rows of an operator emitting aggregates behind a group key look
/// like: the output layout, the group key, and the aggregates.
pub(crate) struct ReduceShape {
    pub(crate) output_schema: SchemaDescriptor,
    pub(crate) key: GroupOutKey,
    pub(crate) aggs: Rc<[Agg]>,
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
        // The output's leading columns: they fit wherever the whole output does,
        // so the output's refusal is the one reported.
        let leading = prefix.finish();
        // A global reduce's one group has a value over no rows.
        let (output_schema, aggs) = resolve_aggs(input, prefix, aggs, key.is_global())?;
        Ok((ReduceShape { output_schema, key, aggs }, leading?))
    }
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
) -> Result<(SchemaDescriptor, Rc<[Agg]>), String> {
    for d in aggs {
        let (src, _) = input.wire_col("aggregate column", d.col_idx)?;
        let tc = d
            .agg_op
            .output_type(src.type_code)
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
