//! `ReducePlan`, everything `op_reduce` needs baked at emit time, and the
//! `ReduceShape` part of it the ad-hoc fold shares.

use crate::schema::{DerivedSchema, OpBuildErr, ReduceOutKey, SchemaBound, SchemaColumn, SchemaDescriptor};

use super::super::group_key::GroupOutKey;
use super::agg::Accumulator;
use super::avi::AviBake;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// Every column index a reduce reads, bounded against the schema it indexes.
fn check_cols(schema: &SchemaDescriptor, group_cols: &[u32], agg_descs: &[AggDescriptor]) -> Result<(), OpBuildErr> {
    schema.check_cols(
        group_cols
            .iter()
            .map(|&c| ("reduce: group column", c))
            .chain(agg_descs.iter().map(|d| ("reduce: aggregate column", d.col_idx))),
    )
}

/// Build the reduce output schema from `out_key`. `Err` for an aggregate its
/// column's type does not admit, and for columns overflowing a schema bound.
pub(super) fn build_reduce_output_schema(
    input: &SchemaDescriptor,
    group_cols: &[u32],
    agg_descs: &[AggDescriptor],
    out_key: ReduceOutKey,
) -> Result<SchemaDescriptor, OpBuildErr> {
    let over = |e: SchemaBound| OpBuildErr::shape(format!("reduce: output {e}"));
    let mut b = DerivedSchema::new();
    for slot in out_key.output_layout(input.pk_indices(), group_cols, group_cols.iter().copied()) {
        match slot {
            gnitz_wire::ReduceOutSlot::SyntheticKey => b.push_pk(super::super::group_key::GROUP_PK_COL),
            gnitz_wire::ReduceOutSlot::Key(c) => b.push_pk(input.columns[c as usize]),
            gnitz_wire::ReduceOutSlot::Carried(c) => b.push(input.columns[c as usize]),
        }
        .map_err(over)?;
    }
    // Nullability covers what `emit_agg_col` writes.
    let ungrouped = group_cols.is_empty();
    for ad in agg_descs {
        let src = input.columns[ad.col_idx as usize];
        let tc = gnitz_wire::agg_output_type(ad.agg_op, src.type_code).ok_or_else(|| {
            OpBuildErr::shape(format!(
                "reduce: {:?} is not defined over type code {}",
                ad.agg_op, src.type_code
            ))
        })?;
        let nullable = ad.agg_op.raw_output_nullable(src.nullable != 0, ungrouped);
        b.push(SchemaColumn::new(tc, nullable as u8)).map_err(over)?;
    }
    Ok(b.finish())
}

/// What a reduce's rows look like, shared by the circuit reduce and the ad-hoc
/// fold: the output layout, the group key, and the accumulator set.
pub struct ReduceShape {
    pub output_schema: SchemaDescriptor,
    pub(super) key: GroupOutKey,
    /// The accumulator set in its empty-group state, cloned per use.
    pub(super) acc_template: Vec<Accumulator>,
}

impl ReduceShape {
    /// A circuit reduce's shape: the output is keyed as `reduce_out_key` decides.
    fn for_circuit(input: &SchemaDescriptor, group_cols: &[u32], aggs: &[AggDescriptor]) -> Result<Self, OpBuildErr> {
        check_cols(input, group_cols, aggs)?;
        Self::build(input, group_cols, aggs, input.reduce_out_key(group_cols))
    }

    /// The ad-hoc fold's shape: always keyed by the synthetic group key.
    pub(super) fn for_fold(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
    ) -> Result<Self, OpBuildErr> {
        check_cols(input, group_cols, aggs)?;
        Self::build(input, group_cols, aggs, ReduceOutKey::SyntheticFold)
    }

    /// Grouped by the empty set: one group, V₀.
    pub(super) fn is_global(&self) -> bool {
        self.key.group.cols.is_empty()
    }

    /// `Err` for an output [`build_reduce_output_schema`] refuses and a group
    /// column the group key refuses.
    fn build(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        out_key: ReduceOutKey,
    ) -> Result<Self, OpBuildErr> {
        let output_schema = build_reduce_output_schema(input, group_cols, aggs, out_key)?;
        // The aggregates are the trailing output columns.
        let cbase = output_schema.num_columns() - aggs.len();
        let acc_template = aggs
            .iter()
            .enumerate()
            .map(|(k, d)| {
                Accumulator::new(
                    d.agg_op,
                    input.locate(d.col_idx as usize),
                    output_schema.locate(cbase + k),
                )
            })
            .collect();
        let key = GroupOutKey::new(input, group_cols, out_key, &output_schema)?;
        Ok(ReduceShape { output_schema, key, acc_template })
    }
}

/// The baked per-instruction circuit reduce plan, built by
/// [`ReducePlan::from_wire`] only.
pub struct ReducePlan {
    pub shape: ReduceShape,
    /// This worker publishes the global-aggregate ground row.
    pub seeds_ground: bool,
    /// Position of the COUNT(*) carrying a group's net cardinality.
    pub(super) cardinality: usize,
    /// The value index the non-linear aggregates read their history from;
    /// `Some` iff there is one.
    pub avi: Option<AviBake>,
}

impl ReducePlan {
    /// True iff this reduce needs its input delta folded to net weights: only
    /// the non-linear aggregates, which walk the value index.
    #[inline]
    pub fn consolidates_input(&self) -> bool {
        self.avi.is_some()
    }

    /// `Err` for a shape [`ReduceShape`] refuses, a ground row over a group set,
    /// and a reduce without a COUNT(*).
    pub fn from_wire(
        input_schema: &SchemaDescriptor,
        group_by_cols: &[u32],
        agg_descs: &[AggDescriptor],
        global_ground: bool,
        i_am_owner: bool,
    ) -> Result<Self, OpBuildErr> {
        // The ground row carries no group columns.
        if global_ground && !group_by_cols.is_empty() {
            return Err(OpBuildErr::shape("reduce: global-ground over a non-empty group set"));
        }
        let shape = ReduceShape::for_circuit(input_schema, group_by_cols, agg_descs)?;
        // Only the net row count tells an emptied group from a live one.
        let cardinality = agg_descs
            .iter()
            .position(|d| d.agg_op == AggFunc::Count)
            .ok_or_else(|| OpBuildErr::shape("reduce: a circuit reduce needs a COUNT(*)"))?;
        let avi = AviBake::new(input_schema, group_by_cols, &shape.acc_template)?;
        Ok(ReducePlan {
            shape,
            seeds_ground: global_ground && i_am_owner,
            cardinality,
            avi,
        })
    }
}
