//! `ReducePlan`, everything `op_reduce` needs baked at emit time, and the
//! `ReduceShape` part of it the ad-hoc fold shares.

use crate::schema::{DerivedSchema, OpBuildErr, SchemaBound, SchemaColumn, SchemaDescriptor};

use super::super::group_key::GroupOutKey;
use super::agg::Accumulator;
use super::avi::AviBake;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// What a reduce's rows look like, shared by the circuit reduce and the ad-hoc
/// fold: the output layout, the group key, and the accumulator set.
pub struct ReduceShape {
    pub output_schema: SchemaDescriptor,
    pub(super) key: GroupOutKey,
    /// The accumulator set in its empty-group state, cloned per use.
    pub(super) acc_template: Vec<Accumulator>,
}

impl ReduceShape {
    /// `aggs` over `input`, behind `key`'s leading `prefix` columns. `Err` for an
    /// aggregate column out of range or of a type its aggregate does not admit,
    /// and for columns overflowing a schema bound.
    pub(super) fn new(
        input: &SchemaDescriptor,
        key: GroupOutKey,
        mut prefix: DerivedSchema,
        aggs: &[AggDescriptor],
    ) -> Result<Self, OpBuildErr> {
        let over = |e: SchemaBound| OpBuildErr::shape(format!("reduce: output {e}"));
        // Nullability covers what `emit_agg_col` writes.
        let ungrouped = key.group.cols.is_empty();
        for d in aggs {
            let src = input
                .column(d.col_idx as usize)
                .ok_or_else(|| OpBuildErr::oob_col("reduce: aggregate column", d.col_idx, input))?;
            let tc = gnitz_wire::agg_output_type(d.agg_op, src.type_code).ok_or_else(|| {
                OpBuildErr::shape(format!(
                    "reduce: {:?} is not defined over type code {}",
                    d.agg_op, src.type_code
                ))
            })?;
            prefix
                .push(SchemaColumn::new(
                    tc,
                    d.agg_op.raw_output_nullable(src.nullable, ungrouped),
                ))
                .map_err(over)?;
        }
        let output_schema = prefix.finish();
        // The aggregates are the trailing output columns.
        let cbase = output_schema.num_columns() - aggs.len();
        let acc_template = aggs
            .iter()
            .zip(cbase..)
            .map(|(d, c)| Accumulator::new(d.agg_op, input.locate(d.col_idx as usize), output_schema.locate(c)))
            .collect();
        Ok(ReduceShape { output_schema, key, acc_template })
    }

    /// Grouped by the empty set: one group, V₀.
    pub(super) fn is_global(&self) -> bool {
        self.key.group.cols.is_empty()
    }
}

/// The baked per-instruction circuit reduce plan.
pub struct ReducePlan {
    pub shape: ReduceShape,
    /// This worker publishes the global-aggregate ground row.
    pub seeds_ground: bool,
    /// Position of the aggregate holding a group's net row count.
    pub(super) cardinality: usize,
    /// The value index the non-linear aggregates read their history from.
    pub avi: Option<AviBake>,
}

impl ReducePlan {
    /// `Err` for a shape `ReduceShape` refuses and a reduce without a COUNT(*).
    pub fn from_wire(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        seeds_ground: bool,
    ) -> Result<Self, OpBuildErr> {
        debug_assert!(
            !seeds_ground || group_cols.is_empty(),
            "the ground row carries no group columns"
        );
        let (key, prefix) = GroupOutKey::for_group_cols(input, group_cols, group_cols.iter().copied())?;
        let shape = ReduceShape::new(input, key, prefix, aggs)?;
        let avi = AviBake::new(input, group_cols, &shape.acc_template)?;
        Ok(ReducePlan {
            cardinality: cardinality(aggs)?,
            shape,
            seeds_ground,
            avi,
        })
    }

    /// The global reduce over the relayed outputs of an exact linear
    /// [`Self::from_wire`] over `aggs` and no group: `[V₀ | aggregates…]`, each
    /// aggregate folding into its own column by its [`AggFunc::merge_op`].
    pub fn combine(
        partials: &SchemaDescriptor,
        aggs: &[AggDescriptor],
        seeds_ground: bool,
    ) -> Result<Self, OpBuildErr> {
        debug_assert_eq!(partials.num_columns(), 1 + aggs.len());
        let (key, _) = GroupOutKey::synthetic(partials, &[], [])?;
        let acc_template = aggs
            .iter()
            .zip(1..)
            .map(|(d, c)| {
                let col = partials.locate(c);
                Accumulator::new(d.agg_op.merge_op(), col, col)
            })
            .collect();
        let plan = ReducePlan {
            cardinality: cardinality(aggs)?,
            shape: ReduceShape {
                output_schema: *partials,
                key,
                acc_template,
            },
            seeds_ground,
            avi: None,
        };
        debug_assert!(plan.is_exact_linear(), "only exact linear partials are split off");
        Ok(plan)
    }

    /// True iff every aggregate is a count or an integer sum, whose value is the
    /// same whatever the row order and the partition.
    pub fn is_exact_linear(&self) -> bool {
        self.shape.acc_template.iter().all(Accumulator::is_exact_linear)
    }
}

/// The position of the COUNT(*) holding a group's net row count: only it tells
/// an emptied group from a live one.
fn cardinality(aggs: &[AggDescriptor]) -> Result<usize, OpBuildErr> {
    aggs.iter()
        .position(|d| d.agg_op == AggFunc::Count)
        .ok_or_else(|| OpBuildErr::shape("reduce: a circuit reduce needs a COUNT(*)"))
}
