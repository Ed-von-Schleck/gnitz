//! `ReducePlan`: everything `op_reduce` needs baked at emit time, over the
//! `ReduceShape` the ad-hoc fold shares.

use super::avi::{avi_batch, AviBake};
use crate::algebra::{emit_reduce_row, ground_pk, Agg, AggValues, ReduceShape};
use crate::repr::Batch;
use crate::schema::SchemaDescriptor;
use gnitz_wire::AggDescriptor;
use gnitz_wire::AggFunc;

/// The baked per-instruction circuit reduce plan.
pub struct ReducePlan {
    pub(super) shape: ReduceShape,
    /// The row a global aggregate publishes over an empty input; `None` for a
    /// reduce that owes none.
    pub(super) ground: Option<Batch>,
    /// Position of the aggregate holding a group's net row count.
    pub(super) cardinality: usize,
    /// The value index the non-linear aggregates read their history from.
    pub(super) avi: Option<AviBake>,
}

impl ReducePlan {
    /// `Err` for a shape `ReduceShape` refuses and a reduce without a COUNT(*).
    pub fn from_wire(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        seeds_ground: bool,
    ) -> Result<Self, String> {
        Self::new(input, group_cols, aggs, cardinality(aggs)?, seeds_ground)
    }

    /// One worker's share of a global reduce over `aggs`, or `None` for a float
    /// sum, whose value depends on the order a split would change.
    pub fn partial(input: &SchemaDescriptor, aggs: &[AggDescriptor]) -> Result<Option<Self>, String> {
        // No ground row: a worker with no rows contributes no partial.
        let plan = Self::from_wire(input, &[], aggs, false)?;
        let splits = |agg: &Agg| agg.is_exact_linear() || !agg.is_linear();
        Ok(plan.shape.aggs.iter().all(splits).then_some(plan))
    }

    /// The global reduce over relayed [`Self::partial`] outputs `[V₀ | aggregates…]`,
    /// each aggregate folding its own column by its [`AggFunc::merge_op`].
    pub fn combine(partials: &SchemaDescriptor, aggs: &[AggDescriptor], seeds_ground: bool) -> Result<Self, String> {
        let merged: Vec<AggDescriptor> = aggs
            .iter()
            .zip(1..)
            .map(|(d, col_idx)| AggDescriptor { col_idx, agg_op: d.agg_op.merge_op() })
            .collect();
        // A COUNT merges as a SUM, so its position is read off `aggs`.
        let plan = Self::new(partials, &[], &merged, cardinality(aggs)?, seeds_ground)?;
        debug_assert_eq!(plan.shape.output_schema, *partials);
        Ok(plan)
    }

    fn new(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        aggs: &[AggDescriptor],
        cardinality: usize,
        seeds_ground: bool,
    ) -> Result<Self, String> {
        debug_assert!(
            !seeds_ground || group_cols.is_empty(),
            "the ground row carries no group columns"
        );
        let (shape, _) = ReduceShape::new(input, group_cols, aggs)?;
        let avi = AviBake::new(input, group_cols, &shape.aggs)?;
        let ground = seeds_ground.then(|| {
            let mut v0 = Batch::with_capacity(&shape.output_schema, 1);
            let values = AggValues::new(&shape.aggs, 1);
            emit_reduce_row(&mut v0, None, ground_pk().bytes(), &shape.aggs, &values, 0);
            v0
        });
        Ok(ReducePlan { shape, ground, cardinality, avi })
    }

    /// The row a global aggregate publishes over an empty input; `None` for a
    /// reduce that owes none.
    pub fn ground_row(&self) -> Option<&Batch> {
        self.ground.as_ref()
    }

    /// The layout `op_reduce` emits under this plan.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.shape.output_schema
    }

    /// The layout of the value index a MIN/MAX of this reduce is maintained
    /// through; `None` for a reduce without one.
    pub fn index_schema(&self) -> Option<&SchemaDescriptor> {
        self.avi.as_ref().map(|bake| &bake.schema)
    }

    /// The index entries `delta` contributes, each at its row's weight,
    /// unsorted; `None` for a reduce without a value index.
    pub fn index_batch(&self, delta: &Batch) -> Option<Batch> {
        self.avi.as_ref().map(|bake| avi_batch(delta, bake))
    }

    /// True iff every aggregate is a count or an integer sum, whose value is the
    /// same whatever the row order and the partition.
    pub fn is_exact_linear(&self) -> bool {
        self.shape.aggs.iter().all(Agg::is_exact_linear)
    }
}

/// The position of the COUNT(*) holding a group's net row count: only it tells
/// an emptied group from a live one.
fn cardinality(aggs: &[AggDescriptor]) -> Result<usize, String> {
    aggs.iter()
        .position(|d| d.agg_op == AggFunc::Count)
        .ok_or_else(|| "reduce: a circuit reduce needs a COUNT(*)".to_string())
}
