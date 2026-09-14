//! `TopNPlan` — everything `op_topn` needs that is a pure function of
//! compile-time facts, baked once at emit time.

use crate::schema::{DerivedSchema, OpBuildErr, ReduceOutKey, SchemaDescriptor};
use crate::storage::MemBatch;
use gnitz_wire::{OrderKey, ReduceOutSlot};

use super::super::group_key::{group_out_pk, GroupKeyCols, OutPk, GROUP_PK_COL};
use super::index::TopNIndex;

/// The baked per-instruction top-N plan. Built by [`TopNPlan::from_wire`] only.
pub struct TopNPlan {
    /// The output layout: the group key region a reduce over the same group set
    /// would have ([`ReduceOutKey`]), then every input column not in that region,
    /// in input schema order. Derived here so no caller derives it a second time.
    pub output_schema: SchemaDescriptor,
    pub(super) out_key: ReduceOutKey,
    pub(super) group_key: GroupKeyCols,
    /// Weight slots to skip, then to keep, per group. `limit ≥ 1`.
    pub(super) offset: u64,
    pub(super) limit: u64,
    /// The ordered index of every input row this operator is maintained through.
    pub index: TopNIndex,
}

/// The top-N output schema and the input columns it carries as payload. `None`
/// when the columns would overflow the fixed schema array.
fn build_topn_output_schema(
    input: &SchemaDescriptor,
    out_key: ReduceOutKey,
    group_cols: &[u32],
) -> Option<(SchemaDescriptor, Vec<u32>)> {
    let mut b = DerivedSchema::new();
    let mut carried = Vec::new();
    for slot in out_key.output_layout(input.pk_indices(), group_cols, 0..input.num_columns() as u32) {
        match slot {
            ReduceOutSlot::SyntheticKey => b.push_pk(GROUP_PK_COL)?,
            ReduceOutSlot::Key(c) => b.push_pk(input.columns[c as usize])?,
            ReduceOutSlot::Carried(c) => {
                b.push(input.columns[c as usize])?;
                carried.push(c);
            }
        }
    }
    Some((b.finish(), carried))
}

impl TopNPlan {
    /// The one authority over everything derived from `(input_schema,
    /// group_cols, order, limit, offset)`. `Err` for a shape an untrusted
    /// producer can name and the operator cannot execute.
    pub fn from_wire(
        input_schema: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, OpBuildErr> {
        input_schema.check_cols(
            group_cols
                .iter()
                .map(|&c| ("top-n: group column", c))
                .chain(order.iter().map(|k| ("top-n: order column", k.col as u32))),
        )?;
        if limit == 0 {
            return Err(OpBuildErr::shape("top-n: a zero limit selects nothing"));
        }
        let out_key = input_schema.reduce_out_key(group_cols);
        let (output_schema, carried) = build_topn_output_schema(input_schema, out_key, group_cols)
            .ok_or_else(|| OpBuildErr::shape("top-n: output exceeds MAX_COLUMNS"))?;
        let index = TopNIndex::new(input_schema, group_cols, order, &carried)?;
        Ok(TopNPlan {
            output_schema,
            out_key,
            group_key: GroupKeyCols::new(input_schema, group_cols),
            offset,
            limit,
            index,
        })
    }

    /// The output PK of the group `row` of `mb` belongs to.
    #[inline]
    pub(super) fn out_pk<'a>(&self, mb: &'a MemBatch, row: usize) -> OutPk<'a> {
        group_out_pk(self.out_key, &self.group_key, self.output_schema.pk_stride(), mb, row)
    }
}
