//! `TopNPlan` — everything `op_topn` needs that is a pure function of
//! compile-time facts, baked once at emit time.

use crate::schema::{DerivedSchema, OpBuildErr, ReduceOutKey, SchemaBound, SchemaDescriptor, SchemaFacts};
use gnitz_wire::{OrderKey, ReduceOutSlot};

use super::super::group_key::{GroupOutKey, GROUP_PK_COL};
use super::index::TopNIndex;

/// The baked per-instruction top-N plan.
pub struct TopNPlan {
    /// The output layout: the group key region a reduce over the same group set
    /// would have ([`ReduceOutKey`]), then every input column not in that region,
    /// in input schema order. Derived here so no caller derives it a second time.
    pub output_schema: SchemaDescriptor,
    /// How a row's group is found and keyed in the output.
    pub(super) key: GroupOutKey,
    /// Weight slots to skip, then to keep, per group. `limit ≥ 1`.
    pub(super) offset: u64,
    pub(super) limit: u64,
    /// The ordered index of every input row this operator is maintained through.
    pub index: TopNIndex,
}

/// The top-N output schema and the input columns it carries as payload. `Err`
/// with the bound the columns overflowed.
fn build_topn_output_schema(
    input: &SchemaDescriptor,
    out_key: ReduceOutKey,
    group_cols: &[u32],
) -> Result<(SchemaDescriptor, Vec<u32>), SchemaBound> {
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
    Ok((b.finish(), carried))
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
            .map_err(|e| OpBuildErr::shape(format!("top-n: output {e}")))?;
        let index = TopNIndex::new(input_schema, group_cols, order, &carried)?;
        let key = GroupOutKey::new(input_schema, group_cols, out_key, &output_schema)?;
        Ok(TopNPlan { output_schema, key, offset, limit, index })
    }

    /// One worker's window of a global top-N: its first `limit + offset` slots,
    /// which hold every global slot.
    pub fn partial(
        input_schema: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, OpBuildErr> {
        Self::from_wire(input_schema, &[], order, limit.saturating_add(offset), 0)
    }

    /// The global window over relayed [`Self::partial`] outputs, grouped on their
    /// key. `Err` unless the output keeps the partials' layout.
    pub fn combine(
        partials: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, OpBuildErr> {
        let mismatch = || OpBuildErr::shape("top-n: the partials do not match the order");
        // A partial's key, then the input's columns in order.
        let key = partials.pk_indices();
        let shifted: Vec<OrderKey> = order
            .iter()
            .map(|k| {
                let col = k.col.checked_add(key.len() as u16).ok_or_else(mismatch)?;
                Ok(OrderKey { col, ..*k })
            })
            .collect::<Result<_, OpBuildErr>>()?;
        let plan = Self::from_wire(partials, key, &shifted, limit, offset)?;
        if plan.output_schema != *partials {
            return Err(mismatch());
        }
        Ok(plan)
    }
}
