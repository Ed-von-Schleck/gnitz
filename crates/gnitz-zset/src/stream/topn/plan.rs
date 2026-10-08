//! `TopNPlan` — everything `op_topn` needs that is a pure function of
//! compile-time facts, baked once at emit time.
use crate::schema::SchemaDescriptor;
use gnitz_wire::OrderKey;

use super::index::TopNIndex;
use crate::algebra::GroupOutKey;

/// The baked per-instruction top-N plan.
pub struct TopNPlan {
    /// The group key region a reduce over the same group set would have, then
    /// every input column not in that region, in input schema order.
    pub(super) output_schema: SchemaDescriptor,
    /// How a row's group is found and keyed in the output.
    pub(super) key: GroupOutKey,
    /// Weight slots to skip, then to keep, per group. `limit ≥ 1`.
    pub(super) offset: u64,
    pub(super) limit: u64,
    /// The ordered index of every input row this operator is maintained through.
    pub(super) index: TopNIndex,
}

impl TopNPlan {
    /// `Err` for a shape an untrusted producer can name and the operator cannot
    /// execute.
    pub fn from_wire(
        input_schema: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        if limit == 0 {
            return Err("top-n: a zero limit selects nothing".to_string());
        }
        let (key, prefix) = GroupOutKey::new(input_schema, group_cols, 0..input_schema.num_columns() as u32)?;
        let output_schema = prefix.finish().map_err(|e| format!("top-n: output {e}"))?;
        let index = TopNIndex::new(input_schema, group_cols, order, &output_schema)?;
        Ok(TopNPlan { output_schema, key, offset, limit, index })
    }

    /// The layout `op_topn` emits under this plan.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.output_schema
    }

    /// The layout of the ordered index the operator is maintained through.
    pub fn index_schema(&self) -> &SchemaDescriptor {
        &self.index.schema
    }

    /// One worker's window of a global top-N: its first `limit + offset` slots,
    /// which hold every global slot.
    pub fn partial(
        input_schema: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        Self::from_wire(input_schema, &[], order, limit.saturating_add(offset), 0)
    }

    /// The global window over relayed [`Self::partial`] outputs, grouped on their
    /// key.
    pub fn combine(partials: &SchemaDescriptor, order: &[OrderKey], limit: u64, offset: u64) -> Result<Self, String> {
        // A partial's key, then the input's columns in order.
        let key = partials.pk_cols();
        let shifted: Vec<OrderKey> = order
            .iter()
            .map(|k| OrderKey {
                col: k.col.saturating_add(key.len() as u16),
                ..*k
            })
            .collect();
        let plan = Self::from_wire(partials, key, &shifted, limit, offset)?;
        debug_assert_eq!(plan.output_schema, *partials);
        Ok(plan)
    }
}
