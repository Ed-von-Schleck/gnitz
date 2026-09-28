//! `TopNPlan` — everything `op_topn` needs that is a pure function of
//! compile-time facts, baked once at emit time.

use crate::schema::ColumnTable;
use crate::schema::{OpBuildErr, SchemaDescriptor};
use crate::storage::Batch;
use gnitz_wire::OrderKey;

use super::super::group_key::GroupOutKey;
use super::index::TopNIndex;

/// The baked per-instruction top-N plan.
pub struct TopNPlan {
    /// The group key region a reduce over the same group set would have, then
    /// every input column not in that region, in input schema order.
    pub output_schema: SchemaDescriptor,
    /// How a row's group is found and keyed in the output.
    pub(super) key: GroupOutKey,
    /// Weight slots to skip, then to keep, per group. `limit ≥ 1`.
    pub(super) offset: u64,
    pub(super) limit: u64,
    /// The ordered index of every input row this operator is maintained through.
    pub index: TopNIndex,
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
    ) -> Result<Self, OpBuildErr> {
        if limit == 0 {
            return Err(OpBuildErr::shape("top-n: a zero limit selects nothing"));
        }
        let (key, prefix) = GroupOutKey::new(input_schema, group_cols, 0..input_schema.num_columns() as u32)?;
        let output_schema = prefix.finish();
        let index = TopNIndex::new(input_schema, group_cols, order, &output_schema)?;
        Ok(TopNPlan { output_schema, key, offset, limit, index })
    }

    /// The index entries `delta` contributes; see [`TopNIndex::batch`].
    pub fn index_batch(&self, delta: &Batch) -> Batch {
        self.index.batch(delta, self.key.carried())
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
    /// key.
    pub fn combine(
        partials: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, OpBuildErr> {
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
