//! `WindowPlan` — everything `op_window` needs that is a pure function of
//! compile-time facts, baked once at emit time.
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use gnitz_wire::{AggDescriptor, AggFunc, OrderKey};

use crate::algebra::{Agg, GroupOutKey, ReduceShape};
use crate::stream::order_index::OrderIndex;

/// The baked per-instruction window plan.
pub struct WindowPlan {
    /// The group key region a reduce over the same group set would have, every
    /// input column not in that region, in input schema order, then the values:
    /// the rank, the dense rank, the aggregates.
    pub(super) output_schema: SchemaDescriptor,
    /// How a row's group is found and keyed in the output.
    pub(super) key: GroupOutKey,
    /// The ordered index of every input row this operator is maintained through.
    pub(super) index: OrderIndex,
    /// The payload slot of each rank the output has.
    pub(super) rank: Option<usize>,
    pub(super) dense_rank: Option<usize>,
    /// The aggregates, each reading its column off an index row.
    pub(super) aggs: Vec<Agg>,
}

impl WindowPlan {
    /// `Err` for a shape an untrusted producer can name and the operator cannot
    /// execute.
    pub fn from_wire(
        input: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        aggs: &[AggDescriptor],
        rank: bool,
        dense_rank: bool,
    ) -> Result<Self, String> {
        if aggs.is_empty() && !rank && !dense_rank {
            return Err("window: no value columns".to_string());
        }
        // A COUNT(*) reads no column.
        let reads = |d: &&AggDescriptor| d.agg_op != AggFunc::Count;
        let read = aggs.iter().filter(reads).map(|d| d.col_idx);
        let (index, key, mut out) = OrderIndex::new(input, group_cols, order, read)?;

        let mut slot = key.carried().len();
        let mut value = |has: bool| {
            has.then(|| {
                out.push(SchemaColumn::new(TypeCode::I64, false));
                slot += 1;
                slot - 1
            })
        };
        let (rank, dense_rank) = (value(rank), value(dense_rank));
        // The aggregates as they read an index row.
        let over_index: Vec<AggDescriptor> = aggs
            .iter()
            .map(|d| {
                let carried = Some(d).filter(reads).map(|d| {
                    let at = index.column_of(input.locate(d.col_idx as usize));
                    at.expect("the index carries every column an aggregate reads") as u32
                });
                AggDescriptor { col_idx: carried.unwrap_or(0), ..*d }
            })
            .collect();
        // A row's own frame holds the row, so no value is taken over no rows.
        let ReduceShape { output_schema, key, aggs } = ReduceShape::new(&index.schema, key, out, &over_index, false)?;
        Ok(WindowPlan {
            output_schema,
            key,
            index,
            rank,
            dense_rank,
            aggs,
        })
    }

    /// The layout `op_window` emits under this plan.
    pub fn output_schema(&self) -> &SchemaDescriptor {
        &self.output_schema
    }

    /// The layout of the ordered index the operator is maintained through.
    pub fn index_schema(&self) -> &SchemaDescriptor {
        &self.index.schema
    }
}
