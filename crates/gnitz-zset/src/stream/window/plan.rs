//! The window's plan: everything `op_window` needs that is a pure function of
//! compile-time facts, baked once at emit time.
use crate::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_wire::{AggDescriptor, AggFunc, OrderKey, TypeCode};

use std::rc::Rc;

use crate::algebra::{resolve_aggs, Agg};
use crate::stream::order_index::{OrderIndex, OrderedKind, OrderedPlan};

/// The values a window adds behind its input's columns: the rank, the dense
/// rank, the aggregates.
pub(in crate::stream) struct Window {
    /// The payload slot of each rank the output has.
    pub(super) rank: Option<usize>,
    pub(super) dense_rank: Option<usize>,
    /// The aggregates, each reading its column off an index row.
    pub(super) aggs: Rc<[Agg]>,
}

impl OrderedPlan {
    /// A window. `Err` for a shape an untrusted producer can name and the
    /// operator cannot execute.
    pub fn window(
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
        let (index, mut out) = OrderIndex::new(input, group_cols, order, read)?;

        let mut slot = index.row.len();
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
        let (output_schema, aggs) = resolve_aggs(&index.schema, out, &over_index, false)?;
        Ok(OrderedPlan {
            index,
            output_schema,
            kind: OrderedKind::Window(Window { rank, dense_rank, aggs }),
        })
    }
}
