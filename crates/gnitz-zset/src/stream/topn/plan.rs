//! The top-N's plans: everything `op_topn` needs that is a pure function of
//! compile-time facts, baked once at emit time.
use crate::schema::SchemaDescriptor;
use gnitz_wire::OrderKey;

use crate::stream::order_index::{OrderIndex, OrderedKind, OrderedPlan};

/// The weight slots a top-N skips, then keeps, per group. `limit ≥ 1`.
pub(in crate::stream) struct TopN {
    pub(super) offset: u64,
    pub(super) limit: u64,
}

impl OrderedPlan {
    /// A top-N. `Err` for a shape an untrusted producer can name and the
    /// operator cannot execute.
    pub fn top_n(
        input_schema: &SchemaDescriptor,
        group_cols: &[u32],
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        if limit == 0 {
            return Err("top-n: a zero limit selects nothing".to_string());
        }
        let (index, row) = OrderIndex::new(input_schema, group_cols, order, [])?;
        let output_schema = row.finish().map_err(|e| format!("top-n: output {e}"))?;
        Ok(OrderedPlan {
            index,
            output_schema,
            kind: OrderedKind::TopN(TopN { offset, limit }),
        })
    }

    /// One worker's window of a global top-N: its first `limit + offset` slots,
    /// which hold every global slot.
    pub fn top_n_partial(
        input_schema: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        Self::top_n(input_schema, &[], order, limit.saturating_add(offset), 0)
    }

    /// The global window over relayed [`Self::top_n_partial`] outputs, grouped on
    /// their key.
    pub fn top_n_combine(
        partials: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        // A partial's key, then the input's columns in order.
        let key = partials.pk_cols();
        let shifted: Vec<OrderKey> = order
            .iter()
            .map(|k| OrderKey {
                col: k.col.saturating_add(key.len() as u16),
                ..*k
            })
            .collect();
        let plan = Self::top_n(partials, key, &shifted, limit, offset)?;
        debug_assert_eq!(plan.output_schema, *partials);
        Ok(plan)
    }
}
