//! Incremental top-N operator: per group, the rows filling weight slots
//! `offset .. offset + limit` of the group in ORDER BY order. It is maintained
//! through the order index of every input row (the integral, `I` in
//! `Q^Δ = D ∘ Q ∘ I`), so a retracted leader's successor is the next row of a
//! prefix walk of it: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use gnitz_expr::WeightSlots;
use gnitz_wire::OrderKey;

use super::{OrderIndex, OrderedKind, OrderedPlan, Walk};
use crate::repr::Batch;
use crate::schema::SchemaDescriptor;

impl OrderedPlan {
    /// A top-N. `Err` for a shape an untrusted producer can name and the
    /// operator cannot execute, a zero limit among them.
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
            kind: OrderedKind::TopN(WeightSlots { skip: offset, keep: limit }),
        })
    }

    /// One worker's share of a global top-N: its first `limit + offset` slots,
    /// which hold every global slot.
    pub fn top_n_partial(
        input_schema: &SchemaDescriptor,
        order: &[OrderKey],
        limit: u64,
        offset: u64,
    ) -> Result<Self, String> {
        Self::top_n(input_schema, &[], order, limit.saturating_add(offset), 0)
    }

    /// The global top-N over relayed [`Self::top_n_partial`] outputs, grouped on
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

/// The walk of one touched group, each group starting from the slots `fresh`:
/// its stored rows and delta entries together in index order, each element
/// emitted at its share of the group's new slots less its share of the old.
/// δ_out holds each element once, in the order of the walk.
#[inline]
pub(super) fn group(fresh: WeightSlots, out: &mut Batch, out_pk: &[u8], walk: &mut Walk<'_>) {
    let (mut old, mut new) = (fresh, fresh);
    // Past the group's last entry, slots in one state take alike from every later row.
    while (walk.pending() || old != new) && (old.keep != 0 || new.keep != 0) {
        let Some(element) = walk.next() else { break };
        let share = new.take(element.new) - old.take(element.old);
        if share != 0 {
            out.begin_row(out_pk, share);
            walk.append_row(out, element.at);
            out.commit_row();
        }
        // Full new slots take nothing more, so no later entry moves anything.
        if element.entry && new.keep == 0 {
            walk.skip_entries();
        }
    }
}

#[cfg(test)]
#[path = "tests/topn.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/topn.rs"]
mod bench;
