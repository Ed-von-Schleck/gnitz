//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use gnitz_expr::WeightSlots;

use crate::repr::Batch;
use crate::stream::order_index::{OrderedEpoch, OrderedPlan, Walk};
use crate::stream::OpenAt;

use super::plan::TopN;

/// Walks each touched group's stored rows and delta entries together in
/// index order, emitting every element at its share of the group's new slots
/// less its share of the old. δ_out holds each element once, in the order of
/// the walk.
pub(in crate::stream) fn op_topn(delta: &Batch, history: OpenAt<'_>, plan: &OrderedPlan, top: &TopN) -> OrderedEpoch {
    let fresh = WeightSlots { skip: top.offset, keep: top.limit };
    let group = |out: &mut Batch, out_pk: &[u8], walk: &mut Walk<'_>| {
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
    };
    plan.index.epoch(delta, history, &plan.output_schema, group)
}
