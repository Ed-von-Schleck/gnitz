//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use crate::repr::Batch;
use crate::stream::order_index::{OrderedEpoch, OrderedPlan, Walk};
use crate::stream::OpenAt;

use super::plan::TopN;

/// The weight slots of one group still to skip, then to keep.
#[derive(Clone, Copy, PartialEq)]
struct Slots {
    skip: u64,
    keep: u64,
}

impl Slots {
    /// How many of the next element's `weight` slots are kept.
    #[inline]
    fn take(&mut self, weight: i64) -> i64 {
        let slots = weight.max(0) as u64;
        let skipped = slots.min(self.skip);
        self.skip -= skipped;
        let take = (slots - skipped).min(self.keep);
        self.keep -= take;
        take as i64
    }
}

/// Walks each touched group's stored rows and delta entries together in
/// index order, emitting every element at its share of the group's new slots
/// less its share of the old. δ_out holds each element once, in the order of
/// the walk.
pub(in crate::stream) fn op_topn(delta: &Batch, history: OpenAt<'_>, plan: &OrderedPlan, top: &TopN) -> OrderedEpoch {
    let fresh = Slots { skip: top.offset, keep: top.limit };
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
