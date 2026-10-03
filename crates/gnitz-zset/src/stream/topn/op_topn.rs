//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use crate::repr::Batch;
use crate::schema::MAX_PK_BYTES;
use crate::stream::OpenAt;

use super::plan::TopNPlan;

/// The most rows the output batch reserves up front; it grows past that.
const MAX_TOPN_CAP_HINT: usize = 1 << 16;

/// Visits only the groups `delta` touched — the only ones whose window can have
/// moved — retracting each stored window and re-emitting the post-delta one,
/// both `O(offset + limit)`; the output fold cancels a row that held its slot.
/// `trace_out` opens the output's history and `history` the operator's index,
/// populated with this epoch's rows before the call; each is opened at the
/// touched groups.
pub fn op_topn(delta: &Batch, trace_out: OpenAt<'_>, history: OpenAt<'_>, plan: &TopNPlan) -> Batch {
    let output_schema = &plan.output_schema;
    let n = delta.count;
    let mb = delta.as_mem_batch();

    let groups = plan.key.ordinals(delta);
    // Each group's first live row; a group whose every delta row is a ghost was
    // not touched.
    const UNTOUCHED: u32 = u32::MAX;
    let mut exemplars = vec![UNTOUCHED; groups.len()];
    for (row, &g) in groups.ord.iter().enumerate() {
        if exemplars[g as usize] == UNTOUCHED && mb.get_weight(row) != 0 {
            exemplars[g as usize] = row as u32;
        }
    }

    // Groups ascend in output-PK order, so the `trace_out` probes do too.
    let touched = || {
        groups.by_pk.iter().filter_map(|&g| match exemplars[g as usize] {
            UNTOUCHED => None,
            row => Some(row as usize),
        })
    };
    let (Some(lo), Some(hi)) = (touched().next(), touched().next_back()) else {
        return Batch::empty_with_schema(output_schema);
    };
    let mut trace_out = trace_out(plan.key.out_pk(&mb, lo).bytes(), plan.key.out_pk(&mb, hi).bytes());
    let (first, last) = plan.index.group_span(&mb, touched()).expect("a touched group");
    let mut history = history(first.pk_bytes(), last.pk_bytes());

    // A retracted and a re-emitted window per touched group.
    let window = usize::try_from(plan.limit).map_or(usize::MAX, |l| l.saturating_mul(2));
    let cap = window
        .saturating_mul(groups.len())
        .min(n)
        .max(window)
        .min(MAX_TOPN_CAP_HINT);
    let mut out = Batch::with_capacity(output_schema, cap);
    let mut key = [0u8; MAX_PK_BYTES];

    for exemplar in touched() {
        let out_pk = plan.key.out_pk(&mb, exemplar);
        let out_pk_bytes = out_pk.bytes();

        // −TopN(history): the stored rows, at minus their stored weight.
        trace_out.for_each_positive_with_prefix(out_pk_bytes, |c| c.copy_current_row_into(&mut out, -c.current_weight));

        // +TopN(history + δ): the window of the post-delta index. The seek's
        // verdict is redundant — it is false exactly when the walk's guard fails.
        let prefix = plan.index.group_prefix(&mut key, &mb, exemplar);
        let mut skip = plan.offset;
        let mut budget = plan.limit;
        history.seek_first_positive_with_prefix(prefix);
        while history.valid && budget > 0 && history.current_pk_bytes().starts_with(prefix) {
            // A non-positive entry fills no slot: the input is a relation,
            // bag-positive by the contract every reduce reads.
            let slots = history.current_weight.max(0) as u64;
            let skipped = slots.min(skip);
            skip -= skipped;
            let take = (slots - skipped).min(budget);
            if take > 0 {
                budget -= take;
                let (src, row) = history.current_row_source();
                out.begin_row(out_pk_bytes, take as i64);
                out.append_cells_from(0, &plan.index.carried_in_index, src, row);
                out.commit_row();
            }
            history.advance();
        }
    }

    let out = out.into_consolidated();
    gnitz_debug!("op_topn: in={} groups={} out={}", n, groups.len(), out.count);
    out
}
