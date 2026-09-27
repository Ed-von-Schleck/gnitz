//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use crate::schema::MAX_PK_BYTES;
use crate::storage::{Batch, ReadCursor};

use super::plan::TopNPlan;

/// The most rows the output batch reserves up front; it grows past that.
const MAX_TOPN_CAP_HINT: usize = 1 << 16;

/// Visits only the groups `delta` touched — the only ones whose window can have
/// moved — retracting each stored window and re-emitting the post-delta one,
/// both `O(offset + limit)`; the output fold cancels a row that held its slot.
/// `history` is the operator's index, populated with this epoch's rows before the
/// call.
pub fn op_topn(delta: &Batch, trace_out: &mut ReadCursor, history: &mut ReadCursor, plan: &TopNPlan) -> Batch {
    let output_schema = &plan.output_schema;
    let n = delta.count;
    if n == 0 {
        return Batch::empty_with_schema(output_schema);
    }
    let mb = delta.as_mem_batch();

    let runs = plan.key.runs(delta);

    // A retracted and a re-emitted window per touched group.
    let window = usize::try_from(plan.limit).map_or(usize::MAX, |l| l.saturating_mul(2));
    let cap = window
        .saturating_mul(runs.len())
        .min(n)
        .max(window)
        .min(MAX_TOPN_CAP_HINT);
    let mut out = Batch::with_capacity(output_schema, cap);
    let mut key = [0u8; MAX_PK_BYTES];

    // Runs ascend in output-PK order, so the `trace_out` probes do too.
    for run in runs.iter() {
        // A group whose every delta row is a ghost was not touched.
        let Some(exemplar) = run.map(|p| runs.row(p)).find(|&r| mb.get_weight(r) != 0) else {
            continue;
        };
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
                let mut null_word = 0u64;
                out.begin_row(out_pk_bytes, take as i64);
                out.append_cells_from(0, &plan.index.carried_in_index, src, row, &mut null_word);
                out.commit_row(null_word);
            }
            history.advance();
        }
    }

    let out = out.into_consolidated(output_schema);
    gnitz_debug!("op_topn: in={} groups={} out={}", n, runs.len(), out.count);
    out
}
