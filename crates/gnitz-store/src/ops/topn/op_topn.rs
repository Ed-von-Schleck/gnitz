//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use crate::schema::key::pk_bytes_eq;
use crate::schema::MAX_PK_BYTES;
use crate::storage::{Batch, ReadCursor};
use gnitz_wire::ReduceOutKey;

use super::plan::TopNPlan;

/// Visits only the groups `delta` touched — the only ones whose window can have
/// moved — retracting each stored window and re-emitting the post-delta one,
/// both `O(offset + limit)`; a row that held its slot cancels itself. `history`
/// is the operator's index, populated with this epoch's rows before the call.
pub fn op_topn(delta: &Batch, trace_out: &mut ReadCursor, history: &mut ReadCursor, plan: &TopNPlan) -> Batch {
    let output_schema = &plan.output_schema;
    let n = delta.count;
    if n == 0 {
        return Batch::empty_with_schema(output_schema);
    }
    let mb = delta.as_mem_batch();

    let groups = touched_groups(&mb, n, plan);

    // A hint: one window per touched group, capped by the delta rows that could
    // have touched one, and never below the window a single-row delta still fills.
    let window = 2 * plan.limit as usize;
    let cap = window.saturating_mul(groups.len()).min(n).max(window);
    let mut out = Batch::with_capacity(output_schema, cap);
    let mut key = [0u8; MAX_PK_BYTES];

    for &exemplar in &groups {
        let exemplar = exemplar as usize;
        let out_pk = plan.out_pk(&mb, exemplar);
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
                for (pi, loc) in plan.index.carried_in_index.iter().enumerate() {
                    out.append_cell_from(pi, loc, src, row, &mut null_word);
                }
                out.commit_row(null_word);
            }
            history.advance();
        }
    }

    gnitz_debug!("op_topn: in={} groups={} out={}", n, groups.len(), out.count);
    out
}

/// One row of each group `delta` touches. The digest only makes a group's rows
/// adjacent; under `PkPermutation` the output PK decides which stored rows the
/// retraction cancels, so a digest collision there must not merge two groups.
/// Every other kind keys the output *by* the digest.
fn touched_groups(mb: &crate::storage::MemBatch, n: usize, plan: &TopNPlan) -> Vec<u32> {
    // An empty group set folds to one key, so the first weighted row is the whole
    // answer — the sort and the `n`-sized allocation below are pure waste.
    let pk_keyed = plan.out_key == ReduceOutKey::PkPermutation;
    if !pk_keyed && plan.group_key.cols.is_empty() {
        return (0..n)
            .find(|&r| mb.get_weight(r) != 0)
            .map(|r| r as u32)
            .into_iter()
            .collect();
    }
    let mut groups: Vec<(u128, u32)> = Vec::with_capacity(n);
    groups.extend(
        (0..n)
            .filter(|&r| mb.get_weight(r) != 0)
            .map(|r| (plan.group_key.key_row(mb, r), r as u32)),
    );
    groups.sort_unstable();
    groups.dedup_by(|a, b| {
        a.0 == b.0 && (!pk_keyed || pk_bytes_eq(mb.get_pk_bytes(a.1 as usize), mb.get_pk_bytes(b.1 as usize)))
    });
    groups.into_iter().map(|(_, row)| row).collect()
}
