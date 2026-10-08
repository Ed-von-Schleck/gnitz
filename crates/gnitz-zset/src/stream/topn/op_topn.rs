//! Incremental TOP-N: δ_out = TopN(history + δ_in) − TopN(history), per group
//! the delta touched.

use std::cmp::Ordering;

use crate::repr::{pk_prefix_group_end, Batch};
use crate::schema::payload_order::compare_full_rows;
use crate::stream::OpenAt;

use super::plan::TopNPlan;

/// The most rows the output batch reserves up front; it grows past that.
const MAX_TOPN_CAP_HINT: usize = 1 << 16;

/// The weight slots of one group still to skip, then to keep.
#[derive(Clone, Copy, PartialEq)]
struct Window {
    skip: u64,
    keep: u64,
}

impl Window {
    /// How many of the next element's `weight` slots fall in the window.
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

/// One epoch of a top-N.
pub struct TopNEpoch {
    /// δ_out.
    pub out: Batch,
    /// What the epoch adds to the index, which `op_topn` read without it.
    pub index_entries: Batch,
}

/// Walks each touched group's stored rows and delta entries together in index
/// order, emitting every element at its share of the new window less its share
/// of the old.
pub fn op_topn(delta: &Batch, history: OpenAt<'_>, plan: &TopNPlan) -> TopNEpoch {
    let index = &plan.index;
    // Entry `i` is delta row `i`'s, until a fold moves it to where `moved` records.
    let mut folded = index.batch(delta, plan.key.carried());
    debug_assert_eq!(folded.count, delta.count);
    let mut moved: Vec<(u32, u32, i64)> = Vec::new();
    let folds = !folded.stands_consolidated();
    match folds {
        true => folded = Batch::consolidated_from(&folded, &mut moved),
        false => folded.certify_consolidated(),
    }
    let delta_row = |entry: usize| if folds { moved[entry].1 as usize } else { entry };
    let entries = folded.as_mem_batch();
    let n = entries.count;
    if n == 0 {
        return TopNEpoch {
            out: Batch::empty_with_schema(&plan.output_schema),
            index_entries: folded,
        };
    }
    let group = index.group_bytes();
    let prefix = |row: usize| &entries.get_pk_bytes(row)[..group];
    let mut history = history(prefix(0), prefix(n - 1));

    let mb = delta.as_mem_batch();
    let mut out = Batch::with_capacity(&plan.output_schema, n.min(MAX_TOPN_CAP_HINT));
    let fresh = Window { skip: plan.offset, keep: plan.limit };
    let mut next = 0;
    while next < n {
        let end = pk_prefix_group_end(&entries, next, group);
        let out_pk = plan.key.out_pk(&mb, delta_row(next));
        let out_pk = out_pk.bytes();
        let prefix = prefix(next);
        history.seek_pk_group_ascending(prefix);
        let (mut old, mut new) = (fresh, fresh);
        // Past the group's last entry, windows in one state take alike from every later row.
        while (next < end || old != new) && (old.keep != 0 || new.keep != 0) {
            let stored = history.valid && history.current_pk_bytes().starts_with(prefix);
            // The next element in index order: the stored row, the entry, or both as one.
            let ord = match (stored, next < end) {
                (false, false) => break,
                (true, false) => Ordering::Less,
                (false, true) => Ordering::Greater,
                (true, true) => {
                    let (src, at) = history.current_row_source();
                    compare_full_rows(&index.schema, src, at, &entries, next)
                }
            };
            let (at_stored, at_entry) = (ord.is_le(), ord.is_ge());
            let w_old = if at_stored { history.current_weight } else { 0 };
            let w_new = w_old.wrapping_add(if at_entry { entries.get_weight(next) } else { 0 });
            let share = new.take(w_new) - old.take(w_old);
            if share != 0 {
                out.begin_row(out_pk, share);
                match at_stored {
                    true => {
                        let (src, at) = history.current_row_source();
                        out.append_cells_from(0, &index.carried_in_index, src, at);
                    }
                    false => out.append_cells_from(0, &index.carried_in_index, &entries, next),
                }
                out.commit_row();
            }
            if at_stored {
                history.advance();
            }
            if at_entry {
                // A full new window takes nothing more, so no later entry moves anything.
                next = if new.keep == 0 { end } else { next + 1 };
            }
        }
        next = end;
    }
    TopNEpoch {
        out: out.into_consolidated(),
        index_entries: folded,
    }
}
