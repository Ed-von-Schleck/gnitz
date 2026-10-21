//! Incremental window: δ_out = Window(history + δ_in) − Window(history), per
//! group the delta touched.

use crate::algebra::AggValues;
use crate::repr::Batch;
use crate::stream::order_index::{with_row, At, OrderedEpoch, OrderedPlan, Walk};
use crate::stream::OpenAt;

use super::plan::Window;

/// The group's contents before the epoch, and after it.
const OLD: usize = 0;
const NEW: usize = 1;

/// One row of a peer group, at its weight under each contents.
struct Peer {
    at: At,
    weight: [i64; 2],
}

/// What the rows of a group walked so far amount to, under each contents.
struct Running {
    aggs: AggValues,
    /// The weight of those rows.
    rows: [i64; 2],
    /// Their peer groups.
    groups: [i64; 2],
}

impl Running {
    /// Whether the two contents give a row the same aggregates.
    /// Whether the two contents give a row the same ranks.
    fn same_ranks(&self, plan: &Window) -> bool {
        (plan.rank.is_none() || self.rows[OLD] == self.rows[NEW])
            && (plan.dense_rank.is_none() || self.groups[OLD] == self.groups[NEW])
    }

    /// Fold the stored rows before the next entry's peers. The two contents
    /// amount to the same on entry and hold the same such rows, so each is folded
    /// once, and none is emitted.
    fn fold_to_entry(&mut self, plan: &Window, walk: &mut Walk<'_>) {
        let mut last: Option<At> = None;
        while let Some(row) = walk.next_stored_before_entry() {
            let (at, weight) = (row.at, row.new);
            // A row at a non-positive weight is not in the group.
            if weight > 0 {
                if plan.dense_rank.is_some() {
                    self.groups[NEW] += last.is_none_or(|last| !walk.peers(last, at)) as i64;
                    last = Some(at);
                }
                with_row!(walk, at, |src, at| self.aggs.step(NEW, src, at, weight));
                self.rows[NEW] = self.rows[NEW].wrapping_add(weight);
            }
        }
        self.aggs.copy(NEW, OLD);
        (self.rows[OLD], self.groups[OLD]) = (self.rows[NEW], self.groups[NEW]);
    }

    /// Emit the row `at` at `weight`, with the values the contents `side` give it.
    #[inline]
    fn emit(
        &self,
        out: &mut Batch,
        plan: &Window,
        walk: &Walk<'_>,
        out_pk: &[u8],
        (at, weight): (At, i64),
        side: usize,
    ) {
        out.begin_row(out_pk, weight);
        walk.append_row(out, at);
        // A rank counts the rows, the groups, before the row's peers.
        if let Some(slot) = plan.rank {
            out.extend_col(slot, &(self.rows[side] + 1).to_le_bytes());
        }
        if let Some(slot) = plan.dense_rank {
            out.extend_col(slot, &(self.groups[side] + 1).to_le_bytes());
        }
        self.aggs.emit(side, out);
        out.commit_row();
    }
}

/// Walks each touched group's stored rows and delta entries together in index
/// order. The rows before an entry's peers are folded once; from there a peer
/// group at a time is folded into the running values of the group's old and its
/// new contents and every row of it emitted at its new values less its old ones,
/// until the two contents amount to the same again. δ_out holds each element
/// once, in the order of the walk.
pub(in crate::stream) fn op_window(
    delta: &Batch,
    history: OpenAt<'_>,
    plan: &OrderedPlan,
    window: &Window,
) -> OrderedEpoch {
    let mut running = Running {
        aggs: AggValues::new(&window.aggs, 2),
        rows: [0; 2],
        groups: [0; 2],
    };
    let mut peers: Vec<Peer> = Vec::new();
    let group = |out: &mut Batch, out_pk: &[u8], walk: &mut Walk<'_>| {
        running.aggs.clear();
        (running.rows, running.groups) = ([0; 2], [0; 2]);
        while walk.pending() {
            running.fold_to_entry(window, walk);
            loop {
                // The next peer group in index order.
                peers.clear();
                while let Some(row) = walk.next_peer(peers.first().map(|first| first.at)) {
                    // A row at a non-positive weight is not in the group.
                    let weight = [row.old.max(0), row.new.max(0)];
                    peers.push(Peer { at: row.at, weight });
                }
                if peers.is_empty() {
                    return;
                }

                let mut weight = [0i64; 2];
                for peer in &peers {
                    for side in [OLD, NEW] {
                        let w = peer.weight[side];
                        if w > 0 {
                            weight[side] = weight[side].wrapping_add(w);
                            with_row!(walk, peer.at, |src, row| running.aggs.step(side, src, row, w));
                        }
                    }
                }
                // An aggregate folds this peer group; a rank counts up to it.
                let same_aggs = running.aggs.same(OLD, NEW);
                let same = same_aggs && running.same_ranks(window);
                for peer in &peers {
                    let [old, new] = peer.weight;
                    if same {
                        if new != old {
                            running.emit(out, window, walk, out_pk, (peer.at, new - old), NEW);
                        }
                        continue;
                    }
                    if old > 0 {
                        running.emit(out, window, walk, out_pk, (peer.at, -old), OLD);
                    }
                    if new > 0 {
                        running.emit(out, window, walk, out_pk, (peer.at, new), NEW);
                    }
                }
                for side in [OLD, NEW] {
                    running.rows[side] = running.rows[side].wrapping_add(weight[side]);
                    running.groups[side] += (weight[side] > 0) as i64;
                }
                // Every later row has the values it had, until the next entry.
                if same_aggs && running.same_ranks(window) {
                    break;
                }
            }
        }
    };
    plan.index.epoch(delta, history, &plan.output_schema, group)
}
