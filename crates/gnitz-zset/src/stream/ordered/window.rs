//! Incremental window operator: every row of a group, widened by its running
//! values — aggregates and ranks over the rows of the group at or before it in
//! ORDER BY order. It is maintained through the order index of every
//! input row (the integral, `I` in `Q^Δ = D ∘ Q ∘ I`): a touched group is walked
//! once in order, under its old and its new contents at the same time, and each
//! row whose values differ between the two is retracted and re-emitted:
//! δ_out = Window(history + δ_in) − Window(history), per group the delta touched.

use std::rc::Rc;

use gnitz_wire::{AggDescriptor, AggFunc, OrderKey, TypeCode};

use super::{with_row, At, Element, OrderIndex, OrderedKind, OrderedPlan, Walk};
use crate::algebra::{resolve_aggs, Agg, AggValues, RangeGroups};
use crate::repr::{Batch, Run};
use crate::schema::{SchemaColumn, SchemaDescriptor};

/// The values a window adds behind its input's columns: the rank, the dense
/// rank, the aggregates.
pub(super) struct Window {
    /// The payload slot of each rank the output has.
    rank: Option<usize>,
    dense_rank: Option<usize>,
    /// The aggregates, each reading its column off an index row.
    aggs: Rc<[Agg]>,
    /// Whether the rows before an entry fold as sums over the index's runs:
    /// every value is a count or a wrapping integer sum, so no row order or
    /// merge changes it, and no dense rank counts peer groups.
    summed_prefix: bool,
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
        let summed_prefix = dense_rank.is_none() && aggs.iter().all(Agg::is_exact_linear);
        Ok(OrderedPlan {
            index,
            output_schema,
            kind: OrderedKind::Window(Window { rank, dense_rank, aggs, summed_prefix }),
        })
    }
}

/// The group's contents before the epoch, and after it.
const OLD: usize = 0;
const NEW: usize = 1;

/// One row of a peer group, at its weight under each contents.
struct Peer {
    at: At,
    weight: [i64; 2],
}

impl Peer {
    /// The row of `element`. A row at a non-positive weight is not in the group.
    #[inline(always)]
    fn of(element: &Element) -> Self {
        Peer {
            at: element.at,
            weight: [element.old.max(0), element.new.max(0)],
        }
    }
}

/// A window's walk of one epoch: what the rows of a group walked so far amount
/// to, under each contents.
pub(super) struct Running<'a> {
    window: &'a Window,
    aggs: AggValues,
    /// The weight of those rows.
    rows: [i64; 2],
    /// How many peer groups they form.
    groups: [i64; 2],
    /// The peer group being folded.
    peers: Vec<Peer>,
}

impl<'a> Running<'a> {
    pub(super) fn new(window: &'a Window) -> Self {
        Running {
            window,
            aggs: AggValues::new(&window.aggs, 2),
            rows: [0; 2],
            groups: [0; 2],
            peers: Vec::new(),
        }
    }

    /// Whether the two contents give a row the same ranks.
    fn same_ranks(&self) -> bool {
        (self.window.rank.is_none() || self.rows[OLD] == self.rows[NEW])
            && (self.window.dense_rank.is_none() || self.groups[OLD] == self.groups[NEW])
    }

    /// Fold the stored rows before the next entry's peers. The two contents
    /// amount to the same on entry and hold the same such rows, so each is folded
    /// once, and none is emitted.
    fn fold_to_entry(&mut self, walk: &mut Walk<'_>) {
        if self.window.summed_prefix {
            // Each run's rows below the entry's PK, unmerged: a sum of `w · v`
            // over them is the sum over the merged elements, a cancelled pair
            // adding nothing. The index integrates a relation, so no element nets
            // a negative weight the walk below would leave out.
            let (aggs, rows, n) = (&mut self.aggs, &mut self.rows[NEW], self.window.aggs.len());
            walk.skip_stored_below_entry_pk(|src, at, net| match (src, net) {
                (Run::Mem(b), None) => {
                    let mb = b.as_mem_batch();
                    let ranges = [(at.start, at.end)];
                    for k in 0..n {
                        aggs.fold(k, &mb, &ranges, RangeGroups::One(NEW));
                    }
                    *rows = rows.wrapping_add(mb.sum_weights(at.start, at.end));
                }
                _ => {
                    for row in at {
                        let weight = net.unwrap_or_else(|| src.get_weight(row));
                        aggs.step(NEW, src, row, weight);
                        *rows = rows.wrapping_add(weight);
                    }
                }
            });
        }
        let mut last: Option<At> = None;
        while let Some((at, weight)) = walk.next_stored_before_entry() {
            // A row at a non-positive weight is not in the group.
            if weight > 0 {
                if self.window.dense_rank.is_some() {
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
    #[inline(always)]
    fn emit(&self, out: &mut Batch, walk: &Walk<'_>, out_pk: &[u8], at: At, weight: i64, side: usize) {
        out.begin_row(out_pk, weight);
        walk.append_row(out, at);
        // A rank counts the rows, the groups, before the row's peers.
        if let Some(slot) = self.window.rank {
            out.extend_col(slot, &(self.rows[side] + 1).to_le_bytes());
        }
        if let Some(slot) = self.window.dense_rank {
            out.extend_col(slot, &(self.groups[side] + 1).to_le_bytes());
        }
        self.aggs.emit(side, out);
        out.commit_row();
    }

    /// The walk of one touched group: its stored rows and delta entries
    /// together in index order. The rows before an entry's peers are folded
    /// once; from there a peer group at a time is folded into the running values
    /// of the group's old and its new contents and every row of it emitted at its
    /// new values less its old ones, until the two contents amount to the same
    /// again. δ_out holds each element once, in the order of the walk.
    #[inline]
    pub(super) fn group(&mut self, out: &mut Batch, out_pk: &[u8], walk: &mut Walk<'_>) {
        self.aggs.clear();
        (self.rows, self.groups) = ([0; 2], [0; 2]);
        while walk.pending() {
            self.fold_to_entry(walk);
            loop {
                // The next peer group in index order.
                self.peers.clear();
                let Some(first) = walk.next() else { return };
                self.peers.push(Peer::of(&first));
                while let Some(row) = walk.next_peer(first.at) {
                    self.peers.push(Peer::of(&row));
                }

                let mut weight = [0i64; 2];
                for peer in &self.peers {
                    for side in [OLD, NEW] {
                        let w = peer.weight[side];
                        if w > 0 {
                            weight[side] = weight[side].wrapping_add(w);
                            with_row!(walk, peer.at, |src, row| self.aggs.step(side, src, row, w));
                        }
                    }
                }
                // An aggregate folds this peer group; a rank counts up to it.
                let same_aggs = self.aggs.same(OLD, NEW);
                let same = same_aggs && self.same_ranks();
                for peer in &self.peers {
                    let [old, new] = peer.weight;
                    if same {
                        if new != old {
                            self.emit(out, walk, out_pk, peer.at, new - old, NEW);
                        }
                        continue;
                    }
                    if old > 0 {
                        self.emit(out, walk, out_pk, peer.at, -old, OLD);
                    }
                    if new > 0 {
                        self.emit(out, walk, out_pk, peer.at, new, NEW);
                    }
                }
                for side in [OLD, NEW] {
                    self.rows[side] = self.rows[side].wrapping_add(weight[side]);
                    self.groups[side] += (weight[side] > 0) as i64;
                }
                // Every later row has the values it had, until the next entry.
                if same_aggs && self.same_ranks() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "tests/window.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/window.rs"]
mod bench;
